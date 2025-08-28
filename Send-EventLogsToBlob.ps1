<# 
.SYNOPSIS
  Continuously forward Windows Event Logs to Azure Blob Storage using user-delegation SAS (no stored secrets).

.PREREQS
  - Az PowerShell: Az.Accounts, Az.Storage
  - The host is an Azure VM or Arc-enabled server with System-Assigned Managed Identity enabled
  - RBAC: Managed Identity (or signed-in user) has "Storage Blob Data Contributor" on target Storage (account or container)

.NOTES
  - Writes NDJSON lines to Append Blobs: events/<LogName>/YYYY/MM/DD.jsonl
  - Maintains per-log bookmarks in %ProgramData%\EventLogForwarder
  - Rotates SAS (~1 hour by default) automatically

#>

# ================== USER CONFIG ==================
$StorageAccountName = "<yourStorageAccountName>"
$ContainerName      = "eventlogs"             # existing or will be created
$LogNames           = @('Application','System','Security')  # add more if needed
$IntervalSeconds    = 30                      # poll interval
$SasLifetimeMinutes = 60                      # user-delegation SAS lifetime
$StateRoot          = "$env:ProgramData\EventLogForwarder"

# ================== MODULES & LOGIN ==================
# Ensure Az modules (idempotent)
$modules = @('Az.Accounts','Az.Storage')
foreach ($m in $modules) {
    if (-not (Get-Module -ListAvailable -Name $m)) {
        try { Install-Module $m -Scope AllUsers -Force -ErrorAction Stop } catch { Install-Module $m -Scope CurrentUser -Force }
    }
}

# Prefer managed identity; fall back to interactive
try { Connect-AzAccount -Identity -ErrorAction Stop } catch { Connect-AzAccount -ErrorAction Stop }

# Build storage OAuth context (no keys used)
$ctx = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

# ================== RUNTIME SAS CACHE ==================
$script:SasCache = [pscustomobject]@{ Uri=$null; Expires=[datetime]::MinValue }

function Get-ContainerSasUri {
    $now = (Get-Date).ToUniversalTime()
    if ($SasCache.Uri -and $SasCache.Expires -gt $now.AddMinutes(5)) { return $SasCache.Uri }

    $expiry = $now.AddMinutes($SasLifetimeMinutes)
    $sasUri = New-AzStorageContainerSASToken `
        -Name $ContainerName `
        -Context $ctx `
        -Permission acwl `           # Add, Create, Write, List
        -ExpiryTime $expiry `
        -FullUri `
        -AsUser                      # important: OAuth user-delegation SAS

    $script:SasCache.Uri     = $sasUri
    $script:SasCache.Expires = $expiry
    return $sasUri
}

# ================== HELPERS ==================
function Get-CanonicalUtcRfc1123 { (Get-Date).ToUniversalTime().ToString("R") }

function Get-BlobUrl([string]$BlobPath) {
    # Compose from container SAS URI (auto-rotates)
    $containerSas = Get-ContainerSasUri
    $parts     = $containerSas -split '\?'
    $base      = $parts[0].TrimEnd('/')
    $query     = $parts[1]
    return "$base/$BlobPath`?$query"
}

function Ensure-Container {
    try { New-AzStorageContainer -Name $ContainerName -Context $ctx -ErrorAction SilentlyContinue | Out-Null } catch {}
}

function Ensure-AppendBlob([string]$BlobPath) {
    $url = Get-BlobUrl $BlobPath
    $headers = @{
        "x-ms-date"      = Get-CanonicalUtcRfc1123
        "x-ms-version"   = "2023-11-03"
        "x-ms-blob-type" = "AppendBlob"
    }
    try {
        Invoke-RestMethod -Uri $url -Method Put -Headers $headers -ContentType "application/octet-stream" -ErrorAction Stop | Out-Null
    } catch {
        # 409 = already exists (expected on subsequent appends)
        if ($_.Exception.Response.StatusCode.value__ -ne 409) { throw }
    }
}

function Append-ToBlob([string]$BlobPath, [byte[]]$Bytes) {
    if (-not $Bytes -or $Bytes.Length -eq 0) { return }
    $url = (Get-BlobUrl $BlobPath) + "&comp=appendblock"
    $headers = @{
        "x-ms-date"     = Get-CanonicalUtcRfc1123
        "x-ms-version"  = "2023-11-03"
        "Content-Length"= $Bytes.Length
    }
    Invoke-RestMethod -Uri $url -Method Put -Headers $headers -Body $Bytes -ContentType "application/octet-stream" -ErrorAction Stop | Out-Null
}

function Get-BookmarkPath([string]$LogName) { Join-Path $StateRoot ("bookmark-" + $LogName + ".json") }

function Load-Bookmark([string]$LogName) {
    $path = Get-BookmarkPath $LogName
    if (Test-Path $path) { try { return (Get-Content $path -Raw | ConvertFrom-Json) } catch {} }
    return $null
}

function Save-Bookmark([string]$LogName, [int64]$LastRecordId) {
    $path = Get-BookmarkPath $LogName
    @{ LastRecordId = $LastRecordId } | ConvertTo-Json | Set-Content -Path $path -Encoding UTF8
}

function Get-NewEvents([string]$LogName, [Nullable[Int64]]$SinceRecordId) {
    # If first run (no bookmark), don't backfill—just establish tail
    if (-not $SinceRecordId) { return @() }

    # Efficient tail: Get-WinEvent without message expansion until needed
    # Pull recent window to avoid scanning whole log; adjust if logs are extremely busy
    $events = Get-WinEvent -LogName $LogName -ErrorAction Stop
    $events | Where-Object { $_.RecordId -gt $SinceRecordId } | Sort-Object RecordId
}

function Serialize-Events-NDJSON($events, $LogName) {
    $lines = foreach ($e in $events) {
        # Expand Message lazily (only for new events)
        $msg = $e.Message
        [pscustomobject]@{
            timestamp     = ($e.TimeCreated.ToUniversalTime().ToString("o"))
            recordId      = $e.RecordId
            logName       = $LogName
            providerName  = $e.ProviderName
            levelDisplay  = $e.LevelDisplayName
            task          = $e.Task
            opCode        = $e.Opcode
            keywords      = $e.KeywordsDisplay
            eventId       = $e.Id
            machine       = $e.MachineName
            message       = $msg
        } | ConvertTo-Json -Depth 5 -Compress
    }
    if ($lines) { ($lines -join "`n") + "`n" } else { "" }
}

function Get-DailyBlobPath([string]$LogName) {
    $d = (Get-Date).ToUniversalTime()
    "events/$LogName/$($d.ToString('yyyy'))/$($d.ToString('MM'))/$($d.ToString('dd')).jsonl"
}

# ================== INIT ==================
New-Item -ItemType Directory -Path $StateRoot -Force | Out-Null
Ensure-Container

# Initialize bookmarks to current tail so we don't backfill on first run
foreach ($ln in $LogNames) {
    $bm = Load-Bookmark $ln
    if (-not $bm) {
        $last = Get-WinEvent -LogName $ln -MaxEvents 1 -ErrorAction SilentlyContinue
        if ($last) { Save-Bookmark $ln $last.RecordId } else { Save-Bookmark $ln 0 }
    }
}

Write-Host "Streaming logs ($($LogNames -join ', ')) to container '$ContainerName' in account '$StorageAccountName'. Ctrl+C to stop."

# ================== MAIN LOOP ==================
while ($true) {
    foreach ($ln in $LogNames) {
        try {
            $bm      = Load-Bookmark $ln
            $sinceId = if ($bm) { [int64]$bm.LastRecordId } else { $null }

            $newEvents = Get-NewEvents -LogName $ln -SinceRecordId $sinceId
            if ($newEvents.Count -gt 0) {
                $blobPath = Get-DailyBlobPath $ln
                Ensure-AppendBlob $blobPath

                $payload = Serialize-Events-NDJSON $newEvents $ln
                $bytes   = [System.Text.Encoding]::UTF8.GetBytes($payload)
                Append-ToBlob -BlobPath $blobPath -Bytes $bytes

                $maxId = ($newEvents | Select-Object -ExpandProperty RecordId | Measure-Object -Maximum).Maximum
                Save-Bookmark $ln $maxId

                Write-Host ("{0:u}  {1,-12}  +{2}  -> {3}" -f (Get-Date), $ln, $newEvents.Count, $blobPath)
            }
        } catch {
            Write-Warning "Error processing $ln: $_"
        }
    }

    # Refresh SAS proactively if near expiry (Get-ContainerSasUri handles this on demand)
    [void](Get-ContainerSasUri)

    Start-Sleep -Seconds $IntervalSeconds
}
