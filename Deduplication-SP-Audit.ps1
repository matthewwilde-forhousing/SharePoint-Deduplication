<# 
.SYNOPSIS
Audit-only SharePoint Online deduplication (hash/content-based), optimized for very large libraries.
Includes: Graph Delta, progress + ETA, checkpoint/resume, verbose/debug, skipped.csv diagnostics,
and flexible duplicate key selection (quickXorHash, sha1Hash, size+name, size).

.VERSION
3.9.0 (AUDIT+ diagnostics + alt keys) — 2025-09-06
#>

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseApprovedVerbs','', Scope='Function', Target='Flush-PartialBuffer')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseApprovedVerbs','', Scope='Function', Target='Finalize-Partial')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseApprovedVerbs','', Scope='Function', Target='Flush-PartialFromFilesOut')]

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]  [string]$SiteUrl,
  [Parameter(Mandatory=$true)]  [string]$ClientId,
  [Parameter(Mandatory=$true)]  [string]$TenantId,
  [Parameter(Mandatory=$true)]  [string]$LibraryTitle,

  [Parameter(Mandatory=$false)] [string]$ScopeFolder = "",

  [Parameter(Mandatory=$false)] [int]$MinSizeKB = 0,
  [Parameter(Mandatory=$false)] [string[]]$IncludeExtensions,
  [Parameter(Mandatory=$false)] [string[]]$ExcludeExtensions,
  [Parameter(Mandatory=$false)] [datetime]$ModifiedAfter,
  [Parameter(Mandatory=$false)] [datetime]$ModifiedBefore,

  [switch]$HashDuringScan,
  [Parameter(Mandatory=$false)] [string]$MasterIndexCsv,

  [switch]$OutputJson,
  [Parameter(Mandatory=$false)] [int]$TopNHashesByWaste = 10,

  [Parameter(Mandatory=$false)] [ValidateSet("quickXorHash","sha1Hash","sha1OrQuickXor","sizeAndName","sizeOnly")][string]$DuplicateKey = "quickXorHash",
  [Parameter(Mandatory=$false)] [ValidateSet("None","quickXorHash","sha1Hash","sha1OrQuickXor","sizeAndName","sizeOnly")][string]$FallbackDuplicateKey = "None",

  [switch]$EmailReport,
  [Parameter(Mandatory=$false)] [string]$SendGridApiKey,
  [Parameter(Mandatory=$false)] [string]$EmailTo,
  [Parameter(Mandatory=$false)] [string]$EmailFrom,
  [Parameter(Mandatory=$false)] [bool]$EmailAttachCsv = $true,
  [Parameter(Mandatory=$false)] [bool]$EmailAttachSummary = $true,

  [Parameter(Mandatory=$false)] [decimal]$StorageCostPerGBPerYear = 1.94,
  [Parameter(Mandatory=$false)] [string]$CurrencySymbol = "£",

  [Parameter(Mandatory=$false)] [string]$QuarantineFolderName = "_Dedup_Quarantine",

  [Parameter(Mandatory=$false)] [bool]$UseDelta = $true,

  [Parameter(Mandatory=$false)] [int]$ProgressEveryItems = 2000,
  [Parameter(Mandatory=$false)] [int]$ProgressEverySeconds = 20,
  [Parameter(Mandatory=$false)] [int]$ExpectedTotalFiles = 0,
  [Parameter(Mandatory=$false)] [int]$EtaAfterPages = 5,
  [Parameter(Mandatory=$false)] [bool]$AutoSeedExpectedTotal = $true,

  [Parameter(Mandatory=$false)] [bool]$DebugLogItems = $false,
  [Parameter(Mandatory=$false)] [int]$DebugItemSampleEvery = 500,

  [Parameter(Mandatory=$false)] [bool]$EnableResume = $true,
  [Parameter(Mandatory=$false)] [string]$StatePath,
  [Parameter(Mandatory=$false)] [int]$CheckpointEveryItems = 5000,
  [Parameter(Mandatory=$false)] [int]$PartialFlushEvery = 1000,
  [Parameter(Mandatory=$false)] [int]$CheckpointEverySeconds = 60
)

$ErrorActionPreference = "Stop"
$script:RunStart = Get-Date

# Run folder naming
$u = [Uri]$SiteUrl
$hostPart = $u.Host -replace "[^a-zA-Z0-9\-\.]","-"
$sitePart = $u.AbsolutePath.Trim("/") -replace "/","-"
if ([string]::IsNullOrWhiteSpace($sitePart)) { $sitePart = "root" }
$libPart  = ($LibraryTitle -replace "[^a-zA-Z0-9\-\._]","-")
$tsPart   = $script:RunStart.ToString("yyyyMMdd-HHmmss")
$runName  = "Deduplicate-AUDIT-{0}-{1}-{2}-{3}" -f $hostPart,$sitePart,$libPart,$tsPart

$script:RunFolderRoot = Join-Path -Path (Resolve-Path ".").Path -ChildPath "DeduplicateLogs"
$null = New-Item -Path $script:RunFolderRoot -ItemType Directory -Force
$script:RunFolder = Join-Path -Path $script:RunFolderRoot -ChildPath $runName
$null = New-Item -Path $script:RunFolder -ItemType Directory -Force

# Paths
$script:TranscriptPath   = Join-Path $script:RunFolder "transcript.log"
$script:StreamsPath      = Join-Path $script:RunFolder "streams.log"
$script:ReportPath       = Join-Path $script:RunFolder "report.csv"
$script:ReportJsonPath   = Join-Path $script:RunFolder "report.json"
$script:SummaryCsvPath   = Join-Path $script:RunFolder "summary.csv"
$script:SummaryJsonPath  = Join-Path $script:RunFolder "summary.json"
$script:FilesPartialPath = Join-Path $script:RunFolder "files.partial.csv"
$script:FilesFinalPath   = Join-Path $script:RunFolder "files.csv"
$script:StatePathFinal   = if ($StatePath) { $StatePath } else { Join-Path $script:RunFolder "state.json" }
if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("Files.partial path: {0}" -f $script:FilesPartialPath) } else { Write-Host ("Files.partial path: {0}" -f $script:FilesPartialPath) }
$script:SkippedCsvPath   = Join-Path $script:RunFolder "skipped.csv"

Start-Transcript -Path $script:TranscriptPath -Append | Out-Null
"Reason,ServerRel,Name,Extension,SizeBytes,ModifiedUTC,HashQuick,HashSha1" | Out-File -FilePath $script:SkippedCsvPath -Encoding UTF8

function Write-Log {
  param([string]$Message, [ValidateSet("INFO","WARN","ERROR")] [string]$Level = "INFO")
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[{0}] [{1}] {2}" -f $ts, $Level, $Message
  Write-Host $line
  $line | Out-File -FilePath $script:StreamsPath -Append -Encoding UTF8
}
# === Partial CSV helpers (early) ===
function Add-PartialFileRow {
  param([object]$Row)
  if ($null -eq $script:PartialRows)   { $script:PartialRows = 0 ; if ($null -eq $script:FilesOutCount) { $script:FilesOutCount = 0 }}
  if ($null -eq $script:PartialBuffer) { $script:PartialBuffer = New-Object System.Collections.Generic.List[string] }

  function _esc([string]$s) {
    if ($null -eq $s) { return '""' }
    return '"' + ($s -replace '"','""') + '"'
  }

  # Ensure header (once)
  if (-not (Test-Path $script:FilesPartialPath)) {
    $header = 'Id,ServerRel,Name,SizeBytes,ModifiedUTC,WebUrl,HashQuick,HashSha1'
    $null = New-Item -ItemType File -Path $script:FilesPartialPath -Force
    [System.IO.File]::WriteAllText($script:FilesPartialPath, $header + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
    if (Get-Command Write-Log -ErrorAction SilentlyContinue) {
      Write-Log ("[partial:init] Created {0}" -f $script:FilesPartialPath)
    } else {
      Write-Host ("[partial:init] Created {0}" -f $script:FilesPartialPath)
    }
  }

  $size = 0
  if ($Row -and $Row.PSObject.Properties.Match("SizeBytes").Count -gt 0 -and $Row.SizeBytes) {
    $size = [int64]$Row.SizeBytes
  }

  $line = ('{0},{1},{2},{3},{4},{5},{6},{7}' -f `
    (_esc([string]$Row.Id)),
    (_esc([string]$Row.ServerRel)),
    (_esc([string]$Row.Name)),
    $size,
    (_esc([string]$Row.ModifiedUTC)),
    (_esc([string]$Row.WebUrl)),
    (_esc([string]$Row.HashQuick)),
    (_esc([string]$Row.HashSha1))
  )

  # Buffer
  [void]$script:PartialBuffer.Add($line)
  $script:PartialRows++

  # Sanity direct append
  if ($script:PartialRows -eq 1 -or $script:PartialRows % 100 -eq 0) {
    try {
      $enc = [System.Text.UTF8Encoding]::new($false)
      $fs = [System.IO.File]::Open($script:FilesPartialPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      try {
        $sw = New-Object System.IO.StreamWriter($fs, $enc)
        $sw.Write($line + [Environment]::NewLine)
        $sw.Flush()
        $fs.Flush($true)
      } finally {
        if ($sw) { $sw.Dispose() }
        $fs.Dispose()
      }
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("[partial:sanity-append] wrote row={0}" -f $script:PartialRows) } else { Write-Host ("[partial:sanity-append] wrote row={0}" -f $script:PartialRows) }
    } catch {
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("[partial:sanity-append:ERROR] {0}" -f $_.Exception.Message) "ERROR" } else { Write-Host ("[partial:sanity-append:ERROR] {0}" -f $_.Exception.Message) }
    }
  }

  if ($script:PartialRows % $PartialFlushEvery -eq 0) {
    Flush-PartialBuffer
  }
}

function Flush-PartialBuffer {
  if ($null -eq $script:PartialBuffer -or $script:PartialBuffer.Count -eq 0) { return }
  $enc = [System.Text.UTF8Encoding]::new($false)
  $payload = [string]::Join([Environment]::NewLine, $script:PartialBuffer) + [Environment]::NewLine

  for ($i=0; $i -lt 3; $i++) {
    try {
      $fs = [System.IO.File]::Open($script:FilesPartialPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      $fs.Dispose()
      $fs = [System.IO.File]::Open($script:FilesPartialPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      try {
        $sw = New-Object System.IO.StreamWriter($fs, $enc)
        $sw.Write($payload)
        $sw.Flush()
        $fs.Flush($true)
      } finally {
        if ($sw) { $sw.Dispose() }
        $fs.Dispose()
      }
      try {
        $szItem = Get-Item -LiteralPath $script:FilesPartialPath -ErrorAction SilentlyContinue
        $sz = if ($szItem) { $szItem.Length } else { 0 }
        if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("[partial:flush] wrote {0} buffered rows, total rows {1}, size {2:N0} bytes" -f $script:PartialBuffer.Count, $script:PartialRows, $sz) } else { Write-Host ("[partial:flush] wrote {0} buffered rows, total rows {1}, size {2:N0} bytes" -f $script:PartialBuffer.Count, $script:PartialRows, $sz) }
      } catch {}
      Confirm-PartialUpdated -Context "flush-buffer"
    $script:PartialBuffer.Clear()
      break
    } catch {
      Start-Sleep -Milliseconds 150
      if ($i -eq 2) {
        if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("[partial:flush:ERROR] {0}" -f $_.Exception.Message) "ERROR" } else { Write-Host ("[partial:flush:ERROR] {0}" -f $_.Exception.Message) }
        throw
      }
    }
  }
}

function Finalize-Partial { Flush-PartialBuffer; Flush-PartialFromFilesOut }

# Verify that files.partial.csv actually grew on disk; throw if not
function Confirm-PartialUpdated {
  param([string]$Context = "unknown")
  try {
    if (-not (Test-Path $script:FilesPartialPath)) {
      $msg = "[partial:verify:ERROR] Expected $script:FilesPartialPath but file does not exist (ctx={0})" -f $Context
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log $msg "ERROR" } else { Write-Host $msg }
      throw $msg
    }

    $item  = Get-Item -LiteralPath $script:FilesPartialPath -ErrorAction Stop
    $size  = [int64]$item.Length
    $mtime = $item.LastWriteTimeUtc

    if ($null -eq $script:LastPartialSize)     { $script:LastPartialSize = 0 }
    if ($null -eq $script:LastPartialWriteUtc) { $script:LastPartialWriteUtc = [datetime]'1900-01-01' }
    if ($null -eq $script:PartialRows)         { $script:PartialRows = 0 ; if ($null -eq $script:FilesOutCount) { $script:FilesOutCount = 0 }}
    if ($null -eq $script:PartialWritten)      { $script:PartialWritten = 0 }

    # Compute how many rows *should* be on disk but may not be:
    $filesOutCount = $script:FilesOutCount
    $bufferCount   = 0
    try { if ($null -ne $script:PartialBuffer) { $bufferCount = [int]$script:PartialBuffer.Count } } catch {}

    $pendingFromFilesOut = [math]::Max(0, $filesOutCount - $script:PartialWritten)
    $pendingTotal        = $pendingFromFilesOut + $bufferCount

    $grew = ($size -gt $script:LastPartialSize) -or ($mtime -gt $script:LastPartialWriteUtc)

    $diag = "[partial:verify] ctx={0} size={1:N0} (prev={2:N0}) mtime={3:o} (prev={4:o}) grew={5} rowsSeen={6} filesOut={7} partialWritten={8} buffer={9} pending={10}" -f `
      $Context, $size, $script:LastPartialSize, $mtime, $script:LastPartialWriteUtc, $grew, $script:PartialRows, $filesOutCount, $script:PartialWritten, $bufferCount, $pendingTotal

    if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log $diag } else { Write-Host $diag }

    if (-not $grew -and $pendingTotal -gt 0) {
      $err = "[partial:verify:FAIL] No growth detected for files.partial.csv after {0} (pending={1}, filesOut={2}, partialWritten={3}, buffer={4})" -f $Context, $pendingTotal, $filesOutCount, $script:PartialWritten, $bufferCount
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log $err "ERROR" } else { Write-Host $err }
      throw $err
    }

    # record latest observed
    $script:LastPartialSize     = $size
    $script:LastPartialWriteUtc = $mtime
  } catch {
    throw $_
  }
}



# Append any unwritten $filesOut rows to files.partial.csv (used at checkpoints)
function Flush-PartialFromFilesOut {
  if ($null -eq $script:PartialWritten) { $script:PartialWritten = 0 }
  if ($null -eq $filesOut -or $filesOut.Count -le $script:PartialWritten) { return }

  function _esc2([string]$s) {
    if ($null -eq $s) { return '""' }
    return '"' + ($s -replace '"','""') + '"'
  }

  # Ensure header exists
  if (-not (Test-Path $script:FilesPartialPath)) {
    $header = 'Id,ServerRel,Name,SizeBytes,ModifiedUTC,WebUrl,HashQuick,HashSha1'
    $null = New-Item -ItemType File -Path $script:FilesPartialPath -Force
    [System.IO.File]::WriteAllText($script:FilesPartialPath, $header + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
  }

  $enc = [System.Text.UTF8Encoding]::new($false)
  $sb  = New-Object System.Text.StringBuilder

  for ($i = $script:PartialWritten; $i -lt $filesOut.Count; $i++) {
    $r = $filesOut[$i]
    $size = 0
    if ($r -and $r.PSObject.Properties.Match("SizeBytes").Count -gt 0 -and $r.SizeBytes) { $size = [int64]$r.SizeBytes }

    $line = ('{0},{1},{2},{3},{4},{5},{6},{7}' -f `
      (_esc2([string]$r.Id)),
      (_esc2([string]$r.ServerRel)),
      (_esc2([string]$r.Name)),
      $size,
      (_esc2([string]$r.ModifiedUTC)),
      (_esc2([string]$r.WebUrl)),
      (_esc2([string]$r.HashQuick)),
      (_esc2([string]$r.HashSha1))
    )
    [void]$sb.AppendLine($line)
  }

  $payload = $sb.ToString()
  if ($payload.Length -gt 0) {
    $fs = [System.IO.File]::Open($script:FilesPartialPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
      try {
        $sw = New-Object System.IO.StreamWriter($fs, $enc)
        $sw.Write($payload)
        $sw.Flush()
        $fs.Flush($true)
      } finally {
        if ($sw) { $sw.Dispose() }
        $fs.Dispose()
      }
    $script:PartialWritten = $filesOut.Count

    try {
      $szItem = Get-Item -LiteralPath $script:FilesPartialPath -ErrorAction SilentlyContinue
      $sz = if ($szItem) { $szItem.Length } else { 0 }
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) {
        Write-Log ("[partial:checkpoint-flush] wrote {0} rows from filesOut, total written {1}, size {2:N0} bytes" -f ($filesOut.Count - $script:PartialWritten), $filesOut.Count, $sz)
      } else {
        Write-Host ("[partial:checkpoint-flush] wrote rows up to {0}, size {1:N0} bytes" -f $filesOut.Count, $sz)
      }
    } catch {}
  }
}


function Add-SkippedRow {
  param([string]$Reason,[string]$ServerRel,[string]$Name,[string]$Extension,[long]$SizeBytes,[string]$ModifiedUTC,[string]$HashQuick,[string]$HashSha1)
  $line = ('"{0}","{1}","{2}","{3}",{4},"{5}","{6}","{7}"' -f
    $Reason.Replace('"','""'),
    ($ServerRel ?? "").Replace('"','""'),
    ($Name ?? "").Replace('"','""'),
    ($Extension ?? "").Replace('"','""'),
    [int64]($SizeBytes ?? 0),
    ($ModifiedUTC ?? "").Replace('"','""'),
    ($HashQuick ?? "").Replace('"','""'),
    ($HashSha1 ?? "").Replace('"','""')
  )
  Add-Content -Path $script:SkippedCsvPath -Value $line -Encoding UTF8
}
function Stop-Run { 
  Write-Progress -Activity "Deduplicate AUDIT" -Completed
  Stop-Transcript | Out-Null 
}

# Progress
$script:EnumCount = 0
$script:EnumPage  = 0
$script:EnumFiles = 0
$script:LastProgressTime = Get-Date
$script:SwEnum = [System.Diagnostics.Stopwatch]::StartNew()
$script:LastCheckpointTime = Get-Date
$script:LastPageItems = 0
$script:TotalPageItems = 0

function Show-Progress {
  param([string]$Phase)
  try {
    $elapsed = $script:SwEnum.Elapsed
    $rateItems = if ($elapsed.TotalSeconds -gt 0) { ($script:EnumCount / $elapsed.TotalSeconds) } else { 0 }
    $rateFiles = if ($elapsed.TotalSeconds -gt 0) { ($script:EnumFiles / $elapsed.TotalSeconds) } else { 0 }
    $avgPage = if ($script:EnumPage -gt 0) { [math]::Round($script:TotalPageItems / $script:EnumPage,0) } else { 0 }

    $etaText = ""
    if ($script:ExpectedTotalFiles -gt 0 -and $script:EnumPage -ge $EtaAfterPages -and $rateFiles -gt 0) {
      $pct = [math]::Min( ($script:EnumFiles / [double]$script:ExpectedTotalFiles) * 100.0, 99.9)
      $remain = [math]::Max(0, $script:ExpectedTotalFiles - $script:EnumFiles)
      $etaSec = [math]::Round($remain / [double]$rateFiles, 0)
      $eta = [TimeSpan]::FromSeconds([double]$etaSec)
      $etaText = (" | ~{0}% done, ETA {1}" -f ([math]::Round($pct,1)), $eta.ToString("c"))
    }

    $status = ("Items={0:N0}, Pages={1:N0} (last={2:N0}, avg={3:N0}), Files={4:N0}, Elapsed={5}, Rate: items/s={6:N1}, files/s={7:N1}{8}" -f `
                $script:EnumCount, $script:EnumPage, $script:LastPageItems, $avgPage, $script:EnumFiles, $elapsed.ToString("c"), $rateItems, $rateFiles, $etaText)

    $pctBar = 0
    if ($script:ExpectedTotalFiles -gt 0) {
      $pctBar = [math]::Min(100,[math]::Max(0,[math]::Round(($script:EnumFiles / [double]$script:ExpectedTotalFiles) * 100,0)))
    }
    Write-Progress -Id 1 -Activity $Phase -Status $status -PercentComplete $pctBar
  } catch {
    Write-Log ("[WARN] Progress render failed: {0}" -f $_.Exception.Message) "WARN"
  }
}
function Update-ProgressReport {
  param([string]$Phase)
  $now = Get-Date
  $dueCount = ($script:EnumCount % [math]::Max(1,$ProgressEveryItems)) -eq 0
  $dueTime  = ($now - $script:LastProgressTime).TotalSeconds -ge [math]::Max(1,$ProgressEverySeconds)

  if ($dueCount -or $dueTime) {
    Show-Progress -Phase $Phase
    try {
      if ($script:ExpectedTotalFiles -and $script:ExpectedTotalFiles -gt 0) {
        $pct = [math]::Min(100,[math]::Max(0,[math]::Round(($script:EnumFiles / [double]$script:ExpectedTotalFiles) * 100,0)))
        Write-Log ("Progress: Items={0:N0} Pages={1:N0} Files={2:N0} (~{3}%)" -f $script:EnumCount,$script:EnumPage,$script:EnumFiles,$pct)
      } else {
        Write-Log ("Progress: Items={0:N0} Pages={1:N0} Files={2:N0}" -f $script:EnumCount,$script:EnumPage,$script:EnumFiles)
      }
    } catch {
      Write-Log ("Progress logging failed: {0}" -f $_.Exception.Message) "WARN"
    }

    # Lightweight % from rows written
    try {
      if ($script:ExpectedTotalFiles -and $script:ExpectedTotalFiles -gt 0 -and $script:PartialRows -gt 0) {
        $pctLite = [math]::Min(100,[math]::Max(0,[math]::Round(($script:PartialRows / [double]$script:ExpectedTotalFiles) * 100,0)))
        Write-Log ("Progress: ~{0}% (written={1:N0}/{2:N0})" -f $pctLite,$script:PartialRows,$script:ExpectedTotalFiles)
      }
    } catch { }

    $script:LastProgressTime = $now
  }
}

# Checkpoint
function Save-State {
  param([string]$Mode,[string]$NextLink,[string]$DeltaLink)
  $state = @{
    Timestamp = (Get-Date).ToString("s")
    Mode = $Mode
    SiteUrl = $SiteUrl
    LibraryTitle = $LibraryTitle
    DriveId = $script:DriveId
    RootPath = $script:RootPathResolved
    NextLink = $NextLink
    DeltaLink = $DeltaLink
    Enum = @{
      Items = $script:EnumCount
      Pages = $script:EnumPage
      Files = $script:EnumFiles
      ElapsedSec = [math]::Round($script:SwEnum.Elapsed.TotalSeconds,2)
    }
  }
  ($state | ConvertTo-Json -Depth 6) | Out-File -FilePath $script:StatePathFinal -Encoding UTF8
}
function Save-CheckpointState { param([string]$Mode,[string]$NextLink,[string]$DeltaLink)
  $now = Get-Date
  $dueCount = ($script:EnumCount % [math]::Max(1,$CheckpointEveryItems)) -eq 0
  $dueTime  = ($now - $script:LastCheckpointTime).TotalSeconds -ge [math]::Max(5,$CheckpointEverySeconds)
  if ($dueCount -or $dueTime) {
    Save-State -Mode $Mode -NextLink $NextLink -DeltaLink $DeltaLink
    $script:LastCheckpointTime = $now
    Flush-PartialBuffer
    Flush-PartialFromFilesOut
    Confirm-PartialUpdated -Context "checkpoint"
    Write-Log ("[partial:counts] filesOut={0} partialWritten={1} buffered={2} rowsSeen={3}" -f $script:FilesOutCount, $script:PartialWritten, ($script:PartialBuffer?.Count ?? 0), $script:PartialRows)
    Write-Log ("Checkpoint saved: Items={0:N0}, Files={1:N0}, State={2}" -f $script:EnumCount,$script:EnumFiles,$script:StatePathFinal)
  }
}

# Normalize extension arrays
function Convert-ExtensionList { param([string[]]$Extensions)
  if (-not $Extensions) { return @() }
  return $Extensions | ForEach-Object {
    $e = $_.Trim()
    if (-not $e.StartsWith(".")) { $e = "." + $e }
    $e.ToLowerInvariant()
  }
}
$IncludeExtensions = Convert-ExtensionList -Extensions $IncludeExtensions
$ExcludeExtensions = Convert-ExtensionList -Extensions $ExcludeExtensions

# Email helper
function Send-EmailSummary {
  param([string]$Subject, [string]$BodyText)
  if (-not $EmailReport -or [string]::IsNullOrEmpty($EmailTo) -or [string]::IsNullOrEmpty($EmailFrom) -or [string]::IsNullOrEmpty($SendGridApiKey)) { return }
  
   # --- ZIP creation for email attachments (avoid size limits) ---
  $zipPath = Join-Path $script:RunFolder "Deduplication-Results.zip"
  if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

  $toZip = @()
  if ($EmailAttachCsv -and (Test-Path $script:ReportPath)) { $toZip += $script:ReportPath }
  if ($EmailAttachSummary -and (Test-Path $script:SummaryCsvPath)) { $toZip += $script:SummaryCsvPath }

  $attachments = @()
  if ($toZip.Count -gt 0) {
      Compress-Archive -Path $toZip -DestinationPath $zipPath -Force
      $zipBytes = [System.IO.File]::ReadAllBytes($zipPath)
      $attachments += @{
          content     = [Convert]::ToBase64String($zipBytes)
          type        = "application/zip"
          filename    = "Deduplication-Results.zip"
          disposition = "attachment"
      }
  }




  $payload = @{
    personalizations = @(@{ to = @(@{ email = $EmailTo }) })
    from = @{ email = $EmailFrom }
    subject = $Subject
    content = @(@{ type = "text/plain"; value = $BodyText })
  }
  if ($attachments.Count -gt 0) { $payload["attachments"] = $attachments }
  try {
    Invoke-RestMethod -Method Post -Uri "https://api.sendgrid.com/v3/mail/send" -Headers @{
      "Authorization" = "Bearer $SendGridApiKey"
      "Content-Type"  = "application/json"
    } -Body ($payload | ConvertTo-Json -Depth 6) | Out-Null
    Write-Log "Email summary sent."
  } catch { Write-Log ("Failed to send email: {0}" -f $_.Exception.Message) "WARN" }
}

# HTML email body generator (AUDIT ONLY)
function New-DedupHtmlEmailBody {
    [CmdletBinding()]
    param(
        [string]   $SiteUrl,
        [string]   $LibraryTitle,
        [string]   $ScopeFolder,
        [decimal]  $TotalWasteGB,
        [decimal]  $TotalAnnualCost,
        [object[]] $TopGroups,
        [decimal]  $StorageCostPerGBPerYear,
        [string]   $CurrencySymbol = "£",
        [switch]   $AuditOnly
    )

    # (Optional) keep your original Add-Type, but it isn't required on PS7+
    try { Add-Type -AssemblyName System.Web -ErrorAction SilentlyContinue } catch { }

    # PS7-safe HTML encoder (works on PS5 too)
    if (-not (Get-Command Encode-Html -ErrorAction SilentlyContinue)) {
        function Encode-Html {
            param([Parameter(Mandatory)][string]$Value)
            [System.Net.WebUtility]::HtmlEncode($Value)
        }
    }

    $scopeLine = if ([string]::IsNullOrWhiteSpace($ScopeFolder)) { "—" } else { [System.Web.HttpUtility]::HtmlEncode([string]$ScopeFolder) }
    $modeBadge = if ($AuditOnly) { "<strong style='color:#cc0000;'>Audit Only</strong>" } else { "<strong style='color:#007700;'>Execute</strong>" }

    $wasteBold  = ("{0:N2}" -f [decimal]$TotalWasteGB)
    $annualBold = ("{0:N2}" -f [decimal]$TotalAnnualCost)

    # Build table rows for TopGroups (calculate WasteGB & Annual from DuplicateWasteBytes if needed)
    $rows = @()
    foreach ($g in $TopGroups) {
        $wasteGB = $null
        if ($g.PSObject.Properties.Name -contains 'DuplicateWasteGB') {
            $wasteGB = [decimal]$g.DuplicateWasteGB
        } elseif ($g.PSObject.Properties.Name -contains 'DuplicateWasteBytes') {
            $wasteGB = [math]::Round(([decimal]$g.DuplicateWasteBytes / 1GB), 4)
        }
        if ($null -eq $wasteGB) { continue }

        $annual  = [math]::Round(($wasteGB * $StorageCostPerGBPerYear), 2)
        $count   = if ($g.PSObject.Properties.Name -contains 'FileCount') { $g.FileCount } else { $null }
        $sample  = if ($g.PSObject.Properties.Name -contains 'SamplePath') { [System.Web.HttpUtility]::HtmlEncode([string]$g.SamplePath) } else { "" }
        $keyType = if ($g.PSObject.Properties.Name -contains 'KeyType') { [System.Web.HttpUtility]::HtmlEncode([string]$g.KeyType) } else { "" }

        $rows += "<tr>
            <td style='text-align:right;'>{0:N4}</td>
            <td style='text-align:right;'>{1}{2:N2}</td>
            <td style='text-align:center;'>{3}</td>
            <td><code style='font-size:12px;'>{4}</code><br/><span style='font-size:12px;color:#666;'>KeyType={5}</span></td>
        </tr>" -f $wasteGB, $CurrencySymbol, $annual, $count, $sample, $keyType
    }

    if ($rows.Count -eq 0) {
        $rows = @("<tr><td colspan='4' style='text-align:center; color:#666;'>No duplicate groups found in this run.</td></tr>")
    }

    $rowsHtml = ($rows -join [Environment]::NewLine)

    $html = @"
<html>
  <body style='font-family:Segoe UI, Arial, sans-serif; font-size:14px; color:#333; line-height:1.45;'>
    <h2 style='color:#2c6e91; margin:0 0 12px 0;'>SharePoint Deduplication — Audit Summary</h2>

    <p style='margin:0 0 10px 0;'>Hello,</p>

    <p style='margin:0 0 10px 0;'>
      The script has completed an $modeBadge run against:
    </p>

    <table cellpadding='6' cellspacing='0' style='border-collapse:collapse; font-size:13px; margin:0 0 10px 0;'>
      <tr><td style='color:#666;'>Site</td><td><a href='$SiteUrl' style='color:#2c6e91; text-decoration:none;'>$SiteUrl</a></td></tr>
      <tr><td style='color:#666;'>Library</td><td><strong>$LibraryTitle</strong></td></tr>
      <tr><td style='color:#666;'>Folder scope</td><td><strong>$scopeLine</strong></td></tr>
    </table>

    <div style='padding:10px 12px; background:#f7fbf7; border:1px solid #e1f0e1; border-radius:6px; margin:12px 0;'>
      <div style='font-size:15px; margin-bottom:4px;'>Estimated savings if duplicates are removed:</div>
      <div style='font-size:18px;'>
        <strong>$wasteBold GB</strong> storage
        &nbsp;·&nbsp;
        <strong style='color:#006400;'>$CurrencySymbol$annualBold per year</strong>
      </div>
    </div>

    <h3 style='margin:16px 0 8px 0;'>Top Duplicate Groups</h3>
    <table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse; font-size:13px; width:100%;'>
      <tr style='background:#f0f0f0;'>
        <th style='text-align:right; width:110px;'>Waste (GB)</th>
        <th style='text-align:right; width:120px;'>Annual ($CurrencySymbol)</th>
        <th style='text-align:center; width:90px;'>Count</th>
        <th>Sample File (and KeyType)</th>
      </tr>
      $rowsHtml
    </table>

    <p style='margin:14px 0 0 0; color:#555;'>
      <em>This was an <strong>Audit Only</strong> run. No files have been moved or deleted.</em>
    </p>

    <p style='margin:12px 0 0 0;'>Regards,<br/>
       SharePoint Deduplication Script</p>
  </body>
</html>
"@
    return $html
}

# SendGrid HTML sender (keeps attachment behaviour)
function Send-EmailSummaryHtml {
  param([string]$Subject, [string]$BodyHtml)
  if (-not $EmailReport -or [string]::IsNullOrEmpty($EmailTo) -or [string]::IsNullOrEmpty($EmailFrom) -or [string]::IsNullOrEmpty($SendGridApiKey)) { return }

  # --- ZIP creation for email attachments (avoid size limits) ---
  $zipPath = Join-Path $script:RunFolder "Deduplication-Results.zip"
  if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

  $toZip = @()
  if ($EmailAttachCsv -and (Test-Path $script:ReportPath)) { $toZip += $script:ReportPath }
  if ($EmailAttachSummary -and (Test-Path $script:SummaryCsvPath)) { $toZip += $script:SummaryCsvPath }

  $attachments = @()
  if ($toZip.Count -gt 0) {
      Compress-Archive -Path $toZip -DestinationPath $zipPath -Force
      $zipBytes = [System.IO.File]::ReadAllBytes($zipPath)
      $attachments += @{
          content     = [Convert]::ToBase64String($zipBytes)
          type        = "application/zip"
          filename    = "Deduplication-Results.zip"
          disposition = "attachment"
      }
  }


  $payload = @{
    personalizations = @(@{ to = @(@{ email = $EmailTo }) })
    from = @{ email = $EmailFrom }
    subject = $Subject
    content = @(@{ type = "text/html"; value = $BodyHtml })
  }
  if ($attachments.Count -gt 0) { $payload["attachments"] = $attachments }

  try {
    Invoke-RestMethod -Method Post -Uri "https://api.sendgrid.com/v3/mail/send" -Headers @{
      "Authorization" = "Bearer $SendGridApiKey"
      "Content-Type"  = "application/json"
    } -Body ($payload | ConvertTo-Json -Depth 6) | Out-Null
    Write-Log "HTML email summary sent."
  } catch { Write-Log ("Failed to send HTML email: {0}" -f $_.Exception.Message) "WARN" }
}



# Graph helper
function Invoke-GraphSafe {
  param([string]$Url, [ValidateSet("GET","POST","PATCH","DELETE")] [string]$Method = "GET")
  Write-Verbose ("Graph {0} {1}" -f $Method, $Url)
  $attempt = 0; $max = 5; $delay = 1
  while ($true) {
    try { return Invoke-PnPGraphMethod -Url $Url -Method $Method }
    catch {
      $statusCode = $null
      if ($_.Exception.Response -and $_.Exception.Response.StatusCode) { $statusCode = [int]$_.Exception.Response.StatusCode }
      if ($statusCode -in 429,500,502,503,504 -and $attempt -lt $max) {
        $attempt++; Write-Debug ("Graph {0} failed with {1}. Sleeping {2}s before retry {3}/{4}" -f $Method,$statusCode,$delay,$attempt,$max)
        Start-Sleep -Seconds $delay; $delay = [Math]::Min($delay * 2, 30); continue
      } else { throw }
    }
  }
}

# Connect & resolve
Write-Log ("Starting AUDIT+. SiteUrl='{0}' Library='{1}'" -f $SiteUrl,$LibraryTitle)
Write-Log "Connecting with device code..."
Connect-PnPOnline -Url $SiteUrl -ClientId $ClientId -Tenant $TenantId -DeviceLogin

$list = Get-PnPList -Identity $LibraryTitle
if (-not $list) { throw "List/Library '$LibraryTitle' not found." }
$libRootRel = $list.RootFolder.ServerRelativeUrl
Write-Log ("Library root: {0}" -f $libRootRel)

# Auto-seed ExpectedTotalFiles from ItemCount (approx, items include folders)
if ($ExpectedTotalFiles -le 0 -and $AutoSeedExpectedTotal) {
  try {
    $seed = $list.ItemCount
    if ($seed -gt 0) { $script:ExpectedTotalFiles = [int]$seed; Write-Log ("Auto-seeded ExpectedTotalFiles = {0} (SharePoint ItemCount: files + folders; ETA is approximate)" -f $script:ExpectedTotalFiles) }
  } catch { Write-Log ("Auto-seed of ExpectedTotalFiles failed: {0}" -f $_.Exception.Message) "WARN" }
} else { $script:ExpectedTotalFiles = $ExpectedTotalFiles }

# Resolve site/drive
function Resolve-GraphSiteId { param([string]$TargetSiteUrl)
  $siteHost = ([Uri]$TargetSiteUrl).Host
  $search = Invoke-GraphSafe -Url ("v1.0/sites?search={0}" -f [Uri]::EscapeDataString($siteHost))
  if ($search.value) { foreach ($s in $search.value) { if ($s.webUrl -eq $TargetSiteUrl) { return $s.id } } }
  $path = ([Uri]$TargetSiteUrl).AbsolutePath.Trim("/")
  $attempt = Invoke-GraphSafe -Url ("v1.0/sites/{0}:/{1}:" -f $siteHost, $path)
  if ($attempt -and $attempt.id) { return $attempt.id }
  throw "Unable to resolve Graph site id for $TargetSiteUrl"
}
$siteId = Resolve-GraphSiteId -TargetSiteUrl $SiteUrl
Write-Log ("Graph siteId: {0}" -f $siteId)

function Get-GraphDrives { param([string]$SiteId) (Invoke-GraphSafe -Url ("v1.0/sites/{0}/drives" -f $SiteId)).value }
$drives = Get-GraphDrives -SiteId $siteId
$drive = $drives | Where-Object { $_.name -eq $LibraryTitle } | Select-Object -First 1
if (-not $drive) { throw "Graph drive for library '$LibraryTitle' not found." }
$script:DriveId = $drive.id
Write-Log ("Graph driveId: {0} (name={1})" -f $drive.id, $drive.name)

# Scope
$script:RootPathResolved = ""
if ($ScopeFolder) {
  $prefix = ($libRootRel.Trim("/"))
  $scope = $ScopeFolder.Trim("/")
  if ($scope -like "$prefix*") { $script:RootPathResolved = $scope.Substring($prefix.Length).Trim("/") }
  else { $script:RootPathResolved = $ScopeFolder.Trim("/") }
}
Write-Log ("Scope: {0}" -f ($(if ($script:RootPathResolved) { $script:RootPathResolved } else { "<root>" })))

# Helpers
function Get-ServerRelFromGraphPath {
  param([string]$ParentPath, [string]$Name)
  if ([string]::IsNullOrWhiteSpace($ParentPath)) { return ($libRootRel.TrimEnd("/") + "/" + $Name) }
  $idx = $ParentPath.IndexOf("root:/")
  if ($idx -ge 0) {
    $tail = $ParentPath.Substring($idx + 6).TrimStart("/")
    return ($libRootRel.TrimEnd("/") + "/" + $tail + "/" + $Name)
  } else { return ($libRootRel.TrimEnd("/") + "/" + $Name) }
}

# Enumerators
function Get-DriveItemsDelta {
  param([string]$DriveId,[string]$RootPath,[string]$ResumeNextLink)
  $mode = "delta"
  if ($ResumeNextLink) { Write-Log "Resuming delta from saved nextLink..."; $page = Invoke-GraphSafe -Url $ResumeNextLink }
  else {
    $deltaUrl = if ([string]::IsNullOrEmpty($RootPath)) {
      "v1.0/drives/{0}/root/delta?`$select=id,name,size,webUrl,parentReference,file,fileSystemInfo,folder,deleted,ctag" -f $DriveId
    } else {
      "v1.0/drives/{0}/root:/{1}:/delta?`$select=id,name,size,webUrl,parentReference,file,fileSystemInfo,folder,deleted,ctag" -f $DriveId,$RootPath
    }
    $page = Invoke-GraphSafe -Url $deltaUrl
  }
  $results = New-Object System.Collections.Generic.List[object]
  $lastNext = $null; $lastDelta = $null
  while ($true) {
    $script:EnumPage++
    Write-Verbose ("Delta page #{0} returned {1} items" -f $script:EnumPage, ($page.value | Measure-Object).Count)
    $script:LastPageItems = ($page.value | Measure-Object).Count
    $script:TotalPageItems += $script:LastPageItems
    foreach ($it in $page.value) {
      $results.Add($it) | Out-Null
      $script:EnumCount++
      if ($it.file) { $script:EnumFiles++ }
      Update-ProgressReport -Phase "Enumerating (delta)"
      Save-CheckpointState -Mode $mode -NextLink $lastNext -DeltaLink $lastDelta
    }
    if ($null -ne $page.'@odata.nextLink') {
      $lastNext = $page.'@odata.nextLink'
      Write-Verbose ("Delta nextLink: {0}" -f $lastNext)
      Save-State -Mode $mode -NextLink $lastNext -DeltaLink $lastDelta
      $page = Invoke-GraphSafe -Url $lastNext
    } else {
      if ($null -ne $page.'@odata.deltaLink') { $lastDelta = $page.'@odata.deltaLink'; Save-State -Mode $mode -NextLink $null -DeltaLink $lastDelta }
      break
    }
  }
  return $results
}
function Get-DriveItemsChildren {
  param([string]$DriveId,[string]$RootPath)
  $mode = "children"
  $baseSel = "`$select=id,name,size,webUrl,parentReference,file,fileSystemInfo,folder,ctag"
  $startUrl = if ([string]::IsNullOrEmpty($RootPath)) { "v1.0/drives/{0}/root/children?{1}" -f $DriveId,$baseSel } else { "v1.0/drives/{0}/root:/{1}:/children?{2}" -f $DriveId, $RootPath, $baseSel }
  $page = Invoke-GraphSafe -Url $startUrl
  $results = New-Object System.Collections.Generic.List[object]
  while ($true) {
    $script:EnumPage++
    Write-Verbose ("Children page #{0} returned {1} items" -f $script:EnumPage, ($page.value | Measure-Object).Count)
    $script:LastPageItems = ($page.value | Measure-Object).Count
    $script:TotalPageItems += $script:LastPageItems
    foreach ($it in $page.value) {
      $results.Add($it) | Out-Null
      $script:EnumCount++
      if ($it.file) { $script:EnumFiles++ }
      Update-ProgressReport -Phase "Enumerating (children)"
      Save-CheckpointState -Mode $mode -NextLink $null -DeltaLink $null
    }
    if ($null -ne $page.'@odata.nextLink') {
      $next = $page.'@odata.nextLink'
      Write-Verbose ("Children nextLink: {0}" -f $next)
      Save-State -Mode $mode -NextLink $next -DeltaLink $null
      $page = Invoke-GraphSafe -Url $next
    } else { break }
  }
  return $results
}

# Resume bootstrap
$processedIds = New-Object System.Collections.Generic.HashSet[string]
$files = @()
if ($EnableResume -and (Test-Path $script:FilesPartialPath)) {
  try {
    $partial = Import-Csv -Path $script:FilesPartialPath
    foreach ($row in $partial) {
      $null = $processedIds.Add($row.Id)
      $files += $row
    }
    Write-Log ("Resume: loaded {0} previously processed files from partial cache" -f $processedIds.Count)
  } catch { Write-Log ("Resume: failed to load partial cache: {0}" -f $_.Exception.Message) "WARN" }
}
if (-not (Test-Path $script:FilesPartialPath)) {
  "Id,ServerRel,Name,SizeBytes,ModifiedUTC,WebUrl,HashQuick,HashSha1" | Out-File -FilePath $script:FilesPartialPath -Encoding UTF8
}
# (removed Add-PartialFileRow)

# Enumerate
Write-Log ("Enumerating via Graph ({0})..." -f ($(if ($UseDelta) { "delta" } else { "children" })))
$ResumeNextLink = $null
if ($EnableResume -and (Test-Path $script:StatePathFinal)) {
  try {
    $prev = Get-Content -Raw -Path $script:StatePathFinal | ConvertFrom-Json
    if ($prev.Mode -eq "delta" -and $UseDelta -and $prev.DriveId -eq $script:DriveId -and $prev.SiteUrl -eq $SiteUrl -and $prev.LibraryTitle -eq $LibraryTitle) {
      $ResumeNextLink = $prev.NextLink
      if ($ResumeNextLink) { Write-Log "Found saved delta nextLink; will resume paging." }
    } else { Write-Log "State exists but not compatible with current run; ignoring." "WARN" }
  } catch { Write-Log ("Failed to parse state file: {0}" -f $_.Exception.Message) "WARN" }
}

try {
  $items = if ($UseDelta) { Get-DriveItemsDelta -DriveId $script:DriveId -RootPath $script:RootPathResolved -ResumeNextLink $ResumeNextLink } else { Get-DriveItemsChildren -DriveId $script:DriveId -RootPath $script:RootPathResolved }
} catch {
  Write-Log ("Primary enumeration failed: {0} -- falling back to children listing" -f $_.Exception.Message) "WARN"
  $items = Get-DriveItemsChildren -DriveId $script:DriveId -RootPath $script:RootPathResolved
}
Show-Progress -Phase "Enumerating - complete"

# Filter & project + diagnostics
$minBytes = [int64]($MinSizeKB * 1024)
$filesOut = @()
$filesSeen = 0; $withQuick = 0; $withSha1 = 0; $withoutAnyHash = 0
foreach ($it in $items) {
  if ($null -ne $it.deleted) { continue }
  if ($null -eq $it.file) { continue }

  $filesSeen++

  if ($filesSeen % 1000 -eq 0) {
    try {
      $szItem = Get-Item -LiteralPath $script:FilesPartialPath -ErrorAction SilentlyContinue
      $sz = if ($szItem) { $szItem.Length } else { 0 }
      Write-Log ("Heartbeat: seen={0}, files.partial.csv size={1:N0} bytes" -f $filesSeen, $sz)
    } catch {
      Write-Log ("Heartbeat: seen={0}, files.partial.csv not created yet" -f $filesSeen) "WARN"
    }
  }
  $serverRel = Get-ServerRelFromGraphPath -ParentPath $it.parentReference.path -Name $it.name
  $ext = [System.IO.Path]::GetExtension($it.name).ToLowerInvariant()
  $size = [int64]$it.size
  $mod  = [datetime]$it.fileSystemInfo.lastModifiedDateTime
  $hQuick = $it.file.hashes.quickXorHash
  $hSha1  = $it.file.hashes.sha1Hash
  if ($hQuick) { $withQuick++ }
  if ($hSha1)  { $withSha1++ }
  if (-not $hQuick -and -not $hSha1) { $withoutAnyHash++ }

  # Skip reasons
  if ($serverRel -match "/$([regex]::Escape($QuarantineFolderName))(/|$)")  { Add-SkippedRow -Reason "InQuarantine"     -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($serverRel -match "/_layouts/")                                       { Add-SkippedRow -Reason "InLayouts"        -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($serverRel -match "/_catalogs/")                                      { Add-SkippedRow -Reason "InCatalogs"       -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($ext -eq ".aspx")                                                     { Add-SkippedRow -Reason "Extension=.aspx"  -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($size -lt $minBytes)                                                  { Add-SkippedRow -Reason "SizeBelowMin"     -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($ModifiedAfter  -and $mod -lt $ModifiedAfter)                         { Add-SkippedRow -Reason "ModifiedBeforeMin"-ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($ModifiedBefore -and $mod -gt $ModifiedBefore)                        { Add-SkippedRow -Reason "ModifiedAfterMax" -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($IncludeExtensions -and $IncludeExtensions.Count -gt 0 -and ($IncludeExtensions -notcontains $ext)) { Add-SkippedRow -Reason "NotInIncludeList" -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }
  if ($ExcludeExtensions -and $ExcludeExtensions.Count -gt 0 -and ($ExcludeExtensions -contains $ext))    { Add-SkippedRow -Reason "InExcludeList"    -ServerRel $serverRel -Name $it.name -Extension $ext -SizeBytes $size -ModifiedUTC $mod.ToString("s") -HashQuick $hQuick -HashSha1 $hSha1; continue }

  $row = [pscustomobject]@{
    Id          = $it.id
    ServerRel   = $serverRel
    Name        = $it.name
    SizeBytes   = $size
    ModifiedUTC = $mod.ToString("s")
    WebUrl      = $it.webUrl
    HashQuick   = $hQuick
    HashSha1    = $hSha1
}
  $filesOut += $row
  $script:FilesOutCount++
  if ($script:FilesOutCount % 1000 -eq 0) { if (Get-Command Write-Log -ErrorAction SilentlyContinue) { Write-Log ("[partial:add] filesOut={0} rowsSeen={1}" -f $script:FilesOutCount, $script:PartialRows) } else { Write-Host ("[partial:add] filesOut={0} rowsSeen={1}" -f $script:FilesOutCount, $script:PartialRows) } }
  Add-PartialFileRow -Row $row
}
Write-Log ("Seen files: {0}, quickXorHash: {1}, sha1Hash: {2}, no hash: {3}" -f $filesSeen,$withQuick,$withSha1,$withoutAnyHash)
Write-Log ("Discovered {0} files kept for duplicate analysis (post-filters)" -f $filesOut.Count)

Finalize-Partial
# Finalize files cache (unique by Id)
$filesUnique = $filesOut | Group-Object Id | ForEach-Object { $_.Group[-1] }
$filesUnique | Select-Object Id,ServerRel,Name,SizeBytes,ModifiedUTC,WebUrl,HashQuick,HashSha1 | Export-Csv -Path $script:FilesFinalPath -NoTypeInformation -Encoding UTF8
Write-Log ("Files cache written: {0}" -f $script:FilesFinalPath)

# Master Index
if ($HashDuringScan -and $MasterIndexCsv) {
  $existing = @()
  if (Test-Path $MasterIndexCsv) { try { $existing = Import-Csv -Path $MasterIndexCsv -Encoding UTF8 } catch { $existing = Import-Csv -Path $MasterIndexCsv } }
  $merged = @(); $merged += $existing; $merged += ($filesUnique | Select-Object ServerRel,Name,SizeBytes,ModifiedUTC,WebUrl,HashQuick,HashSha1)
  $merged = $merged | Group-Object ServerRel | ForEach-Object { $_.Group[-1] }
  $merged | Export-Csv -Path $MasterIndexCsv -NoTypeInformation -Encoding UTF8
  Write-Log "MasterIndex updated."
}

# Duplicate key computation
function Get-DuplicateKeyForFile {
  param([object]$f,[string]$Primary,[string]$Fallback)
  $key = $null
  switch ($Primary) {
    "quickXorHash"    { $key = $f.HashQuick }
    "sha1Hash"        { $key = $f.HashSha1 }
    "sha1OrQuickXor"  { $key = if ($f.HashSha1) { $f.HashSha1 } else { $f.HashQuick } }
    "sizeAndName"     { $key = "{0}|{1}" -f ([int64]$f.SizeBytes), $f.Name.ToLowerInvariant() }
    "sizeOnly"        { $key = "{0}" -f ([int64]$f.SizeBytes) }
    default           { $key = $f.HashQuick }
  }
  if (-not $key -and $Fallback -and $Fallback -ne "None") {
    switch ($Fallback) {
      "quickXorHash"    { $key = $f.HashQuick }
      "sha1Hash"        { $key = $f.HashSha1 }
      "sha1OrQuickXor"  { $key = if ($f.HashSha1) { $f.HashSha1 } else { $f.HashQuick } }
      "sizeAndName"     { $key = "{0}|{1}" -f ([int64]$f.SizeBytes), $f.Name.ToLowerInvariant() }
      "sizeOnly"        { $key = "{0}" -f ([int64]$f.SizeBytes) }
    }
  }
  return $key
}

$keyed = @()
$noKey = 0
foreach ($f in $filesUnique) {
  $k = Get-DuplicateKeyForFile -f $f -Primary $DuplicateKey -Fallback $FallbackDuplicateKey
  if ($k) {
    $f | Add-Member -NotePropertyName "DupKey" -NotePropertyValue $k
    $f | Add-Member -NotePropertyName "KeyType" -NotePropertyValue $DuplicateKey
    $keyed += $f
  } else {
    $noKey++
    Add-SkippedRow -Reason "NoDuplicateKey" -ServerRel $f.ServerRel -Name $f.Name -Extension ([System.IO.Path]::GetExtension($f.Name).ToLowerInvariant()) -SizeBytes $f.SizeBytes -ModifiedUTC $f.ModifiedUTC -HashQuick $f.HashQuick -HashSha1 $f.HashSha1
  }
}
Write-Log ("Files with usable duplicate key: {0}; without key: {1}" -f $keyed.Count, $noKey)

# Duplicate detection + outputs
$dupGroups = $keyed | Group-Object DupKey | Where-Object { $_.Count -gt 1 }

$report = @()
foreach ($grp in $dupGroups) {
  $groupFiles = $grp.Group | Sort-Object { [datetime]$_.ModifiedUTC } -Descending
  $master = $groupFiles[0]
  $dups   = $groupFiles | Select-Object -Skip 1
  foreach ($d in $dups) {
    $report += [pscustomobject]@{
      DuplicateKey = $grp.Name
      KeyType      = ($groupFiles[0].KeyType)
      MasterPath   = $master.ServerRel
      MasterUrl    = $master.WebUrl
      Duplicate    = $d.ServerRel
      DuplicateUrl = $d.WebUrl
      SizeBytes    = $d.SizeBytes
      ModifiedUTC  = $d.ModifiedUTC
    }
  }
}

$GB = [decimal](1024*1024*1024)
$summary = @()
foreach ($grp in $dupGroups) {
  $g = $grp.Group
  $count = $g.Count
  $totalSize = [decimal](($g | Measure-Object -Property SizeBytes -Sum).Sum)
  $newest = ($g | Sort-Object { [datetime]$_.ModifiedUTC } -Descending | Select-Object -First 1).ModifiedUTC
  $oldest = ($g | Sort-Object { [datetime]$_.ModifiedUTC } | Select-Object -First 1).ModifiedUTC
  $sample = ($g | Select-Object -First 1).ServerRel
  $keptSize = [decimal](($g | Sort-Object { [datetime]$_.ModifiedUTC } -Descending | Select-Object -First 1).SizeBytes)
  $wasteBytes = [decimal]($totalSize - $keptSize)
  $wasteGB    = [math]::Round(($wasteBytes / $GB), 4)
  $annualCost = [math]::Round(($wasteGB * $StorageCostPerGBPerYear), 2)
  $summary += [pscustomobject]@{
    DuplicateKey         = $grp.Name
    KeyType              = ($grp.Group[0].KeyType)
    FileCount            = $count
    TotalSizeBytes       = [int64]$totalSize
    DuplicateWasteBytes  = [int64]$wasteBytes
    DuplicateWasteGB     = [decimal]$wasteGB
    AnnualCost           = [decimal]$annualCost
    NewestUTC            = $newest
    OldestUTC            = $oldest
    SamplePath           = $sample
  }
}

$report | Export-Csv -Path $script:ReportPath -NoTypeInformation -Encoding UTF8
$summary | Sort-Object -Property DuplicateWasteBytes -Descending | Export-Csv -Path $script:SummaryCsvPath -NoTypeInformation -Encoding UTF8
if ($OutputJson) {
  $report  | ConvertTo-Json -Depth 6 | Out-File -FilePath $script:ReportJsonPath  -Encoding UTF8
  $summary | ConvertTo-Json -Depth 6 | Out-File -FilePath $script:SummaryJsonPath -Encoding UTF8
}

Write-Log ("Report CSV : {0}" -f $script:ReportPath)
Write-Log ("Summary CSV: {0}" -f $script:SummaryCsvPath)
if ($OutputJson) {
  Write-Log ("Report JSON : {0}" -f $script:ReportJsonPath)
  Write-Log ("Summary JSON: {0}" -f $script:SummaryJsonPath)
}

# Summary + cost
$totalPairs = $report.Count
$totalGroups= $summary.Count
$totalFiles = $filesUnique.Count
$totalWasteBytes = [decimal](($summary | Measure-Object -Property DuplicateWasteBytes -Sum).Sum)
$totalWasteGB    = [math]::Round(($totalWasteBytes / $GB), 4)
$totalAnnualCost = [math]::Round(($totalWasteGB * $StorageCostPerGBPerYear), 2)

$topN = $summary | Sort-Object -Property DuplicateWasteBytes -Descending | Select-Object -First $TopNHashesByWaste

$lines = @()
$lines += "Deduplicate AUDIT+ (content-based) completed at $(Get-Date -Format s)"
$lines += "Host   : $hostPart"
$lines += "Site   : /$($u.AbsolutePath.Trim('/'))"
$lines += "Library: $LibraryTitle"
$lines += "Scope  : " + ($(if ($script:RootPathResolved) { $script:RootPathResolved } else { "<root>" }))
$lines += ("Files scanned (post-filters) : {0:N0}" -f $totalFiles)
$lines += ("Duplicate groups (key)       : {0:N0}" -f $totalGroups)
$lines += ("Duplicate pairs (master/dup) : {0:N0}" -f $totalPairs)
$lines += ("Estimated duplicate waste    : {0:N0} bytes ({1:N4} GB)" -f $totalWasteBytes, $totalWasteGB)
$lines += ("Estimated annual savings     : {0}{1:N2} @ cost {0}{2:N2}/GB/yr" -f $CurrencySymbol, $totalAnnualCost, $StorageCostPerGBPerYear)
$lines += ("Report.csv : {0}" -f $script:ReportPath)
$lines += ("Summary.csv: {0}" -f $script:SummaryCsvPath)
$lines += ("Files.csv  : {0}" -f $script:FilesFinalPath)
$lines += ("State.json : {0}" -f $script:StatePathFinal)
$lines += ""
$lines += "Top duplicate groups by waste & cost:"
foreach ($row in $topN) {
  $lines += (" - Waste={0:N4} GB | Annual={3}{1:N2} | Count={2} | Sample={4} | KeyType={5}" -f $row.DuplicateWasteGB, ($row.AnnualCost), $row.FileCount, $CurrencySymbol, $row.SamplePath, $row.KeyType)
}
$summaryText = $lines -join [Environment]::NewLine
Write-Log $summaryText



if ($EmailReport) {
  try {
    $bodyHtml = New-DedupHtmlEmailBody -SiteUrl $SiteUrl -LibraryTitle $LibraryTitle -ScopeFolder $ScopeFolder -TotalWasteGB $totalWasteGB -TotalAnnualCost $totalAnnualCost -TopGroups $topN -StorageCostPerGBPerYear $StorageCostPerGBPerYear -CurrencySymbol $CurrencySymbol -AuditOnly
    if ([string]::IsNullOrWhiteSpace($bodyHtml)) { throw 'HTML body empty' }
    Send-EmailSummaryHtml -Subject ("Deduplicate AUDIT+ - {0}" -f $LibraryTitle) -BodyHtml $bodyHtml
  } catch {
    Write-Log ("HTML email failed, falling back to plain text: {0}" -f $_.Exception.Message) "WARN"
    Send-EmailSummary -Subject ("Deduplicate AUDIT+ - {0}" -f $LibraryTitle) -BodyText $summaryText
  }
}



Stop-Run
