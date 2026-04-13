param(
    [string]$Repository = "apache/airflow",
    [int]$Major = 3,
    [int]$Minor = 0,
    [string]$AsOfDate = "",
    [int]$WindowDays = 30,
    [string]$OutputCsv = "airflow_patch_metrics.csv"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-IssueCount {
    param([string]$Query)

    $apiPath = "search/issues?q=$Query&per_page=1"
    $count = gh api $apiPath --jq ".total_count"
    return [int]$count
}

function Get-K1Score {
    param([int]$OpenCritical)

    if ($OpenCritical -eq 0) { return 2 }
    if ($OpenCritical -le 2) { return 1 }
    return 0
}

function Get-K2Score {
    param([double]$CloseRatePct, [int]$TotalBugs)

    if ($TotalBugs -le 0) { return $null }
    if ($CloseRatePct -ge 85.0) { return 2 }
    if ($CloseRatePct -ge 75.0) { return 1 }
    return 0
}

if (-not $AsOfDate) {
    $asOf = Get-Date
} else {
    $asOf = [datetime]::ParseExact($AsOfDate, "yyyy-MM-dd", $null)
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) not found. Install gh and run 'gh auth login'."
}

Write-Host "Collecting releases for $Repository, branch $Major.$Minor.x ..."
$releaseJson = gh release list --repo $Repository --limit 300 --json tagName,publishedAt | ConvertFrom-Json
$releaseItems = @($releaseJson)

$releases = $releaseItems |
    Where-Object {
        $_.tagName -like "$Major.$Minor.*" -and
        $_.tagName -notlike "*/*" -and
        ($_.tagName.Split('.').Count -eq 3)
    } |
    ForEach-Object {
        [PSCustomObject]@{
            tagName = $_.tagName
            publishedAt = $_.publishedAt
            patch = [int]($_.tagName.Split('.')[-1])
        }
    } |
    Sort-Object patch

if (-not $releases -or $releases.Count -eq 0) {
    throw "No releases found for $Major.$Minor.x in $Repository"
}

$rows = @()

for ($i = 0; $i -lt $releases.Count; $i++) {
    $tag = $releases[$i].tagName
    $patch = [int]$releases[$i].patch
    $releaseDate = [datetime]$releases[$i].publishedAt

    $nextReleaseDate = if ($i -lt $releases.Count - 1) {
        [datetime]$releases[$i + 1].publishedAt
    } else {
        $asOf
    }

    $windowEnd = $releaseDate.AddDays($WindowDays)
    if ($nextReleaseDate -lt $windowEnd) { $windowEnd = $nextReleaseDate }
    if ($asOf -lt $windowEnd) { $windowEnd = $asOf }

    if ($windowEnd -le $releaseDate) {
        continue
    }

    $from = $releaseDate.ToString("yyyy-MM-dd")
    $to = $windowEnd.ToString("yyyy-MM-dd")
    $window = "$from..$to"

    $totalQuery = "repo:$Repository+is:issue+label:kind:bug+created:$window"
    $openQuery = "$totalQuery+is:open"
    $criticalOpenQuery = "$openQuery+label:priority:critical"

    $total = Get-IssueCount -Query $totalQuery
    $open = Get-IssueCount -Query $openQuery
    $criticalOpen = Get-IssueCount -Query $criticalOpenQuery

    $closed = $total - $open
    $closeRate = if ($total -gt 0) { [math]::Round(($closed / $total) * 100, 1) } else { 0 }

    $k1 = Get-K1Score -OpenCritical $criticalOpen
    $k2 = Get-K2Score -CloseRatePct $closeRate -TotalBugs $total
    $blocked = $criticalOpen -ge 3

    $rows += [PSCustomObject]@{
        Tag = $tag
        Patch = $patch
        ReleaseDate = $from
        Window = $window
        WindowDays = [int]([timespan]($windowEnd - $releaseDate)).TotalDays
        TotalBugs = $total
        OpenBugs = $open
        ClosedBugs = $closed
        CloseRatePct = $closeRate
        OpenCritical = $criticalOpen
        K1 = $k1
        K2 = $k2
        Blocked = $blocked
    }
}

if ($rows.Count -eq 0) {
    throw "No rows collected. Check release dates and AsOfDate."
}

$rows = $rows | Sort-Object Patch

Write-Host ""
Write-Host "Patch metrics for $Major.$Minor.x as of $($asOf.ToString('yyyy-MM-dd'))"
$rows | Format-Table Tag,ReleaseDate,Window,TotalBugs,OpenBugs,OpenCritical,CloseRatePct,K1,K2,Blocked -AutoSize

$rows | Export-Csv -Path $OutputCsv -NoTypeInformation -Encoding UTF8
Write-Host ""
Write-Host "Saved CSV: $OutputCsv"

$recommended = $rows |
    Where-Object { -not $_.Blocked } |
    Sort-Object @{Expression = 'OpenCritical'; Ascending = $true},
                @{Expression = 'OpenBugs'; Ascending = $true},
                @{Expression = 'CloseRatePct'; Ascending = $false},
                @{Expression = 'Patch'; Ascending = $false} |
    Select-Object -First 1

Write-Host ""
if ($recommended) {
    Write-Host "Recommended patch in $Major.$Minor.x: $($recommended.Tag)"
    Write-Host "Reason: min open/critical bug load with highest closure quality among non-blocked patches."
} else {
    Write-Host "No non-blocked patch found in $Major.$Minor.x (all candidates blocked by open critical defects)."
}