param(
    [Parameter(Mandatory = $true)]
    [string]$Version,

    [Parameter(Mandatory = $true)]
    [string]$NotesFile,

    [string]$Repo = "riosbotchen-source/stock-vault-windows-handoff",
    [string]$Target = "main",
    [string]$Title,
    [switch]$Draft,
    [switch]$Latest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$normalizedVersion = if ($Version.StartsWith("v")) { $Version.Substring(1) } else { $Version }

if ($normalizedVersion -notmatch '^\d+\.\d+\.\d+$') {
    throw "Version must look like 1.2.3 or v1.2.3."
}

$tag = "v$normalizedVersion"

if (-not $Title) {
    $Title = "Stock Vault $tag"
}

$resolvedNotesFile = if ([System.IO.Path]::IsPathRooted($NotesFile)) {
    $NotesFile
} else {
    Join-Path $repoRoot $NotesFile
}

$bannerAsset = Join-Path $repoRoot "docs\assets\banner.svg"
$zipAssetName = "stock-vault-$tag-windows-public-handoff.zip"
$zipAssetPath = Join-Path ([System.IO.Path]::GetTempPath()) $zipAssetName

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is required."
}

if (-not (Test-Path -LiteralPath $resolvedNotesFile)) {
    throw "Notes file not found: $resolvedNotesFile"
}

if (-not (Test-Path -LiteralPath $bannerAsset)) {
    throw "Banner asset not found: $bannerAsset"
}

Push-Location $repoRoot
try {
    $workingTree = git status --porcelain
    if ($workingTree) {
        throw "Working tree is not clean. Commit or stash changes before publishing a versioned release."
    }

    & gh release view $tag --repo $Repo 1>$null 2>$null
    if ($LASTEXITCODE -eq 0) {
        throw "Release '$tag' already exists in $Repo."
    }

    if (Test-Path -LiteralPath $zipAssetPath) {
        Remove-Item -LiteralPath $zipAssetPath -Force
    }

    & git archive --format=zip "--output=$zipAssetPath" HEAD
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to build release zip from git archive."
    }

    $releaseArgs = @(
        "release", "create", $tag,
        "$zipAssetPath#Windows public handoff zip",
        "$bannerAsset#Release banner (SVG)",
        "--repo", $Repo,
        "--target", $Target,
        "--title", $Title,
        "--notes-file", $resolvedNotesFile
    )

    if ($Draft) {
        $releaseArgs += "--draft"
    }

    if ($Latest) {
        $releaseArgs += "--latest"
    }

    & gh @releaseArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create GitHub release '$tag'."
    }

    & gh release view $tag --repo $Repo --json name,tagName,url,isDraft,publishedAt
} finally {
    Pop-Location

    if (Test-Path -LiteralPath $zipAssetPath) {
        Remove-Item -LiteralPath $zipAssetPath -Force
    }
}
