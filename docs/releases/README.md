# Release Templates

This folder contains reusable GitHub Releases copy templates for this repository.

These templates are designed to keep release messaging:

- clear for public readers
- honest about current Windows-first scope
- consistent about future macOS availability
- easy to reuse for both formal releases and smaller public updates

## Available Templates

- [Traditional Chinese release template](./RELEASE_TEMPLATE.zh-TW.md)
- [English release template](./RELEASE_TEMPLATE.en.md)
- [Initial public handoff draft (Traditional Chinese)](./INITIAL_PUBLIC_HANDOFF.zh-TW.md)
- [Initial public handoff draft (English)](./INITIAL_PUBLIC_HANDOFF.en.md)
- [Initial public handoff draft (Bilingual)](./INITIAL_PUBLIC_HANDOFF.bilingual.md)
- [Versioned release template (Bilingual)](./VERSIONED_RELEASE_TEMPLATE.bilingual.md)

## Suggested Use

Use these templates when publishing:

- a public package update
- a documentation refresh worth announcing
- a Windows-facing release
- a repository milestone that should mention future macOS availability clearly

## Ready-To-Use Draft

The initial public handoff draft files are prewritten release notes that can be pasted into a future GitHub Release with only small edits if you want to publish a first formal announcement later.

## Versioned Release Workflow

For future releases such as `v1.0.0`, `v1.0.1`, or `v1.1.0`, this repository now includes a reusable PowerShell publishing script:

- [publish-versioned-release.ps1](../../scripts/publish-versioned-release.ps1)

Recommended flow:

1. Copy the bilingual template into a version-specific notes file.
2. Replace the placeholders with the real release details.
3. Make sure the git working tree is clean.
4. Run the script to create a draft or published versioned release with:
   - a release tag like `v1.2.3`
   - a public handoff zip asset
   - the repository banner as an asset

Example draft command:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\publish-versioned-release.ps1 `
  -Version 1.0.0 `
  -NotesFile docs/releases/v1.0.0.bilingual.md `
  -Title "Stock Vault v1.0.0" `
  -Draft
```

Example published command:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\publish-versioned-release.ps1 `
  -Version 1.0.0 `
  -NotesFile docs/releases/v1.0.0.bilingual.md `
  -Title "Stock Vault v1.0.0" `
  -Latest
```
