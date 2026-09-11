## [Unreleased]
### Features
- Add opt-in recursive directory mode with grouped sidebar navigation, based
  on [txsmith/mdserve](https://github.com/txsmith/mdserve/commit/fc55605782224441681c77984bd5a093036e9638)

## [1.2.0] - 2026-09-11
### Features
- Set page title from filename (#74)
- Prefer first H1 for page titles
- Add compact MP favicon
### Bug Fixes
- Bypass mtime check when file watcher triggers refresh (#76)
- Update bytes and time to resolve security advisories (#79)
### Refactoring
- Move stray HashMap import to top-level import block (#73)
- Deduplicate file extension extraction in image helpers (#71)
- Remove unused WebSocket message variants (#72)
- Remove redundant mtime check from HTTP handlers (#77)
### Build
- Release this fork on GitHub with Linux and macOS binaries
- Point installation instructions and the Linux installer at this fork
- Remove automatic crates.io publishing

## [1.1.0] - 2026-03-07
### Features
- Auto-increment port when requested port is in use (#64)
### Bug Fixes
- Use kebab-case for marketplace name
- Use native background tasks in mdserve skill
- Allow serving images from subdirectories (#66)
### Documentation
- Add Claude Code plugin installation instructions to README (#62)
### CI
- Enforce conventional commits on PRs (#67)

## [1.0.0] - 2026-02-07
### Features
- Add --open flag to launch browser (#59)
- Add Claude Code plugin metadata and mdserve skill (#60)
### Refactoring
- Convert to binary-only crate (#58)
### Documentation
- Rewrite README for AI agent companion focus
- Add CLAUDE.md for AI agent contributors
- Add changelog workflow to CLAUDE.md
### Build
- Add crates.io publishing to workflow
### Miscellaneous Tasks
- Update crate description to match new focus
- Skip release commits in git-cliff output

## [0.5.1] - 2025-10-28
### Bug Fixes
- Handle temp-file-rename edits in file watcher

## [0.5.0] - 2025-10-23
### Features
- Add directory mode for serving multiple markdown files
- Add YAML and TOML frontmatter support
### Bug Fixes
- Center content in folder mode with sidebar collapsed
- Prevent 404 race during neovim saves
### Refactoring
- Simplify server startup output messages
- Migrate to minijinja template engine
### Documentation
- Update Cargo install instructions
- Update Arch linux install instructions to use official package
- Update README with new folder serving feature
- Add changelog and improve git-cliff config
- Fix changelog duplicate 0.4.1 entry
### Build
- Downgrade to edition 2021 and set MSRV to 1.82.0
- Add git-cliff configuration
### CI
- Run on `aarch64-linux` as well
### Miscellaneous Tasks
- Add package metadata for cargo publish
- Remove macOS support, direct users to Homebrew

## [0.4.1] - 2025-10-04
### Bug Fixes
- Change default hostname to 127.0.0.1 to prevent port conflicts
### Documentation
- Update homebrew install instructions

## [0.4.0] - 2025-10-03
### Features
- Add ETag support for mermaid.min.js
### Refactoring
- Asref avoid clone
- Impl AsRef<Path>
### Documentation
- Add Arch Linux install instructions
### Build
- Optimize and reduce size of release binary (#8)
- Add nix flake packaging
- Update min Rust version to 1.85+ (2024)
- Bundle mermaid.min.js (#10)
- Remove cargo install instructions, add warning about naming conflict
- Add `-H|--hostname` to support listening on non-localhost

## [0.3.0] - 2025-09-27
- Prevent theme flash on page load
- Replace WebSocket content updates with reload signals (#4)
- Add mermaid diagram support (#5)

## [0.2.0] - 2025-09-24
- Add install script and update README
- Add macOS install instructions
- Add image support
- Add screenshot of mdserve serving README.md
- Enable HTML tag rendering in markdown files (#2)

## [0.1.0] - 2025-09-22
