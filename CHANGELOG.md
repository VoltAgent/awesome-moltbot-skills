# Changelog

All notable changes to this collection will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- "Recently Added" section in README.md for better visibility of new skills
- Automated link-checking CI workflow (runs weekly with lychee)
- Enhanced PR validation workflow (format checking, duplicate detection, sort order, count sync)
- "Which Burgess Skill Should I Use?" decision guide in burgess/README.md
- CHANGELOG.md for tracking collection changes
- "Back to top" navigation link at the end of README.md

### Fixed
- Garbled "View all" links caused by unescaped `&` in sed replacement strings
- `sync-skill-counts.sh` now escapes `&` in display names for idempotent operation
- "Discover N+" header regex now correctly matches the literal `+` character
- Skill counts reconciled across badge, header, Table of Contents, and "View all" links
- Table of Contents empty cell filled with [Health & Fitness](#health--fitness) count

### Changed
- `sync-skill-counts.sh` now also updates Table of Contents counts (previously manual)
- Table of Contents grid rebalanced to eliminate empty cells
