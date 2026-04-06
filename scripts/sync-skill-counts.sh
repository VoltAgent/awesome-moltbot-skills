#!/usr/bin/env bash
#
# sync-skill-counts.sh
#
# Counts the actual number of skills (lines matching "^- \[") in each
# category file and updates:
#   1. The "**N skills**" header in each category file
#   2. The "View all N skills" links in README.md
#   3. The badge count in README.md
#   4. The "Discover N+" header text in README.md
#
# Usage: bash scripts/sync-skill-counts.sh
#
# Note: The Table of Contents counts in README.md must be updated manually
# since they use a complex markdown table layout.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CATEGORIES_DIR="$REPO_ROOT/categories"
README="$REPO_ROOT/README.md"

total=0

declare -A DISPLAY_NAMES=(
  ["ai-and-llms"]="AI & LLMs"
  ["apple-apps-and-services"]="Apple Apps & Services"
  ["browser-and-automation"]="Browser & Automation"
  ["calendar-and-scheduling"]="Calendar & Scheduling"
  ["clawdbot-tools"]="Clawdbot Tools"
  ["cli-utilities"]="CLI Utilities"
  ["coding-agents-and-ides"]="Coding Agents & IDEs"
  ["communication"]="Communication"
  ["data-and-analytics"]="Data & Analytics"
  ["devops-and-cloud"]="DevOps & Cloud"
  ["gaming"]="Gaming"
  ["git-and-github"]="Git & GitHub"
  ["health-and-fitness"]="Health & Fitness"
  ["image-and-video-generation"]="Image & Video Generation"
  ["ios-and-macos-development"]="iOS & macOS Development"
  ["marketing-and-sales"]="Marketing & Sales"
  ["media-and-streaming"]="Media & Streaming"
  ["moltbook"]="Moltbook"
  ["notes-and-pkm"]="Notes & PKM"
  ["pdf-and-documents"]="PDF & Documents"
  ["personal-development"]="Personal Development"
  ["productivity-and-tasks"]="Productivity & Tasks"
  ["search-and-research"]="Search & Research"
  ["security-and-passwords"]="Security & Passwords"
  ["self-hosted-and-automation"]="Self-Hosted & Automation"
  ["shopping-and-e-commerce"]="Shopping & E-commerce"
  ["smart-home-and-iot"]="Smart Home & IoT"
  ["speech-and-transcription"]="Speech & Transcription"
  ["transportation"]="Transportation"
  ["web-and-frontend-development"]="Web & Frontend Development"
)

echo "Syncing skill counts..."
echo ""

for file in "$CATEGORIES_DIR"/*.md; do
  basename_no_ext="$(basename "$file" .md)"
  count=$(grep -c "^- \[" "$file")
  total=$((total + count))

  display="${DISPLAY_NAMES[$basename_no_ext]:-$basename_no_ext}"

  # 1. Update the "**N skills**" header in the category file
  sed -i "s/^\*\*[0-9]\+ skills\*\*$/\*\*${count} skills\*\*/" "$file"

  # 2. Update "View all N skills in CategoryName" in README.md
  sed -i "s|View all [0-9]\+ skills in ${display}|View all ${count} skills in ${display}|g" "$README"

  echo "  $display: $count"
done

echo ""
echo "Total skills: $total"

# 3. Update the badge count in README.md
sed -i "s|skills-[0-9]\+-blue|skills-${total}-blue|g" "$README"

# 4. Update the "Discover N+" text in README.md
rounded=$(( (total / 100) * 100 ))
sed -i "s|Discover [0-9]\+\+ community-built|Discover ${rounded}+ community-built|g" "$README"

echo "Updated badge to $total and header to ${rounded}+"
echo "Done."
