<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 700+ community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 

![Skills Count](https://img.shields.io/badge/skills-700+-blue?style=flat-square)
![Last Update](https://img.shields.io/github/last-commit/VoltAgent/awesome-clawdbot-skills?label=Last%20update&style=flat-square)
[![Discord](https://img.shields.io/discord/1361559153780195478.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://s.voltagent.dev/discord)
[![GitHub forks](https://img.shields.io/github/forks/VoltAgent/awesome-clawdbot-skills?style=social)](https://github.com/VoltAgent/awesome-claude-skills/network/members)
</div>

# Awesome OpenClaw(Moltbot(Clawdbot)) Skills

OpenClaw (previously known as Moltbot, originally Clawdbot... identity crisis included, no extra charge) is a locally-running AI assistant that operates directly on your machine. Skills extend its capabilities, allowing it to interact with external services, automate workflows, and perform specialized tasks. This collection helps you discover and install the right skills for your needs.

Skills in this list are sourced from [OpenClaw](https://clawdhub.com) (OpenClaw's public skills registry) and categorized for easier discovery.

These skills follow the Agent Skill convention develop by Anthropic, an open standard for AI coding assistants.

## Installation

### ClawdHub CLI

> **Note:** As you probably know, they keep renaming things. This reflects the current official docs. We'll update this when they rename it again.

```bash
npx clawdhub@latest install <skill-slug>
```

### Manual Installation

Copy the skill folder to one of these locations:

| Location | Path |
|----------|------|
| Global | `~/.openclaw/skills/` |
| Workspace | `<project>/skills/` |

Priority: Workspace > Local > Bundled


## Table of Contents

- [Web & Frontend Development](#web--frontend-development) (14)
- [Coding Agents & IDEs](#coding-agents--ides) (15)
- [Git & GitHub](#git--github) (9)
- [DevOps & Cloud](#devops--cloud) (42)
- [Browser & Automation](#browser--automation) (11)
- [Image & Video Generation](#image--video-generation) (19)
- [Apple Apps & Services](#apple-apps--services) (14)
- [Search & Research](#search--research) (23)
- [Clawdbot Tools](#clawdbot-tools) (17)
- [CLI Utilities](#cli-utilities) (41)
- [Marketing & Sales](#marketing--sales) (42)
- [Productivity & Tasks](#productivity--tasks) (42)
- [AI & LLMs](#ai--llms) (38)
- [Finance](#finance) (29)
- [Media & Streaming](#media--streaming) (29)
- [Notes & PKM](#notes--pkm) (44)
- [iOS & macOS Development](#ios--macos-development) (13)
- [Transportation](#transportation) (34)
- [Personal Development](#personal-development) (27)
- [Health & Fitness](#health--fitness) (26)
- [Communication](#communication) (26)
- [Speech & Transcription](#speech--transcription) (21)
- [Smart Home & IoT](#smart-home--iot) (31)
- [Shopping & E-commerce](#shopping--e-commerce) (22)
- [Calendar & Scheduling](#calendar--scheduling) (16)
- [PDF & Documents](#pdf--documents) (12)
- [Self-Hosted & Automation](#self-hosted--automation) (11)
- [Security & Passwords](#security--passwords) (6)

<br/>

<a href="https://github.com/VoltAgent/voltagent">
<img width="1390" height="296" alt="social" src="https://github.com/user-attachments/assets/4c40affa-8e20-443a-9ec5-1abb6679b170" />
</a>

<br/>

<details open>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [discord](https://github.com/openclaw/skills/tree/main/skills/steipete/discord/SKILL.md) - Use when you need to control Discord from Clawdbot via the discord tool: send messages, react.
- [frontend-design](https://github.com/openclaw/skills/tree/main/skills/steipete/frontend-design/SKILL.md) - Create distinctive, production-grade frontend interfaces with high design quality.
- [linux-service-triage](https://github.com/openclaw/skills/tree/main/skills/kowl64/linux-service-triage/SKILL.md) - Diagnoses common Linux service issues using logs, systemd/PM2, file permissions.
- [miniflux-news](https://github.com/openclaw/skills/tree/main/skills/hartlco/miniflux-news/SKILL.md) - Fetch and triage the latest unread RSS/news entries from a Miniflux instance.
- [pinak-frontend-guru](https://github.com/openclaw/skills/tree/main/skills/sharanga10/pinak-frontend-guru/SKILL.md) - Expert UI/UX and React performance auditor (PinakBot persona).
- [remotion-best-practices](https://github.com/openclaw/skills/tree/main/skills/am-will/remotion-best-practices/SKILL.md) - Best practices for Remotion - Video creation in React.
- [remotion-server](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/remotion-server/SKILL.md) - Headless video rendering with Remotion. Works on any Linux server - no Mac or GUI needed.
- [slack](https://github.com/openclaw/skills/tree/main/skills/steipete/slack/SKILL.md) - Use when you need to control Slack from Clawdbot via the slack tool.
- [ui-audit](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ui-audit/SKILL.md) - AI skill for automated UI audits. Evaluate interfaces against proven UX principles.
- [ui-skills](https://github.com/openclaw/skills/tree/main/skills/correctroadh/ui-skills/SKILL.md) - Opinionated constraints for building better interfaces with agents.
- [ux-audit](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ux-audit/SKILL.md) - AI skill for automated design audits. Evaluate interfaces against proven UX principles.
- [ux-decisions](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ux-decisions/SKILL.md) - AI skill for the Making UX Decisions framework (uxdecisions.com) by Tommy Geoco.
- [vercel-react-best-practices](https://github.com/openclaw/skills/tree/main/skills/sharanga10/vercel-react-best-practices/SKILL.md) - React and Next.js performance optimization guidelines from Vercel Engineering.
- [web-design-guidelines](https://github.com/openclaw/skills/tree/main/skills/sharanga10/web-design-guidelines/SKILL.md) - Review UI code for Web Interface Guidelines compliance.

</details>

<details open>
<summary><h3 style="display:inline">Coding Agents & IDEs</h3></summary>

- [agentlens](https://github.com/openclaw/skills/tree/main/skills/nguyenphutrong/agentlens/SKILL.md) - Navigate and understand codebases using agentlens hierarchical documentation.
- [claude-team](https://github.com/openclaw/skills/tree/main/skills/jalehman/claude-team/SKILL.md) - Orchestrate multiple Claude Code workers via iTerm2 using the claude-team MCP server.
- [codex-account-switcher](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-account-switcher/SKILL.md) - Manage multiple OpenAI Codex accounts. Capture current login tokens.
- [codex-monitor](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-monitor/SKILL.md) - Browse OpenAI Codex session logs stored in ~/.codex/sessions.
- [codex-orchestration](https://github.com/openclaw/skills/tree/main/skills/shanelindsay/codex-orchestration/SKILL.md) - General-purpose orchestration for Codex.
- [codex-quota](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-quota/SKILL.md) - Check OpenAI Codex CLI rate limit status (daily/weekly quotas) using local session logs.
- [codexmonitor](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codexmonitor/SKILL.md) - List/inspect/watch local OpenAI Codex sessions (CLI + VS Code)
- [coding-agent](https://github.com/openclaw/skills/tree/main/skills/steipete/coding-agent/SKILL.md) - Run Codex CLI, Claude Code, OpenCode, or Pi Coding Agent.
- [cursor-agent](https://github.com/openclaw/skills/tree/main/skills/swiftlysingh/cursor-agent/SKILL.md) - A comprehensive skill for using the Cursor CLI agent.
- [factory-ai](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/factory-ai/SKILL.md) - Use Factory AI's droid CLI for software engineering tasks.
- [model-usage](https://github.com/openclaw/skills/tree/main/skills/steipete/model-usage/SKILL.md) - Use CodexBar CLI local cost usage to summarize per-model usage for Codex.
- [opencode-acp-control](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/opencode-acp-control/SKILL.md) - Control OpenCode directly via the Agent Client Protocol (ACP).
- [perry-coding-agents](https://github.com/openclaw/skills/tree/main/skills/gricha/perry-coding-agents/SKILL.md) - Dispatch coding tasks to OpenCode or Claude Code on Perry workspaces.
- [perry-workspaces](https://github.com/openclaw/skills/tree/main/skills/gricha/perry-workspaces/SKILL.md) - Create and manage isolated Docker workspaces on your tailnet with Claude Code.
- [prompt-log](https://github.com/openclaw/skills/tree/main/skills/thesash/prompt-log/SKILL.md) - Extract conversation transcripts from AI coding session logs (Clawdbot, Claude Code, Codex).

</details>

<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [conventional-commits](https://github.com/openclaw/skills/tree/main/skills/bastos/conventional-commits/SKILL.md) - Format commit messages using the Conventional Commits specification.
- [deepwiki](https://github.com/openclaw/skills/tree/main/skills/arun-8687/deepwiki/SKILL.md) - Query the DeepWiki MCP server for GitHub repository documentation, wiki structure.
- [deepwork-tracker](https://github.com/openclaw/skills/tree/main/skills/adunne09/deepwork-tracker/SKILL.md) - Track deep work sessions locally (start/stop/status)
- [deploy-agent](https://github.com/openclaw/skills/tree/main/skills/sherajdev/deploy-agent/SKILL.md) - Multi-step deployment agent for full-stack apps.
- [github](https://github.com/openclaw/skills/tree/main/skills/steipete/github/SKILL.md) - Interact with GitHub using the `gh` CLI.
- [github-pr](https://github.com/openclaw/skills/tree/main/skills/dbhurley/github-pr/SKILL.md) - Fetch, preview, merge, and test GitHub PRs locally.
- [gitload](https://github.com/openclaw/skills/tree/main/skills/waldekmastykarz/gitload/SKILL.md) - Download files or folders from GitHub without cloning the entire repo.
- [pr-commit-workflow](https://github.com/openclaw/skills/tree/main/skills/joshp123/pr-commit-workflow/SKILL.md) - This skill should be used when creating commits or pull requests.
- [read-github](https://github.com/openclaw/skills/tree/main/skills/am-will/read-github/SKILL.md) - Read GitHub repos via gitmcp.io with semantic search and LLM-optimized output.

</details>

<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [Azure CLI](https://github.com/openclaw/skills/tree/main/skills/ddevaal/azure-cli/SKILL.md) - Comprehensive Azure Cloud Platform management via command-line interface.
- [cloudflare](https://github.com/openclaw/skills/tree/main/skills/asleep123/wrangler/SKILL.md) - Manage Cloudflare Workers, KV, D1, R2, and secrets using the Wrangler CLI.
- [coolify](https://github.com/openclaw/skills/tree/main/skills/visiongeist/coolify/SKILL.md) - Manage Coolify deployments, applications, databases, and services via the Coolify API.
- [digital-ocean](https://github.com/openclaw/skills/tree/main/skills/dbhurley/digital-ocean/SKILL.md) - Manage Digital Ocean droplets, domains, and infrastructure via DO API.
- [dokploy](https://github.com/openclaw/skills/tree/main/skills/joshuarileydev/dokploy/SKILL.md) - Manage Dokploy deployments, projects, applications, and domains via the Dokploy API.
- [domain-dns-ops](https://github.com/openclaw/skills/tree/main/skills/steipete/domain-dns-ops/SKILL.md) - Domain/DNS ops across Cloudflare, DNSimple, and Namecheap.
- [domaindetails](https://github.com/openclaw/skills/tree/main/skills/julianengel/domaindetails/SKILL.md) - Look up domain WHOIS/RDAP info and check marketplace listings. Free API, no auth required.
- [agentns](https://github.com/openclaw/skills/tree/main/skills/voyclawd/agentns/SKILL.md) - Register domains for AI agents via ICANN registrar.
- [exa-plus](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/exa-plus/SKILL.md) - Neural web search via Exa AI. Search people, companies, news, research, code.
- [exe-dev](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/exe-dev/SKILL.md) - Manage persistent VMs on exe.dev. Create VMs, configure HTTP proxies, share access.
- [hetzner](https://github.com/openclaw/skills/tree/main/skills/thesethrose/hetzner/SKILL.md) - Hetzner Cloud server management using the hcloud CLI.
- [hetzner-cloud](https://github.com/openclaw/skills/tree/main/skills/pasogott/hetzner-cloud/SKILL.md) - Hetzner Cloud CLI for managing servers, volumes, firewalls, networks, DNS, and snapshots.
- [Joan Workflow](https://github.com/openclaw/skills/tree/main/skills/donny-son/joan-workflow/SKILL.md) - This skill should be used when the user asks about "joan", "pods", "workspace", "domain knowledge".
- [komodo](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/komodo/SKILL.md) - Manage Komodo infrastructure - servers, Docker deployments, stacks, builds, and procedures.
- [kubectl-skill](https://github.com/openclaw/skills/tree/main/skills/ddevaal/kubectl/SKILL.md) - Execute and manage Kubernetes clusters via kubectl commands.
- [linearis](https://github.com/openclaw/skills/tree/main/skills/whoisnnamdi/linearis/SKILL.md) - Linear.app CLI for issue tracking. Use for listing, creating, updating.
- [npm-proxy](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/npm-proxy/SKILL.md) - Manage Nginx Proxy Manager (NPM) hosts, certificates, and access lists.
- [portainer](https://github.com/openclaw/skills/tree/main/skills/asteinberger/portainer/SKILL.md) - Control Docker containers and stacks via Portainer API.
- [premium-domains](https://github.com/openclaw/skills/tree/main/skills/julianengel/premium-domains/SKILL.md) - Search for premium domains for sale across Afternic, Sedo, Atom, Dynadot, Namecheap, NameSilo.
- [private-connect](https://github.com/openclaw/skills/tree/main/skills/dantelex/private-connect/SKILL.md) - Access private services by name, from anywhere. No VPN or SSH tunnels.
- [proxmox](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/proxmox/SKILL.md) - Manage Proxmox VE clusters via REST API.
- [proxmox-full](https://github.com/openclaw/skills/tree/main/skills/msarheed/proxmox-full/SKILL.md) - Complete Proxmox VE management - create/clone/start/stop VMs.
- [Send Me My Files - R2 upload with short lived signed urls](https://github.com/openclaw/skills/tree/main/skills/julianengel/r2-upload/SKILL.md) - Upload files to Cloudflare R2, AWS S3, or any S3-compatible storage.
- [servicenow-agent](https://github.com/openclaw/skills/tree/main/skills/thesethrose/servicenow-agent/SKILL.md) - Read-only CLI access to ServiceNow Table, Attachment, Aggregate.
- [servicenow-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/servicenow-docs/SKILL.md) - Search and retrieve ServiceNow documentation, release notes.
- [supabase](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/supabase/SKILL.md) - Connect to Supabase for database operations, vector search, and storage.
- [sysadmin-toolbox](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/sysadmin-toolbox/SKILL.md) - Tool discovery and shell one-liner reference for sysadmin, DevOps, and security tasks.
- [tailscale](https://github.com/openclaw/skills/tree/main/skills/jmagar/tailscale/SKILL.md) - Manage Tailscale tailnet via CLI and API.
- [tailscale-serve](https://github.com/openclaw/skills/tree/main/skills/snopoke/tailscale-serve/SKILL.md) - tailscale-serve
- [tavily](https://github.com/openclaw/skills/tree/main/skills/bert-builder/tavily/SKILL.md) - AI-optimized web search using Tavily Search API.
- [unraid](https://github.com/openclaw/skills/tree/main/skills/jmagar/unraid/SKILL.md) - Query and monitor Unraid servers via the GraphQL API.
- [uptime-kuma](https://github.com/openclaw/skills/tree/main/skills/msarheed/uptime-kuma/SKILL.md) - Interact with Uptime Kuma monitoring server.
- [vercel](https://github.com/openclaw/skills/tree/main/skills/thesethrose/vercel/SKILL.md) - Deploy applications and manage projects with complete CLI reference.
- [vercel-deploy](https://github.com/openclaw/skills/tree/main/skills/sharanga10/vercel-deploy-claimable/SKILL.md) - Deploy applications and websites to Vercel.
- [pi-admin](https://github.com/openclaw/skills/tree/main/skills/thesethrose/pi-admin/SKILL.md) - Raspberry Pi system administration. Monitor resources, manage services, perform updates.

- [k8s-skills](https://github.com/openclaw/skills/tree/main/skills/rohitg00) - 6 Kubernetes skills: autoscaling (HPA/VPA/KEDA), backup (Velero), multi-cluster, Cluster API, cert-manager, dashboard browser.
- [kubernetes](https://github.com/openclaw/skills/tree/main/skills/kcns008/kubernetes/SKILL.md) - Comprehensive Kubernetes and OpenShift cluster management.
- [flyio-cli](https://github.com/openclaw/skills/tree/main/skills/justinburdett/flyio-cli/SKILL.md) - Fly.io deploy, logs, SSH, secrets, scaling, and Postgres management.
- [nomad](https://github.com/openclaw/skills/tree/main/skills/danfedick/nomad/SKILL.md) - Query HashiCorp Nomad clusters: jobs, nodes, allocations, services.
- [pm2](https://github.com/openclaw/skills/tree/main/skills/asteinberger/pm2/SKILL.md) - Manage Node.js apps with PM2 process manager.
- [cloudflare](https://github.com/openclaw/skills/tree/main/skills/dbhurley/cloudflare/SKILL.md) - Cloudflare CLI: DNS records, cache purge, Workers routes.
- [tmux-agents](https://github.com/openclaw/skills/tree/main/skills/cuba6112/tmux-agents/SKILL.md) - Manage background coding agents in tmux sessions.

</details>

<details>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [Agent Browser](https://github.com/openclaw/skills/tree/main/skills/thesethrose/agent-browser/SKILL.md) - A fast Rust-based headless browser automation CLI with Node.js fallback that enables AI agents to.
- [browsh](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/browsh/SKILL.md) - A modern text-based browser. Renders web pages in the terminal using headless Firefox.
- [browser-use](https://github.com/openclaw/skills/tree/main/skills/shawnpana/browser-use/SKILL.md) - Cloud-based browser automation with managed sessions and autonomous task execution.
- [context7](https://github.com/openclaw/skills/tree/main/skills/am-will/context7-api/SKILL.md) - |.
- [guru-mcp](https://github.com/openclaw/skills/tree/main/skills/pvoo/guru-mcp/SKILL.md) - Access Guru knowledge base via MCP - ask AI questions, search documents, create drafts.
- [mcporter](https://github.com/openclaw/skills/tree/main/skills/steipete/mcporter/SKILL.md) - Use the mcporter CLI to list, configure, auth, and call MCP servers/tools directly (HTTP.
- [verify-on-browser](https://github.com/openclaw/skills/tree/main/skills/myestery/verify-on-browser/SKILL.md) - Control browser via Chrome DevTools Protocol - full CDP access.
- [playwright-cli](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/playwright-cli/SKILL.md) - Browser automation via Playwright CLI for testing and scraping.
- [tekin](https://github.com/openclaw/skills/tree/main/skills/gwqwghksvq-sketch/tekin/SKILL.md) - Rust-based headless browser CLI with Node.js fallback.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/murphykobe/agent-browser-2/SKILL.md) - Web testing, form filling, screenshots, data extraction.
- [agent-zero](https://github.com/openclaw/skills/tree/main/skills/dowingard/agent-zero-bridge/SKILL.md) - Delegate tasks to Agent Zero autonomous coding framework.

</details>

<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [clinkding](https://github.com/openclaw/skills/tree/main/skills/daveonkels/clinkding/SKILL.md) - Manage linkding bookmarks - save URLs, search, tag, organize.
- [coloring-page](https://github.com/openclaw/skills/tree/main/skills/borahm/coloring-page/SKILL.md) - Turn an uploaded photo into a printable black-and-white coloring page.
- [comfy-cli](https://github.com/openclaw/skills/tree/main/skills/johntheyoung/comfy-cli/SKILL.md) - Install, manage, and run ComfyUI instances.
- [Excalidraw Flowchart](https://github.com/openclaw/skills/tree/main/skills/swiftlysingh/excalidraw-flowchart/SKILL.md) - Create Excalidraw flowcharts from descriptions.
- [gamma](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/gamma/SKILL.md) - Generate AI-powered presentations, documents, and social posts using Gamma.app API.
- [krea-api](https://github.com/openclaw/skills/tree/main/skills/fossilizedcarlos/krea-api/SKILL.md) - Generate images via Krea.ai API (Flux, Imagen, Ideogram, Seedream, etc.).
- [meshy-ai](https://github.com/openclaw/skills/tree/main/skills/sabatesduran/clawdbot-meshyai-skill/SKILL.md) - Use the Meshy.ai REST API to generate assets: (1) text-to-2d (Meshy Text to Image)
- [vap-media](https://clawdhub.com/skills/vap-media) - AI image, video, and music generation via VAP API. Flux, Veo 3.1, Suno V5 with transparent pricing.
- [veo](https://github.com/openclaw/skills/tree/main/skills/buddyh/veo/SKILL.md) - Generate video using Google Veo (Veo 3.1 / Veo 3.0).
- [video-frames](https://github.com/openclaw/skills/tree/main/skills/steipete/video-frames/SKILL.md) - Extract frames or short clips from videos using ffmpeg.
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work. Send build123d commands, receive rendered images.
- [figma](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/figma/SKILL.md) - Figma design analysis, asset export, accessibility audit.
- [venice-ai](https://github.com/openclaw/skills/tree/main/skills/nhannah/venice-ai-media/SKILL.md) - Venice AI: image/video generation, upscaling, AI editing.
- [pollinations](https://github.com/openclaw/skills/tree/main/skills/isaacgounton/pollinations/SKILL.md) - Pollinations.ai: text, images, videos, audio with 25+ models.
- [reve-ai](https://github.com/openclaw/skills/tree/main/skills/dpaluy/reve-ai/SKILL.md) - Reve AI image generation, editing, and remixing.
- [vap-media](https://github.com/openclaw/skills/tree/main/skills/elestirelbilinc-sketch/vap-media/SKILL.md) - AI media generation: Flux, Veo 3.1, Suno V5.
- [gifhorse](https://github.com/openclaw/skills/tree/main/skills/coyote-git/gifhorse/SKILL.md) - Search video dialogue and create reaction GIFs with subtitles.
- [superdesign](https://github.com/openclaw/skills/tree/main/skills/mpociot/superdesign/SKILL.md) - Expert frontend design guidelines for modern UIs.
- [comfyui](https://github.com/openclaw/skills/tree/main/skills/xtopher86/comfyui-request/SKILL.md) - Send workflow requests to ComfyUI and get image results.
- [fal-ai](https://github.com/openclaw/skills/tree/main/skills/agmmnn/fal-ai/SKILL.md) - Generate images, videos and audio using Fal.ai's generative media API.

</details>

<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [apple-contacts](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-contacts/SKILL.md) - Look up contacts from macOS Contacts.app.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-music/SKILL.md) - Search Apple Music, add songs to library, manage playlists, control playback and AirPlay.
- [apple-photos](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-photos/SKILL.md) - Apple Photos.app integration for macOS. List albums, browse photos, search by date/person/content.
- [get-focus-mode](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/get-focus-mode/SKILL.md) - Get the current macOS Focus mode.
- [healthkit-sync](https://github.com/openclaw/skills/tree/main/skills/mneves75/healthkit-sync/SKILL.md) - iOS HealthKit data sync CLI commands and patterns.
- [homebrew](https://github.com/openclaw/skills/tree/main/skills/thesethrose/homebrew/SKILL.md) - Homebrew package manager for macOS. Search, install, manage, and troubleshoot packages and casks.
- [icloud-findmy](https://github.com/openclaw/skills/tree/main/skills/liamnichols/icloud-findmy/SKILL.md) - Query Find My locations and battery status for family devices via iCloud.
- [media-backup](https://github.com/openclaw/skills/tree/main/skills/dbhurley/media-backup/SKILL.md) - Archive Clawdbot conversation media (photos, videos) to a local folder.
- [mole-mac-cleanup](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/mole-mac-cleanup/SKILL.md) - Mac cleanup & optimization tool combining CleanMyMac, AppCleaner, DaisyDisk features.
- [shortcuts-generator](https://github.com/openclaw/skills/tree/main/skills/erik-agens/shortcuts-skill/SKILL.md) - Generate macOS/iOS Shortcuts by creating plist files.
- [apple-remind-me](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/apple-remind-me/SKILL.md) - Natural language reminders via Apple Reminders.app.
- [apple-mail-search](https://github.com/openclaw/skills/tree/main/skills/mneves75/apple-mail-search/SKILL.md) - Fast Apple Mail search via SQLite (~50ms vs 8+ min).
- [alter-actions](https://github.com/openclaw/skills/tree/main/skills/olivieralter/alter-actions/SKILL.md) - Trigger 84+ Alter macOS app actions via x-callback-urls.
- [voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/voice-wake-say/SKILL.md) - Speak responses aloud on macOS via the say command.

</details>

<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [brave-search](https://github.com/openclaw/skills/tree/main/skills/steipete/brave-search/SKILL.md) - Web search and content extraction via Brave Search API.
- [brightdata](https://github.com/openclaw/skills/tree/main/skills/meirkad/bright-data/SKILL.md) - Web scraping and search via Bright Data API.
- [clawdbot-logs](https://github.com/openclaw/skills/tree/main/skills/satriapamudji/clawdbot-logs/SKILL.md) - Analyze Clawdbot logs and diagnostics. Use when the user asks about bot performance.
- [exa](https://github.com/openclaw/skills/tree/main/skills/fardeenxyz/exa/SKILL.md) - Neural web search and code context via Exa AI API. Requires EXA_API_KEY.
- [kagi-search](https://github.com/openclaw/skills/tree/main/skills/silversteez/kagi-search/SKILL.md) - Web search using Kagi Search API. Use when you need to search the web.
- [literature-review](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/literature-review/SKILL.md) - Assistance with writing literature reviews by searching for academic sources.
- [parallel](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/parallel/SKILL.md) - High-accuracy web search and research via Parallel.ai API.
- [seo-audit](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/seo-audit/SKILL.md) - When the user wants to audit, review, or diagnose SEO issues on their site.
- [spots](https://github.com/openclaw/skills/tree/main/skills/foeken/spots/SKILL.md) - Exhaustive Google Places search using grid-based scanning.
- [tavily](https://github.com/openclaw/skills/tree/main/skills/arun-8687/tavily-search/SKILL.md) - AI-optimized web search via Tavily API. Returns concise, relevant results for AI agents.
- [tweet-writer](https://github.com/openclaw/skills/tree/main/skills/sanky369/tweet-writer/SKILL.md) - Write viral, persuasive, engaging tweets and threads.
- [web-search-plus](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/web-search-plus/SKILL.md) - Unified search skill with Intelligent Auto-Routing.
- [seo-analytics](https://github.com/openclaw/skills/tree/main/skills/adamkristopher) - 3 SEO/analytics skills: DataForSEO keywords, GA4 + Search Console, YouTube analytics.
- [last30days](https://github.com/openclaw/skills/tree/main/skills/zats/last30days/SKILL.md) - Research any topic from last 30 days on Reddit, X, and web.
- [youtube-summarizer](https://github.com/openclaw/skills/tree/main/skills/abe238/youtube-summarizer/SKILL.md) - Fetch YouTube transcripts and generate structured summaries.
- [gemini-yt-transcript](https://github.com/openclaw/skills/tree/main/skills/odrobnik/gemini-yt-video-transcript/SKILL.md) - YouTube transcript via Google Gemini with speaker labels.
- [arxiv-watcher](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/arxiv-watcher/SKILL.md) - Search and summarize papers from ArXiv.
- [serpapi](https://github.com/openclaw/skills/tree/main/skills/ianpcook/serpapi/SKILL.md) - Unified search across Google, Amazon, Yelp, OpenTable, Walmart.
- [grok-search](https://github.com/openclaw/skills/tree/main/skills/notabhay/grok-search/SKILL.md) - Search web or X/Twitter using xAI Grok.
- [perplexity](https://github.com/openclaw/skills/tree/main/skills/dronnick/perplexity-bash/SKILL.md) - Perplexity API for web-grounded search with citations.
- [parallel](https://github.com/openclaw/skills/tree/main/skills/pntrivedy/parallel-1-0-1/SKILL.md) - Parallel.ai web search optimized for AI agents.
- [searxng](https://github.com/openclaw/skills/tree/main/skills/abk234/searxng/SKILL.md) - Privacy-respecting metasearch via local SearXNG instance.
- [news-aggregator](https://github.com/openclaw/skills/tree/main/skills/cclank/news-aggregator-skill/SKILL.md) - News from 8 sources: Hacker News, GitHub Trending, Product Hunt, and more.

</details>

<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-browser-clawdbot/SKILL.md) - Headless browser automation CLI optimized for AI agents with accessibility tree snapshots.
- [auto-updater](https://github.com/openclaw/skills/tree/main/skills/maximeprades/auto-updater/SKILL.md) - Automatically update Clawdbot and all installed skills once daily.
- [claude-code-usage](https://github.com/openclaw/skills/tree/main/skills/azaidi94/claude-code-usage/SKILL.md) - Check Claude Code OAuth usage limits (session & weekly quotas).
- [claude-connect](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-connect/SKILL.md) - Connect Claude to Clawdbot instantly and keep it connected 24/7.
- [clawd-modifier](https://github.com/openclaw/skills/tree/main/skills/masonc15/clawd-modifier/SKILL.md) - Modify Clawd, the Claude Code mascot. Use this skill when users want to customize Clawd's.
- [clawdbot-documentation-expert](https://github.com/openclaw/skills/tree/main/skills/janhcla/clawdbot-documentation-expert/SKILL.md) - clawdbot-documentation-expert
- [clawdbot-skill-update](https://github.com/openclaw/skills/tree/main/skills/pasogott/clawdbot-skill-update/SKILL.md) - Comprehensive backup, update, and restore workflow with dynamic workspace detection.
- [clawdbot-workspace-template-review](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-clawdbot-workspace-template-review/SKILL.md) - Compare a Clawdbot workspace against the official templates installed with Clawdbot (npm.
- [clawddocs](https://github.com/openclaw/skills/tree/main/skills/nicholasspisak/clawddocs/SKILL.md) - Clawdbot documentation expert with decision tree navigation, search scripts, doc fetching.
- [clawdhub](https://github.com/openclaw/skills/tree/main/skills/steipete/clawdhub/SKILL.md) - Use the ClawdHub CLI to search, install, update, and publish agent skills from clawdhub.com.
- [clawdlink](https://github.com/openclaw/skills/tree/main/skills/davemorin/clawdlink/SKILL.md) - Encrypted Clawdbot-to-Clawdbot messaging.
- [skills-audit](https://github.com/openclaw/skills/tree/main/skills/morozred/skill-audit/SKILL.md) - Audit locally installed agent skills for security/policy issues.
- [skills-search](https://github.com/openclaw/skills/tree/main/skills/thesethrose/skills-search/SKILL.md) - Search skills.sh registry from CLI. Find and discover agent skills from the skills.sh ecosystem.
- [git-notes-memory](https://github.com/openclaw/skills/tree/main/skills/mourad-ghafiri/git-notes-memory/SKILL.md) - Git-notes based persistent memory across sessions.
- [triple-memory](https://github.com/openclaw/skills/tree/main/skills/ktpriyatham/triple-memory/SKILL.md) - LanceDB + Git-Notes + file-based memory system.
- [self-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/self-reflect/SKILL.md) - Self-improvement through conversation analysis and learning.
- [mcporter-skill](https://github.com/openclaw/skills/tree/main/skills/livvux/mcporter-skill/SKILL.md) - List, configure, auth, and call MCP servers/tools via mcporter CLI.

</details>

<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [bible](https://github.com/openclaw/skills/tree/main/skills/dbhurley/bible-votd/SKILL.md) - Get the Bible.com Verse of the Day with shareable image.
- [camsnap](https://github.com/openclaw/skills/tree/main/skills/steipete/camsnap/SKILL.md) - Capture frames or clips from RTSP/ONVIF cameras.
- [canvas-lms](https://github.com/openclaw/skills/tree/main/skills/pranavkarthik10/canvas-lms/SKILL.md) - Access Canvas LMS (Instructure) for course data, assignments, grades, and submissions.
- [Cat Fact](https://github.com/openclaw/skills/tree/main/skills/thesethrose/catfact/SKILL.md) - Random cat facts and breed information from catfact.ninja (free, no API key).
- [content-advisory](https://github.com/openclaw/skills/tree/main/skills/dbhurley/content-advisory/SKILL.md) - Lookup detailed content ratings for movies and TV shows (sex/nudity, violence/gore.
- [create-cli](https://github.com/openclaw/skills/tree/main/skills/steipete/create-cli/SKILL.md) - Design CLI arguments, flags, subcommands, and help text.
- [data-reconciliation-exceptions](https://github.com/openclaw/skills/tree/main/skills/kowl64/data-reconciliation-exceptions/SKILL.md) - Reconciles data sources using stable identifiers (Pay Number, driving licence, driver card.
- [dilbert](https://github.com/openclaw/skills/tree/main/skills/hjanuschka/dilbert/SKILL.md) - dilbert
- [dropbox](https://github.com/openclaw/skills/tree/main/skills/ryanlisse/dropbox/SKILL.md) - dropbox
- [duckdb-en](https://github.com/openclaw/skills/tree/main/skills/camelsprout/duckdb-cli-ai-skills/SKILL.md) - DuckDB CLI specialist for SQL analysis, data processing and file conversion.
- [entr](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/entr/SKILL.md) - Run arbitrary commands when files change. Useful for watching files and triggering builds or tests.
- [gifgrep](https://github.com/openclaw/skills/tree/main/skills/steipete/gifgrep/SKILL.md) - Search GIF providers with CLI/TUI, download results, and extract stills/sheets.
- [goplaces](https://github.com/openclaw/skills/tree/main/skills/steipete/goplaces/SKILL.md) - Query Google Places API (New) via the goplaces CLI for text search, place details, resolve.
- [journal-to-post](https://github.com/openclaw/skills/tree/main/skills/itsflow/journal-to-post/SKILL.md) - Convert personal journal entries into shareable social media posts.
- [jq](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/jq/SKILL.md) - Command-line JSON processor. Extract, filter, transform JSON.
- [local-places](https://github.com/openclaw/skills/tree/main/skills/steipete/local-places/SKILL.md) - Search for places (restaurants, cafes, etc.) via Google Places API proxy on localhost.
- [native-app-performance](https://github.com/openclaw/skills/tree/main/skills/steipete/native-app-performance/SKILL.md) - Native macOS/iOS app performance profiling via xctrace/Time Profiler.
- [office-quotes](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/office-quotes/SKILL.md) - Generate random quotes from The Office (US).
- [parcel-package-tracking](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/parcel-package-tracking/SKILL.md) - Track and add deliveries via Parcel API.
- [peekaboo](https://github.com/openclaw/skills/tree/main/skills/steipete/peekaboo/SKILL.md) - Capture and automate macOS UI with the Peekaboo CLI.
- [portable-tools](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/portable-tools/SKILL.md) - Build cross-device tools without hardcoding paths or account names.
- [post-at](https://github.com/openclaw/skills/tree/main/skills/krausefx/post-at/SKILL.md) - Manage Austrian Post (post.at) deliveries - list packages, check delivery status.
- [process-watch](https://github.com/openclaw/skills/tree/main/skills/dbhurley/process-watch/SKILL.md) - Monitor system processes - CPU, memory, disk I/O, network, open files, ports.
- [sag](https://github.com/openclaw/skills/tree/main/skills/steipete/sag/SKILL.md) - ElevenLabs text-to-speech with mac-style say UX.
- [shorten](https://github.com/openclaw/skills/tree/main/skills/kesslerio/shorten/SKILL.md) - Shorten URLs using is.gd (no auth required). Returns a permanent short link.
- [simple-backup](https://github.com/openclaw/skills/tree/main/skills/vacinc/simple-backup/SKILL.md) - Backup agent brain (workspace) and body (state) to local folder.
- [smalltalk](https://github.com/openclaw/skills/tree/main/skills/johnmci/smalltalk/SKILL.md) - Interact with live Smalltalk image (Cuis or Squeak).
- [songsee](https://github.com/openclaw/skills/tree/main/skills/steipete/songsee/SKILL.md) - Generate spectrograms and feature-panel visualizations from audio with the songsee CLI.
- [steam](https://github.com/openclaw/skills/tree/main/skills/mjrussell/steam/SKILL.md) - Browse, filter, and discover games in a Steam library.
- [sudoku](https://github.com/openclaw/skills/tree/main/skills/odrobnik/sudoku/SKILL.md) - Fetch Sudoku puzzles and store them as JSON in the workspace; render images on demand; reveal.
- [tldr](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/tldr/SKILL.md) - Simplified man pages from tldr-pages. Use this to quickly understand CLI tools.
- [tmdb](https://github.com/openclaw/skills/tree/main/skills/dbhurley/tmdb/SKILL.md) - Search movies/TV, get cast, ratings, streaming info, and personalized recommendations via TMDb API.
- [tmux](https://github.com/openclaw/skills/tree/main/skills/steipete/tmux/SKILL.md) - Remote-control tmux sessions for interactive CLIs by sending keystrokes and scraping pane output.
- [track17](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/track17/SKILL.md) - Track parcels via the 17TRACK API (local SQLite DB, polling + optional webhook ingestion).
- [units](https://github.com/openclaw/skills/tree/main/skills/asleep123/units/SKILL.md) - Perform unit conversions and calculations using GNU Units.
- [voicenotes](https://github.com/openclaw/skills/tree/main/skills/shawnhansen/voicenotes/SKILL.md) - Sync and access voice notes from Voicenotes.com.
- [xkcd](https://github.com/openclaw/skills/tree/main/skills/dbhurley/xkcd/SKILL.md) - Fetch xkcd comics - latest, random, by number, or search by keyword.
- [ecto](https://github.com/openclaw/skills/tree/main/skills/visionik/ecto/SKILL.md) - Ghost.io blog management via Admin API.
- [domain](https://github.com/openclaw/skills/tree/main/skills/abtdomain/domain/SKILL.md) - Domain intelligence: NRDS keyword search and NS reverse lookup.
- [george](https://github.com/openclaw/skills/tree/main/skills/odrobnik/george/SKILL.md) - George online banking automation (Erste Bank / Sparkasse Austria).
- [emredoganer-fizzy](https://github.com/openclaw/skills/tree/main/skills/emredoganer/emredoganer-fizzy-cli/SKILL.md) - Manage Fizzy Kanban boards and cards via CLI.

</details>

<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [apple-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/apple-docs/SKILL.md) - Query Apple Developer Documentation, APIs, and WWDC videos (2014-2025).
- [apple-docs-mcp](https://github.com/openclaw/skills/tree/main/skills/janhcla/apple-docs-mcp/SKILL.md) - apple-docs-mcp
- [instruments-profiling](https://github.com/openclaw/skills/tree/main/skills/steipete/instruments-profiling/SKILL.md) - Use when profiling native macOS or iOS apps with Instruments/xctrace.
- [ios-simulator](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/ios-simulator/SKILL.md) - Automate iOS Simulator workflows (simctl + idb): create/boot/erase devices, install/launch apps.
- [macos-spm-app-packaging](https://github.com/openclaw/skills/tree/main/skills/dimillian/macos-spm-app-packaging/SKILL.md) - Scaffold, build, and package SwiftPM-based macOS apps without an Xcode project.
- [PagerKit](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/pagerkit/SKILL.md) - Expert guidance on PagerKit, a SwiftUI library for advanced, customizable page-based navigation.
- [sfsymbol-generator](https://github.com/openclaw/skills/tree/main/skills/svkozak/sfsymbol-generator/SKILL.md) - Generate an Xcode SF Symbol asset catalog .symbolset from an SVG.
- [swift-concurrency-expert](https://github.com/openclaw/skills/tree/main/skills/steipete/swift-concurrency-expert/SKILL.md) - Swift Concurrency review and remediation for Swift 6.2+.
- [swiftui-empty-app-init](https://github.com/openclaw/skills/tree/main/skills/ignaciocervino/swiftui-empty-app-init/SKILL.md) - Initialize a minimal SwiftUI iOS app in the current directory by generating a single `.xcodeproj`.
- [swiftui-liquid-glass](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-liquid-glass/SKILL.md) - Implement, review, or improve SwiftUI features using the iOS 26+ Liquid Glass API.
- [swiftui-performance-audit](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-performance-audit/SKILL.md) - Audit and improve SwiftUI runtime performance from code review and architecture.
- [swiftui-ui-patterns](https://github.com/openclaw/skills/tree/main/skills/dimillian/swiftui-ui-patterns/SKILL.md) - Best practices and example-driven guidance for building SwiftUI views and components.
- [swiftui-view-refactor](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-view-refactor/SKILL.md) - Refactor and review SwiftUI view files for consistent structure, dependency injection.

</details>

<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [ab-test-setup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/ab-test-setup/SKILL.md) - When the user wants to plan, design, or implement an A/B test or experiment.
- [apollo](https://github.com/openclaw/skills/tree/main/skills/jhumanj/apollo/SKILL.md) - Interact with Apollo.io REST API (people/org enrichment, search, lists).
- [basecamp-cli](https://github.com/openclaw/skills/tree/main/skills/emredoganer/basecamp-cli/SKILL.md) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects, to-dos, messages.
- [bearblog](https://github.com/openclaw/skills/tree/main/skills/azade-c/bearblog/SKILL.md) - Create and manage blog posts on Bear Blog (bearblog.dev).
- [bird](https://github.com/openclaw/skills/tree/main/skills/steipete/bird/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics.
- [bluesky](https://github.com/openclaw/skills/tree/main/skills/jeffaf/bluesky/SKILL.md) - Read, post, and interact with Bluesky (AT Protocol) via CLI.
- [competitor-alternatives](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/competitor-alternatives/SKILL.md) - When the user wants to create competitor comparison or alternative pages for SEO.
- [copy-editing](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/copy-editing/SKILL.md) - When the user wants to edit, review, or improve existing marketing copy.
- [copywriting](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/copywriting/SKILL.md) - When the user wants to write, rewrite, or improve marketing copy.
- [email-sequence](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/email-sequence/SKILL.md) - When the user wants to create or optimize an email sequence, drip campaign, automated email flow.
- [form-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/form-cro/SKILL.md) - When the user wants to optimize any form that is NOT signup/registration — including lead capture.
- [free-tool-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/free-tool-strategy/SKILL.md) - When the user wants to plan, evaluate, or build a free tool.
- [ga4](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/ga4/SKILL.md) - Query Google Analytics 4 (GA4) data via the Analytics Data API.
- [gong](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/gong/SKILL.md) - Gong API for searching calls, transcripts, and conversation intelligence.
- [google-ads](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/google-ads/SKILL.md) - Query, audit, and optimize Google Ads campaigns.
- [gsc](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/gsc/SKILL.md) - Query Google Search Console for SEO data - search queries, top pages, CTR opportunities.
- [hubspot](https://github.com/openclaw/skills/tree/main/skills/kwall1/hubspot/SKILL.md) - HubSpot CRM and CMS API integration for contacts, companies, deals, owners, and content management.
- [humanizer](https://github.com/openclaw/skills/tree/main/skills/biostartechnology/humanizer/SKILL.md) - |.
- [marketing-mode](https://github.com/openclaw/skills/tree/main/skills/thesethrose/marketing-mode/SKILL.md) - Marketing Mode combines 23 comprehensive marketing skills covering strategy, psychology, content.
- [marketing-psychology](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-psychology/SKILL.md) - When the user wants to apply psychological principles, mental models.
- [marketing-skills](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/SKILL.md) - TL;DR: 23 marketing playbooks (CRO, SEO, copy, analytics, experiments, pricing, launches, ads.
- [otter](https://github.com/openclaw/skills/tree/main/skills/dbhurley/otter/SKILL.md) - Otter.ai transcription CLI - list, search, download, and sync meeting transcripts to CRM.
- [paywall-upgrade-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/paywall-upgrade-cro/SKILL.md) - When the user wants to create or optimize in-app paywalls, upgrade screens, upsell modals.
- [pinch-to-post](https://github.com/openclaw/skills/tree/main/skills/nickhamze/pinch-to-post/SKILL.md) - WordPress automation for Clawdbot. Manage posts, pages, WooCommerce products, orders, inventory.
- [popup-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/popup-cro/SKILL.md) - When the user wants to create or optimize popups, modals, overlays, slide-ins.
- [pricing-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/pricing-strategy/SKILL.md) - When the user wants help with pricing decisions, packaging, or monetization strategy.
- [programmatic-seo](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/programmatic-seo/SKILL.md) - When the user wants to create SEO-driven pages at scale using templates and data.
- [reddit](https://github.com/openclaw/skills/tree/main/skills/theglove44/reddit/SKILL.md) - Browse, search, post, and moderate Reddit.
- [reddit-search](https://github.com/openclaw/skills/tree/main/skills/thesethrose/reddit-search/SKILL.md) - Search Reddit for subreddits and get information about them.
- [referral-program](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/referral-program/SKILL.md) - When the user wants to create, optimize, or analyze a referral program, affiliate program.
- [schema-markup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/schema-markup/SKILL.md) - When the user wants to add, fix, or optimize schema markup and structured data on their site.
- [signup-flow-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/signup-flow-cro/SKILL.md) - When the user wants to optimize signup, registration, account creation, or trial activation flows.
- [solobuddy](https://github.com/openclaw/skills/tree/main/skills/humanji7/solobuddy/SKILL.md) - Build-in-public companion for indie hackers — content workflow, Twitter engagement.
- [twenty-crm](https://github.com/openclaw/skills/tree/main/skills/jhumanj/twenty-crm/SKILL.md) - Interact with Twenty CRM (self-hosted) via REST/GraphQL.
- [typefully](https://github.com/openclaw/skills/tree/main/skills/thesethrose/typefully/SKILL.md) - |.
- [x-article-editor](https://github.com/openclaw/skills/tree/main/skills/jchopard69/x-article-editor/SKILL.md) - TL;DR: Turn a topic or draft into a high-engagement X Article. STEP 1 final copy/paste article.
- [shashwatgtm-skills](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm) - B2B marketing: competitive intelligence, content writing, newsletters, personal branding, social media management.
- [sales-toolkit](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker) - Sales toolkit: Apollo.io enrichment, Attio CRM, PhantomBuster automation, Firecrawl web scraping.
- [octolens](https://github.com/openclaw/skills/tree/main/skills/garrrikkotua/octolens/SKILL.md) - Brand mention tracking across Twitter, Reddit, GitHub, LinkedIn with sentiment analysis.
- [geo-optimization](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/geo-optimization/SKILL.md) - Optimize content for AI search visibility (ChatGPT, Perplexity, Claude).
- [recruitment-automation](https://github.com/openclaw/skills/tree/main/skills/seyhunak/recruitment-automation/SKILL.md) - Automated recruitment: job specs, candidate evaluation, outreach emails.
- [late-api](https://github.com/openclaw/skills/tree/main/skills/mikipalet/late-api/SKILL.md) - Late API for scheduling posts across 13 social media platforms.

</details>

<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [clawd-docs-v2](https://github.com/openclaw/skills/tree/main/skills/aranej/clawd-docs-v2/SKILL.md) - Smart ClawdBot documentation access with local search index, cached snippets, and on-demand fetch.
- [clickup-mcp](https://github.com/openclaw/skills/tree/main/skills/pvoo/clickup-mcp/SKILL.md) - Manage ClickUp tasks, docs, time tracking, comments, chat, and search via official MCP.
- [dex](https://github.com/openclaw/skills/tree/main/skills/gricha/dex/SKILL.md) - Task tracking for async/multi-step work.
- [dexcom](https://github.com/openclaw/skills/tree/main/skills/chris-clem/dexcom/SKILL.md) - Monitor blood glucose via Dexcom G7/G6 CGM.
- [dexter](https://github.com/openclaw/skills/tree/main/skills/igorhvr/dexter/SKILL.md) - Autonomous financial research agent for stock analysis, financial statements, metrics, prices.
- [dvsa-tc-audit-readiness-operator-licence-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/dvsa-tc-audit-readiness-operator-licence-uk/SKILL.md) - Builds DVSA/Traffic Commissioner “show me” audit readiness checklists and evidence indexes.
- [gno](https://github.com/openclaw/skills/tree/main/skills/gmickel/gno/SKILL.md) - Search local documents, files, notes, and knowledge bases.
- [jira](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/jira/SKILL.md) - Manage Jira issues, boards, sprints, and projects via the jira-cli.
- [linear](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/linear/SKILL.md) - Query and manage Linear issues, projects, and team workflows.
- [morning-manifesto](https://github.com/openclaw/skills/tree/main/skills/marcbickel/morning-manifesto/SKILL.md) - Daily morning reflection workflow with task sync to Obsidian, Apple Reminders, and Linear.
- [plan-my-day](https://github.com/openclaw/skills/tree/main/skills/itsflow/plan-my-day/SKILL.md) - Generate an energy-optimized, time-blocked daily plan.
- [prd](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/prd/SKILL.md) - Create and manage Product Requirements Documents (PRDs).
- [prowlarr](https://github.com/openclaw/skills/tree/main/skills/jmagar/prowlarr/SKILL.md) - Search indexers and manage Prowlarr. Use when the user asks to "search.
- [qmd](https://github.com/openclaw/skills/tree/main/skills/steipete/qmd/SKILL.md) - Local search/indexing CLI (BM25 + vectors + rerank) with MCP mode.
- [samsung-smart-tv](https://github.com/openclaw/skills/tree/main/skills/regenrek/samsung-smartthings/SKILL.md) - Control Samsung TVs via SmartThings (OAuth app + device control).
- [task](https://github.com/openclaw/skills/tree/main/skills/amirbrooks/task/SKILL.md) - Tasker docstore task management via tool-dispatch.
- [task-tracker](https://github.com/openclaw/skills/tree/main/skills/kesslerio/task-tracker/SKILL.md) - Personal task management with daily standups and weekly reviews.
- [things-mac](https://github.com/openclaw/skills/tree/main/skills/steipete/things-mac/SKILL.md) - Manage Things 3 via the `things` CLI on macOS (add/update projects+todos.
- [ticktick](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/ticktick/SKILL.md) - Manage TickTick tasks and projects from the command line with OAuth2 auth, batch operations.
- [timesheet](https://github.com/openclaw/skills/tree/main/skills/florianrauscha/timesheet/SKILL.md) - Track time, manage projects and tasks using timesheet.io CLI.
- [todo-tracker](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/todo-tracker/SKILL.md) - Persistent TODO scratch pad for tracking tasks across sessions.
- [todoist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/todoist/SKILL.md) - Manage tasks and projects in Todoist. Use when user asks about tasks, to-dos, reminders.
- [topydo](https://github.com/openclaw/skills/tree/main/skills/bastos/topydo/SKILL.md) - Manage todo.txt tasks using topydo CLI. Add, list, complete, prioritize, tag.
- [trello](https://github.com/openclaw/skills/tree/main/skills/steipete/trello/SKILL.md) - Manage Trello boards, lists, and cards via the Trello REST API.
- [vikunja](https://github.com/openclaw/skills/tree/main/skills/dbhurley/vikunja/SKILL.md) - Manage projects and tasks in Vikunja, an open-source project management tool.
- [vikunja-fast](https://github.com/openclaw/skills/tree/main/skills/tmigone/vikunja-fast/SKILL.md) - Manage Vikunja projects and tasks (overdue/due/today), mark done.
- [web-perf](https://github.com/openclaw/skills/tree/main/skills/elithrar/web-perf/SKILL.md) - Analyzes web performance using Chrome DevTools MCP.
- [withings-health](https://github.com/openclaw/skills/tree/main/skills/hisxo/withings-health/SKILL.md) - Fetches health data from the Withings API including weight, body composition (fat, muscle, bone.
- [outlook](https://github.com/openclaw/skills/tree/main/skills/jotamed/outlook/SKILL.md) - Outlook email and calendar via Microsoft Graph API.
- [imap-email](https://github.com/openclaw/skills/tree/main/skills/mvarrieur/imap-email/SKILL.md) - Read and manage email via IMAP (ProtonMail, Gmail, etc.).
- [google-workspace](https://github.com/openclaw/skills/tree/main/skills/dru-ca/google-workspace-mcp/SKILL.md) - Gmail, Calendar, Drive with simple OAuth sign-in.
- [gogcli](https://github.com/openclaw/skills/tree/main/skills/luccast/gogcli/SKILL.md) - Google Workspace CLI: Gmail, Calendar, Drive, Sheets, Docs, Slides.
- [ticktick](https://github.com/openclaw/skills/tree/main/skills/kaiofreitas/ticktick-tasks/SKILL.md) - TickTick task manager: projects, tasks, reminders.
- [omnifocus](https://github.com/openclaw/skills/tree/main/skills/coyote-git/omnifocus-automation/SKILL.md) - OmniFocus tasks, projects, and GTD workflows via Omni Automation.
- [todoist](https://github.com/openclaw/skills/tree/main/skills/2mawi2/todoist-task-manager/SKILL.md) - Todoist CLI: list, add, modify, complete, delete tasks.
- [taskleef](https://github.com/openclaw/skills/tree/main/skills/xatter/taskleef/SKILL.md) - Taskleef.com todos, projects, and kanban boards.
- [linear-issues](https://github.com/openclaw/skills/tree/main/skills/emrekilinc/linear-issues/SKILL.md) - Linear issue tracking: create, update, list, search issues.
- [jira](https://github.com/openclaw/skills/tree/main/skills/kyjus25/clawdbot-jira-skill/SKILL.md) - Jira issues, transitions, and worklogs via REST API.
- [atlassian-mcp](https://github.com/openclaw/skills/tree/main/skills/atakanermis/atlassian-mcp/SKILL.md) - Atlassian MCP: Jira and Confluence integration via Docker.
- [asana](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/asana/SKILL.md) - Asana tasks, projects, and workspaces via REST API.
- [mogcli](https://github.com/openclaw/skills/tree/main/skills/visionik/mogcli/SKILL.md) - Microsoft 365 CLI: Mail, Calendar, Drive, Word, Excel, OneNote.


</details>

<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [antigravity-quota](https://github.com/openclaw/skills/tree/main/skills/mukhtharcm/antigravity-quota/SKILL.md) - Check Antigravity account quotas for Claude and Gemini models.
- [ask-questions-if-underspecified](https://github.com/openclaw/skills/tree/main/skills/lc0rp/ask-questions-if-underspecified/SKILL.md) - Clarify requirements before implementing. Do not use automatically, only when invoked explicitly.
- [claude-oauth-refresher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-oauth-refresher/SKILL.md) - Keep your Claude access token fresh 24/7.
- [council](https://github.com/openclaw/skills/tree/main/skills/emasoudy/council/SKILL.md) - Council Chamber orchestration with Memory Bridge.
- [de-ai-ify](https://github.com/openclaw/skills/tree/main/skills/itsflow/de-ai-ify/SKILL.md) - Remove AI-generated jargon and restore human voice to text.
- [gemini](https://github.com/openclaw/skills/tree/main/skills/steipete/gemini/SKILL.md) - Gemini CLI for one-shot Q&A, summaries, and generation.
- [gemini-computer-use](https://github.com/openclaw/skills/tree/main/skills/am-will/gemini-computer-use/SKILL.md) - Build and run Gemini 2.5 Computer Use browser-control agents with Playwright.
- [gemini-deep-research](https://github.com/openclaw/skills/tree/main/skills/arun-8687/gemini-deep-research/SKILL.md) - Perform complex, long-running research tasks using Gemini Deep Research Agent.
- [gemini-stt](https://github.com/openclaw/skills/tree/main/skills/araa47/gemini-stt/SKILL.md) - Transcribe audio files using Google's Gemini API or Vertex AI.
- [llm-council](https://github.com/openclaw/skills/tree/main/skills/am-will/llm-council/SKILL.md) - Orchestrate multi-LLM councils to produce and merge implementation plans.
- [lmstudio-subagents](https://github.com/openclaw/skills/tree/main/skills/t-sinclair2500/lm-studio-subagents/SKILL.md) - Equips agents to search for and offload tasks to local models in LM Studio.
- [minimax-usage](https://github.com/openclaw/skills/tree/main/skills/thesethrose/minimax-usage/SKILL.md) - Monitor Minimax Coding Plan usage to stay within API limits.
- [model-router](https://github.com/openclaw/skills/tree/main/skills/digitaladaption/model-router/SKILL.md) - A comprehensive AI model routing system that automatically selects the optimal model for any task.
- [nano-banana-pro](https://github.com/openclaw/skills/tree/main/skills/steipete/nano-banana-pro/SKILL.md) - Generate/edit images with Nano Banana Pro (Gemini 3 Pro Image).
- [openai-docs-skill](https://github.com/openclaw/skills/tree/main/skills/am-will/openai-docs/SKILL.md) - Query the OpenAI developer documentation via the OpenAI Docs MCP server using CLI (curl/jq).
- [openai-image-gen](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-image-gen/SKILL.md) - Batch-generate images via OpenAI Images API. Random prompt sampler + `index.html` gallery.
- [openai-tts](https://github.com/openclaw/skills/tree/main/skills/pors/openai-tts/SKILL.md) - Text-to-speech via OpenAI Audio Speech API.
- [openrouter-transcribe](https://github.com/openclaw/skills/tree/main/skills/obviyus/openrouter-transcribe/SKILL.md) - Transcribe audio files via OpenRouter using audio-capable models (Gemini, GPT-4o-audio, etc).
- [oracle](https://github.com/openclaw/skills/tree/main/skills/steipete/oracle/SKILL.md) - Use the @steipete/oracle CLI to bundle a prompt plus the right files.
- [perplexity](https://github.com/openclaw/skills/tree/main/skills/zats/perplexity/SKILL.md) - Search the web with AI-powered answers via Perplexity API.
- [personas](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/personas/SKILL.md) - Transform into 31 specialized AI personalities on demand.
- [pi-orchestration](https://github.com/openclaw/skills/tree/main/skills/dbhurley/pi-orchestration/SKILL.md) - Orchestrate multiple AI models (GLM, MiniMax, etc.) as workers using Pi Coding Agent.
- [recipe-to-list](https://github.com/openclaw/skills/tree/main/skills/borahm/recipe-to-list/SKILL.md) - Turn recipes into a Todoist Shopping list.
- [research](https://github.com/openclaw/skills/tree/main/skills/pors/research/SKILL.md) - Deep research via Gemini CLI — runs in background sub-agent so you don't burn your Claude tokens.
- [screen-monitor](https://github.com/openclaw/skills/tree/main/skills/emasoudy/screen-monitor/SKILL.md) - Dual-mode screen sharing and analysis. Model-agnostic (Gemini/Claude/Qwen3-VL).
- [search-x](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/search-x/SKILL.md) - Search X/Twitter in real-time using Grok. Find tweets, trends, and discussions with citations.
- [self-improvement](https://github.com/openclaw/skills/tree/main/skills/pskoett/self-improving-agent/SKILL.md) - Captures learnings, errors, and corrections to enable continuous improvement.
- [smart-followups](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/smart-followups/SKILL.md) - Generate contextual follow-up suggestions after AI responses.
- [xai](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/xai/SKILL.md) - Chat with Grok models via xAI API. Supports Grok-3, Grok-3-mini, vision, and more.
- [manus](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/manus/SKILL.md) - Create and manage AI agent tasks via Manus API.
- [hokipoki](https://github.com/openclaw/skills/tree/main/skills/budjoskop/hokipoki/SKILL.md) - Switch between Claude, Codex, and Gemini when one gets stuck.
- [council-of-the-wise](https://github.com/openclaw/skills/tree/main/skills/jeffaf/council-of-the-wise/SKILL.md) - Multi-perspective feedback from spawned sub-agent personas.
- [multi-viewpoint-debates](https://github.com/openclaw/skills/tree/main/skills/latentfreedom/multi-viewpoint-debates/SKILL.md) - Debate decisions from multiple worldviews to expose blind spots.
- [chaos-lab](https://github.com/openclaw/skills/tree/main/skills/jbbottoms/chaos-lab/SKILL.md) - AI alignment exploration through conflicting optimization targets.
- [ralph-loop](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/ralph-loop/SKILL.md) - Generate bash scripts for AI agent loops (Codex, Claude, OpenCode).
- [research-tracker](https://github.com/openclaw/skills/tree/main/skills/julian1645/research-tracker/SKILL.md) - Manage autonomous AI research agents with SQLite tracking.
- [claude-code-wingman](https://github.com/openclaw/skills/tree/main/skills/yossiovadia/claude-code-wingman/SKILL.md) - Run Claude Code as a skill, control from WhatsApp.
- [adversarial-prompting](https://github.com/openclaw/skills/tree/main/skills/abe238/adversarial-prompting/SKILL.md) - Adversarial analysis to critique, fix, and consolidate solutions.

</details>

<details>
<summary><h3 style="display:inline">Finance</h3></summary>

- [analytics-tracking](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/analytics-tracking/SKILL.md) - When the user wants to set up, improve, or audit analytics tracking and measurement.
- [api-credentials-hygiene](https://github.com/openclaw/skills/tree/main/skills/kowl64/api-credentials-hygiene/SKILL.md) - Audits and hardens API credential handling (env vars, separation, rotation plan, least privilege.
- [app-store-changelog](https://github.com/openclaw/skills/tree/main/skills/dimillian/app-store-changelog/SKILL.md) - Create user-facing App Store release notes by collecting.
- [clawdbot-release-check](https://github.com/openclaw/skills/tree/main/skills/pors/clawdbot-release-check/SKILL.md) - Check for new clawdbot releases and notify once per new version.
- [copilot-money](https://github.com/openclaw/skills/tree/main/skills/jayhickey/copilot-money/SKILL.md) - Query Copilot Money personal finance data (accounts, transactions, net worth, holdings.
- [create-content](https://github.com/openclaw/skills/tree/main/skills/itsflow/create-content/SKILL.md) - Thinking partner that transforms ideas into platform-optimized content.
- [harvey](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/harvey/SKILL.md) - Harvey is an imaginary friend and conversation companion.
- [ibkr-trading](https://github.com/openclaw/skills/tree/main/skills/flokiew/ibkr-trader/SKILL.md) - Interactive Brokers (IBKR) trading automation via Client Portal API.
- [idea](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/idea/SKILL.md) - Launch background Claude sessions to explore and analyze business ideas.
- [just-fucking-cancel](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/just-fucking-cancel/SKILL.md) - Analyze bank transaction CSVs to find recurring charges, categorize subscriptions.
- [launch-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/launch-strategy/SKILL.md) - When the user wants to plan a product launch, feature announcement, or release strategy.
- [marketing-ideas](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-ideas/SKILL.md) - When the user needs marketing ideas, inspiration, or strategies for their SaaS or software product.
- [nordpool-fi](https://github.com/openclaw/skills/tree/main/skills/ovaris/nordpool-fi/SKILL.md) - Hourly electricity prices for Finland with optimal EV charging window calculation (3h, 4h, 5h).
- [openssl](https://github.com/openclaw/skills/tree/main/skills/asleep123/openssl/SKILL.md) - Generate secure random strings, passwords, and cryptographic tokens using OpenSSL.
- [page-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/page-cro/SKILL.md) - When the user wants to optimize, improve, or increase conversions on any marketing page — including.
- [plaid](https://github.com/openclaw/skills/tree/main/skills/jverdi/plaid/SKILL.md) - plaid-cli a cli for interacting with the plaid finance platform.
- [publisher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/publisher/SKILL.md) - Make your skills easy to understand and impossible to ignore.
- [relationship-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8/relationship-skills/SKILL.md) - Improve relationships with communication tools, conflict resolution, and connection ideas.
- [solo-cli](https://github.com/openclaw/skills/tree/main/skills/rursache/solo-cli/SKILL.md) - Monitor and interact with SOLO.ro accounting platform via CLI.
- [swissweather](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swissweather/SKILL.md) - Get current weather and forecasts from MeteoSwiss (official Swiss weather service).
- [yahoo-finance](https://github.com/openclaw/skills/tree/main/skills/ajanraj/yahoo-finance/SKILL.md) - Get stock prices, quotes, fundamentals, earnings, options, dividends.
- [ynab](https://github.com/openclaw/skills/tree/main/skills/obviyus/ynab/SKILL.md) - Manage YNAB budgets, accounts, categories, and transactions via CLI.
- [monarch-money](https://github.com/openclaw/skills/tree/main/skills/davideasaf/monarch-money/SKILL.md) - Monarch Money budget management and transaction tracking.
- [tax-professional](https://github.com/openclaw/skills/tree/main/skills/scottfo/tax-professional/SKILL.md) - US tax advisor, deduction optimizer, and expense tracker.
- [card-optimizer](https://github.com/openclaw/skills/tree/main/skills/scottfo/card-optimizer/SKILL.md) - Credit card rewards optimizer for cashback, points, and miles.
- [watch-my-money](https://github.com/openclaw/skills/tree/main/skills/andreolf/watch-my-money/SKILL.md) - Analyze bank transactions, categorize spending, track budgets.
- [refund-radar](https://github.com/openclaw/skills/tree/main/skills/andreolf/refund-radar/SKILL.md) - Scan bank statements for recurring charges and draft refund requests.
- [expense-tracker-pro](https://github.com/openclaw/skills/tree/main/skills/jhillin8/expense-tracker-pro/SKILL.md) - Track expenses via natural language with budget summaries.
- [financial-market-analysis](https://github.com/openclaw/skills/tree/main/skills/seyhunak/financial-market-analysis/SKILL.md) - Stock and market sentiment analysis via Yahoo Finance.

</details>

<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [apple-media](https://github.com/openclaw/skills/tree/main/skills/aaronn/apple-media/SKILL.md) - Control Apple TV, HomePod, and AirPlay devices via pyatv (scan, stream, playback, volume.
- [blucli](https://github.com/openclaw/skills/tree/main/skills/steipete/blucli/SKILL.md) - BluOS CLI (blu) for discovery, playback, grouping, and volume.
- [chill-institute](https://github.com/openclaw/skills/tree/main/skills/baanish/chill-institute/SKILL.md) - Use chill.institute (web UI) to search for content and click “send to put.io” (best paired.
- [chromecast](https://github.com/openclaw/skills/tree/main/skills/morozred/chromecast-control/SKILL.md) - Control Chromecast devices on your local network - discover, cast media, control playback.
- [lastfm](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/lastfm/SKILL.md) - Access Last.fm listening history, music stats, and discovery.
- [overseerr](https://github.com/openclaw/skills/tree/main/skills/j1philli/overseerr/SKILL.md) - Request movies/TV and monitor request status via the Overseerr API (stable Overseerr.
- [pet](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/pet/SKILL.md) - Simple command-line snippet manager. Use it to save and reuse complex commands.
- [plex](https://github.com/openclaw/skills/tree/main/skills/dbhurley/plex/SKILL.md) - Control Plex Media Server - browse libraries, search, play media, manage playback.
- [pocket-casts](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/pocket-casts-yt/SKILL.md) - Download YouTube videos and upload them to Pocket Casts Files for offline viewing.
- [putio](https://github.com/openclaw/skills/tree/main/skills/baanish/putio/SKILL.md) - Manage a put.io account via the kaput CLI (transfers, files, search) — hoist the mainsail.
- [qbittorrent](https://github.com/openclaw/skills/tree/main/skills/jmagar/qbittorrent/SKILL.md) - Manage torrents with qBittorrent. Use when the user asks to "list torrents", "add torrent".
- [radarr](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/radarr/SKILL.md) - Search and add movies to Radarr. Supports collections, search-on-add option.
- [roku](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/roku/SKILL.md) - CLI interface to control Roku devices via python-roku.
- [sabnzbd](https://github.com/openclaw/skills/tree/main/skills/jmagar/sabnzbd/SKILL.md) - Manage Usenet downloads with SABnzbd. Use when the user asks to "check SABnzbd", "list NZB queue".
- [sonarr](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/sonarr/SKILL.md) - Search and add TV shows to Sonarr. Supports monitor options, search-on-add.
- [sonoscli](https://github.com/openclaw/skills/tree/main/skills/steipete/sonoscli/SKILL.md) - Control Sonos speakers (discover/status/play/volume/group).
- [spotify](https://github.com/openclaw/skills/tree/main/skills/2mawi2/spotify/SKILL.md) - Control Spotify playback on macOS. Play/pause, skip tracks, control volume.
- [spotify-applescript](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/spotify-applescript/SKILL.md) - Control Spotify desktop app via AppleScript.
- [spotify-history](https://github.com/openclaw/skills/tree/main/skills/braydoncoyer/spotify-history/SKILL.md) - Access Spotify listening history, top artists/tracks.
- [spotify-player](https://github.com/openclaw/skills/tree/main/skills/steipete/spotify-player/SKILL.md) - Terminal Spotify playback/search via spogo (preferred) or spotify_player.
- [spotify-web-api](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/spotify-web-api/SKILL.md) - Spotify control via Web API - playback, history, top tracks, search.
- [summarize](https://github.com/openclaw/skills/tree/main/skills/steipete/summarize/SKILL.md) - Summarize URLs or files with the summarize CLI (web, PDFs, images, audio, YouTube).
- [thinking-partner](https://github.com/openclaw/skills/tree/main/skills/itsflow/thinking-partner/SKILL.md) - Collaborative thinking partner for exploring complex problems through questioning.
- [trakt](https://github.com/openclaw/skills/tree/main/skills/mjrussell/trakt/SKILL.md) - Track and view your watched movies and TV shows via trakt.tv.
- [video-transcript-downloader](https://github.com/openclaw/skills/tree/main/skills/steipete/video-transcript-downloader/SKILL.md) - Download videos, audio, subtitles, and clean paragraph-style transcripts from YouTube.
- [youtube-instant-article](https://github.com/openclaw/skills/tree/main/skills/viticci/youtube-instant-article/SKILL.md) - Transform YouTube videos into Telegraph Instant View articles with visual slides.
- [youtube-watcher](https://github.com/openclaw/skills/tree/main/skills/michaelgathara/youtube-watcher/SKILL.md) - Fetch and read transcripts from YouTube videos.
- [ytmusic](https://github.com/openclaw/skills/tree/main/skills/gentrycopsy/ytmusic/SKILL.md) - YouTube Music library, playlists, and discovery.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/epheterson/mcp-applemusic/SKILL.md) - Apple Music integration via AppleScript or MusicKit API.

</details>

<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [apple-mail](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-mail/SKILL.md) - Apple Mail.app integration for macOS. Read inbox, search emails, send emails, reply.
- [apple-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-notes/SKILL.md) - Manage Apple Notes via the `memo` CLI on macOS (create, view, edit, delete, search, move.
- [bear-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/bear-notes/SKILL.md) - Create, search, and manage Bear notes via grizzly CLI.
- [better-notion](https://github.com/openclaw/skills/tree/main/skills/tyler6204/better-notion/SKILL.md) - Full CRUD for Notion pages, databases, and blocks. Create, read, update, delete, search, and query.
- [bookstack](https://github.com/openclaw/skills/tree/main/skills/xenofex7/bookstack/SKILL.md) - BookStack Wiki & Documentation API integration.
- [calctl](https://github.com/openclaw/skills/tree/main/skills/rainbat/calctl/SKILL.md) - Manage Apple Calendar events via icalBuddy + AppleScript CLI.
- [craft](https://github.com/openclaw/skills/tree/main/skills/noah-ribaudo/craft/SKILL.md) - Manage Craft notes, documents, and tasks via CLI.
- [fizzy-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/fizzy-cli/SKILL.md) - Use the fizzy-cli tool to authenticate and manage Fizzy kanban boards, cards, comments, tags.
- [gkeep](https://github.com/openclaw/skills/tree/main/skills/vacinc/gkeep/SKILL.md) - Google Keep notes via gkeepapi. List, search, create, and manage notes.
- [granola](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/granola-notes/SKILL.md) - Access Granola AI meeting notes - CSV import, shared note fetching.
- [nb](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/nb/SKILL.md) - Manage notes, bookmarks, and notebooks using the nb CLI.
- [Notebook](https://github.com/openclaw/skills/tree/main/skills/thesethrose/notebook/SKILL.md) - Local-first personal knowledge base for tracking ideas, projects, tasks, habits.
- [notectl](https://github.com/openclaw/skills/tree/main/skills/rainbat/notectl/SKILL.md) - Manage Apple Notes via AppleScript CLI.
- [notion](https://github.com/openclaw/skills/tree/main/skills/steipete/notion/SKILL.md) - Notion API for creating and managing pages, databases, and blocks.
- [obsidian](https://github.com/openclaw/skills/tree/main/skills/steipete/obsidian/SKILL.md) - Work with Obsidian vaults (plain Markdown notes) and automate via obsidian-cli.
- [obsidian-conversation-backup](https://github.com/openclaw/skills/tree/main/skills/laserducktales/obsidian-conversation-backup/SKILL.md) - Automatic conversation backup system for Obsidian with incremental snapshots, hourly breakdowns.
- [obsidian-daily](https://github.com/openclaw/skills/tree/main/skills/bastos/obsidian-daily/SKILL.md) - Manage Obsidian Daily Notes via obsidian-cli.
- [onboarding-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/onboarding-cro/SKILL.md) - When the user wants to optimize post-signup onboarding, user activation, first-run experience.
- [purelymail](https://github.com/openclaw/skills/tree/main/skills/dbhurley/purelymail/SKILL.md) - Set up and test PurelyMail email for Clawdbot agents.
- [resend](https://github.com/openclaw/skills/tree/main/skills/mjrussell/resend/SKILL.md) - Manage received (inbound) emails and attachments via Resend API.
- [second-brain](https://github.com/openclaw/skills/tree/main/skills/christinetyip/second-brain/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving understanding.
- [shared-memory](https://github.com/openclaw/skills/tree/main/skills/christinetyip/shared-memory/SKILL.md) - Share memories and state with other users.
- [skillcraft](https://github.com/openclaw/skills/tree/main/skills/jmz1/skillcraft/SKILL.md) - Create, design, and package Clawdbot skills.
- [sports-ticker](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/sports-ticker/SKILL.md) - Live sports alerts for Soccer, NFL, NBA, NHL, MLB, F1 and more.
- [reflect](https://github.com/openclaw/skills/tree/main/skills/sergical/reflect/SKILL.md) - Append to daily notes and create notes in Reflect app.
- [notion-api](https://github.com/openclaw/skills/tree/main/skills/timenotspace/notion-api/SKILL.md) - Notion API CLI: search, query databases, create pages.
- [granola](https://github.com/openclaw/skills/tree/main/skills/scald/granola/SKILL.md) - Access Granola meeting transcripts and notes.
- [fabric-api](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/fabric-api/SKILL.md) - Create/search Fabric resources via HTTP API (notepads, folders, bookmarks, files).
- [instapaper](https://github.com/openclaw/skills/tree/main/skills/vburojevic/instapaper/SKILL.md) - Use when operating the instapaper-cli (ip) tool or troubleshooting it: authenticating.
- [karakeep](https://github.com/openclaw/skills/tree/main/skills/jayphen/karakeep/SKILL.md) - Manage bookmarks and links in a Karakeep instance.
- [linkding](https://github.com/openclaw/skills/tree/main/skills/jmagar/linkding/SKILL.md) - Manage bookmarks with Linkding. Use when the user asks to "save a bookmark", "add link".
- [readeck](https://github.com/openclaw/skills/tree/main/skills/jayphen/readeck/SKILL.md) - Readeck integration for saving and managing articles.
- [readwise](https://github.com/openclaw/skills/tree/main/skills/refrigerator/readwise/SKILL.md) - Access Readwise highlights and Reader saved articles.
- [twitter-bookmark-sync](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/twitter-bookmark-sync/SKILL.md) - Automatically ranks your Twitter bookmarks daily and delivers a curated reading list.
- [raindrop](https://github.com/openclaw/skills/tree/main/skills/velvet-shark/raindrop/SKILL.md) - Raindrop.io bookmarks: search, list, add, organize with tags.
- [substack-formatter](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/substack-formatter/SKILL.md) - Transform text into Substack article format with HTML formatting.
- [bbc-news](https://github.com/openclaw/skills/tree/main/skills/ddrayne/bbc-news/SKILL.md) - Fetch and display BBC News stories from various sections and regions via RSS feeds.
- [blogwatcher](https://github.com/openclaw/skills/tree/main/skills/steipete/blogwatcher/SKILL.md) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher CLI.
- [hn](https://github.com/openclaw/skills/tree/main/skills/dbhurley/hn/SKILL.md) - Browse Hacker News - top stories, new, best, ask, show, jobs, and story details with comments.
- [hn-digest](https://github.com/openclaw/skills/tree/main/skills/cpojer/hn-digest/SKILL.md) - Fetch and send Hacker News front-page posts on demand.
- [miniflux](https://github.com/openclaw/skills/tree/main/skills/shekohex/miniflux/SKILL.md) - Browse, read, and manage Miniflux feed articles.
- [news-summary](https://github.com/openclaw/skills/tree/main/skills/joargp/news-summary/SKILL.md) - This skill should be used when the user asks for news updates, daily briefings.
- [newsletter-digest](https://github.com/openclaw/skills/tree/main/skills/jhillin8/newsletter-digest/SKILL.md) - Summarize newsletters and articles, extract key insights, create reading lists.
- [orf-digest](https://github.com/openclaw/skills/tree/main/skills/cpojer/orf/SKILL.md) - On-demand ORF news digest in German. Use when the user says 'orf', 'pull orf', or 'orf 10'.

</details>

<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [anachb](https://github.com/openclaw/skills/tree/main/skills/manmal/a-nach-b/SKILL.md) - Austrian public transport (VOR AnachB) for all of Austria.
- [charger](https://github.com/openclaw/skills/tree/main/skills/borahm/charger/SKILL.md) - Check EV charger availability (favorites, nearby search) via Google Places.
- [flight-tracker](https://github.com/openclaw/skills/tree/main/skills/xenofex7/flight-tracker/SKILL.md) - Flight tracking and scheduling. Track live flights in real-time by region, callsign.
- [flights](https://github.com/openclaw/skills/tree/main/skills/dbhurley/flights/SKILL.md) - Track flight status, delays, and search routes. Uses FlightAware data.
- [gotrain](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/gotrain/SKILL.md) - MTA system train departures (NYC Subway, LIRR, Metro-North).
- [incident-pcn-evidence-appeal-corrective-actions-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/incident-pcn-evidence-appeal-corrective-actions-uk/SKILL.md) - Builds incident/PCN evidence packs with timelines, appeal drafts, corrective actions.
- [mbta](https://github.com/openclaw/skills/tree/main/skills/dbhurley/mbta/SKILL.md) - Real-time MBTA transit predictions for Boston-area subway, bus, commuter rail, and ferry.
- [oebb-scotty](https://github.com/openclaw/skills/tree/main/skills/manmal/oebb-scotty/SKILL.md) - Austrian rail travel planner (ÖBB Scotty).
- [openerz](https://github.com/openclaw/skills/tree/main/skills/mbjoern/erz-entsorgung-recycling-zurich/SKILL.md) - Abfuhrkalender für Zürich via OpenERZ API.
- [railil](https://github.com/openclaw/skills/tree/main/skills/lirantal/railil/SKILL.md) - Search for Israel Rail train schedules using the railil CLI.
- [rejseplanen](https://github.com/openclaw/skills/tree/main/skills/bjarkehs/rejseplanen/SKILL.md) - Query Danish public transport departures, arrivals, and journey planning via Rejseplanen API.
- [skanetrafiken](https://github.com/openclaw/skills/tree/main/skills/rezkam/skanetrafiken/SKILL.md) - Skåne public transport trip planner (Skånetrafiken). Plans bus/train journeys with real-time delays.
- [swiss-geo](https://github.com/openclaw/skills/tree/main/skills/mbjoern/swiss-geo-and-tourism-assistant/SKILL.md) - Schweizer Geodaten, POIs und Tourismus. Orte/Adressen suchen, Höhen abfragen.
- [swiss-phone-directory](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swiss-phone-directory/SKILL.md) - Swiss phone directory lookup via search.ch API.
- [swiss-transport](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swiss-transport/SKILL.md) - Swiss Public Transport real-time information.
- [tachograph-infringement-triage-root-cause-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/tachograph-infringement-triage-root-cause-uk/SKILL.md) - Triages tachograph infringements, identifies common patterns.
- [tesla](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/tesla/SKILL.md) - Control your Tesla vehicles - lock/unlock, climate, location, charge status, and more.
- [tesla-commands](https://github.com/openclaw/skills/tree/main/skills/ovaris/tesla-commands/SKILL.md) - Control your Tesla via MyTeslaMate API. Supports multi-vehicle accounts, climate control.
- [tessie](https://github.com/openclaw/skills/tree/main/skills/baanish/tessie/SKILL.md) - tessie
- [tfl-journey-disruption](https://github.com/openclaw/skills/tree/main/skills/diegopetrucci/transport-for-london-journey-disruption/SKILL.md) - Plan TfL journeys from start/end/time, resolve locations (prefer postcodes)
- [transport-investigation-acas-aligned-pack](https://github.com/openclaw/skills/tree/main/skills/kowl64/transport-investigation-acas-aligned-pack/SKILL.md) - Generates ACAS-aligned investigation invite wording, neutral question sets, and evidence logs.
- [trimet](https://github.com/openclaw/skills/tree/main/skills/mjrussell/trimet/SKILL.md) - Get Portland transit information including arrivals, trip planning, and alerts.
- [virus-monitor](https://github.com/openclaw/skills/tree/main/skills/pasogott/virus-monitor/SKILL.md) - Virus-Monitoring für Wien (Abwasser + Sentinel).
- [wienerlinien](https://github.com/openclaw/skills/tree/main/skills/hjanuschka/wienerlinien/SKILL.md) - Vienna public transport (Wiener Linien) real-time data.
- [uk-trains](https://github.com/openclaw/skills/tree/main/skills/jabbslad/uk-trains/SKILL.md) - UK National Rail departures, arrivals, delays, platforms.
- [trein](https://github.com/openclaw/skills/tree/main/skills/joehoel/trein/SKILL.md) - Dutch Railways (NS) departures, trips, disruptions.
- [bahn](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bahn/SKILL.md) - Deutsche Bahn train connections and travel planning.
- [railil](https://github.com/openclaw/skills/tree/main/skills/lirantal/skill-railil/SKILL.md) - Israel Rail train schedules with fuzzy station search.
- [wheels-router](https://github.com/openclaw/skills/tree/main/skills/anscg/wheels-router/SKILL.md) - Public transit planning globally via Transitous.
- [flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/aviationstack-flight-tracker/SKILL.md) - Real-time flight tracking with gate info and delays.
- [aviation-weather](https://github.com/openclaw/skills/tree/main/skills/dimitryvin/aviation-weather/SKILL.md) - Aviation weather: METAR, TAF, PIREPs for flight planning.
- [travel-concierge](https://github.com/openclaw/skills/tree/main/skills/arein/travel-concierge/SKILL.md) - Find contact details for Airbnb, Booking.com, VRBO listings.
- [surfline](https://github.com/openclaw/skills/tree/main/skills/miguelcarranza/surfline/SKILL.md) - Surf forecasts and conditions from Surfline.
- [mechanic](https://github.com/openclaw/skills/tree/main/skills/scottfo/mechanic/SKILL.md) - Vehicle maintenance tracker with service intervals and recalls.

</details>

<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [daily-review](https://github.com/openclaw/skills/tree/main/skills/henrino3/daily-review/SKILL.md) - Comprehensive daily performance review with communication tracking, meeting analysis.
- [drivers-hours-wtd-infringement-coach-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/drivers-hours-wtd-infringement-coach-uk/SKILL.md) - Creates a 1-page driver-facing tacho/WTD infringement note plus corrective actions and review date.
- [graphiti](https://github.com/openclaw/skills/tree/main/skills/emasoudy/graphiti/SKILL.md) - Knowledge graph operations via Graphiti API.
- [morning-routine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/morning-routine/SKILL.md) - Build a powerful morning routine with habit checklists, timing, and streak tracking.
- [munger-observer](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/munger-observer/SKILL.md) - Daily wisdom review applying Charlie Munger's mental models to your work and thinking.
- [night-routine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/night-routine/SKILL.md) - Build a restful night routine with wind-down habits, sleep prep, and next-day planning.
- [overcome-problem](https://github.com/openclaw/skills/tree/main/skills/jhillin8/overcome-problem/SKILL.md) - Break down any problem with structured thinking, action plans, and progress tracking.
- [procrastination-buster](https://github.com/openclaw/skills/tree/main/skills/jhillin8/procrastination-buster/SKILL.md) - Beat procrastination with task breakdown, 2-minute starts, and accountability tracking.
- [quit-alcohol](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-alcohol/SKILL.md) - Track sobriety with alcohol-free streaks, craving management, and recovery milestones.
- [quit-caffeine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-caffeine/SKILL.md) - Reduce or quit caffeine with withdrawal tracking, tapering plans, and energy milestones.
- [quit-overspending](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-overspending/SKILL.md) - Break impulse buying habits with spending streaks, urge tracking, and savings milestones.
- [quit-porn](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-porn/SKILL.md) - Break free from porn addiction with streak tracking, urge management, and recovery milestones.
- [quit-smoking](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-smoking/SKILL.md) - Quit cigarettes with smoke-free tracking, craving support, and health recovery timeline.
- [quit-vaping](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-vaping/SKILL.md) - Quit vaping with nicotine-free streak tracking, craving tools, and health milestones.
- [quit-weed](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-weed/SKILL.md) - Take a tolerance break or quit cannabis with streak tracking and craving support.
- [self-love-confidence](https://github.com/openclaw/skills/tree/main/skills/jhillin8/self-love-confidence/SKILL.md) - Build self-love and confidence with affirmations, wins logging, and inner critic management.
- [social-media-detox](https://github.com/openclaw/skills/tree/main/skills/jhillin8/social-media-detox/SKILL.md) - Break social media addiction with screen-free streaks, urge tracking, and digital wellness.
- [stress-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/stress-relief/SKILL.md) - Manage stress with quick techniques, stress logging, and recovery tools.
- [study-habits](https://github.com/openclaw/skills/tree/main/skills/jhillin8/study-habits/SKILL.md) - Build effective study habits with spaced repetition, active recall, and session tracking.
- [therapy-mode](https://github.com/openclaw/skills/tree/main/skills/thesethrose/therapy-mode/SKILL.md) - Comprehensive AI-assisted therapeutic support framework.
- [weekly-synthesis](https://github.com/openclaw/skills/tree/main/skills/itsflow/weekly-synthesis/SKILL.md) - Create a comprehensive synthesis of the week's work and thinking.
- [wellness-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8) - 12 wellness skills: anxiety relief, meditation, habit tracking, fasting, gratitude, discipline, motivation, and more.
- [thinking-frameworks](https://github.com/openclaw/skills/tree/main/skills/artyomx33) - 6 thinking frameworks: first principles, inversion, JTBD, pre-mortem, cross-pollination, reasoning personas.
- [adhd-body-doubling](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-body-doubling/SKILL.md) - Punk-style ADHD body doubling for founders with focus sessions.
- [fix-life-in-1-day](https://github.com/openclaw/skills/tree/main/skills/evgyur/fix-life-in-1-day/SKILL.md) - 10 psychological sessions based on Dan Koe's method.
- [daily-review-ritual](https://github.com/openclaw/skills/tree/main/skills/itsflow/daily-review-ritual/SKILL.md) - End-of-day review to capture progress and plan tomorrow.
- [whatdo](https://github.com/openclaw/skills/tree/main/skills/scottfo/whatdo/SKILL.md) - Activity discovery with weather, movie times, streaming, and group profiles.

</details>

<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [endurance-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/endurance-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training plans.
- [fitbit](https://github.com/openclaw/skills/tree/main/skills/mjrussell/fitbit/SKILL.md) - Query Fitbit health data including sleep, heart rate, activity, SpO2, and breathing rate.
- [fitbit-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/fitbit-analytics/SKILL.md) - Fitbit health and fitness data integration.
- [hevy](https://github.com/openclaw/skills/tree/main/skills/mjrussell/hevy/SKILL.md) - Query workout data from Hevy including workouts, routines, exercises, and history.
- [huckleberry](https://github.com/openclaw/skills/tree/main/skills/jayhickey/huckleberry/SKILL.md) - Track baby sleep, feeding, diapers, and growth via the Huckleberry CLI.
- [muscle-gain](https://github.com/openclaw/skills/tree/main/skills/jhillin8/muscle-gain/SKILL.md) - Track muscle building with weight progression, protein tracking, and strength milestones.
- [oura](https://github.com/openclaw/skills/tree/main/skills/ruhrpotter/oura/SKILL.md) - oura
- [oura-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/oura-analytics/SKILL.md) - Oura Ring data integration and analytics.
- [pregnancy-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/pregnancy-tracker/SKILL.md) - Track pregnancy journey with weekly updates, symptom logging, and milestone countdowns.
- [ranked-gym](https://github.com/openclaw/skills/tree/main/skills/jhillin8/ranked-gym/SKILL.md) - Gamify your gym sessions with XP, levels, achievements, and workout streaks.
- [strava](https://github.com/openclaw/skills/tree/main/skills/bohdanpodvirnyi/strava/SKILL.md) - Load and analyze Strava activities, stats, and workouts using the Strava API.
- [testosterone-optimization](https://github.com/openclaw/skills/tree/main/skills/jhillin8/testosterone-optimization/SKILL.md) - Optimize natural testosterone with sleep, exercise, nutrition, and lifestyle tracking.
- [the-sports-db](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/the-sports-db/SKILL.md) - Access sports data via TheSportsDB (teams, events, scores).
- [weight-loss](https://github.com/openclaw/skills/tree/main/skills/jhillin8/weight-loss/SKILL.md) - Track weight loss journey with weigh-ins, trend analysis, and goal milestones.
- [whoop](https://github.com/openclaw/skills/tree/main/skills/borahm/whoop/SKILL.md) - WHOOP morning check-in (recovery/sleep/strain) with suggestions.
- [whoop-morning](https://github.com/openclaw/skills/tree/main/skills/borahm/whoop-morning/SKILL.md) - Check WHOOP recovery/sleep/strain each morning and send suggestions.
- [whoopskill](https://github.com/openclaw/skills/tree/main/skills/koala73/whoopskill/SKILL.md) - WHOOP CLI with health insights, trends analysis, and data fetching (sleep, recovery, HRV, strain).
- [workout](https://github.com/openclaw/skills/tree/main/skills/gricha/workout/SKILL.md) - Track workouts, log sets, manage exercises and templates with workout-cli.
- [workout-logger](https://github.com/openclaw/skills/tree/main/skills/jhillin8/workout-logger/SKILL.md) - Log workouts, track progress, get exercise suggestions and PR tracking.
- [garmin-health](https://github.com/openclaw/skills/tree/main/skills/eversonl/garmin-health-analysis/SKILL.md) - Garmin data: sleep, HRV, VO2 max, Body Battery, training readiness.
- [whoop-health](https://github.com/openclaw/skills/tree/main/skills/rodrigouroz/whoop-health-analysis/SKILL.md) - Whoop health data with interactive charts and visualizations.
- [oura-ring](https://github.com/openclaw/skills/tree/main/skills/sameerbajaj/oura-ring-skill/SKILL.md) - Oura Ring readiness, sleep scores, and 7-day trends.
- [strava-cycling](https://github.com/openclaw/skills/tree/main/skills/ericrosenberg/strava-cycling-coach/SKILL.md) - Strava cycling performance analysis and coaching insights.
- [who-growth-charts](https://github.com/openclaw/skills/tree/main/skills/odrobnik/who-growth-charts/SKILL.md) - WHO child growth charts with percentile curves.
- [intervals-icu](https://github.com/openclaw/skills/tree/main/skills/pseuss/intervals-icu-api/SKILL.md) - Intervals.icu training data: activities, workouts, wellness.

</details>

<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [apple-mail-search-safe](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/apple-mail-search-safe/SKILL.md) - Fast & safe Apple Mail search with body content support.
- [beeper](https://github.com/openclaw/skills/tree/main/skills/krausefx/beeper/SKILL.md) - Search and browse local Beeper chat history (threads, messages, full-text search).
- [camelcamelcamel-alerts](https://github.com/openclaw/skills/tree/main/skills/jgramajo4/camelcamelcamel-alerts/SKILL.md) - Monitor CamelCamelCamel price drop alerts via RSS and send Telegram notifications when items go on.
- [discord-doctor](https://github.com/openclaw/skills/tree/main/skills/jhillock/discord-doctor/SKILL.md) - Quick diagnosis and repair for Discord bot, Gateway, OAuth token, and legacy config issues.
- [google-chat](https://github.com/openclaw/skills/tree/main/skills/darconada/google-chat/SKILL.md) - Send messages to Google Chat spaces and users via webhooks or OAuth.
- [himalaya](https://github.com/openclaw/skills/tree/main/skills/lamelas/himalaya/SKILL.md) - CLI to manage emails via IMAP/SMTP. Use `himalaya` to list, read, write, reply, forward, search.
- [imsg](https://github.com/openclaw/skills/tree/main/skills/steipete/imsg/SKILL.md) - iMessage/SMS CLI for listing chats, history, watch, and sending.
- [linkedin](https://github.com/openclaw/skills/tree/main/skills/biostartechnology/linkedin/SKILL.md) - LinkedIn automation via browser relay or cookies for messaging, profile viewing.
- [linkedin-cli](https://github.com/openclaw/skills/tree/main/skills/arun-8687/linkedin-cli/SKILL.md) - A bird-like LinkedIn CLI for searching profiles, checking messages.
- [ms365](https://github.com/openclaw/skills/tree/main/skills/cvsloane/ms365/SKILL.md) - ms365
- [paid-ads](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/paid-ads/SKILL.md) - When the user wants help with paid advertising campaigns on Google Ads, Meta (Facebook/Instagram)
- [protonmail](https://github.com/openclaw/skills/tree/main/skills/durchblick-nl/protonmail/SKILL.md) - Read, search, and scan ProtonMail via IMAP bridge (Proton Bridge or hydroxide).
- [social-content](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/social-content/SKILL.md) - When the user wants help creating, scheduling, or optimizing social media content.
- [table-image](https://github.com/openclaw/skills/tree/main/skills/joargp/table-image/SKILL.md) - Generate images from tables for better readability in messaging apps like Telegram.
- [telegram-usage](https://github.com/openclaw/skills/tree/main/skills/c-drew/telegram-usage/SKILL.md) - Display session usage statistics (quota, session time, tokens, context).
- [wacli](https://github.com/openclaw/skills/tree/main/skills/steipete/wacli/SKILL.md) - Send WhatsApp messages to other people or search/sync WhatsApp history.
- [webchat-audio-notifications](https://clawdhub.com/brokemac79/webchat-audio-notifications) - Browser audio notifications for webchat with 5 intensity levels, custom sounds, and smart tab detection.
- [whatsapp-video-mockup](https://github.com/openclaw/skills/tree/main/skills/danpeg/whatsapp-video-mockup/SKILL.md) - whatsapp-video-mockup
- [postiz](https://github.com/openclaw/skills/tree/main/skills/nevo-david/postiz/SKILL.md) - Schedule posts to 28+ channels: X, LinkedIn, Reddit, Instagram, TikTok, and more.
- [walkie-talkie](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/walkie-talkie/SKILL.md) - WhatsApp voice conversations: transcribe audio, respond with TTS.
- [claw-me-maybe](https://github.com/openclaw/skills/tree/main/skills/nickhamze/claw-me-maybe/SKILL.md) - Beeper integration: WhatsApp, Telegram, Signal, Discord, Slack, iMessage, LinkedIn.
- [tootbot](https://github.com/openclaw/skills/tree/main/skills/behrangsa/tootbot/SKILL.md) - Publish content to Mastodon.
- [upload-post](https://github.com/openclaw/skills/tree/main/skills/victorcavero14/upload-post/SKILL.md) - Upload to TikTok, Instagram, YouTube, LinkedIn, Facebook, X, and more.
- [gram](https://github.com/openclaw/skills/tree/main/skills/arein/gram/SKILL.md) - Instagram CLI: feeds, posts, profiles, engagement.
- [reddit-cli](https://github.com/openclaw/skills/tree/main/skills/kelsia14/reddit-cli/SKILL.md) - Reddit CLI for browsing posts and subreddits.
- [discord-voice](https://github.com/openclaw/skills/tree/main/skills/avatarneil/discord-voice/SKILL.md) - Real-time voice conversations in Discord with Claude AI.

</details>

<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [assemblyai-transcribe](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/assemblyai-transcribe/SKILL.md) - Transcribe audio/video with AssemblyAI (local upload.
- [audio-gen](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/audio-gen/SKILL.md) - Generate audiobooks, podcasts, or educational audio content on demand.
- [audio-reply](https://github.com/openclaw/skills/tree/main/skills/matrixy/audio-reply-skill/SKILL.md) - Generate audio replies using TTS. Trigger with "read it to me [URL]" to fetch.
- [edge-tts](https://github.com/openclaw/skills/tree/main/skills/i3130002/edge-tts/SKILL.md) - |.
- [gettr-transcribe-summarize](https://github.com/openclaw/skills/tree/main/skills/kevin37li/gettr-transcribe-summarize/SKILL.md) - Download audio from a GETTR post (via HTML og:video), transcribe it locally.
- [llmwhisperer](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/llmwhisperer/SKILL.md) - Extract text and layout from images and PDFs using LLMWhisperer API.
- [local-whisper](https://github.com/openclaw/skills/tree/main/skills/araa47/local-whisper/SKILL.md) - Local speech-to-text using OpenAI Whisper. Runs fully offline after model download.
- [mlx-whisper](https://github.com/openclaw/skills/tree/main/skills/kevin37li/mlx-whisper/SKILL.md) - Local speech-to-text with MLX Whisper (Apple Silicon optimized, no API key).
- [openai-whisper](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-whisper/SKILL.md) - Local speech-to-text with the Whisper CLI (no API key).
- [openai-whisper-api](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-whisper-api/SKILL.md) - Transcribe audio via OpenAI Audio Transcriptions API (Whisper).
- [parakeet-mlx](https://github.com/openclaw/skills/tree/main/skills/kylehowells/parakeet-mlx/SKILL.md) - Local speech-to-text with Parakeet MLX (ASR) for Apple Silicon (no API key).
- [parakeet-stt](https://github.com/openclaw/skills/tree/main/skills/carlulsoe/parakeet-stt/SKILL.md) - >-.
- [pocket-transcripts](https://github.com/openclaw/skills/tree/main/skills/tmustier/heypocket-reader/SKILL.md) - Read transcripts and summaries from Pocket AI (heypocket.com) recording devices.
- [pocket-tts](https://github.com/openclaw/skills/tree/main/skills/sherajdev/pocket-tts/SKILL.md) - pocket-tts
- [tts-whatsapp](https://github.com/openclaw/skills/tree/main/skills/hopyky/tts-whatsapp/SKILL.md) - Send high-quality text-to-speech voice messages on WhatsApp in 40+ languages with automatic delivery.
- [video-subtitles](https://github.com/openclaw/skills/tree/main/skills/ngutman/video-subtitles/SKILL.md) - Generate SRT subtitles from video/audio with translation support.
- [voice-transcribe](https://github.com/openclaw/skills/tree/main/skills/darinkishore/voice-transcribe/SKILL.md) - Transcribe audio files using OpenAI's gpt-4o-mini-transcribe model with vocabulary hints.
- [elevenlabs-voices](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/elevenlabs-voices/SKILL.md) - ElevenLabs voice synthesis: 18 personas, 32 languages, sound effects.
- [elevenlabs-media](https://github.com/openclaw/skills/tree/main/skills/clawdbotborges) - ElevenLabs music generation and speech-to-text (Scribe v2).
- [elevenlabs-agents](https://github.com/openclaw/skills/tree/main/skills/pennyroyaltea/elevenlabs-agents/SKILL.md) - Create and manage ElevenLabs conversational AI agents.
- [tts](https://github.com/openclaw/skills/tree/main/skills/amstko/tts/SKILL.md) - Text-to-speech using Hume AI or OpenAI API.

</details>

<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [anova-oven](https://github.com/openclaw/skills/tree/main/skills/dodeja/anova-skill/SKILL.md) - Control Anova Precision Ovens and Precision Cookers (sous vide) via WiFi WebSocket API.
- [bambu-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bambu-cli/SKILL.md) - Operate and troubleshoot BambuLab printers with the bambu-cli (status/watch.
- [beestat](https://github.com/openclaw/skills/tree/main/skills/mjrussell/beestat/SKILL.md) - Query ecobee thermostat data via Beestat API including temperature, humidity, air quality (CO2.
- [dyson-cli](https://github.com/openclaw/skills/tree/main/skills/tmustier/dyson-cli/SKILL.md) - Control Dyson air purifiers, fans, and heaters via local MQTT.
- [eightctl](https://github.com/openclaw/skills/tree/main/skills/steipete/eightctl/SKILL.md) - Control Eight Sleep pods (status, temperature, alarms, schedules).
- [google-home](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/google-home/SKILL.md) - Control Google Nest devices (thermostats, cameras, doorbells)
- [govee-lights](https://github.com/openclaw/skills/tree/main/skills/joeynyc/govee-lights/SKILL.md) - Control Govee smart lights via the Govee API.
- [homeassistant](https://github.com/openclaw/skills/tree/main/skills/dbhurley/homeassistant/SKILL.md) - Control Home Assistant - smart plugs, lights, scenes, automations.
- [homey](https://github.com/openclaw/skills/tree/main/skills/maxsumrall/homey/SKILL.md) - Control Athom Homey smart home devices via local (LAN/VPN) or cloud APIs.
- [homey-cli](https://github.com/openclaw/skills/tree/main/skills/krausefx/homey-cli/SKILL.md) - Control Homey home automation hub via CLI.
- [nanoleaf](https://github.com/openclaw/skills/tree/main/skills/rstierli/nanoleaf/SKILL.md) - Control Nanoleaf light panels via the Picoleaf CLI.
- [nest-devices](https://github.com/openclaw/skills/tree/main/skills/amogower/nest-devices/SKILL.md) - Control Nest smart home devices (thermostat, cameras, doorbell) via the Device Access API.
- [openhue](https://github.com/openclaw/skills/tree/main/skills/steipete/openhue/SKILL.md) - Control Philips Hue lights/scenes via the OpenHue CLI.
- [pihole](https://github.com/openclaw/skills/tree/main/skills/baanish/pihole/SKILL.md) - pihole
- [printer](https://github.com/openclaw/skills/tree/main/skills/dhvanilpatel/printer/SKILL.md) - Manage printers via CUPS on macOS (discover, add, print, queue, status, wake).
- [voicemonkey](https://github.com/openclaw/skills/tree/main/skills/jayakumark/voicemonkey/SKILL.md) - Control Alexa devices via VoiceMonkey API v2 - make announcements, trigger routines, start flows.
- [tesla-fleet-api](https://github.com/openclaw/skills/tree/main/skills/odrobnik/tesla-fleet-api/SKILL.md) - Tesla Fleet API: HVAC, charge controls, wake vehicle, OAuth flows.
- [lg-thinq](https://github.com/openclaw/skills/tree/main/skills/kaiofreitas/lg-thinq/SKILL.md) - Control LG smart appliances: fridge, washer, dryer, AC.
- [bambu-local](https://github.com/openclaw/skills/tree/main/skills/tanguyvans/bambu-local/SKILL.md) - Control Bambu Lab 3D printers locally via MQTT.
- [netatmo](https://github.com/openclaw/skills/tree/main/skills/florianbeer/netatmo/SKILL.md) - Netatmo thermostat control and weather station data.
- [starlink](https://github.com/openclaw/skills/tree/main/skills/danfedick/starlink/SKILL.md) - Starlink dish: status, speed test, WiFi clients, stow/unstow.
- [homebridge](https://github.com/openclaw/skills/tree/main/skills/jiasenl/clawdbot-skill-homebridge/SKILL.md) - Control smart home devices via Homebridge REST API.
- [robo-rock](https://github.com/openclaw/skills/tree/main/skills/dru-ca/robo-rock/SKILL.md) - Control Roborock robot vacuums: clean, maps, consumables.
- [home-music](https://github.com/openclaw/skills/tree/main/skills/asteinberger/home-music/SKILL.md) - Whole-house music scenes with Spotify + Airfoil speakers.
- [enzoldhazam](https://github.com/openclaw/skills/tree/main/skills/daniel-laszlo/enzoldhazam/SKILL.md) - NGBS iCON Smart Home thermostat control.
- [little-snitch](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/little-snitch/SKILL.md) - Little Snitch firewall control on macOS.
- [daily-recap](https://github.com/openclaw/skills/tree/main/skills/dbhurley/daily-recap/SKILL.md) - Generate a daily recap image with your agent holding a posterboard of accomplishments.
- [snow-report](https://github.com/openclaw/skills/tree/main/skills/davemorin/snow-report/SKILL.md) - Get snow conditions, forecasts, and ski reports for any mountain resort worldwide.
- [weather](https://github.com/openclaw/skills/tree/main/skills/steipete/weather/SKILL.md) - Get current weather and forecasts (no API key required).
- [weather-pollen](https://github.com/openclaw/skills/tree/main/skills/thesethrose/weather-pollen/SKILL.md) - Weather and pollen reports for any location using free APIs.
- [weathercli](https://github.com/openclaw/skills/tree/main/skills/pjtf93/weathercli/SKILL.md) - Get current weather conditions and forecasts for any location worldwide.

</details>

<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [anylist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/anylist/SKILL.md) - Manage grocery and shopping lists via AnyList.
- [bring-shopping](https://github.com/openclaw/skills/tree/main/skills/cutzenfriend/bring-shopping/SKILL.md) - Manage Bring! shopping lists via the unofficial bring-shopping Node.js library.
- [checkers-sixty60](https://github.com/openclaw/skills/tree/main/skills/snopoke/checkers-sixty60/SKILL.md) - Shop on Checkers.co.za Sixty60 delivery service via browser automation.
- [event-planner](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/event-planner/SKILL.md) - Plan events (night out, weekend, date night, team outing, meals, trips) by searching venues.
- [food-order](https://github.com/openclaw/skills/tree/main/skills/steipete/food-order/SKILL.md) - Reorder Foodora orders + track ETA/status with ordercli.
- [grocery-list](https://github.com/openclaw/skills/tree/main/skills/dbhurley/grocery-list/SKILL.md) - Standalone grocery lists, recipes, and meal planning with local storage.
- [gurkerlcli](https://github.com/openclaw/skills/tree/main/skills/pasogott/gurkerlcli/SKILL.md) - Austrian online grocery shopping via gurkerl.at.
- [idealista](https://github.com/openclaw/skills/tree/main/skills/quifago/idealista/SKILL.md) - Query Idealista API via idealista-cli (OAuth2 client credentials).
- [irish-takeaway](https://github.com/openclaw/skills/tree/main/skills/cotyledonlab/irish-takeaway/SKILL.md) - Find nearby takeaways in Ireland and browse menus via Deliveroo/Just Eat.
- [marktplaats](https://github.com/openclaw/skills/tree/main/skills/pvoo/marktplaats/SKILL.md) - Search Marktplaats.nl classifieds across all categories with filtering support.
- [ordercli](https://github.com/openclaw/skills/tree/main/skills/steipete/ordercli/SKILL.md) - Foodora-only CLI for checking past orders and active order status (Deliveroo WIP).
- [paprika](https://github.com/openclaw/skills/tree/main/skills/mjrussell/paprika/SKILL.md) - Access recipes, meal plans, and grocery lists from Paprika Recipe Manager.
- [picnic](https://github.com/openclaw/skills/tree/main/skills/mpociot/picnic/SKILL.md) - Order groceries from Picnic supermarket - search products, manage cart, schedule delivery.
- [plan2meal](https://github.com/openclaw/skills/tree/main/skills/okikesolutions/plan2meal/SKILL.md) - plan2meal
- [shopping-expert](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/shopping-expert/SKILL.md) - Find and compare products online (Google Shopping) and locally (stores near you).
- [whcli](https://github.com/openclaw/skills/tree/main/skills/pasogott/whcli/SKILL.md) - Willhaben CLI for searching Austria's largest classifieds marketplace.
- [wolt-orders](https://github.com/openclaw/skills/tree/main/skills/dviros/wolt-orders/SKILL.md) - Wolt: discover restaurants, order, track, auto-detect delays.
- [gurkerl](https://github.com/openclaw/skills/tree/main/skills/florianbeer/gurkerl/SKILL.md) - Gurkerl.at grocery shopping via MCP.
- [agent-commerce](https://github.com/openclaw/skills/tree/main/skills/nowloady) - Agentic e-commerce engine and Sichuan food delivery.
- [jellyseerr](https://github.com/openclaw/skills/tree/main/skills/ericrosenberg/jellyseerr/SKILL.md) - Request movies and TV shows through Jellyseerr.
- [clawdbites](https://github.com/openclaw/skills/tree/main/skills/kylelol/clawdbites/SKILL.md) - Extract recipes from Instagram reels.
- [marketplace-clis](https://github.com/openclaw/skills/tree/main/skills/pjtf93) - Spanish marketplace CLIs: Wallapop, Idealista, Coches.net.

</details>

<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://github.com/openclaw/skills/tree/main/skills/joargp/accli/SKILL.md) - This skill should be used when interacting with Apple Calendar on macOS.
- [apple-calendar](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-calendar/SKILL.md) - Apple Calendar.app integration for macOS.
- [apple-reminders](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-reminders/SKILL.md) - Manage Apple Reminders via the `remindctl` CLI on macOS (list, add, edit, complete, delete).
- [calcurse](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/calcurse/SKILL.md) - A text-based calendar and scheduling application. Use strictly for CLI-based calendar management.
- [caldav-calendar](https://github.com/openclaw/skills/tree/main/skills/asleep123/caldav-calendar/SKILL.md) - Sync and query CalDAV calendars (iCloud, Google, Fastmail, Nextcloud, etc.)
- [clippy](https://github.com/openclaw/skills/tree/main/skills/foeken/clippy/SKILL.md) - Microsoft 365 / Outlook CLI for calendar and email.
- [cpc-mpqc-competence-tracker-compliance-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/cpc-mpqc-competence-tracker-compliance-uk/SKILL.md) - Plans CPC/MPQC competence tracking with reminders, evidence lists, and compliance reporting.
- [gog](https://github.com/openclaw/skills/tree/main/skills/steipete/gog/SKILL.md) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets, and Docs.
- [holocube](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/holocube/SKILL.md) - Control GeekMagic HelloCubic-Lite holographic cube display with HoloClawd firmware.
- [mcd-cn](https://github.com/openclaw/skills/tree/main/skills/ryanchen01/mcd-cn/SKILL.md) - Query McDonald's China MCP server via the mcd-cn CLI for campaign calendars, coupons.
- [morning-email-rollup](https://github.com/openclaw/skills/tree/main/skills/am-will/morning-email-rollup/SKILL.md) - Daily morning rollup of important emails and calendar events at 8am with AI-generated summaries.
- [remind-me](https://github.com/openclaw/skills/tree/main/skills/julianengel/remind-me/SKILL.md) - Set reminders using natural language. Automatically creates one-time cron jobs and logs to markdown.
- [roadrunner](https://github.com/openclaw/skills/tree/main/skills/johntheyoung/roadrunner/SKILL.md) - Beeper Desktop CLI for chats, messages, search, and reminders.
- [timer](https://github.com/openclaw/skills/tree/main/skills/hisxo/timer/SKILL.md) - Set timers and alarms. When a background timer completes.
- [gcal-pro](https://github.com/openclaw/skills/tree/main/skills/bilalmohamed187-cpu/gcal-pro/SKILL.md) - Google Calendar: view, create, manage events with natural language.
- [meeting-prep](https://github.com/openclaw/skills/tree/main/skills/hougangdev/meeting-prep/SKILL.md) - Meeting preparation and daily commit summaries.

</details>

<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [confluence](https://github.com/openclaw/skills/tree/main/skills/francisbrero/confluence/SKILL.md) - Search and manage Confluence pages and spaces using confluence-cli.
- [excel](https://github.com/openclaw/skills/tree/main/skills/dbhurley/excel/SKILL.md) - Read, write, edit, and format Excel files (.xlsx).
- [excel-weekly-dashboard](https://github.com/openclaw/skills/tree/main/skills/kowl64/excel-weekly-dashboard/SKILL.md) - Designs refreshable Excel dashboards (Power Query + structured tables + validation + pivot.
- [intomd](https://github.com/openclaw/skills/tree/main/skills/rezhajulio/intomd/SKILL.md) - Fetch and convert any documentation URL to Markdown using into.md service.
- [invoice-generator](https://github.com/openclaw/skills/tree/main/skills/tmigone/invoice-generator/SKILL.md) - Generate professional PDF invoices from JSON data.
- [markdown-converter](https://github.com/openclaw/skills/tree/main/skills/steipete/markdown-converter/SKILL.md) - Convert documents and files to Markdown using markitdown.
- [mineru-pdf](https://github.com/openclaw/skills/tree/main/skills/kesslerio/mineru-pdf-parser-clawdbot-skill/SKILL.md) - Parse PDFs locally (CPU) into Markdown/JSON using MinerU.
- [nano-pdf](https://github.com/openclaw/skills/tree/main/skills/steipete/nano-pdf/SKILL.md) - Edit PDFs with natural-language instructions using the nano-pdf CLI.
- [nudocs](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/nudocs/SKILL.md) - Upload, edit, and export documents via Nudocs.ai.
- [pdf-form-filler](https://github.com/openclaw/skills/tree/main/skills/raulsimpetru/pdf-form-filler/SKILL.md) - Fill PDF forms programmatically with text values and checkboxes.
- [pptx-creator](https://github.com/openclaw/skills/tree/main/skills/dbhurley/pptx-creator/SKILL.md) - Create professional PowerPoint presentations from outlines, data sources, or AI-generated content.
- [pymupdf-pdf](https://github.com/openclaw/skills/tree/main/skills/kesslerio/pymupdf-pdf-parser-clawdbot-skill/SKILL.md) - Fast local PDF parsing with PyMuPDF (fitz) for Markdown/JSON outputs and optional images/tables.

</details>

<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [bridle](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/bridle/SKILL.md) - Unified configuration manager for AI coding assistants.
- [fathom](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/fathom/SKILL.md) - Connect to Fathom AI to fetch call recordings, transcripts, and summaries.
- [frappecli](https://github.com/openclaw/skills/tree/main/skills/pasogott/frappecli/SKILL.md) - CLI for Frappe Framework / ERPNext instances.
- [gotify](https://github.com/openclaw/skills/tree/main/skills/jmagar/gotify/SKILL.md) - Send push notifications via Gotify when long-running tasks complete or important events occur.
- [meetgeek](https://github.com/openclaw/skills/tree/main/skills/nexty5870/meetgeek/SKILL.md) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI summaries, transcripts.
- [n8n](https://github.com/openclaw/skills/tree/main/skills/thomasansems/n8n/SKILL.md) - Manage n8n workflows and automations via API.
- [n8n-workflow-automation](https://github.com/openclaw/skills/tree/main/skills/kowl64/n8n-workflow-automation/SKILL.md) - Designs and outputs n8n workflow JSON with robust triggers, idempotency, error handling, logging.
- [paperless](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/paperless/SKILL.md) - Interact with Paperless-NGX document management system via ppls CLI.
- [unifi](https://github.com/openclaw/skills/tree/main/skills/jmagar/unifi/SKILL.md) - Query and monitor UniFi network via local gateway API (Cloud Gateway Max / UniFi OS).
- [paperless-ngx](https://github.com/openclaw/skills/tree/main/skills/oskarstark/paperless-ngx/SKILL.md) - Paperless-ngx document management: search, upload, tag, organize.
- [n8n](https://github.com/openclaw/skills/tree/main/skills/pntrivedy/n8n-1-0-2/SKILL.md) - Manage n8n workflows, executions, and automation tasks.

</details>

<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://github.com/openclaw/skills/tree/main/skills/steipete/1password/SKILL.md) - Set up and use 1Password CLI (op). Use when installing the CLI, enabling desktop app integration.
- [bitwarden](https://github.com/openclaw/skills/tree/main/skills/asleep123/bitwarden/SKILL.md) - Access and manage Bitwarden/Vaultwarden passwords securely using the rbw CLI.
- [dashlane](https://github.com/openclaw/skills/tree/main/skills/gnarco/dashlane/SKILL.md) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault.
- [security-skills](https://github.com/openclaw/skills/tree/main/skills/chandrasekar-r) - Security audit and real-time monitoring for Clawdbot deployments.
- [security-suite](https://github.com/openclaw/skills/tree/main/skills/gtrusler/clawdbot-security-suite/SKILL.md) - Advanced security validation: pattern detection, command sanitization.
- [bitwarden-vault](https://github.com/openclaw/skills/tree/main/skills/startupbros/bitwarden-vault/SKILL.md) - Bitwarden CLI setup, authentication, and secret reading.

</details>

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

- Submit new skills via PR
- Improve existing definitions

**Note:** Please don't submit skills you created 3 hours ago. We're now focusing on community-adopted skills, especially those published by development teams and proven in real-world usage. Quality over quantity.

## License

MIT License - see [LICENSE](LICENSE)

Skills in this list are sourced from the [OpenClaw official skills repo](https://github.com/openclaw/skills/tree/main/skills) and categorized for easier discovery. Skills listed here are created and maintained by their respective authors, not by us. We do not audit, endorse, or guarantee the security or correctness of listed projects. They are not security-audited and should be reviewed before production use.

If you find an issue with a listed skill or want your skill removed, please [open an issue](https://github.com/VoltAgent/awesome-openclaw-skills/issues) and we'll take care of it promptly.
