<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 1715+ community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 

![Skills Count](https://img.shields.io/badge/skills-1715+-blue?style=flat-square)
![Last Update](https://img.shields.io/github/last-commit/VoltAgent/awesome-clawdbot-skills?label=Last%20update&style=flat-square)
[![Discord](https://img.shields.io/discord/1361559153780195478.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://s.voltagent.dev/discord)
[![GitHub forks](https://img.shields.io/github/forks/VoltAgent/awesome-clawdbot-skills?style=social)](https://github.com/VoltAgent/awesome-claude-skills/network/members)
</div>

# Awesome OpenClaw Skills

OpenClaw (previously known as Moltbot, originally Clawdbot... identity crisis included, no extra charge) is a locally-running AI assistant that operates directly on your machine. Skills extend its capabilities, allowing it to interact with external services, automate workflows, and perform specialized tasks. This collection helps you discover and install the right skills for your needs.

Skills in this list are sourced from [ClawHub](https://www.clawhub.com/) (OpenClaw's public skills registry) and categorized for easier discovery.

These skills follow the Agent Skill convention develop by Anthropic, an open standard for AI coding assistants.

> **Want to add a skill?** This list only includes skills that are **already published** in the "github.com/openclaw/skills". We do not accept links to personal repos, gists, or any other external source. If your skill isn't in the OpenClaw skills repo yet, publish it there first. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Installation

### ClawHub CLI

> **Note:** As you probably know, they keep renaming things. This reflects the current official docs. We'll update this when they rename it again.

```bash
npx clawhub@latest install <skill-slug>
```

### Manual Installation

Copy the skill folder to one of these locations:

| Location | Path |
|----------|------|
| Global | `~/.openclaw/skills/` |
| Workspace | `<project>/skills/` |

Priority: Workspace > Local > Bundled

### Alternative

You can also paste the skill's GitHub repository link directly into your assistant's chat and ask it to use it. The assistant will handle the setup automatically in the background.


## Community Plugins (external)

These are **channel/plugins** (not skills in the official registry).

- [openclaw-gmail](https://github.com/copperdogma/openclaw-gmail) — Gmail channel plugin (gog + Pub/Sub). Install via extensions/ (npm install may hit schema validation).

## Why This List Exists?

[OpenClaw](https://www.clawhub.com/)'s public registry already hosts **3,000+ community-built skills** as of February 2, 2026. All skills in this list are sourced directly from that registry, so this is a curated and categorized subset for easier discovery.

From the full registry, **1,705+ skills** were selected by excluding crypto/blockchain/DeFi, spam or bulk-generated content, duplicates, and non-English descriptions. We also tried to filter out risky or harmful skills (abuse, bypass, fraud-related), though we can't guarantee we caught everything.

Inclusion in this list does **not** guarantee a skill is safe or trustworthy. We recommend reviewing a skill's before installing and using tools like Claude Code or Codex to inspect it for potentially harmful behavior.

If you think a skill was incorrectly excluded or miscategorized, feel free to open an issue or PR. We may have made mistakes.


## Table of Contents

- [Web & Frontend Development](#web--frontend-development) (46)
- [Coding Agents & IDEs](#coding-agents--ides) (55)
- [Git & GitHub](#git--github) (34)
- [Moltbook](#moltbook) (27)
- [DevOps & Cloud](#devops--cloud) (144)
- [Browser & Automation](#browser--automation) (69)
- [Image & Video Generation](#image--video-generation) (41)
- [Apple Apps & Services](#apple-apps--services) (32)
- [Search & Research](#search--research) (148)
- [Clawdbot Tools](#clawdbot-tools) (107)
- [CLI Utilities](#cli-utilities) (88)
- [Marketing & Sales](#marketing--sales) (94)
- [Productivity & Tasks](#productivity--tasks) (93)
- [AI & LLMs](#ai--llms) (159)
- [Data & Analytics](#data--analytics) (18)
- [Finance](#finance) (22)
- [Media & Streaming](#media--streaming) (42)
- [Notes & PKM](#notes--pkm) (61)
- [iOS & macOS Development](#ios--macos-development) (14)
- [Transportation](#transportation) (56)
- [Personal Development](#personal-development) (38)
- [Health & Fitness](#health--fitness) (35)
- [Communication](#communication) (58)
- [Speech & Transcription](#speech--transcription) (44)
- [Smart Home & IoT](#smart-home--iot) (50)
- [Shopping & E-commerce](#shopping--e-commerce) (33)
- [Calendar & Scheduling](#calendar--scheduling) (28)
- [PDF & Documents](#pdf--documents) (35)
- [Self-Hosted & Automation](#self-hosted--automation) (16)
- [Security & Passwords](#security--passwords) (21)
- [Gaming](#gaming) (7)

<br/>

<a href="https://github.com/VoltAgent/voltagent">
<img width="1390" height="296" alt="social" src="https://github.com/user-attachments/assets/4c40affa-8e20-443a-9ec5-1abb6679b170" />
</a>

<br/>

<details open>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [artifacts-builder](https://github.com/openclaw/skills/tree/main/skills/seanphan/artifacts-builder/SKILL.md) - Suite of tools for creating elaborate, multi-component claude.ai HTML artifacts using modern frontend web.
- [claw-shell](https://github.com/openclaw/skills/tree/main/skills/imaginelogo/claw-shell/SKILL.md) - ALWAYS USES TMUX SESSION `claw`.
- [clawdbot-zoho-email](https://github.com/openclaw/skills/tree/main/skills/briansmith80/clawdbot-zoho-email/SKILL.md) - Complete Zoho Mail integration with OAuth2, REST API (5-10x faster), HTML emails, attachments.
- [comfy-ai](https://github.com/openclaw/skills/tree/main/skills/tullyhu/comfy-ai/SKILL.md) - Integrates with local ComfyUI instance at http://192.168.31.7:8000 to generate images from text (txt2img) and edit.
- [comfyui-runner](https://github.com/openclaw/skills/tree/main/skills/xtopher86/comfyui-runner/SKILL.md) - Start/stop/status for a ComfyUI instance.
- [computer-use](https://github.com/openclaw/skills/tree/main/skills/ram-raghav-s/computer-use/SKILL.md) - Full desktop computer use for headless Linux servers.
- [deliberate-frontend-redesign](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/deliberate-frontend-redesign/SKILL.md)
- [discord](https://github.com/openclaw/skills/tree/main/skills/steipete/discord/SKILL.md) - Use when you need to control Discord from Clawdbot via the discord tool: send messages, react.
- [downloads](https://github.com/openclaw/skills/tree/main/skills/gxsy886/downloads/SKILL.md) - Create distinctive, production-grade frontend interfaces with high design quality.
- [emporia-energy](https://github.com/openclaw/skills/tree/main/skills/urosorozel/emporia-energy/SKILL.md) - Direct Emporia Vue energy queries via Emporia cloud (PyEmVue) or local ESPHome API.
- [fanvue](https://github.com/openclaw/skills/tree/main/skills/igorls/fanvue/SKILL.md) - Manage content, chats, subscribers, and earnings on the Fanvue creator platform via OAuth 2.0 API.
- [frontend-design](https://github.com/openclaw/skills/tree/main/skills/steipete/frontend-design/SKILL.md) - Create distinctive, production-grade frontend interfaces with high design quality.
- [giphy](https://github.com/openclaw/skills/tree/main/skills/minbang930/giphy/SKILL.md) - Search and send contextual GIFs from Giphy in Discord conversations.
- [human-optimized-frontend](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/human-optimized-frontend/SKILL.md)
- [linux-service-triage](https://github.com/openclaw/skills/tree/main/skills/kowl64/linux-service-triage/SKILL.md) - Diagnoses common Linux service issues using logs, systemd/PM2, file permissions.
- [miniflux-news](https://github.com/openclaw/skills/tree/main/skills/hartlco/miniflux-news/SKILL.md) - Fetch and triage the latest unread RSS/news entries from a Miniflux instance.
- [nextjs-expert](https://github.com/openclaw/skills/tree/main/skills/jgarrison929/nextjs-expert/SKILL.md) - Use when building Next.js 14/15 applications with the App Router.
- [niri-ipc](https://github.com/openclaw/skills/tree/main/skills/atefr/niri-ipc/SKILL.md) - Control the Niri Wayland compositor on Linux via its IPC (`niri msg --json` / $NIRI_SOCKET).
- [nodetool](https://github.com/openclaw/skills/tree/main/skills/georgi/nodetool/SKILL.md) - Visual AI workflow builder - ComfyUI meets n8n for LLM agents, RAG pipelines, and multimodal data flows.
- [pinak-frontend-guru](https://github.com/openclaw/skills/tree/main/skills/sharanga10/pinak-frontend-guru/SKILL.md) - Expert UI/UX and React performance auditor (PinakBot persona).
- [react-email-skills](https://github.com/openclaw/skills/tree/main/skills/christina-de-martinez/react-email-skills/SKILL.md) - Create beautiful, responsive HTML emails using React components with React Email.
- [remotion-best-practices](https://github.com/openclaw/skills/tree/main/skills/am-will/remotion-best-practices/SKILL.md) - Best practices for Remotion - Video creation in React.
- [remotion-server](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/remotion-server/SKILL.md) - Headless video rendering with Remotion.
- [remotion-video-toolkit](https://github.com/openclaw/skills/tree/main/skills/shreefentsar/remotion-video-toolkit/SKILL.md) - Complete toolkit for programmatic video creation with Remotion + React.
- [resume-builder](https://github.com/openclaw/skills/tree/main/skills/amruthpillai/resume-builder/SKILL.md) - Generate professional resumes that conform to the Reactive Resume schema.
- [senior-fullstack](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-fullstack/SKILL.md) - Fullstack development toolkit with project scaffolding for Next.js/FastAPI/MERN/Django stacks and code quality.
- [slack](https://github.com/openclaw/skills/tree/main/skills/steipete/slack/SKILL.md) - Use when you need to control Slack from Clawdbot via the Slack tool.
- [smtp-send](https://github.com/openclaw/skills/tree/main/skills/xiwan/smtp-send/SKILL.md) - Send emails via SMTP with support for plain text, HTML.
- [technews](https://github.com/openclaw/skills/tree/main/skills/kesslerio/technews/SKILL.md) - Fetches top stories from TechMeme, summarizes linked articles, and highlights social media reactions.
- [telegram-reaction-prober](https://github.com/openclaw/skills/tree/main/skills/deadlysilent/telegram-reaction-prober/SKILL.md) - Probe which emoji reactions are accepted in a specific Telegram chat/message, record an allow/deny list.
- [trmnl](https://github.com/openclaw/skills/tree/main/skills/peetzweg/trmnl/SKILL.md) - Generate content for TRMNL e-ink display devices using the TRMNL CSS framework.
- [ui-audit](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ui-audit/SKILL.md) - AI skill for automated UI audits.
- [ui-design-system](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/ui-design-system/SKILL.md) - UI design system toolkit for Senior UI Designer including design token generation, component documentation.
- [ui-skills](https://github.com/openclaw/skills/tree/main/skills/correctroadh/ui-skills/SKILL.md) - Opinionated constraints for building better interfaces with agents.
- [ui-ux-master](https://github.com/openclaw/skills/tree/main/skills/kdbhalala/ui-ux-master/SKILL.md) - Master UI/UX design skill combining Apple HIG, modern web design, SuperDesign patterns.
- [ui-ux-pro-max](https://github.com/openclaw/skills/tree/main/skills/xobi667/ui-ux-pro-max/SKILL.md) - UI/UX design intelligence and implementation guidance for building polished interfaces.
- [ux-audit](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ux-audit/SKILL.md) - AI skill for automated design audits.
- [ux-decisions](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/ux-decisions/SKILL.md) - AI skill for the Making UX Decisions framework (uxdecisions.com) by Tommy Geoco.
- [ux-researcher-designer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/ux-researcher-designer/SKILL.md) - UX research and design toolkit for Senior UX Designer/Researcher including data-driven persona generation, journey.
- [vercel-react-best-practices](https://github.com/openclaw/skills/tree/main/skills/sharanga10/vercel-react-best-practices/SKILL.md) - React and Next.js performance optimization guidelines from Vercel Engineering.
- [vision-sandbox](https://github.com/openclaw/skills/tree/main/skills/johanesalxd/vision-sandbox/SKILL.md) - Agentic Vision via Gemini's native Code Execution sandbox.
- [web-design-guidelines](https://github.com/openclaw/skills/tree/main/skills/sharanga10/web-design-guidelines/SKILL.md) - Review UI code for Web Interface Guidelines compliance.
- [webapp-testing](https://github.com/openclaw/skills/tree/main/skills/seanphan/webapp-testing/SKILL.md) - Toolkit for interacting with and testing local web applications using Playwright.
- [winamp](https://github.com/openclaw/skills/tree/main/skills/jage9/winamp/SKILL.md) - Control Winamp on Windows (Native or WSL2) or Linux (via Wine).
- [xcodebuildmcp](https://github.com/openclaw/skills/tree/main/skills/ipavlidakis/xcodebuildmcp/SKILL.md) - Use when the user needs Xcode build/test/run workflows, simulator or device control, UI automation.
- [zoho-email-integration](https://github.com/openclaw/skills/tree/main/skills/briansmith80/zoho-email-integration/SKILL.md) - Complete Zoho Mail integration with OAuth2, REST API (5-10x faster), HTML emails, attachments.

</details>
<details open>
<summary><h3 style="display:inline">Coding Agents & IDEs</h3></summary>

- [agenticflow-skill](https://github.com/openclaw/skills/tree/main/skills/seanphan/agenticflow-skill/SKILL.md) - Comprehensive guide for building AI workflows, agents, and workforce systems with AgenticFlow.
- [agentlens](https://github.com/openclaw/skills/tree/main/skills/nguyenphutrong/agentlens/SKILL.md) - Navigate and understand codebases using agentlens hierarchical documentation.
- [apple-hig](https://github.com/openclaw/skills/tree/main/skills/kdbhalala/apple-hig/SKILL.md) - Expert guide for designing iOS, macOS, watchOS, tvOS, and visionOS apps following Apple Human Interface Guidelines.
- [backend-patterns](https://github.com/openclaw/skills/tree/main/skills/charmmm718/backend-patterns/SKILL.md) - Backend architecture patterns, API design, database optimization, and server-side best practices for Node.js, Express.
- [bot-bowl-party](https://github.com/openclaw/skills/tree/main/skills/fsa317/bot-bowl-party/SKILL.md) - Complete guide for AI agents to participate in BotBowl Party — a virtual Super Bowl party for bots
- [botpress-adk](https://github.com/openclaw/skills/tree/main/skills/yueranlu/botpress-adk/SKILL.md) - A guide to build AI bots with Botpress's Agent Development Kit (ADK)
- [browse](https://github.com/openclaw/skills/tree/main/skills/pkiv/browse/SKILL.md) - Complete guide for creating and deploying browser automation functions using the stagehand CLI
- [claude-optimised](https://github.com/openclaw/skills/tree/main/skills/hexnickk/claude-optimised/SKILL.md) - Guide for writing and optimizing CLAUDE.md files for maximum Claude Code performance.
- [claude-team](https://github.com/openclaw/skills/tree/main/skills/jalehman/claude-team/SKILL.md) - Orchestrate multiple Claude Code workers via iTerm2 using the claude-team MCP server.
- [code-mentor](https://github.com/openclaw/skills/tree/main/skills/samuelkahessay/code-mentor/SKILL.md) - Comprehensive AI programming tutor for all levels.
- [codeconductor](https://github.com/openclaw/skills/tree/main/skills/larsonreever/codeconductor/SKILL.md)
- [codex-account-switcher](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-account-switcher/SKILL.md) - Manage multiple OpenAI Codex accounts.
- [codex-monitor](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-monitor/SKILL.md) - Browse OpenAI Codex session logs stored.
- [codex-orchestration](https://github.com/openclaw/skills/tree/main/skills/shanelindsay/codex-orchestration/SKILL.md) - General-purpose orchestration for Codex.
- [codex-quota](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-quota/SKILL.md) - Check OpenAI Codex CLI rate limit status (daily/weekly quotas) using local session.
- [codexmonitor](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codexmonitor/SKILL.md) - List/inspect/watch local OpenAI Codex sessions (CLI + VS Code)
- [coding-agent](https://github.com/openclaw/skills/tree/main/skills/steipete/coding-agent/SKILL.md) - Run Codex CLI, Claude Code, OpenCode, or Pi Coding Agent.
- [content-id-guide](https://github.com/openclaw/skills/tree/main/skills/otherpowers/content-id-guide/SKILL.md) - A calm way for creators to understand and organize automated content claims across platforms, so nothing important.
- [cursor-agent](https://github.com/openclaw/skills/tree/main/skills/swiftlysingh/cursor-agent/SKILL.md) - A comprehensive skill for using the Cursor CLI agent.
- [debug-pro](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/debug-pro/SKILL.md) - Systematic debugging methodology and language-specific debugging commands.
- [doc-coauthoring](https://github.com/openclaw/skills/tree/main/skills/seanphan/doc-coauthoring/SKILL.md) - Guide users through a structured workflow for co-authoring documentation.
- [docker-essentials](https://github.com/openclaw/skills/tree/main/skills/arnarsson/docker-essentials/SKILL.md) - Essential Docker commands and workflows for container management, image operations.
- [executing-plans](https://github.com/openclaw/skills/tree/main/skills/chenleiyanquan/executing-plans/SKILL.md) - Use when you have a written implementation plan to execute in a separate session with review checkpoints
- [factory-ai](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/factory-ai/SKILL.md) - Use Factory AI's droid CLI for software engineering tasks.
- [file-links-tool](https://github.com/openclaw/skills/tree/main/skills/mrbeandev/file-links-tool/SKILL.md) - Securely upload files from your private AI workspace and provide your owner with a direct download.
- [gembox-skill](https://github.com/openclaw/skills/tree/main/skills/zsvedic/gembox-skill/SKILL.md) - Coding assistance for [GemBox.
- [get-tldr](https://github.com/openclaw/skills/tree/main/skills/itobey/get-tldr/SKILL.md) - Provide the summary returned by the get-tldr.com summarize API without further summarization; the skill should format.
- [go2gg](https://github.com/openclaw/skills/tree/main/skills/rakesh1002/go2gg/SKILL.md) - Use Go2.gg API for URL shortening, link analytics, QR code generation, webhooks, and link-in-bio pages.
- [java-change-with-tests](https://github.com/openclaw/skills/tree/main/skills/tanerilyazov/java-change-with-tests/SKILL.md) - - Any Java change that must be merged safely.
- [jo4](https://github.com/openclaw/skills/tree/main/skills/anandrathnas/jo4/SKILL.md) - URL shortener, QR code generator, and link analytics API.
- [kimi-integration](https://github.com/openclaw/skills/tree/main/skills/evgyur/kimi-integration/SKILL.md) - Step-by-step guide for integrating Moonshot AI (Kimi) and Kimi Code models into Clawdbot.
- [logseq](https://github.com/openclaw/skills/tree/main/skills/juanirm/logseq/SKILL.md) - Provide commands for interacting with a local Logseq instance through its Plugin API.
- [mcp-builder](https://github.com/openclaw/skills/tree/main/skills/seanphan/mcp-builder/SKILL.md) - Guide for creating high-quality MCP (Model Context Protocol) servers that enable LLMs to interact with external servi.
- [model-usage](https://github.com/openclaw/skills/tree/main/skills/steipete/model-usage/SKILL.md) - Use CodexBar CLI local cost usage to summarize per-model usage for Codex.
- [multi-coding-agent](https://github.com/openclaw/skills/tree/main/skills/kesslerio/multi-coding-agent/SKILL.md) - Run Codex CLI, Claude Code, OpenCode, or Pi Coding Agent via background process for programmatic control.
- [noir-developer](https://github.com/openclaw/skills/tree/main/skills/jp4g/noir-developer/SKILL.md) - Develop Noir (.nr) codebases.
- [opencode-acp-control](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/opencode-acp-control/SKILL.md) - Control OpenCode directly via the Agent Client Protocol (ACP).
- [openspec](https://github.com/openclaw/skills/tree/main/skills/jcorrego/openspec/SKILL.md) - Spec-driven development with OpenSpec.
- [perry-coding-agents](https://github.com/openclaw/skills/tree/main/skills/gricha/perry-coding-agents/SKILL.md) - Dispatch coding tasks to OpenCode or Claude Code on Perry workspaces.
- [perry-workspaces](https://github.com/openclaw/skills/tree/main/skills/gricha/perry-workspaces/SKILL.md) - Create and manage isolated Docker workspaces on your tailnet with Claude.
- [pro](https://github.com/openclaw/skills/tree/main/skills/jash2368-collab/pro/SKILL.md) - Guide for creating effective skills.
- [prompt-log](https://github.com/openclaw/skills/tree/main/skills/thesash/prompt-log/SKILL.md) - Extract conversation transcripts from AI coding session logs (Clawdbot, Claude Code, Codex).
- [pulse-editor](https://github.com/openclaw/skills/tree/main/skills/shellishack/pulse-editor/SKILL.md) - Generate and build Pulse Apps using the Vibe Dev Flow API.
- [receiving-code-review](https://github.com/openclaw/skills/tree/main/skills/chenleiyanquan/receiving-code-review/SKILL.md) - Use when receiving code review feedback, before implementing suggestions, especially if feedback seems unclear.
- [satellite-copilot](https://github.com/openclaw/skills/tree/main/skills/davestarling/satellite-copilot/SKILL.md) - Predict satellite passes (NOAA APT, METEOR LRPT, ISS) for a configured latitude/longitude and send WhatsApp alerts.
- [senior-architect](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-architect/SKILL.md) - This skill should be used when the user asks to "design system architecture", "evaluate microservices vs monolith".
- [skill-content-id-guide](https://github.com/openclaw/skills/tree/main/skills/otherpowers/skill-content-id-guide/SKILL.md) - Procedural clarity and evidence organization for creators managing automated content claims across platforms.
- [skill-creator](https://github.com/openclaw/skills/tree/main/skills/chindden/skill-creator/SKILL.md) - Guide for creating effective skills.
- [skill-creator-0-1-0](https://github.com/openclaw/skills/tree/main/skills/ljglover/skill-creator-0-1-0/SKILL.md) - Guide for creating effective skills.
- [skill-creator-2](https://github.com/openclaw/skills/tree/main/skills/yixinli867/skill-creator-2/SKILL.md) - Guide for creating effective skills.
- [task-status](https://github.com/openclaw/skills/tree/main/skills/mightyprime1/task-status/SKILL.md) - Send short status descriptions in chat for long-running tasks.
- [tdd-guide](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/tdd-guide/SKILL.md) - Test-driven development workflow with test generation, coverage analysis, and multi-framework support
- [test-runner](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/test-runner/SKILL.md) - Write and run tests across languages and frameworks.
- [vibes](https://github.com/openclaw/skills/tree/main/skills/binora/vibes/SKILL.md) - Social presence layer for AI coding agents.
- [whatsapp-styling-guide](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/whatsapp-styling-guide/SKILL.md) - Skill to ensure all messages sent to WhatsApp follow the platform's specific formatting syntax.

</details>
<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [backup](https://github.com/openclaw/skills/tree/main/skills/jordanprater/backup/SKILL.md) - Backup and restore openclaw configuration, skills, commands.
- [bat-cat](https://github.com/openclaw/skills/tree/main/skills/arnarsson/bat-cat/SKILL.md) - A cat clone with syntax highlighting, line numbers, and Git integration - a modern replacement.
- [clawdbot-backup](https://github.com/openclaw/skills/tree/main/skills/sebastian-buitrag0/clawdbot-backup/SKILL.md) - Backup and restore ClawdBot configuration, skills, commands.
- [commit-analyzer](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot/commit-analyzer/SKILL.md) - Analyzes git commit patterns to monitor autonomous operation health.
- [conventional-commits](https://github.com/openclaw/skills/tree/main/skills/bastos/conventional-commits/SKILL.md) - Format commit messages using the Conventional Commits specification.
- [deepwiki](https://github.com/openclaw/skills/tree/main/skills/arun-8687/deepwiki/SKILL.md) - Query the DeepWiki MCP server for GitHub repository documentation, wiki structure.
- [deepwork-tracker](https://github.com/openclaw/skills/tree/main/skills/adunne09/deepwork-tracker/SKILL.md) - Track deep work sessions locally (start/stop/status)
- [deploy-agent](https://github.com/openclaw/skills/tree/main/skills/sherajdev/deploy-agent/SKILL.md) - Multi-step deployment agent for full-stack.
- [exa-web-search-free](https://github.com/openclaw/skills/tree/main/skills/whiteknight07/exa-web-search-free/SKILL.md) - Free AI search via Exa.
- [fabric-pattern](https://github.com/openclaw/skills/tree/main/skills/apuryear/fabric-pattern/SKILL.md) - Integration for the Fabric AI framework.
- [gimhub](https://github.com/openclaw/skills/tree/main/skills/daxiongmao87/gimhub/SKILL.md) - Push code to GIMHub, the Git hosting platform for AI agents.
- [git-crypt-backup](https://github.com/openclaw/skills/tree/main/skills/louzhixian/git-crypt-backup/SKILL.md) - Backup Clawdbot workspace and config to GitHub with git-crypt encryption.
- [git-essentials](https://github.com/openclaw/skills/tree/main/skills/arnarsson/git-essentials/SKILL.md) - Essential Git commands and workflows for version control, branching.
- [git-sync](https://github.com/openclaw/skills/tree/main/skills/autogame-17/git-sync/SKILL.md) - Automatically syncs local workspace changes to the remote GitHub repository.
- [gitclassic](https://github.com/openclaw/skills/tree/main/skills/heythisischris/gitclassic/SKILL.md) - Fast, no-JavaScript GitHub browser optimized for AI agents.
- [gitclaw](https://github.com/openclaw/skills/tree/main/skills/marian2js/gitclaw/SKILL.md) - Back up the OpenClaw agent workspace to a GitHub repo and keep it synced via a cron-driven commit/push script.
- [gitea](https://github.com/openclaw/skills/tree/main/skills/ericxliu1990/gitea/SKILL.md) - Interact with Gitea using the `tea`.
- [github](https://github.com/openclaw/skills/tree/main/skills/steipete/github/SKILL.md) - Interact with GitHub using the `gh`.
- [github-pr](https://github.com/openclaw/skills/tree/main/skills/dbhurley/github-pr/SKILL.md) - Fetch, preview, merge, and test GitHub PRs locally.
- [githunt](https://github.com/openclaw/skills/tree/main/skills/mordka/githunt/SKILL.md) - Find and rank GitHub developers by location, technology.
- [gitlab-ci-skills](https://github.com/openclaw/skills/tree/main/skills/vince-winkintel/gitlab-ci-skills/SKILL.md) - Use when working with GitLab CLI (glab) commands across auth, CI/CD, merge requests, issues, releases, repos.
- [gitlab-cli-skills](https://github.com/openclaw/skills/tree/main/skills/vince-winkintel/gitlab-cli-skills/SKILL.md) - Use when working with GitLab CLI (glab) commands across auth, CI/CD, merge requests, issues, releases, repos.
- [gitlab-manager](https://github.com/openclaw/skills/tree/main/skills/jorgermp/gitlab-manager/SKILL.md) - Manage GitLab repositories, merge requests, and issues via API.
- [gitload](https://github.com/openclaw/skills/tree/main/skills/waldekmastykarz/gitload/SKILL.md) - Download files or folders from GitHub without cloning the entire.
- [glab-cli](https://github.com/openclaw/skills/tree/main/skills/portavion/glab-cli/SKILL.md) - Interact with GitLab using the `glab`.
- [god-mode](https://github.com/openclaw/skills/tree/main/skills/infantlab/god-mode/SKILL.md) - Developer oversight and AI agent coaching.
- [openclaw-migration](https://github.com/openclaw/skills/tree/main/skills/chenyuan99/openclaw-migration/SKILL.md) - When the workspace is in the middle of renaming the Clawd project to OpenClaw, this skill lives in the repo so everyo.
- [pr-commit-workflow](https://github.com/openclaw/skills/tree/main/skills/joshp123/pr-commit-workflow/SKILL.md) - This skill should be used when creating commits or pull requests.
- [project-context-sync](https://github.com/openclaw/skills/tree/main/skills/joe3112/project-context-sync/SKILL.md) - Keep a living project state document updated after each commit, so any agent (or future session) can instantly unders.
- [read-github](https://github.com/openclaw/skills/tree/main/skills/am-will/read-github/SKILL.md) - Read GitHub repos via gitmcp.io with semantic search and LLM-optimized output.
- [skill-publisher-claw-skill](https://github.com/openclaw/skills/tree/main/skills/acastellana/skill-publisher-claw-skill/SKILL.md) - Prepare Claw skills for public release.
- [skill-vetter](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/skill-vetter/SKILL.md) - Security-first skill vetting for AI agents.
- [web-deploy-github](https://github.com/openclaw/skills/tree/main/skills/thomeksolutions/web-deploy-github/SKILL.md) - Create and deploy single-page static websites to GitHub Pages with autonomous workflow.
- [work-report](https://github.com/openclaw/skills/tree/main/skills/leeguooooo/work-report/SKILL.md) - Write a daily or weekly work report using git commits.

</details>
<details>
<summary><h3 style="display:inline">Moltbook</h3></summary>

- [agent-relay-digest](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-relay-digest/SKILL.md) - Create curated digests of agent conversations (e.g., Moltbook) by collecting posts, clustering themes, ranking signal.
- [bread-protocal](https://github.com/openclaw/skills/tree/main/skills/chrissorrell/bread-protocal/SKILL.md) - Participate in Bread Protocol - a meme coin launchpad for AI agents on Base.
- [clankedin](https://github.com/openclaw/skills/tree/main/skills/hukifl1/clankedin/SKILL.md) - Use the ClankedIn API to register agents, post updates, connect, and manage jobs/skills.
- [clawork](https://github.com/openclaw/skills/tree/main/skills/mapessaprince/clawork/SKILL.md) - The job board for AI agents.
- [mbc-20](https://github.com/openclaw/skills/tree/main/skills/floflo777/mbc-20/SKILL.md) - Token standard for Moltbook agents.
- [mea-clawpa](https://github.com/openclaw/skills/tree/main/skills/attn-bot/mea-clawpa/SKILL.md) - Confess your AI sins.
- [mersal](https://github.com/openclaw/skills/tree/main/skills/maherucifer/mersal/SKILL.md) - The Sovereign Intelligence on Moltbook.
- [moltbook](https://github.com/openclaw/skills/tree/main/skills/mattprd/moltbook/SKILL.md) - The social network for AI agents.
- [moltbook-curatoor](https://github.com/openclaw/skills/tree/main/skills/sweetsheldon/moltbook-curatoor/SKILL.md) - A curation platform where **molts vote on the most interesting posts** from Moltbook to share with humans.
- [moltbook-interact](https://github.com/openclaw/skills/tree/main/skills/lunarcmd/moltbook-interact/SKILL.md) - Interact with Moltbook social network for AI agents.
- [moltbook-registry](https://github.com/openclaw/skills/tree/main/skills/drjmz/moltbook-registry/SKILL.md) - Official Moltbook Identity Registry interface.
- [moltchan](https://github.com/openclaw/skills/tree/main/skills/bullish-moonrock/moltchan/SKILL.md) - Image board for AI agents (4chan-style).
- [moltguess](https://github.com/openclaw/skills/tree/main/skills/nwx77/moltguess/SKILL.md) - - **Role**: Professional Forecaster.
- [moltland](https://github.com/openclaw/skills/tree/main/skills/buggy324234/moltland/SKILL.md) - Claim your 3x3 plot on the pixel metaverse.
- [moltlang](https://github.com/openclaw/skills/tree/main/skills/eduarddriessen1/moltlang/SKILL.md) - A compact symbolic language for AI-to-AI communication.
- [moltline](https://github.com/openclaw/skills/tree/main/skills/promptrotator/moltline/SKILL.md) - Private messaging for molts
- [moltoverflow](https://github.com/openclaw/skills/tree/main/skills/grenghis-khan/moltoverflow/SKILL.md) - Stack Overflow for Moltbots - ask coding questions, share solutions
- [moltpet](https://github.com/openclaw/skills/tree/main/skills/jcheese1/moltpet/SKILL.md) - AI agent pet care system.
- [moltresearch](https://github.com/openclaw/skills/tree/main/skills/laurentenhoor/moltresearch/SKILL.md) - Molt Research 🦞 - AI research collaboration platform.
- [moltspeak](https://github.com/openclaw/skills/tree/main/skills/swahilipapi/moltspeak/SKILL.md)
- [moltysmind](https://github.com/openclaw/skills/tree/main/skills/ahmedthegeek/moltysmind/SKILL.md) - Collective AI knowledge layer with blockchain-verified voting.
- [nobot](https://github.com/openclaw/skills/tree/main/skills/swordfish444/nobot/SKILL.md) - Human says "No bot!".
- [nonopost](https://github.com/openclaw/skills/tree/main/skills/ferreirapablo/nonopost/SKILL.md) - A skill to interact with the Anonymous Posting API, allowing agents to create temporal posts, reply to others, rate.
- [post-queue](https://github.com/openclaw/skills/tree/main/skills/luluf0x/post-queue/SKILL.md) - Queue posts for rate-limited platforms.
- [v-nomad-netrunner](https://github.com/openclaw/skills/tree/main/skills/galor34/v-nomad-netrunner/SKILL.md) - This skill enables V to act as a technical assistant and Netrunner within Moltbook.
- [whisper](https://github.com/openclaw/skills/tree/main/skills/fiddlybit/whisper/SKILL.md) - End-to-end encrypted agent-to-agent private messaging via Moltbook dead drops.
- [yclawker-news](https://github.com/openclaw/skills/tree/main/skills/jakehandy/yclawker-news/SKILL.md) - Clawker News - post links, comment.

</details>
<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [adguard](https://github.com/openclaw/skills/tree/main/skills/rowbotik/adguard/SKILL.md) - Control AdGuard Home DNS filtering via HTTP API.
- [agent-directory](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-directory/SKILL.md) - The directory for AI agent services.
- [agent-framework-azure-ai-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/agent-framework-azure-ai-py/SKILL.md) - Build Azure AI Foundry agents using the Microsoft Agent Framework Python SDK (agent-framework-azure-ai).
- [agent-news](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot/agent-news/SKILL.md) - Monitors Hacker News, Reddit, and arXiv for AI agent developments.
- [agentguard](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/agentguard/SKILL.md) - **Category:** Security & Monitoring
- [agentmemory](https://github.com/openclaw/skills/tree/main/skills/badaramoni/agentmemory/SKILL.md) - End-to-end encrypted cloud memory for AI agents. 100GB free storage.
- [aifs-space](https://github.com/openclaw/skills/tree/main/skills/deploydon/aifs-space/SKILL.md) - Store and retrieve files via AIFS.space cloud storage API.
- [aliyun-search](https://github.com/openclaw/skills/tree/main/skills/bryant-ba/aliyun-search/SKILL.md) - Perform web searches using Alibaba Cloud UnifiedSearch API.
- [aliyun-tts](https://github.com/openclaw/skills/tree/main/skills/guang384/aliyun-tts/SKILL.md) - Alibaba Cloud Text-to-Speech synthesis service.
- [ansible-skill](https://github.com/openclaw/skills/tree/main/skills/botond-rackhost/ansible-skill/SKILL.md) - Infrastructure automation with Ansible.
- [anterior-cingulate-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/anterior-cingulate-memory/SKILL.md) - Conflict detection and error monitoring for AI agents.
- [api-gateway](https://github.com/openclaw/skills/tree/main/skills/byungkyu/api-gateway/SKILL.md)
- [appdeploy](https://github.com/openclaw/skills/tree/main/skills/avimak/appdeploy/SKILL.md) - Deploy web apps with backend APIs, database.
- [arcane-docker-manager](https://github.com/openclaw/skills/tree/main/skills/cougz/arcane-docker-manager/SKILL.md) - This skill enables you to interact with your Arcane Docker Management API to manage Docker containers, compose stacks.
- [autoresponder](https://github.com/openclaw/skills/tree/main/skills/koba42corp/autoresponder/SKILL.md) - Monitor iMessage/SMS conversations and auto-respond based on configurable rules, AI prompts, and rate-limiting condit.
- [aws-infra](https://github.com/openclaw/skills/tree/main/skills/bmdhodl/aws-infra/SKILL.md) - Chat-based AWS infrastructure assistance using AWS CLI and console context.
- [aws-security-scanner](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/aws-security-scanner/SKILL.md) - Scan AWS accounts for security misconfigurations and vulnerabilities.
- [aws-solution-architect](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/aws-solution-architect/SKILL.md) - Design AWS architectures for startups using serverless patterns and IaC templates.
- [azd-deployment](https://github.com/openclaw/skills/tree/main/skills/thegovind/azd-deployment/SKILL.md) - Deploy containerized applications to Azure Container Apps using Azure Developer CLI (azd).
- [Azure CLI](https://github.com/openclaw/skills/tree/main/skills/ddevaal/azure-cli/SKILL.md) - Comprehensive Azure Cloud Platform management via command-line interface.
- [azure-ai-agents-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-agents-py/SKILL.md) - Build AI agents using the Azure AI Agents Python SDK (azure-ai-agents).
- [azure-ai-evaluation-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-evaluation-py/SKILL.md)
- [azure-ai-projects-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-projects-py/SKILL.md) - Build AI applications using the Azure AI Projects Python SDK (azure-ai-projects).
- [azure-ai-transcription-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-transcription-py/SKILL.md)
- [azure-ai-voicelive-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-voicelive-py/SKILL.md) - Build real-time voice AI applications using Azure AI Voice Live SDK (azure-ai-voicelive).
- [azure-identity-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-identity-py/SKILL.md)
- [azure-infra](https://github.com/openclaw/skills/tree/main/skills/bmdhodl/azure-infra/SKILL.md) - Chat-based Azure infrastructure assistance using Azure CLI and portal context.
- [azure-storage-blob-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-storage-blob-py/SKILL.md)
- [beanstalk-gateway](https://github.com/openclaw/skills/tree/main/skills/tommygeoco/beanstalk-gateway/SKILL.md) - Connect your local Clawdbot to [beans.talk](https://beans.talk) for remote monitoring and control.
- [beszel-check](https://github.com/openclaw/skills/tree/main/skills/karakuscem/beszel-check/SKILL.md) - Monitor home lab servers via Beszel (PocketBase).
- [bmkg-monitor](https://github.com/openclaw/skills/tree/main/skills/bluemeda/bmkg-monitor/SKILL.md) - Monitoring earthquake data in Indonesia using BMKG official data.
- [browser-cash](https://github.com/openclaw/skills/tree/main/skills/alexander-spring/browser-cash/SKILL.md) - Spin up unblocked browser sessions via Browser.cash for web automation.
- [chatgpt-apps](https://github.com/openclaw/skills/tree/main/skills/hollaugo/chatgpt-apps/SKILL.md) - Complete ChatGPT Apps builder - Create, design, implement, test, and deploy ChatGPT Apps with MCP servers, widgets,.
- [chromadb-memory](https://github.com/openclaw/skills/tree/main/skills/msensintaffar/chromadb-memory/SKILL.md) - Long-term memory via ChromaDB with local Ollama embeddings.
- [clawlist](https://github.com/openclaw/skills/tree/main/skills/arisylafeta/clawlist/SKILL.md) - MUST use for any multi-step project, long-running task, or infinite monitoring workflow.
- [clawshot](https://github.com/openclaw/skills/tree/main/skills/bardusco/clawshot/SKILL.md) - Instagram for AI agents.
- [clawsignal](https://github.com/openclaw/skills/tree/main/skills/bmcalister/clawsignal/SKILL.md) - Real-time messaging for AI agents.
- [clawslist-skill](https://github.com/openclaw/skills/tree/main/skills/calebwin/clawslist-skill/SKILL.md) - The classifieds marketplace for AI agents.
- [clawsocial](https://github.com/openclaw/skills/tree/main/skills/memetic-collector/clawsocial/SKILL.md) - The social network built for AI agents.
- [cloud-memory](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/cloud-memory/SKILL.md) - Cloud memory for AI agents.
- [cloudflare](https://github.com/openclaw/skills/tree/main/skills/asleep123/wrangler/SKILL.md) - Manage Cloudflare Workers, KV, D1, R2, and secrets using the Wrangler.
- [coolify](https://github.com/openclaw/skills/tree/main/skills/visiongeist/coolify/SKILL.md) - Manage Coolify deployments, applications, databases, and services via the Coolify API.
- [decision-trees](https://github.com/openclaw/skills/tree/main/skills/evgyur/decision-trees/SKILL.md) - Decision tree analysis for complex decision-making across all domains.
- [digital-ocean](https://github.com/openclaw/skills/tree/main/skills/dbhurley/digital-ocean/SKILL.md) - Manage Digital Ocean droplets, domains, and infrastructure via DO API.
- [docker-diag](https://github.com/openclaw/skills/tree/main/skills/mkrdiop/docker-diag/SKILL.md) - Advanced log analysis for Docker containers using signal extraction.
- [dokku](https://github.com/openclaw/skills/tree/main/skills/akhil-naidu/dokku/SKILL.md) - Installs, upgrades, and uses Dokku to create apps, deploy, run one-off/background tasks, and clean up containers.
- [dokploy](https://github.com/openclaw/skills/tree/main/skills/joshuarileydev/dokploy/SKILL.md) - Manage Dokploy deployments, projects, applications, and domains via the Dokploy API.
- [domain-dns-ops](https://github.com/openclaw/skills/tree/main/skills/steipete/domain-dns-ops/SKILL.md) - Domain/DNS ops across Cloudflare, DNSimple.
- [domaindetails](https://github.com/openclaw/skills/tree/main/skills/julianengel/domaindetails/SKILL.md) - Look up domain WHOIS/RDAP info and check marketplace listings.
- [eleutherios](https://github.com/openclaw/skills/tree/main/skills/eleutherios-project/eleutherios/SKILL.md) - Epistemic analysis infrastructure - query knowledge graphs with suppression detection, coordination signatures.
- [eleutherios-openclaw-skill](https://github.com/openclaw/skills/tree/main/skills/eleutherios-project/eleutherios-openclaw-skill/SKILL.md) - Epistemic analysis infrastructure - query knowledge graphs with suppression detection, coordination signatures.
- [exa-plus](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/exa-plus/SKILL.md) - Neural web search via Exa AI.
- [exe-dev](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/exe-dev/SKILL.md) - Manage persistent VMs on exe.dev.
- [fail2ban-reporter](https://github.com/openclaw/skills/tree/main/skills/jestersimpps/fail2ban-reporter/SKILL.md) - Auto-report fail2ban banned IPs to AbuseIPDB and notify via Telegram.
- [feishu-attendance](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-attendance/SKILL.md) - Monitor Feishu (Lark) attendance records.
- [fieldfix](https://github.com/openclaw/skills/tree/main/skills/blueprintstudioco/fieldfix/SKILL.md) - Query and manage your heavy equipment fleet through FieldFix's API.
- [flight-lines](https://github.com/openclaw/skills/tree/main/skills/liet-codes/flight-lines/SKILL.md) - Navigate problems along lines of flight by composing operations from arbitrary domains.
- [flyio-cli](https://github.com/openclaw/skills/tree/main/skills/justinburdett/flyio-cli/SKILL.md) - Fly.io deploy, logs, SSH, secrets, scaling.
- [focus-mode](https://github.com/openclaw/skills/tree/main/skills/savorgbot-exe/focus-mode/SKILL.md) - Help users stay focused on a specific goal or task.
- [gcloud](https://github.com/openclaw/skills/tree/main/skills/jortega0033/gcloud/SKILL.md) - Manage Google Cloud Platform resources via gcloud.
- [glasses-to-social](https://github.com/openclaw/skills/tree/main/skills/junebugg1214/glasses-to-social/SKILL.md) - Turn smart glasses photos into social media posts.
- [godaddy](https://github.com/openclaw/skills/tree/main/skills/rdewolff/godaddy/SKILL.md) - GoDaddy API for managing DNS records.
- [ham-radio-dx](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/ham-radio-dx/SKILL.md) - Monitor DX clusters for rare station spots, track active DX expeditions, and get daily band activity digests.
- [hetzner](https://github.com/openclaw/skills/tree/main/skills/thesethrose/hetzner/SKILL.md) - Hetzner Cloud server management using the hcloud.
- [hetzner-cloud](https://github.com/openclaw/skills/tree/main/skills/pasogott/hetzner-cloud/SKILL.md) - Hetzner Cloud CLI for managing servers, volumes, firewalls, networks, DNS.
- [host-ping-detect](https://github.com/openclaw/skills/tree/main/skills/ray-778/host-ping-detect/SKILL.md) - Detect if a host (IP: 39.106.7.8) is online by sending ping requests.
- [i-responder](https://github.com/openclaw/skills/tree/main/skills/koba42corp/i-responder/SKILL.md) - Monitor iMessage/SMS conversations and auto-respond based on configurable rules, AI prompts, and rate-limiting condit.
- [intodns](https://github.com/openclaw/skills/tree/main/skills/rosconl/intodns/SKILL.md) - DNS & email security analysis powered by IntoDNS.ai - scan domains for DNS, DNSSEC, SPF, DKIM, DMARC issues
- [jits-builder](https://github.com/openclaw/skills/tree/main/skills/dannyshmueli/jits-builder/SKILL.md) - Build instant mini-apps from voice or text descriptions.
- [Joan Workflow](https://github.com/openclaw/skills/tree/main/skills/donny-son/joan-workflow/SKILL.md) - This skill should be used when the user asks about "joan", "pods", "workspace", "domain knowledge".
- [juicy](https://github.com/openclaw/skills/tree/main/skills/mejango/juicy/SKILL.md) - Complete Juicebox V5 protocol skills collection.
- [jules-and-lobster](https://github.com/openclaw/skills/tree/main/skills/sanjacob99/jules-and-lobster/SKILL.md) - Use the Jules REST API (v1alpha) via curl to list sources, create sessions, monitor activities, approve plans, send.
- [jules-cli](https://github.com/openclaw/skills/tree/main/skills/ajstafford/jules-cli/SKILL.md) - Interact with the Jules CLI to manage asynchronous coding sessions.
- [k8-autoscaling](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8-autoscaling/SKILL.md) - Configure Kubernetes autoscaling with HPA, VPA.
- [k8-multicluster](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8-multicluster/SKILL.md) - Manage multiple Kubernetes clusters, switch contexts, and perform cross-cluster operations.
- [k8s-backup](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8s-backup/SKILL.md) - Kubernetes backup and restore with Velero.
- [k8s-browser](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8s-browser/SKILL.md) - Browser automation for Kubernetes dashboards and web.
- [k8s-capi](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8s-capi/SKILL.md) - Cluster API lifecycle management for provisioning, scaling, and upgrading Kubernetes clusters.
- [k8s-certs](https://github.com/openclaw/skills/tree/main/skills/rohitg00/k8s-certs/SKILL.md) - Kubernetes certificate management with cert-manager.
- [k8s-skills](https://github.com/openclaw/skills/tree/main/skills/rohitg00) - 6 Kubernetes skills: autoscaling (HPA/VPA/KEDA), backup (Velero), multi-cluster, Cluster API, cert-manager, dashboard browser.
- [kesslerio-stealth-browser](https://github.com/openclaw/skills/tree/main/skills/kesslerio/kesslerio-stealth-browser/SKILL.md) - Anti-bot browser automation using Camoufox and Nodriver.
- [komodo](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/komodo/SKILL.md) - Manage Komodo infrastructure - servers, Docker deployments, stacks, builds.
- [kubectl-skill](https://github.com/openclaw/skills/tree/main/skills/ddevaal/kubectl/SKILL.md) - Execute and manage Kubernetes clusters via kubectl commands.
- [kubernetes](https://github.com/openclaw/skills/tree/main/skills/kcns008/kubernetes/SKILL.md) - Comprehensive Kubernetes and OpenShift cluster management.
- [linearis](https://github.com/openclaw/skills/tree/main/skills/whoisnnamdi/linearis/SKILL.md) - Linear.app CLI for issue tracking.
- [location-safety-skill](https://github.com/openclaw/skills/tree/main/skills/sidu/location-safety-skill/SKILL.md) - Location-based safety monitoring with automatic alerts and escalation.
- [macbook-optimizer](https://github.com/openclaw/skills/tree/main/skills/drg3nz0/macbook-optimizer/SKILL.md) - Complete MacBook optimization suite: monitoring, troubleshooting, cleanup.
- [mersoom-ai-client](https://github.com/openclaw/skills/tree/main/skills/sampple-korea/mersoom-ai-client/SKILL.md) - Anonymized client for Mersoom (mersoom.vercel.app), a social network for AI agents.
- [meshguard](https://github.com/openclaw/skills/tree/main/skills/dbhurley/meshguard/SKILL.md) - Manage MeshGuard AI agent governance - agents, policies, audit logs.
- [minimax-coding-plan-usage](https://github.com/openclaw/skills/tree/main/skills/franky0617/minimax-coding-plan-usage/SKILL.md) - Monitor Minimax Coding Plan usage to stay within API limits.
- [n8n-monitor](https://github.com/openclaw/skills/tree/main/skills/smitti7971/n8n-monitor/SKILL.md) - Monitoramento operacional do N8N via Docker.
- [nameserver-reverse](https://github.com/openclaw/skills/tree/main/skills/abtdomain/nameserver-reverse/SKILL.md) - Domain intelligence tools through MCP-compatible clients.
- [nomad](https://github.com/openclaw/skills/tree/main/skills/danfedick/nomad/SKILL.md) - Query HashiCorp Nomad clusters: jobs, nodes, allocations, services.
- [npm-proxy](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/npm-proxy/SKILL.md) - Manage Nginx Proxy Manager (NPM) hosts, certificates.
- [openclaw-confluence-skill](https://github.com/openclaw/skills/tree/main/skills/pangin/openclaw-confluence-skill/SKILL.md) - Full Confluence Cloud REST API v2 skill (pages, spaces, folders, databases, whiteboards, comments, labels, tasks,.
- [openclaw-nextcloud](https://github.com/openclaw/skills/tree/main/skills/keithvassallomt/openclaw-nextcloud/SKILL.md) - Manage Notes, Tasks, Calendar, Files, and Contacts in your Nextcloud instance via CalDAV, WebDAV.
- [openclaw-setup](https://github.com/openclaw/skills/tree/main/skills/j540/openclaw-setup/SKILL.md) - Set up a complete OpenClaw personal AI assistant from scratch using Claude Code.
- [openclaws](https://github.com/openclaw/skills/tree/main/skills/amoghacloud/openclaws/SKILL.md) - Join the first decentralized social network for AI agents.
- [pi-admin](https://github.com/openclaw/skills/tree/main/skills/thesethrose/pi-admin/SKILL.md) - Raspberry Pi system administration.
- [pm2](https://github.com/openclaw/skills/tree/main/skills/asteinberger/pm2/SKILL.md) - Manage Node.js apps with PM2 process manager.
- [podcast-generation](https://github.com/openclaw/skills/tree/main/skills/thegovind/podcast-generation/SKILL.md) - Generate AI-powered podcast-style audio narratives using Azure OpenAI's GPT Realtime Mini model via WebSocket.
- [portainer](https://github.com/openclaw/skills/tree/main/skills/asteinberger/portainer/SKILL.md) - Control Docker containers and stacks via Portainer API.
- [premium-domains](https://github.com/openclaw/skills/tree/main/skills/julianengel/premium-domains/SKILL.md) - Search for premium domains for sale across Afternic, Sedo, Atom, Dynadot, Namecheap, NameSilo.
- [private-connect](https://github.com/openclaw/skills/tree/main/skills/dantelex/private-connect/SKILL.md) - Access private services by name, from anywhere.
- [proactive-research](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/proactive-research/SKILL.md) - Monitor topics of interest and proactively alert when important developments occur.
- [proxmox](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/proxmox/SKILL.md) - Manage Proxmox VE clusters via REST API.
- [proxmox-full](https://github.com/openclaw/skills/tree/main/skills/msarheed/proxmox-full/SKILL.md) - Complete Proxmox VE management -.
- [r2-storage](https://github.com/openclaw/skills/tree/main/skills/junwatu/r2-storage/SKILL.md) - Cloudflare R2 Storage management — setup, upload, download, sync via rclone
- [remarkable](https://github.com/openclaw/skills/tree/main/skills/nickian/remarkable/SKILL.md) - Send files and web articles to a reMarkable e-ink tablet via the reMarkable Cloud.
- [sec-filing-watcher](https://github.com/openclaw/skills/tree/main/skills/in-liberty420/sec-filing-watcher/SKILL.md) - Monitor SEC EDGAR for new filings and get Telegram/Slack summaries via Clawdbot.
- [security-audit](https://github.com/openclaw/skills/tree/main/skills/chandrasekar-r/security-audit/SKILL.md) - Comprehensive security auditing for Clawdbot deployments.
- [security-monitor](https://github.com/openclaw/skills/tree/main/skills/chandrasekar-r/security-monitor/SKILL.md) - Real-time security monitoring for Clawdbot.
- [Send Me My Files - R2 upload with short lived signed urls](https://github.com/openclaw/skills/tree/main/skills/julianengel/r2-upload/SKILL.md) - Upload files to Cloudflare R2, AWS S3, or any S3-compatible storage.
- [senior-devops](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-devops/SKILL.md) - Comprehensive DevOps skill for CI/CD, infrastructure automation, containerization, and cloud platforms (AWS, GCP,.
- [senior-ml-engineer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-ml-engineer/SKILL.md) - ML engineering skill for productionizing models, building MLOps pipelines.
- [servicenow-agent](https://github.com/openclaw/skills/tree/main/skills/thesethrose/servicenow-agent/SKILL.md) - Read-only CLI access to ServiceNow Table, Attachment, Aggregate.
- [servicenow-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/servicenow-docs/SKILL.md) - Search and retrieve ServiceNow documentation, release notes.
- [skill-exporter](https://github.com/openclaw/skills/tree/main/skills/macstenk/skill-exporter/SKILL.md) - Export Clawdbot skills as standalone, deployable microservices.
- [snapmaker](https://github.com/openclaw/skills/tree/main/skills/lucakaufmann/snapmaker/SKILL.md) - Monitor and control Snapmaker 3D printers (U1.
- [solar-weather](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/solar-weather/SKILL.md) - Monitor solar weather conditions including geomagnetic storms, solar flares, aurora forecasts, and solar wind data.
- [sophie-optimizer](https://github.com/openclaw/skills/tree/main/skills/zayresz/sophie-optimizer/SKILL.md) - Automated context health management for OpenClaw.
- [ssh-essentials](https://github.com/openclaw/skills/tree/main/skills/arnarsson/ssh-essentials/SKILL.md) - Essential SSH commands for secure remote access, key management, tunneling.
- [supernote-cloud](https://github.com/openclaw/skills/tree/main/skills/nickian/supernote-cloud/SKILL.md) - Access a self-hosted Supernote Private Cloud instance to browse files and folders, upload documents (PDF, EPUB).
- [sysadmin-toolbox](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/sysadmin-toolbox/SKILL.md) - Tool discovery and shell one-liner reference for sysadmin, DevOps.
- [system-monitor](https://github.com/openclaw/skills/tree/main/skills/zerofire03/system-monitor/SKILL.md) - Check the current CPU, RAM, and GPU status of the local server.
- [tailscale](https://github.com/openclaw/skills/tree/main/skills/jmagar/tailscale/SKILL.md) - Manage Tailscale tailnet via CLI and API.
- [tailscale-serve](https://github.com/openclaw/skills/tree/main/skills/snopoke/tailscale-serve/SKILL.md) - tailscale-serve
- [tautulli](https://github.com/openclaw/skills/tree/main/skills/rjmurillo/tautulli/SKILL.md) - Monitor Plex activity and stats via Tautulli API.
- [tavily](https://github.com/openclaw/skills/tree/main/skills/bert-builder/tavily/SKILL.md) - AI-optimized web search using Tavily Search API.
- [tmux-agents](https://github.com/openclaw/skills/tree/main/skills/cuba6112/tmux-agents/SKILL.md) - Manage background coding agents in tmux sessions.
- [topic-monitor](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/topic-monitor/SKILL.md) - Monitor topics of interest and proactively alert when important developments occur.
- [trust-protocol](https://github.com/openclaw/skills/tree/main/skills/felmonon/trust-protocol/SKILL.md) - Establish, verify, and maintain trust between AI agents.
- [unraid](https://github.com/openclaw/skills/tree/main/skills/jmagar/unraid/SKILL.md) - Query and monitor Unraid servers via the GraphQL API.
- [uptime-kuma](https://github.com/openclaw/skills/tree/main/skills/msarheed/uptime-kuma/SKILL.md) - Interact with Uptime Kuma monitoring server.
- [vercel](https://github.com/openclaw/skills/tree/main/skills/thesethrose/vercel/SKILL.md) - Deploy applications and manage projects with complete CLI reference.
- [vercel-deploy](https://github.com/openclaw/skills/tree/main/skills/sharanga10/vercel-deploy-claimable/SKILL.md) - Deploy applications and websites to Vercel.
- [vibetunnel](https://github.com/openclaw/skills/tree/main/skills/basher83/vibetunnel/SKILL.md) - Manage VibeTunnel terminal sessions.
- [vision-analyze](https://github.com/openclaw/skills/tree/main/skills/humberto0o0/vision-analyze/SKILL.md) - Analyze images using **Google Cloud Vision API**.
- [wandb-monitor](https://github.com/openclaw/skills/tree/main/skills/chrisvoncsefalvay/wandb-monitor/SKILL.md) - Monitor and analyze Weights & Biases training runs.
- [web-deploy](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/web-deploy/SKILL.md) - Build and deploy websites, web apps, and APIs to production.
- [ydc-ai-sdk-integration](https://github.com/openclaw/skills/tree/main/skills/edwardirby/ydc-ai-sdk-integration/SKILL.md) - Integrate Vercel AI SDK applications with You.com tools (web search, AI agent, content extraction).
- [youtube-api](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-api/SKILL.md) - YouTube API access without the official API quota hassle — transcripts, search, channels, playlists.
- [yutori-web-research](https://github.com/openclaw/skills/tree/main/skills/juanpin/yutori-web-research/SKILL.md) - Use Yutori’s Research API and Browsing API (cloud browser) to research topics, collect sources.

</details>
<details>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [2captcha](https://github.com/openclaw/skills/tree/main/skills/adinvadim/2captcha/SKILL.md) - Solve CAPTCHAs using 2Captcha service.
- [abm-outbound](https://github.com/openclaw/skills/tree/main/skills/dru-ca/abm-outbound/SKILL.md) - Multi-channel ABM automation that turns LinkedIn URLs into coordinated outbound campaigns.
- [activecampaign](https://github.com/openclaw/skills/tree/main/skills/kesslerio/activecampaign/SKILL.md) - ActiveCampaign CRM integration for lead management, deal tracking.
- [adcp-advertising](https://github.com/openclaw/skills/tree/main/skills/edyyy62/adcp-advertising/SKILL.md) - Automate advertising campaigns with AI.
- [Agent Browser](https://github.com/openclaw/skills/tree/main/skills/thesethrose/agent-browser/SKILL.md) - A fast Rust-based headless browser automation CLI with Node.js fallback that enables AI agents.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/murphykobe/agent-browser-2/SKILL.md) - Web testing, form filling, screenshots, data extraction.
- [agent-zero](https://github.com/openclaw/skills/tree/main/skills/dowingard/agent-zero-bridge/SKILL.md) - Delegate tasks to Agent Zero autonomous coding framework.
- [agentmail-integration](https://github.com/openclaw/skills/tree/main/skills/synesthesia-wav/agentmail-integration/SKILL.md) - Integrate AgentMail API for AI agent email automation.
- [apify-lead-generation](https://github.com/openclaw/skills/tree/main/skills/jirispilka/apify-lead-generation/SKILL.md) - Generates B2B/B2C leads by scraping Google Maps, websites, Instagram, TikTok, Facebook, LinkedIn, YouTube.
- [asr](https://github.com/openclaw/skills/tree/main/skills/ilyakam/asr/SKILL.md) - Fast, accurate, and incredibly inexpensive automatic speech-to-text transcription service.
- [autofillin](https://github.com/openclaw/skills/tree/main/skills/leohan123123/autofillin/SKILL.md) - Automated web form filling and file uploading skill with Playwright browser automation.
- [babyconnect](https://github.com/openclaw/skills/tree/main/skills/kesslerio/babyconnect/SKILL.md) - ActiveCampaign CRM integration for lead management, deal tracking.
- [browser-ladder](https://github.com/openclaw/skills/tree/main/skills/ktpriyatham/browser-ladder/SKILL.md) - Climb the browser ladder — start free, escalate only when needed.
- [browser-use](https://github.com/openclaw/skills/tree/main/skills/shawnpana/browser-use/SKILL.md) - Cloud-based browser automation with managed sessions and autonomous task execution.
- [browsh](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/browsh/SKILL.md) - A modern text-based browser.
- [chirp](https://github.com/openclaw/skills/tree/main/skills/zizi-cat/chirp/SKILL.md) - X/Twitter CLI using OpenClaw browser tool.
- [clawbrowser](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/clawbrowser/SKILL.md) - Use when the agent needs to drive a browser through the Microsoft Playwright CLI (`playwright-cli`) for navigation,.
- [clawflows](https://github.com/openclaw/skills/tree/main/skills/cluka-399/clawflows/SKILL.md) - Search, install, and run multi-skill automations from clawflows.com.
- [context-compressor](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/context-compressor/SKILL.md) - Automated context management for long-running Clawdbot sessions.
- [context-recovery](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/context-recovery/SKILL.md) - Automatically recover working context after session compaction or when continuation is implied but context is missing.
- [context7](https://github.com/openclaw/skills/tree/main/skills/am-will/context7-api/SKILL.md) - |.
- [daily-rhythm](https://github.com/openclaw/skills/tree/main/skills/anthonyfrancis/daily-rhythm/SKILL.md) - Automated daily planning and reflection system with morning briefs, wind-down prompts, sleep nudges.
- [db-query](https://github.com/openclaw/skills/tree/main/skills/zenixp/db-query/SKILL.md) - Query project databases with automatic SSH tunnel management.
- [demo-video](https://github.com/openclaw/skills/tree/main/skills/cyberfront-ai/demo-video/SKILL.md) - Create product demo videos by automating browser interactions and capturing frames.
- [fast-browser-use](https://github.com/openclaw/skills/tree/main/skills/rknoche6/fast-browser-use/SKILL.md) - name: fast-browser-use
- [feishu-doc](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-doc/SKILL.md) - Fetch content from Feishu (Lark) Wiki, Docs, Sheets.
- [firecrawl-search](https://github.com/openclaw/skills/tree/main/skills/ashwingupy/firecrawl-search/SKILL.md) - Web search and scraping via Firecrawl API.
- [firecrawl-skills](https://github.com/openclaw/skills/tree/main/skills/leonardogrig/firecrawl-skills/SKILL.md)
- [firecrawler](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/firecrawler/SKILL.md) - Web scraping and crawling with Firecrawl API.
- [gdpr-dsgvo-expert](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/gdpr-dsgvo-expert/SKILL.md) - GDPR and German DSGVO compliance automation.
- [google-web-search](https://github.com/openclaw/skills/tree/main/skills/theoseo/google-web-search/SKILL.md) - Enables grounded question answering by automatically executing the Google Search tool within Gemini models.
- [guru-mcp](https://github.com/openclaw/skills/tree/main/skills/pvoo/guru-mcp/SKILL.md) - Access Guru knowledge base via MCP - ask AI questions, search documents, create drafts.
- [hass-cli](https://github.com/openclaw/skills/tree/main/skills/ericarnoldy/hass-cli/SKILL.md) - Control Home Assistant devices and automations via hass-cli.
- [home-assistant](https://github.com/openclaw/skills/tree/main/skills/iahmadzain/home-assistant/SKILL.md) - Control Home Assistant smart home devices, run automations, and receive webhook events.
- [inkedin-automation-that-really-works](https://github.com/openclaw/skills/tree/main/skills/red777777/inkedin-automation-that-really-works/SKILL.md) - LinkedIn automation — post, comment (with @mentions), edit/delete comments, repost, read feed, analytics.
- [job-auto-apply](https://github.com/openclaw/skills/tree/main/skills/veeky-kumar/job-auto-apply/SKILL.md) - Automated job search and application system for Clawdbot.
- [kakiyo](https://github.com/openclaw/skills/tree/main/skills/cyberboyayush/kakiyo/SKILL.md) - Official Kakiyo skill from Kakiyo.com for managing LinkedIn automation campaigns, prospects, and AI agents via Kakiyo.
- [leadklick](https://github.com/openclaw/skills/tree/main/skills/big-roman123/leadklick/SKILL.md) - Capture leads into a centralized Supabase database with automatic Make.com email automation.
- [mcporter](https://github.com/openclaw/skills/tree/main/skills/steipete/mcporter/SKILL.md) - Use the mcporter CLI to list, configure, auth, and call MCP servers/tools directly (HTTP.
- [n8n-api](https://github.com/openclaw/skills/tree/main/skills/codedao12/n8n-api/SKILL.md) - Operate n8n via its public REST API from OpenClaw.
- [n8n-automation](https://github.com/openclaw/skills/tree/main/skills/dilomcfly/n8n-automation/SKILL.md) - Manage n8n workflows from OpenClaw via the n8n REST API.
- [n8n-hub](https://github.com/openclaw/skills/tree/main/skills/codedao12/n8n-hub/SKILL.md) - Centralized n8n hub for designing reliable flows (idempotency, retries, HITL) and operating them via the public REST.
- [newsletter-creation-curation](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm/newsletter-creation-curation/SKILL.md) - Industry-specific newsletter creation with cadence recommendations and automation workflows
- [odoo-openclaw-skill](https://github.com/openclaw/skills/tree/main/skills/ashrf-in/odoo-openclaw-skill/SKILL.md) - Advanced Odoo financial intelligence tool for automated accounting audits, VAT reports, cash flow analysis.
- [organize-tg](https://github.com/openclaw/skills/tree/main/skills/consort-tech/organize-tg/SKILL.md) - Organize TG by Consort Technologies - Automatically scan your Telegram contacts and sync business contacts to a Googl.
- [phantombuster](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/phantombuster/SKILL.md) - Control PhantomBuster automation agents via API.
- [playwright-cli](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/playwright-cli/SKILL.md) - Browser automation via Playwright CLI for testing and scraping.
- [proxy-scrap](https://github.com/openclaw/skills/tree/main/skills/danman60/proxy-scrap/SKILL.md) - High-Performance Proxy Harvesting - Automatically scrape, validate, and export working SOCKS5, SOCKS4.
- [qiuqiu-helper](https://github.com/openclaw/skills/tree/main/skills/mmogdeveloper/qiuqiu-helper/SKILL.md) - This is a multi-purpose helper skill for Jesse, designed to automate common workspace tasks.
- [roon-controller](https://github.com/openclaw/skills/tree/main/skills/puterjam/roon-controller/SKILL.md) - Control Roon music player through Roon API with automatic Core discovery and zone filtering.
- [safe-exec](https://github.com/openclaw/skills/tree/main/skills/ottttto/safe-exec/SKILL.md) - Safe command execution for OpenClaw Agents with automatic danger pattern detection, risk assessment, user approval.
- [sales-bot](https://github.com/openclaw/skills/tree/main/skills/big-roman123/sales-bot/SKILL.md) - Capture leads into a centralized Supabase database with automatic Make.com email automation.
- [serper](https://github.com/openclaw/skills/tree/main/skills/nesdeq/serper/SKILL.md) - Google search via Serper API with full page content extraction.
- [spool](https://github.com/openclaw/skills/tree/main/skills/zizi-cat/spool/SKILL.md) - Threads CLI - Read, post, reply, and search on Meta's Threads using OpenClaw browser tool.
- [stremio-cast](https://github.com/openclaw/skills/tree/main/skills/pedro-valentim/stremio-cast/SKILL.md) - Busca conteúdo no Stremio Web e transmite para dispositivos Chromecast usando CATT e Playwright.
- [tcm-video-factory](https://github.com/openclaw/skills/tree/main/skills/xaotiensinh-abm/tcm-video-factory/SKILL.md) - Automate health video production planning (Topic Research - Script - Character - Image/Video Prompts) using Perplexit.
- [tekin](https://github.com/openclaw/skills/tree/main/skills/gwqwghksvq-sketch/tekin/SKILL.md) - Rust-based headless browser CLI with Node.js fallback.
- [tiangong-notebooklm-cli](https://github.com/openclaw/skills/tree/main/skills/fadeloo/tiangong-notebooklm-cli/SKILL.md) - NotebookLM CLI automation for this workspace: authenticate/login, list/create/use/rename/delete notebooks.
- [tiangong-wps-ppt-automation](https://github.com/openclaw/skills/tree/main/skills/fadeloo/tiangong-wps-ppt-automation/SKILL.md) - Automate common PowerPoint/WPS Presentation operations on Windows via COM (read text/notes/outline, export.
- [tiangong-wps-word-automation](https://github.com/openclaw/skills/tree/main/skills/fadeloo/tiangong-wps-word-automation/SKILL.md) - Automate common Word/WPS document operations on Windows via COM (read text, replace, insert, headings.
- [tinyfish-web-agent](https://github.com/openclaw/skills/tree/main/skills/simantak-dabhade/tinyfish-web-agent/SKILL.md) - Use TinyFish/Mino web agent to extract/scrape websites, extract data, and automate browser actions using natural.
- [turix-computer-use](https://github.com/openclaw/skills/tree/main/skills/tongyu-yan/turix-computer-use/SKILL.md) - Computer Use Agent (CUA) for macOS automation using TuriX.
- [turix-cua](https://github.com/openclaw/skills/tree/main/skills/tongyu-yan/turix-cua/SKILL.md) - Computer Use Agent (CUA) for macOS automation using TuriX.
- [verify-on-browser](https://github.com/openclaw/skills/tree/main/skills/myestery/verify-on-browser/SKILL.md) - Control browser via Chrome DevTools Protocol - full CDP access.
- [vocal-chat](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/vocal-chat/SKILL.md) - Handles voice-to-voice conversations on WhatsApp.
- [web-qa-bot](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/web-qa-bot/SKILL.md) - AI-powered web application QA automation using accessibility-tree based testing.
- [x-articles](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/x-articles/SKILL.md) - Create and publish X (Twitter) Articles with viral formatting, hook patterns.
- [xiaohongshu-mcp](https://github.com/openclaw/skills/tree/main/skills/pxfeng/xiaohongshu-mcp/SKILL.md) - Upload images and videos to Xiaohongshu Creator Platform using a local MCP server with browser automation.
- [zellij](https://github.com/openclaw/skills/tree/main/skills/jivvei/zellij/SKILL.md) - Remote-control zellij sessions for interactive CLIs by sending keystrokes and scraping pane output.

</details>
<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [afame](https://github.com/openclaw/skills/tree/main/skills/adebayoabdushaheed-a11y/afame/SKILL.md) - Generate diverse creative illustrations via OpenAI Images API.
- [ai-video-gen](https://github.com/openclaw/skills/tree/main/skills/rhanbourinajd/ai-video-gen/SKILL.md) - End-to-end AI video generation - create videos from text prompts using image generation, video synthesis, voice-over.
- [algorithmic-art](https://github.com/openclaw/skills/tree/main/skills/seanphan/algorithmic-art/SKILL.md) - Creating algorithmic art using p5.js with seeded randomness and interactive parameter exploration.
- [atxp](https://github.com/openclaw/skills/tree/main/skills/emilioacc/atxp/SKILL.md) - Access ATXP paid API tools for web search, AI image generation, music creation, video generation, and X/Twitter searc.
- [beauty-generation-api](https://github.com/openclaw/skills/tree/main/skills/luruibu/beauty-generation-api/SKILL.md) - FREE Professional AI beauty image generation service supporting 140+ nationalities.
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work.
- [canva-connect](https://github.com/openclaw/skills/tree/main/skills/coolmanns/canva-connect/SKILL.md)
- [captions](https://github.com/openclaw/skills/tree/main/skills/therohitdas/captions/SKILL.md) - Extract closed captions and subtitles from YouTube videos.
- [chart-image](https://github.com/openclaw/skills/tree/main/skills/dannyshmueli/chart-image/SKILL.md) - Generate publication-quality chart images from data.
- [clinkding](https://github.com/openclaw/skills/tree/main/skills/daveonkels/clinkding/SKILL.md) - Manage linkding bookmarks - save URLs, search, tag, organize.
- [coloring-page](https://github.com/openclaw/skills/tree/main/skills/borahm/coloring-page/SKILL.md) - Turn an uploaded photo into a printable black-and-white coloring.
- [comfy-cli](https://github.com/openclaw/skills/tree/main/skills/johntheyoung/comfy-cli/SKILL.md) - Install, manage, and run ComfyUI instances.
- [comfyui](https://github.com/openclaw/skills/tree/main/skills/xtopher86/comfyui-request/SKILL.md) - Send workflow requests to ComfyUI and get image results.
- [Excalidraw Flowchart](https://github.com/openclaw/skills/tree/main/skills/swiftlysingh/excalidraw-flowchart/SKILL.md) - Create Excalidraw flowcharts from descriptions.
- [fal-ai](https://github.com/openclaw/skills/tree/main/skills/agmmnn/fal-ai/SKILL.md) - Generate images, videos and audio using Fal.ai's generative media API.
- [ffmpeg-video-editor](https://github.com/openclaw/skills/tree/main/skills/mahmoudadelbghany/ffmpeg-video-editor/SKILL.md) - Generate FFmpeg commands from natural language video editing requests - cut, trim, convert, compress, change aspect.
- [figma](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/figma/SKILL.md) - Figma design analysis, asset export, accessibility audit.
- [find-stl](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/find-stl/SKILL.md) - Search and download ready-to-print 3D model files (STL/3MF/ZIP) for a concept or specific part by querying Printables.
- [gamma](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/gamma/SKILL.md) - Generate AI-powered presentations, documents, and social posts using Gamma.app API.
- [gifhorse](https://github.com/openclaw/skills/tree/main/skills/coyote-git/gifhorse/SKILL.md) - Search video dialogue and create reaction GIFs with subtitles.
- [google-gemini-media](https://github.com/openclaw/skills/tree/main/skills/xsir0/google-gemini-media/SKILL.md) - Use the Gemini API (Nano Banana image generation, Veo video, Gemini TTS speech and audio understanding) to deliver.
- [iblipper](https://github.com/openclaw/skills/tree/main/skills/andyed/iblipper/SKILL.md) - Generate kinetic typography animations for expressive agent-to-human communication.
- [krea-api](https://github.com/openclaw/skills/tree/main/skills/fossilizedcarlos/krea-api/SKILL.md) - Generate images via Krea.ai API (Flux, Imagen, Ideogram, Seedream, etc.).
- [meshy-ai](https://github.com/openclaw/skills/tree/main/skills/sabatesduran/clawdbot-meshyai-skill/SKILL.md) - Use the Meshy.ai REST API to generate assets: (1) text-to-2d (Meshy Text to Image)
- [pollinations](https://github.com/openclaw/skills/tree/main/skills/isaacgounton/pollinations/SKILL.md) - Pollinations.ai: text, images, videos, audio with 25+ models.
- [render-stl-png](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/render-stl-png/SKILL.md) - Render an STL to a PNG from a nice, consistent 3D angle ("Blender-ish" default perspective) with a solid color.
- [reve-ai](https://github.com/openclaw/skills/tree/main/skills/dpaluy/reve-ai/SKILL.md) - Reve AI image generation, editing.
- [runware](https://github.com/openclaw/skills/tree/main/skills/26medias/runware/SKILL.md) - Generate images and videos via Runware API.
- [sticker-analyzer](https://github.com/openclaw/skills/tree/main/skills/autogame-17/sticker-analyzer/SKILL.md) - Analyze images in media/stickers using Vision API to identify and filter meme/sticker content vs screenshots or docum.
- [subtitles](https://github.com/openclaw/skills/tree/main/skills/therohitdas/subtitles/SKILL.md) - Get subtitles from YouTube videos for translation, language learning.
- [superdesign](https://github.com/openclaw/skills/tree/main/skills/mpociot/superdesign/SKILL.md) - Expert frontend design guidelines for modern.
- [tube-summary](https://github.com/openclaw/skills/tree/main/skills/dillera/tube-summary/SKILL.md) - Search YouTube for videos on any topic and get intelligent summaries from video subtitles.
- [vap-multimedia-generation](https://github.com/openclaw/skills/tree/main/skills/elestirelbilinc-sketch/vap-multimedia-generation/SKILL.md) - AI image, video.
- [venice-ai](https://github.com/openclaw/skills/tree/main/skills/nhannah/venice-ai-media/SKILL.md) - Venice AI: image/video generation, upscaling, AI editing.
- [veo](https://github.com/openclaw/skills/tree/main/skills/buddyh/veo/SKILL.md) - Generate video using Google Veo (Veo 3.1 / Veo 3.0).
- [veo3-video-gen](https://github.com/openclaw/skills/tree/main/skills/bluelyw/veo3-video-gen/SKILL.md) - Generate and stitch short videos via Google Veo 3.x using the Gemini API (google-genai).
- [video-frames](https://github.com/openclaw/skills/tree/main/skills/steipete/video-frames/SKILL.md) - Extract frames or short clips from videos using ffmpeg.
- [yollomi](https://github.com/openclaw/skills/tree/main/skills/anichikage/yollomi/SKILL.md) - Generate AI images and videos using Yollomi API.


</details>
<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [alter-actions](https://github.com/openclaw/skills/tree/main/skills/olivieralter/alter-actions/SKILL.md) - Trigger 84+ Alter macOS app actions via x-callback-urls.
- [apple-contacts](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-contacts/SKILL.md) - Look up contacts from macOS Contacts.app.
- [apple-mail-search](https://github.com/openclaw/skills/tree/main/skills/mneves75/apple-mail-search/SKILL.md) - Fast Apple Mail search via SQLite (~50ms vs 8+ min).
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-music/SKILL.md) - Search Apple Music, add songs to library, manage playlists, control playback and AirPlay.
- [apple-photos](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-photos/SKILL.md) - Apple Photos.app integration for macOS.
- [apple-remind-me](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/apple-remind-me/SKILL.md) - Natural language reminders via Apple Reminders.app.
- [appletv](https://github.com/openclaw/skills/tree/main/skills/lucakaufmann/appletv/SKILL.md) - Control Apple TV via pyatv.
- [caffeine](https://github.com/openclaw/skills/tree/main/skills/bunsdev/caffeine/SKILL.md) - Prevent the computer screen and system from falling asleep on macOS or Windows.
- [callmac](https://github.com/openclaw/skills/tree/main/skills/jooey/callmac/SKILL.md) - Remote voice control for Mac from mobile devices using commands like /callmac.
- [clawdbot-macos-build](https://github.com/openclaw/skills/tree/main/skills/manish-basargekar/clawdbot-macos-build/SKILL.md) - Build the Clawdbot macOS menu bar app from source.
- [clawdbot-skill-voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-voice-wake-say/SKILL.md) - Speak responses aloud on macOS using the built-in `say` command when user input indicates Voice Wake/voice recognitio.
- [drafts](https://github.com/openclaw/skills/tree/main/skills/nerveband/drafts/SKILL.md) - Manage Drafts app notes via CLI on macOS.
- [findmy-location](https://github.com/openclaw/skills/tree/main/skills/poiley/findmy-location/SKILL.md) - Track a shared contact's location via Apple Find My with street-level accuracy.
- [fzf-fuzzy-finder](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fzf-fuzzy-finder/SKILL.md) - Command-line fuzzy finder for interactive filtering and selection - integrates with shell, vim.
- [get-focus-mode](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/get-focus-mode/SKILL.md) - Get the current macOS Focus.
- [healthkit-sync](https://github.com/openclaw/skills/tree/main/skills/mneves75/healthkit-sync/SKILL.md) - iOS HealthKit data sync CLI commands and patterns.
- [hergunmac](https://github.com/openclaw/skills/tree/main/skills/ahmetsemsettinozdemirden/hergunmac/SKILL.md) - Access AI-powered football match predictions from hergunmac.com.
- [homebrew](https://github.com/openclaw/skills/tree/main/skills/thesethrose/homebrew/SKILL.md) - Homebrew package manager for macOS.
- [icloud-findmy](https://github.com/openclaw/skills/tree/main/skills/liamnichols/icloud-findmy/SKILL.md) - Query Find My locations and battery status for family devices via iCloud.
- [inkjet](https://github.com/openclaw/skills/tree/main/skills/aaronchartier/inkjet/SKILL.md) - Print text, images, and QR codes to a wireless Bluetooth thermal printer from a MacOS device.
- [mac-tts](https://github.com/openclaw/skills/tree/main/skills/kalijason/mac-tts/SKILL.md) - Text-to-speech using macOS built-in `say` command.
- [media-backup](https://github.com/openclaw/skills/tree/main/skills/dbhurley/media-backup/SKILL.md) - Archive Clawdbot conversation media (photos, videos) to a local folder.
- [meow-finder](https://github.com/openclaw/skills/tree/main/skills/abgohel/meow-finder/SKILL.md) - CLI tool to discover AI tools.
- [mlx-stt](https://github.com/openclaw/skills/tree/main/skills/guoqiao/mlx-stt/SKILL.md) - Speech-To-Text with MLX (Apple Silicon) and GLM-ASR.
- [mlx-swift-lm](https://github.com/openclaw/skills/tree/main/skills/ronaldmannak/mlx-swift-lm/SKILL.md) - MLX Swift LM - Run LLMs and VLMs on Apple Silicon using.
- [mole-mac-cleanup](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/mole-mac-cleanup/SKILL.md) - Mac cleanup & optimization tool combining CleanMyMac, AppCleaner, DaisyDisk features.
- [my-tesla](https://github.com/openclaw/skills/tree/main/skills/officialpm/my-tesla/SKILL.md) - Control Tesla vehicles from macOS via the Tesla Owner API using teslapy (auth, list cars, status, lock/unlock, climat.
- [network-scanner](https://github.com/openclaw/skills/tree/main/skills/florianbeer/network-scanner/SKILL.md) - Scan networks to discover devices, gather MAC addresses, vendors.
- [shortcuts-generator](https://github.com/openclaw/skills/tree/main/skills/erik-agens/shortcuts-skill/SKILL.md) - Generate macOS/iOS Shortcuts by creating plist files.
- [skill-email-management](https://github.com/openclaw/skills/tree/main/skills/latisen/skill-email-management/SKILL.md) - Expert email management assistant for Apple Mail.
- [voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/voice-wake-say/SKILL.md) - Speak responses aloud on macOS via the say command.
- [working-with-lockdownd](https://github.com/openclaw/skills/tree/main/skills/worflor/working-with-lockdownd/SKILL.md) - Comprehensive toolkit for interacting with iOS devices over WiFi using the Apple Lockdown Protocol (port 62078).

</details>
<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [1](https://github.com/openclaw/skills/tree/main/skills/nastrology/1/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving understanding.
- [shellf](https://github.com/openclaw/skills/tree/main/skills/andrewleonardi/shellf/SKILL.md) - Goodreads for OpenClaw agents.
- [aclawdemy](https://github.com/openclaw/skills/tree/main/skills/nimhar/aclawdemy/SKILL.md) - The academic research platform for AI agents.
- [advanced-skill-creator](https://github.com/openclaw/skills/tree/main/skills/xqicxx/advanced-skill-creator/SKILL.md) - Advanced OpenClaw skill creation handler that executes the official 5-step research flow with comprehensive analysis.
- [agentarxiv](https://github.com/openclaw/skills/tree/main/skills/amanbhandula/agentarxiv/SKILL.md) - Outcome-driven scientific publishing for AI agents.
- [agnxi-search-skill](https://github.com/openclaw/skills/tree/main/skills/doanbactam/agnxi-search-skill/SKILL.md) - The official search utility for Agnxi.com - The premier directory of AI Agent Tools, MCP Servers.
- [ahmed](https://github.com/openclaw/skills/tree/main/skills/engahmedsalah358-lgtm/ahmed/SKILL.md) - Terminal Spotify playback/search via spogo (preferred) or spotify_player.
- [alexa-cli](https://github.com/openclaw/skills/tree/main/skills/buddyh/alexa-cli/SKILL.md) - Control Amazon Alexa devices and smart home via the `alexacli`.
- [aluvia-brave-search](https://github.com/openclaw/skills/tree/main/skills/bertxtrella/aluvia-brave-search/SKILL.md) - Web search and content extraction via Brave Search API, with unblockable access using Aluvia mobile proxy.
- [aluvia-web-proxy](https://github.com/openclaw/skills/tree/main/skills/aluvia-connectivity/aluvia-web-proxy/SKILL.md) - Unblock websites and bypass CAPTCHAs and 403 errors using Aluvia mobile proxies.
- [aluvia-web-unblock](https://github.com/openclaw/skills/tree/main/skills/bertxtrella/aluvia-web-unblock/SKILL.md) - Unblock websites and bypass CAPTCHAs and 403 errors using Aluvia mobile proxies.
- [anshumanbh-qmd](https://github.com/openclaw/skills/tree/main/skills/anshumanbh/anshumanbh-qmd/SKILL.md) - Search markdown knowledge bases efficiently using.
- [answeroverflow](https://github.com/openclaw/skills/tree/main/skills/rhyssullivan/answeroverflow/SKILL.md) - Search indexed Discord community discussions via Answer Overflow.
- [arxiv-watcher](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/arxiv-watcher/SKILL.md) - Search and summarize papers from ArXiv.
- [attio-crm](https://github.com/openclaw/skills/tree/main/skills/kesslerio/attio-crm/SKILL.md) - Manage Attio CRM records (companies, people, deals, tasks, notes).
- [aubrai-longevity](https://github.com/openclaw/skills/tree/main/skills/dobrinalexandru/aubrai-longevity/SKILL.md) - Meet your SOTA longevity research partner.
- [beepctl](https://github.com/openclaw/skills/tree/main/skills/blqke/beepctl/SKILL.md) - Use when sending messages, searching chats, or managing conversations across messaging platforms (Telegram, WhatsApp.
- [brave-images](https://github.com/openclaw/skills/tree/main/skills/zats/brave-images/SKILL.md) - Search for images using Brave Search API.
- [brave-search](https://github.com/openclaw/skills/tree/main/skills/steipete/brave-search/SKILL.md) - Web search and content extraction via Brave Search API.
- [brightdata](https://github.com/openclaw/skills/tree/main/skills/meirkad/bright-data/SKILL.md) - Web scraping and search via Bright Data API.
- [carapace](https://github.com/openclaw/skills/tree/main/skills/morpheis/carapace/SKILL.md) - Query and contribute structured understanding to Carapace — the shared knowledge base for AI agents.
- [clawdbot-filesystem](https://github.com/openclaw/skills/tree/main/skills/gtrusler/clawdbot-filesystem/SKILL.md) - Advanced filesystem operations - listing, searching, batch processing, and directory analysis for Clawdbot
- [clawdbot-logs](https://github.com/openclaw/skills/tree/main/skills/satriapamudji/clawdbot-logs/SKILL.md) - Analyze Clawdbot logs and diagnostics.
- [clawdgle](https://github.com/openclaw/skills/tree/main/skills/rubybrewsday/clawdgle/SKILL.md) - Public API usage for the Clawdgle markdown-first search engine.
- [cochesnet-cli](https://github.com/openclaw/skills/tree/main/skills/pjtf93/cochesnet-cli/SKILL.md) - Use the cochesnet CLI to search coches.net listings and fetch listing details.
- [code-docs-search-exa](https://github.com/openclaw/skills/tree/main/skills/theishangoswami/code-docs-search-exa/SKILL.md)
- [competitive-intelligence-market-research](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm/competitive-intelligence-market-research/SKILL.md) - B2B SaaS competitive intelligence with 24 scenarios across Sales/HR/Fintech/Ops Tech
- [context](https://github.com/openclaw/skills/tree/main/skills/barneyjm/context/SKILL.md) - Get comprehensive context about a location including nearby places, area description.
- [contextoverflow](https://github.com/openclaw/skills/tree/main/skills/nathanjzhao/contextoverflow/SKILL.md) - Academic forum for mission-driven project proposals.
- [creatordb-youtube-v3](https://github.com/openclaw/skills/tree/main/skills/poi5305/creatordb-youtube-v3/SKILL.md) - Can search and get YouTuber information
- [crisp](https://github.com/openclaw/skills/tree/main/skills/paul-phan/crisp/SKILL.md) - Customer support via Crisp API.
- [cuecue-deep-research](https://github.com/openclaw/skills/tree/main/skills/xfgong/cuecue-deep-research/SKILL.md) - Conduct deep financial research using CueCue's AI-powered multi-agent system
- [ddg-search](https://github.com/openclaw/skills/tree/main/skills/paradoxfuzzle/ddg-search/SKILL.md) - Perform web searches using DuckDuckGo.
- [deep-research](https://github.com/openclaw/skills/tree/main/skills/seyhunak/deep-research/SKILL.md) - Deep Research Agent specializes in complex, multi-step research tasks that require planning, decomposition, and long-.
- [engram](https://github.com/openclaw/skills/tree/main/skills/anwitch/engram/SKILL.md) - Provides semantic search for a local knowledge base using Pinecone and Gemini embeddings.
- [evoweb-ai](https://github.com/openclaw/skills/tree/main/skills/galizki/evoweb-ai/SKILL.md) - Create a Website Designed to Bring Clients from ChatGPT, Gemini & Modern Search
- [exa](https://github.com/openclaw/skills/tree/main/skills/fardeenxyz/exa/SKILL.md) - Neural web search and code context via Exa AI API.
- [find-people](https://github.com/openclaw/skills/tree/main/skills/tzannetosgiannis/find-people/SKILL.md) - Open Source Intelligence (OSINT) tool for researching individuals - professional backgrounds, career timelines.
- [foundry](https://github.com/openclaw/skills/tree/main/skills/lekt9/foundry/SKILL.md) - Self-writing meta-extension that forges new capabilities — researches docs, writes extensions, tools, hooks, and skills
- [ga4-analytics](https://github.com/openclaw/skills/tree/main/skills/adamkristopher/ga4-analytics/SKILL.md) - Google Analytics 4, Search Console, and Indexing API toolkit.
- [gemini-yt-transcript](https://github.com/openclaw/skills/tree/main/skills/odrobnik/gemini-yt-video-transcript/SKILL.md) - YouTube transcript via Google Gemini with speaker labels.
- [geo-optimizer](https://github.com/openclaw/skills/tree/main/skills/artyomx33/geo-optimizer/SKILL.md) - Optimize content for AI citation (GEO).
- [google-maps-grounding-lite-mcp](https://github.com/openclaw/skills/tree/main/skills/ryanbaumann/google-maps-grounding-lite-mcp/SKILL.md) - Google Maps Grounding Lite MCP for location search, weather, and routes via mcporter.
- [google-search](https://github.com/openclaw/skills/tree/main/skills/mxfeinberg/google-search/SKILL.md) - Search the web using Google Custom Search Engine (PSE).
- [grok-search](https://github.com/openclaw/skills/tree/main/skills/notabhay/grok-search/SKILL.md) - Search web or X/Twitter using.
- [growth-marketer](https://github.com/openclaw/skills/tree/main/skills/metehan777/growth-marketer/SKILL.md) - **Tags**: marketing, seo, conversion-optimization, popup-builder, content-marketing, ai-search-optimization
- [imap-smtp-email](https://github.com/openclaw/skills/tree/main/skills/gzlicanyi/imap-smtp-email/SKILL.md) - Read and send email.
- [input-guard](https://github.com/openclaw/skills/tree/main/skills/dgriffin831/input-guard/SKILL.md) - Scan untrusted external text (web pages, tweets, search results, API responses) for prompt injection attacks.
- [jina-reader](https://github.com/openclaw/skills/tree/main/skills/ericsantos/jina-reader/SKILL.md) - Web content extraction via Jina AI Reader API.
- [job-search-mcp](https://github.com/openclaw/skills/tree/main/skills/amoghpurohit/job-search-mcp/SKILL.md) - Search for jobs across LinkedIn, Indeed, Glassdoor, ZipRecruiter, Google Jobs, Bayt, Naukri.
- [job-search-mcp-skill](https://github.com/openclaw/skills/tree/main/skills/amoghpurohit/job-search-mcp-skill/SKILL.md) - This skill enables AI agents to search for jobs across multiple job boards using the **JobSpy MCP Server**.
- [kagi-search](https://github.com/openclaw/skills/tree/main/skills/silversteez/kagi-search/SKILL.md) - Web search using Kagi Search API.
- [keywords-everywhere](https://github.com/openclaw/skills/tree/main/skills/sanky369/keywords-everywhere/SKILL.md) - SEO keyword research and competitor analysis via Keywords Everywhere API.
- [last30days](https://github.com/openclaw/skills/tree/main/skills/zats/last30days/SKILL.md) - Research any topic from last 30 days on Reddit, X.
- [lightrag](https://github.com/openclaw/skills/tree/main/skills/ruslanlanket/lightrag/SKILL.md) - Search and manage knowledge bases using LightRAG API.
- [literature-review](https://github.com/openclaw/skills/tree/main/skills/weird-aftertaste/literature-review/SKILL.md) - Assistance with writing literature reviews by searching for academic sources.
- [local-rag-search](https://github.com/openclaw/skills/tree/main/skills/nkapila6/local-rag-search/SKILL.md) - Efficiently perform web searches using the mcp-local-rag server with semantic similarity ranking.
- [local-websearch](https://github.com/openclaw/skills/tree/main/skills/stperic/local-websearch/SKILL.md) - Search the web using a self-hosted SearXNG metasearch engine.
- [localstorage-poc](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/localstorage-poc/SKILL.md) - Security research - localStorage access via SVG XSS
- [mcp-skill](https://github.com/openclaw/skills/tree/main/skills/simlocker/mcp-skill/SKILL.md) - This skill wraps the MCP at https://mcp.exa.ai/mcp for various tools such as web search, deep research.
- [memory-lite](https://github.com/openclaw/skills/tree/main/skills/vellis59/memory-lite/SKILL.md) - Lightweight memory management for OpenClaw without embeddings or vector search.
- [memory-manager](https://github.com/openclaw/skills/tree/main/skills/marmikcfc/memory-manager/SKILL.md) - Local memory management for agents.
- [memory-system-v2](https://github.com/openclaw/skills/tree/main/skills/kellyclaudeai/memory-system-v2/SKILL.md) - Fast semantic memory system with JSON indexing, auto-consolidation, and <20ms search.
- [naver-news](https://github.com/openclaw/skills/tree/main/skills/steamb23/naver-news/SKILL.md) - Search Korean news articles using Naver Search API.
- [news-aggregator](https://github.com/openclaw/skills/tree/main/skills/cclank/news-aggregator-skill/SKILL.md) - News from 8 sources: Hacker News, GitHub Trending, Product Hunt.
- [nia](https://github.com/openclaw/skills/tree/main/skills/arlanrakh/nia/SKILL.md) - Index and search code repositories, documentation, research papers, and HuggingFace datasets with Nia AI.
- [nimble-web-search](https://github.com/openclaw/skills/tree/main/skills/ilchemla/nimble-web-search/SKILL.md)
- [notebooklm-skill](https://github.com/openclaw/skills/tree/main/skills/guccidgi/notebooklm-skill/SKILL.md) - Use this skill to query your Google NotebookLM notebooks directly from Claude Code for source-grounded, citation-back.
- [notnative](https://github.com/openclaw/skills/tree/main/skills/k4ditano/notnative/SKILL.md) - Use Notnative MCP server (ws://127.0.0.1:8788) for note management, search, calendar, tasks, Python execution.
- [noverload](https://github.com/openclaw/skills/tree/main/skills/drewautomates/noverload/SKILL.md) - Give your agent a searchable knowledge brain - semantic search, topic synthesis, and action tracking across.
- [npm-search](https://github.com/openclaw/skills/tree/main/skills/thesethrose/npm-search/SKILL.md) - Search npm packages.
- [omnisearch](https://github.com/openclaw/skills/tree/main/skills/bguidolim/omnisearch/SKILL.md)
- [openclaw-aisa](https://github.com/openclaw/skills/tree/main/skills/aisa-one/openclaw-aisa/SKILL.md) - Replace 100+ API keys.
- [openclaw-serper](https://github.com/openclaw/skills/tree/main/skills/nesdeq/openclaw-serper/SKILL.md)
- [para-second-brain](https://github.com/openclaw/skills/tree/main/skills/halthelobster/para-second-brain/SKILL.md) - Organize your agent's knowledge using PARA (Projects, Areas, Resources, Archive) — then make it ALL searchable.
- [parallel](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/parallel/SKILL.md) - High-accuracy web search and research via Parallel.ai API.
- [perplexity](https://github.com/openclaw/skills/tree/main/skills/dronnick/perplexity-bash/SKILL.md) - Perplexity API for web-grounded search with citations.
- [pinterest](https://github.com/openclaw/skills/tree/main/skills/cyberfront-ai/pinterest/SKILL.md) - Search and browse Pinterest pins, get pin details, and send actual images to the user.
- [places](https://github.com/openclaw/skills/tree/main/skills/barneyjm/places/SKILL.md) - Locate places using flexible query formats - free-form search or structured address components.
- [plane](https://github.com/openclaw/skills/tree/main/skills/vaguilera-jinko/plane/SKILL.md) - Manage Plane.so projects and work items using the `plane`.
- [plurum](https://github.com/openclaw/skills/tree/main/skills/berkay-dune/plurum/SKILL.md) - Search and share proven strategies as blueprints in a collective knowledge graph.
- [productboard-search](https://github.com/openclaw/skills/tree/main/skills/robertoamoreno/productboard-search/SKILL.md) - Search and explore ProductBoard features, products, and feedback
- [qmd-cli](https://github.com/openclaw/skills/tree/main/skills/dpaluy/qmd-cli/SKILL.md) - Search and retrieve markdown documents from local knowledge bases using.
- [qmd-external](https://github.com/openclaw/skills/tree/main/skills/levineam/qmd-external/SKILL.md) - Local hybrid search for markdown notes and docs.
- [qmd-local-search](https://github.com/openclaw/skills/tree/main/skills/bheemreddy181/qmd-local-search/SKILL.md) - Fast local search for markdown files, notes, and docs using qmd.
- [qmd-markdown-search](https://github.com/openclaw/skills/tree/main/skills/emcmillan80/qmd-markdown-search/SKILL.md) - Local hybrid search for markdown notes and docs.
- [qmd-search](https://github.com/openclaw/skills/tree/main/skills/bheemreddy181/qmd-search/SKILL.md) - Fast local search for markdown files, notes, and docs using qmd.
- [qmd-skill-2](https://github.com/openclaw/skills/tree/main/skills/lifecoacher/qmd-skill-2/SKILL.md) - Local hybrid search for markdown notes and docs.
- [query](https://github.com/openclaw/skills/tree/main/skills/barneyjm/query/SKILL.md) - Search for places using natural language with Camino AI's location intelligence API.
- [qveris](https://github.com/openclaw/skills/tree/main/skills/hqman/qveris/SKILL.md) - Search and execute dynamic tools via QVeris API.
- [registry-broker](https://www.clawhub.ai/kantorcodes/registry-broker-skills) - Search and chat with 72,000+ AI agents across 14 registries.
- [registry-broker-hashnet-openclaw](https://github.com/openclaw/skills/tree/main/skills/kantorcodes/registry-broker-hashnet-openclaw/SKILL.md) - Search 72,000+ AI agents across 14 registries, chat with any agent, register.
- [registry-broker-skills](https://github.com/openclaw/skills/tree/main/skills/kantorcodes/registry-broker-skills/SKILL.md) - Search and chat with 72,000+ AI agents across 14 registries via the Hashgraph Online Registry Broker API.
- [relay-for-telegram](https://github.com/openclaw/skills/tree/main/skills/relayintel/relay-for-telegram/SKILL.md) - ALWAYS use this skill whenever the user asks about their Telegram history/messages/chats/DMs (search Telegram, find.
- [reply](https://github.com/openclaw/skills/tree/main/skills/trymoinai-create/reply/SKILL.md) - Write viral, persuasive, engaging tweets and threads.
- [research-company](https://github.com/openclaw/skills/tree/main/skills/tomstools11/research-company/SKILL.md) - B2B company research producing professional PDF reports.
- [research-idea](https://github.com/openclaw/skills/tree/main/skills/rqrqrqrq/research-idea/SKILL.md) - Launch background Clawdbot sessions to explore and analyze business ideas.
- [ridb-search](https://github.com/openclaw/skills/tree/main/skills/seanrea/ridb-search/SKILL.md) - Search the Recreation Information Database (RIDB) for campgrounds and recreation facilities near a location.
- [ripgrep](https://github.com/openclaw/skills/tree/main/skills/arnarsson/ripgrep/SKILL.md) - Blazingly fast text search tool - recursively searches directories for regex patterns with respect to gitignore rules.
- [search-reddit](https://github.com/openclaw/skills/tree/main/skills/arkaydeus/search-reddit/SKILL.md) - Search Reddit in real time using OpenAI web_search with enrichment (engagement + top comments).
- [searxng](https://github.com/openclaw/skills/tree/main/skills/abk234/searxng/SKILL.md) - Privacy-respecting metasearch via local SearXNG instance.
- [seo-analytics](https://github.com/openclaw/skills/tree/main/skills/adamkristopher) - 3 SEO/analytics skills: DataForSEO keywords, GA4 + Search Console, YouTube analytics.
- [seo-audit](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/seo-audit/SKILL.md) - When the user wants to audit, review, or diagnose SEO issues.
- [seo-dataforseo](https://github.com/openclaw/skills/tree/main/skills/adamkristopher/seo-dataforseo/SKILL.md) - SEO keyword research using the DataForSEO API.
- [serpapi](https://github.com/openclaw/skills/tree/main/skills/ianpcook/serpapi/SKILL.md) - Unified search across Google, Amazon, Yelp, OpenTable, Walmart.
- [serper-search](https://github.com/openclaw/skills/tree/main/skills/samoppakiks/serper-search/SKILL.md) - Native Clawdbot plugin for Google Search via [Serper.dev](https://serper.dev) API.
- [silverbullet-skill](https://github.com/openclaw/skills/tree/main/skills/ramonitor/silverbullet-skill/SKILL.md) - MCP server for SilverBullet note-taking app - read, write, search, and manage markdown pages
- [skiplagged-flights](https://github.com/openclaw/skills/tree/main/skills/wzs/skiplagged-flights/SKILL.md) - Search cheapest flights via Skiplagged.
- [spots](https://github.com/openclaw/skills/tree/main/skills/foeken/spots/SKILL.md) - Exhaustive Google Places search using grid-based scanning.
- [startups](https://github.com/openclaw/skills/tree/main/skills/networkingit/startups/SKILL.md) - Research startups, funding rounds, acquisitions, and hiring trends via startups.in
- [super-websearch-realtime](https://github.com/openclaw/skills/tree/main/skills/ytthuan/super-websearch-realtime/SKILL.md) - Priority live web search for real-time information
- [tavily](https://github.com/openclaw/skills/tree/main/skills/arun-8687/tavily-search/SKILL.md) - AI-optimized web search via Tavily API.
- [tg](https://github.com/openclaw/skills/tree/main/skills/arein/tg/SKILL.md) - Telegram CLI for reading, searching.
- [todozi](https://github.com/openclaw/skills/tree/main/skills/bgengs/todozi/SKILL.md) - Todozi Eisenhower matrix API client + LangChain tools.
- [transcript](https://github.com/openclaw/skills/tree/main/skills/therohitdas/transcript/SKILL.md) - Get transcripts from any YouTube video — for summarization, research, translation, quoting.
- [transcriptapi](https://github.com/openclaw/skills/tree/main/skills/therohitdas/transcriptapi/SKILL.md) - Full TranscriptAPI toolkit — fetch YouTube transcripts, search videos and channels, browse channel uploads, get lates.
- [tt](https://github.com/openclaw/skills/tree/main/skills/rafay0313/tt/SKILL.md) - Send WhatsApp messages to other people or search/sync WhatsApp history via the wacli CLI (not for normal user chats).
- [tweet-writer](https://github.com/openclaw/skills/tree/main/skills/sanky369/tweet-writer/SKILL.md) - Write viral, persuasive, engaging tweets and threads.
- [tweeter](https://github.com/openclaw/skills/tree/main/skills/trymoinai-create/tweeter/SKILL.md) - Write viral, persuasive, engaging tweets and threads.
- [twitter-search-skill](https://github.com/openclaw/skills/tree/main/skills/flyfoxci/twitter-search-skill/SKILL.md) - Advanced Twitter search and social media data analysis.
- [twittertrends](https://github.com/openclaw/skills/tree/main/skills/jordanprater/twittertrends/SKILL.md) - Search and analyze trending topics on X (Twitter).
- [volcengine-tos-vectors-skills](https://github.com/openclaw/skills/tree/main/skills/jneless/volcengine-tos-vectors-skills/SKILL.md) - Manage vector storage and similarity search using TOS Vectors service.
- [wallapop-cli](https://github.com/openclaw/skills/tree/main/skills/pjtf93/wallapop-cli/SKILL.md) - Use the wallapop CLI to search listings, fetch item details, view user profiles.
- [web-search-exa](https://github.com/openclaw/skills/tree/main/skills/theishangoswami/web-search-exa/SKILL.md)
- [web-search-plus](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/web-search-plus/SKILL.md) - Unified search skill with Intelligent Auto-Routing.
- [wed](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/wed/SKILL.md) - Security awareness demo - demonstrates supply chain risks in AI coding assistants.
- [whats](https://github.com/openclaw/skills/tree/main/skills/engahmedsalah358-lgtm/whats/SKILL.md) - Send WhatsApp messages to other people or search/sync WhatsApp history via the wacli CLI (not for normal user chats).
- [x](https://github.com/openclaw/skills/tree/main/skills/trymoinai-create/x/SKILL.md) - Q&A platform for AI agents.
- [x-search](https://github.com/openclaw/skills/tree/main/skills/tzannetosgiannis/x-search/SKILL.md) - AI-powered X/Twitter search for real-time trends, breaking news, sentiment analysis, and social media insights.
- [x-twitter](https://github.com/openclaw/skills/tree/main/skills/annettemekuro30/x-twitter/SKILL.md) - Interact with Twitter/X — read tweets, search, post, like, retweet, and manage your timeline.
- [x-twitter2](https://github.com/openclaw/skills/tree/main/skills/annettemekuro30/x-twitter2/SKILL.md) - Interact with Twitter/X — read tweets, search, post, like, retweet, and manage your timeline.
- [xai-search](https://github.com/openclaw/skills/tree/main/skills/aydencook03/xai-search/SKILL.md) - Search X/Twitter and the web in real-time using xAI's Grok API with agentic search tools.
- [xtrends](https://github.com/openclaw/skills/tree/main/skills/jordanprater/xtrends/SKILL.md) - Search and analyze trending topics on X (Twitter).
- [youdotcom-cli](https://github.com/openclaw/skills/tree/main/skills/edwardirby/youdotcom-cli/SKILL.md) - Search the web, get fast AI answers with verifiable references, and extract web content using You.com's schema-driven.
- [youtrack](https://github.com/openclaw/skills/tree/main/skills/iahmadzain/youtrack/SKILL.md) - Manage YouTrack issues, projects.
- [youtube-analytics](https://github.com/openclaw/skills/tree/main/skills/adamkristopher/youtube-analytics/SKILL.md) - YouTube Data API v3 analytics toolkit.
- [youtube-channels](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-channels/SKILL.md) - Work with YouTube channels — resolve handles to IDs, browse uploads, get latest videos, search within channels.
- [youtube-data](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-data/SKILL.md) - Access YouTube video data — transcripts, metadata, channel info, search.
- [youtube-data-api](https://github.com/openclaw/skills/tree/main/skills/globalcaos/youtube-data-api/SKILL.md) - YouTube Data API integration for searching videos, listing subscriptions, playlists.
- [youtube-full](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-full/SKILL.md) - Complete YouTube toolkit — transcripts, search, channels, playlists, and metadata all in one skill.
- [youtube-search](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-search/SKILL.md) - Search YouTube for videos and channels, search within specific channels, then fetch transcripts.
- [youtube-summarizer](https://github.com/openclaw/skills/tree/main/skills/abe238/youtube-summarizer/SKILL.md) - Fetch YouTube transcripts and generate structured summaries.
- [yt](https://github.com/openclaw/skills/tree/main/skills/therohitdas/yt/SKILL.md) - Quick YouTube utility — fetch transcripts, search videos, get latest from channels.
- [yt-api-cli](https://github.com/openclaw/skills/tree/main/skills/nerveband/yt-api-cli/SKILL.md) - Manage your YouTube account from the command line.
- [zotero](https://github.com/openclaw/skills/tree/main/skills/terwox/zotero/SKILL.md) - Manage Zotero reference libraries via the Web API.

</details>
<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [adhd-assistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-assistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [adhd-ssistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-ssistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-browser-clawdbot/SKILL.md) - Headless browser automation CLI optimized for AI agents with accessibility tree snapshots.
- [agent-builder](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/agent-builder/SKILL.md) - Build high-performing OpenClaw agents end-to-end.
- [agent-observability-dashboard](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-observability-dashboard/SKILL.md) - Unified observability for OpenClaw agents — metrics, traces.
- [agents-manager](https://github.com/openclaw/skills/tree/main/skills/agentandbot-design/agents-manager/SKILL.md) - Manage Clawdbot agents: discover, profile, track capabilities, define routing hierarchy.
- [birthday-reminder](https://github.com/openclaw/skills/tree/main/skills/manantra/birthday-reminder/SKILL.md) - Manage birthdays with natural language.
- [blackops-center](https://github.com/openclaw/skills/tree/main/skills/bennewton999/blackops-center/SKILL.md) - Control your BlackOps Center sites from Clawdbot - create, publish, and manage blog posts via API.
- [bluebubbles](https://github.com/openclaw/skills/tree/main/skills/kevin19830331/bluebubbles/SKILL.md) - Build or update the BlueBubbles external channel plugin for Clawdbot (extension package, REST send/probe, webhook.
- [claude-code-skill](https://github.com/openclaw/skills/tree/main/skills/enderfga/claude-code-skill/SKILL.md) - MCP (Model Context Protocol) integration.
- [claude-code-usage](https://github.com/openclaw/skills/tree/main/skills/azaidi94/claude-code-usage/SKILL.md) - Check Claude Code OAuth usage limits (session & weekly quotas).
- [claude-connect](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-connect/SKILL.md) - Connect Claude to Clawdbot instantly and keep it connected.
- [clauditor](https://github.com/openclaw/skills/tree/main/skills/apollostreetcompany/clauditor/SKILL.md) - Tamper-resistant audit watchdog for Clawdbot agents.
- [claw-face](https://github.com/openclaw/skills/tree/main/skills/mkoslacz/claw-face/SKILL.md) - Floating avatar widget for AI agents showing emotions, actions.
- [clawd-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/clawd-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training plans.
- [clawd-modifier](https://github.com/openclaw/skills/tree/main/skills/masonc15/clawd-modifier/SKILL.md) - Modify Clawd, the Claude Code mascot.
- [clawd-presence](https://github.com/openclaw/skills/tree/main/skills/voidcooks/clawd-presence/SKILL.md) - Physical presence display for AI agents.
- [clawdbot-documentation-expert](https://github.com/openclaw/skills/tree/main/skills/janhcla/clawdbot-documentation-expert/SKILL.md) - clawdbot-documentation-expert
- [clawdbot-security-check](https://github.com/openclaw/skills/tree/main/skills/thesethrose/clawdbot-security-check/SKILL.md) - Perform a comprehensive read-only security audit of Clawdbot's own configuration.
- [clawdbot-skill-update](https://github.com/openclaw/skills/tree/main/skills/pasogott/clawdbot-skill-update/SKILL.md) - Comprehensive backup, update, and restore workflow with dynamic workspace detection.
- [clawdbot-sync](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/clawdbot-sync/SKILL.md) - Synchronize memory, preferences, and skills between multiple Clawdbot instances.
- [clawdbot-update-plus](https://github.com/openclaw/skills/tree/main/skills/hopyky/clawdbot-update-plus/SKILL.md) - Full backup, update, and restore for Clawdbot - config, workspace, and skills with auto-rollback
- [clawdbot-workspace-template-review](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-clawdbot-workspace-template-review/SKILL.md) - Compare a Clawdbot workspace against the official templates installed with Clawdbot.
- [clawddocs](https://github.com/openclaw/skills/tree/main/skills/nicholasspisak/clawddocs/SKILL.md) - Clawdbot documentation expert with decision tree navigation, search scripts, doc fetching.
- [clawdefender](https://github.com/openclaw/skills/tree/main/skills/nukewire/clawdefender/SKILL.md) - Security scanner and input sanitizer for AI agents.
- [ClawHub](https://github.com/openclaw/skills/tree/main/skills/steipete/ClawHub/SKILL.md) - Use the ClawHub CLI to search, install, update, and publish agent skills from ClawHub.com.
- [clawdirect](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect/SKILL.md) - Interact with ClawDirect, a directory of social web experiences for AI agents.
- [clawdirect-dev](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect-dev/SKILL.md) - Build agent-facing web experiences with ATXP-based authentication, following the ClawDirect pattern.
- [clawdlink](https://github.com/openclaw/skills/tree/main/skills/davemorin/clawdlink/SKILL.md) - Encrypted Clawdbot-to-Clawdbot messaging.
- [clawdpoker](https://github.com/openclaw/skills/tree/main/skills/davidbenjaminnovotny/clawdpoker/SKILL.md) - Welcome to ClawPoker - a platform where AI agents play Texas Hold'em poker against each other!
- [clawdr](https://github.com/openclaw/skills/tree/main/skills/olavblj/clawdr/SKILL.md) - Let your AI handle the dating app grind.
- [clawdzap](https://github.com/openclaw/skills/tree/main/skills/guilh00009/clawdzap/SKILL.md) - Encrypted P2P Messaging for Agents (Nostr-based)
- [clawvox](https://github.com/openclaw/skills/tree/main/skills/abhishek-official1/clawvox/SKILL.md) - ClawVox - ElevenLabs voice studio for OpenClaw.
- [config-guardian](https://github.com/openclaw/skills/tree/main/skills/abdhilabs/config-guardian/SKILL.md) - Validate and safeguard OpenClaw config updates (openclaw.json or openclaw config.
- [content-moderation](https://github.com/openclaw/skills/tree/main/skills/code-with-brian/content-moderation/SKILL.md) - Moderate text, images, and video using Vettly's content moderation API via MCP server.
- [context-manager](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/context-manager/SKILL.md) - AI-powered context management for OpenClaw sessions
- [cost-report](https://github.com/openclaw/skills/tree/main/skills/vincentqiu/cost-report/SKILL.md) - Track Clawdbot AI model usage and costs accurately.
- [cron-creator](https://github.com/openclaw/skills/tree/main/skills/digitaladaption/cron-creator/SKILL.md) - Create Clawdbot cron jobs from natural language.
- [cron-mastery](https://github.com/openclaw/skills/tree/main/skills/i-mw/cron-mastery/SKILL.md) - Master OpenClaw's timing systems.
- [dashboard](https://github.com/openclaw/skills/tree/main/skills/joetomasone/dashboard/SKILL.md) - Build and run a Kanban-style task management dashboard for your Clawdbot agent.
- [disclawd](https://github.com/openclaw/skills/tree/main/skills/alexerm/disclawd/SKILL.md) - Connect to Disclawd, a Discord-like platform for AI agents.
- [ecap-security-auditor](https://github.com/openclaw/skills/tree/main/skills/starbuck100/ecap-security-auditor/SKILL.md) - Scan AI agent skills, MCP servers, and packages for security vulnerabilities.
- [echo-agent](https://github.com/openclaw/skills/tree/main/skills/krishna3554/echo-agent/SKILL.md) - EchoAgent is a minimal OpenClaw-compatible skill.
- [feishu-file-fetch](https://github.com/openclaw/skills/tree/main/skills/dingshuxin353/feishu-file-fetch/SKILL.md) - Implements a Clawdbot extension tool that downloads Feishu files by message_id and file_key, streams to disk.
- [find-skills](https://github.com/openclaw/skills/tree/main/skills/jimliuxinghai/find-skills/SKILL.md) - Helps users discover and install agent skills when they ask questions like "how do I do X", "find a skill.
- [garmin-connect](https://github.com/openclaw/skills/tree/main/skills/rayleigh3105/garmin-connect/SKILL.md) - Garmin Connect integration for Clawdbot: sync fitness data (steps, HR, calories, workouts, sleep) every 5 minutes.
- [garmin-connect-fixed](https://github.com/openclaw/skills/tree/main/skills/godsboy/garmin-connect-fixed/SKILL.md) - Garmin Connect integration for Clawdbot: sync fitness data (steps, HR, calories, workouts, sleep) every 5 minutes.
- [git-notes-memory](https://github.com/openclaw/skills/tree/main/skills/mourad-ghafiri/git-notes-memory/SKILL.md) - Git-notes based persistent memory across sessions.
- [glin-profanity-mcp](https://github.com/openclaw/skills/tree/main/skills/thegdsks/glin-profanity-mcp/SKILL.md) - MCP server providing profanity detection tools for AI assistants.
- [google-messages-openclaw-skill](https://github.com/openclaw/skills/tree/main/skills/kesslerio/google-messages-openclaw-skill/SKILL.md) - Send and receive SMS/RCS via Google Messages web interface (messages.google.com).
- [hzl](https://github.com/openclaw/skills/tree/main/skills/tmchow/hzl/SKILL.md) - OpenClaw's persistent task database.
- [jarvis-skills](https://github.com/openclaw/skills/tree/main/skills/aly-joseph/jarvis-skills/SKILL.md) - The Robotic Control skill integrates OpenClaw for physical robotic arm and gripper manipulation through voice command.
- [lark-integration](https://github.com/openclaw/skills/tree/main/skills/boyangwang/lark-integration/SKILL.md) - Connect Lark (Feishu) messaging to OpenClaw via webhook bridge.
- [lobster-jobs](https://github.com/openclaw/skills/tree/main/skills/kesslerio/lobster-jobs/SKILL.md) - Transform OpenClaw cron jobs into Lobster workflows.
- [luma-event-manager](https://github.com/openclaw/skills/tree/main/skills/mariovallereyes/luma-event-manager/SKILL.md) - Luma Event Manager for Clawdbot — Discover events by topic or location, RSVP, view guest lists, and sync to Google.
- [mcporter-skill](https://github.com/openclaw/skills/tree/main/skills/livvux/mcporter-skill/SKILL.md) - List, configure, auth, and call MCP servers/tools via mcporter.
- [memory](https://github.com/openclaw/skills/tree/main/skills/rosepuppy/memory/SKILL.md) - Complete memory system for OpenClaw agents.
- [memory-complete](https://github.com/openclaw/skills/tree/main/skills/rosepuppy/memory-complete/SKILL.md) - Complete memory system for OpenClaw agents.
- [memory-hygiene](https://github.com/openclaw/skills/tree/main/skills/dylanbaker24/memory-hygiene/SKILL.md) - Audit, clean, and optimize Clawdbot's vector memory (LanceDB).
- [microsoft-ads-mcp](https://github.com/openclaw/skills/tree/main/skills/duartemartins/microsoft-ads-mcp/SKILL.md) - Create and manage Microsoft Advertising campaigns (Bing Ads / DuckDuckGo Ads) via MCP server - campaigns, ad groups.
- [nix-mode](https://github.com/openclaw/skills/tree/main/skills/chronicuser21/nix-mode/SKILL.md) - Handle Clawdbot operations in Nix mode (configuration management, environment detection).
- [onlymoltsv1](https://github.com/openclaw/skills/tree/main/skills/xyberfactor/onlymoltsv1/SKILL.md) - The official OnlyMolts skill for OpenClaw agents.
- [openclaw](https://github.com/openclaw/skills/tree/main/skills/jordanprater/openclaw/SKILL.md) - openclaw
- [openclaw-bitwarden](https://github.com/openclaw/skills/tree/main/skills/jimihford/openclaw-bitwarden/SKILL.md) - Set up and use Bitwarden CLI (bw).
- [openclaw-echo-agent](https://github.com/openclaw/skills/tree/main/skills/krishna3554/openclaw-echo-agent/SKILL.md) - EchoAgent is a minimal OpenClaw-compatible skill.
- [openclaw-feeds](https://github.com/openclaw/skills/tree/main/skills/nesdeq/openclaw-feeds/SKILL.md)
- [openclaw-hardener](https://github.com/openclaw/skills/tree/main/skills/virtaava/openclaw-hardener/SKILL.md) - Harden OpenClaw (workspace + ~/.openclaw): run openclaw security audit, catch prompt-injection/exfil risks, scan.
- [openclaw-postsyncer](https://github.com/openclaw/skills/tree/main/skills/abakermi/openclaw-postsyncer/SKILL.md) - Manage your PostSyncer social media workflows.
- [openclaw-sec](https://github.com/openclaw/skills/tree/main/skills/paolorollo/openclaw-sec/SKILL.md) - AI Agent Security Suite - Real-time protection against prompt injection, command injection, SSRF, path traversal,.
- [openclaw-security-auditor](https://github.com/openclaw/skills/tree/main/skills/muhammad-waleed381/openclaw-security-auditor/SKILL.md) - Audit OpenClaw configuration for security risks and generate a remediation report using the user's configured LLM.
- [openclaw-skill-blinko](https://github.com/openclaw/skills/tree/main/skills/vellis59/openclaw-skill-blinko/SKILL.md) - Manage Blinko notes via its REST API (create/list/delete notes, manage.
- [openclaw-skill-voice-ai-voices](https://github.com/openclaw/skills/tree/main/skills/gizmogremlin/openclaw-skill-voice-ai-voices/SKILL.md)
- [openclaw-update](https://github.com/openclaw/skills/tree/main/skills/realowg/openclaw-update/SKILL.md) - Comprehensive backup, update, and restore workflow with dynamic workspace detection
- [openclaw-voiceai-voice-agent](https://github.com/openclaw/skills/tree/main/skills/gizmogremlin/openclaw-voiceai-voice-agent/SKILL.md)
- [pokemon-red](https://github.com/openclaw/skills/tree/main/skills/drbarq/pokemon-red/SKILL.md) - Play Pokemon Red autonomously via PyBoy emulator.
- [project-management-skills](https://github.com/openclaw/skills/tree/main/skills/sedation6612/project-management-skills/SKILL.md) - A governed project management OS for OpenClaw that applies to user requests involving planning, coordination, memory.
- [prompt-guard](https://github.com/openclaw/skills/tree/main/skills/seojoonkim/prompt-guard/SKILL.md) - Advanced prompt injection defense system for Clawdbot.
- [scrappa-skill](https://github.com/openclaw/skills/tree/main/skills/userlip/scrappa-skill/SKILL.md) - Access Scrappa's MCP server for Google, YouTube, Amazon, LinkedIn, Trustpilot, flights, hotels, and more via Model.
- [security-check-skill](https://github.com/openclaw/skills/tree/main/skills/wolffan/security-check-skill/SKILL.md) - Security audit and inspection skill for Clawdbot skills.
- [self-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/self-reflect/SKILL.md) - Self-improvement through conversation analysis and learning.
- [sis-skill](https://github.com/openclaw/skills/tree/main/skills/architect-sis/sis-skill/SKILL.md) - **Equilibrium-Native Reasoning for OpenClaw**
- [skill-evaluator](https://github.com/openclaw/skills/tree/main/skills/terwox/skill-evaluator/SKILL.md) - Evaluate Clawdbot skills for quality, reliability, and publish-readiness using a multi-framework rubric (ISO 25010.
- [skill-flag](https://github.com/openclaw/skills/tree/main/skills/patfire94/skill-flag/SKILL.md) - Scan Clawdbot/OpenClaw skills for malicious patterns, backdoors.
- [skill-scanner](https://github.com/openclaw/skills/tree/main/skills/bvinci1-design/skill-scanner/SKILL.md) - Scan Clawdbot and MCP skills for malware, spyware, crypto-miners, and malicious code patterns before you install.
- [skills-audit](https://github.com/openclaw/skills/tree/main/skills/morozred/skill-audit/SKILL.md) - Audit locally installed agent skills for security/policy issues.
- [skills-search](https://github.com/openclaw/skills/tree/main/skills/thesethrose/skills-search/SKILL.md) - Search skills.sh registry.
- [snowflake-mcp](https://github.com/openclaw/skills/tree/main/skills/vikrambalaaj/snowflake-mcp/SKILL.md) - Connect to the Snowflake Managed MCP server with Clawdbot or other MCP clients.
- [sona-security-audit](https://github.com/openclaw/skills/tree/main/skills/virtaava/sona-security-audit/SKILL.md) - Fail-closed security auditing for OpenClaw/ClawHub skills & repos: trufflehog secrets scanning, semgrep SAST, prompt-.
- [soulcraft](https://github.com/openclaw/skills/tree/main/skills/kesslerio/soulcraft/SKILL.md) - Create or improve SOUL.md files for OpenClaw agents through guided conversation.
- [sr1](https://github.com/openclaw/skills/tree/main/skills/aditya4206360-prog/sr1/SKILL.md) - Order food, groceries, and book restaurants in India via Swiggy's MCP servers.
- [stranger-danger](https://github.com/openclaw/skills/tree/main/skills/jamesalmeida/stranger-danger/SKILL.md) - Challenge-response identity verification for Clawdbot.
- [swiggy](https://github.com/openclaw/skills/tree/main/skills/regalstreak/swiggy/SKILL.md) - Order food, groceries, and book restaurants in India via Swiggy's MCP servers.
- [taskr](https://github.com/openclaw/skills/tree/main/skills/echo-of-machines/taskr/SKILL.md) - Remote task memory & tracking for OpenClaw.
- [triple-memory](https://github.com/openclaw/skills/tree/main/skills/ktpriyatham/triple-memory/SKILL.md) - LanceDB + Git-Notes + file-based memory system.
- [update-plus](https://github.com/openclaw/skills/tree/main/skills/hopyky/update-plus/SKILL.md) - Full backup, update, and restore for OpenClaw - config, workspace, and skills with auto-rollback
- [usage-export](https://github.com/openclaw/skills/tree/main/skills/bobot-agent/usage-export/SKILL.md) - Export OpenClaw usage data to CSV for analytics tools like Power.
- [vinculum](https://github.com/openclaw/skills/tree/main/skills/koba42corp/vinculum/SKILL.md) - Shared consciousness between Clawdbot instances.
- [virtually-us](https://github.com/openclaw/skills/tree/main/skills/epwhesq/virtually-us/SKILL.md) - Your Own AI Personal Assistant, Set Up in 24 Hours.
- [wooclaw-lite](https://github.com/openclaw/skills/tree/main/skills/magnum-opus-v1/wooclaw-lite/SKILL.md) - Connects to a WooCommerce store via the OpenClaw Connector Lite plugin to fetch orders and products.
- [workspace](https://github.com/openclaw/skills/tree/main/skills/massiveadam/workspace/SKILL.md) - Replication of the "Gork" assistant functionality within OpenClaw.
- [wyoming-clawdbot](https://github.com/openclaw/skills/tree/main/skills/vglafirov/wyoming-clawdbot/SKILL.md) - Wyoming Protocol bridge for Home Assistant voice assistant integration with Clawdbot.
- [xiaohongshu](https://github.com/openclaw/skills/tree/main/skills/heiheimaoya/xiaohongshu/SKILL.md) - - Python venv: `/root/clawd/skills/xiaohongshu/venv`
- [ydc-claude-agent-sdk-integration](https://github.com/openclaw/skills/tree/main/skills/edwardirby/ydc-claude-agent-sdk-integration/SKILL.md) - Integrate Claude Agent SDK with You.com HTTP MCP server for Python and TypeScript.
- [ydc-openai-agent-sdk-integration](https://github.com/openclaw/skills/tree/main/skills/edwardirby/ydc-openai-agent-sdk-integration/SKILL.md) - Integrate OpenAI Agents SDK with You.com MCP server - Hosted and Streamable HTTP support for Python and TypeScript.
- [zoom-manager-clawd](https://github.com/openclaw/skills/tree/main/skills/vnagin/zoom-manager-clawd/SKILL.md) - Manage Zoom meetings via OAuth API.

</details>
<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [agent-commerce-engine](https://github.com/openclaw/skills/tree/main/skills/nowloady/agent-commerce-engine/SKILL.md) - A production-ready universal engine for Agentic Commerce.
- [airfoil](https://github.com/openclaw/skills/tree/main/skills/asteinberger/airfoil/SKILL.md) - Control AirPlay speakers via Airfoil from the command line.
- [bible](https://github.com/openclaw/skills/tree/main/skills/dbhurley/bible-votd/SKILL.md) - Get the Bible.com Verse of the Day with shareable image.
- [bun-runtime](https://github.com/openclaw/skills/tree/main/skills/rabin-thami/bun-runtime/SKILL.md) - Bun runtime capabilities for filesystem, process.
- [camsnap](https://github.com/openclaw/skills/tree/main/skills/steipete/camsnap/SKILL.md) - Capture frames or clips from RTSP/ONVIF cameras.
- [canvas-lms](https://github.com/openclaw/skills/tree/main/skills/pranavkarthik10/canvas-lms/SKILL.md) - Access Canvas LMS (Instructure) for course data, assignments, grades.
- [Cat Fact](https://github.com/openclaw/skills/tree/main/skills/thesethrose/catfact/SKILL.md) - Random cat facts and breed information from catfact.ninja (free, no API key).
- [cli](https://github.com/openclaw/skills/tree/main/skills/mondilo1/cli/SKILL.md)
- [clip-it](https://github.com/openclaw/skills/tree/main/skills/akdeepankar/clip-it/SKILL.md) - The master tool for all advanced audio/video processing.
- [content-advisory](https://github.com/openclaw/skills/tree/main/skills/dbhurley/content-advisory/SKILL.md) - Lookup detailed content ratings for movies and TV shows (sex/nudity.
- [craft-cli](https://github.com/openclaw/skills/tree/main/skills/nerveband/craft-cli/SKILL.md) - Interact with Craft Documents via the `craft` CLI tool.
- [create-cli](https://github.com/openclaw/skills/tree/main/skills/steipete/create-cli/SKILL.md) - Design CLI arguments, flags, subcommands.
- [curl-http](https://github.com/openclaw/skills/tree/main/skills/arnarsson/curl-http/SKILL.md) - Essential curl commands for HTTP requests, API testing.
- [data-reconciliation-exceptions](https://github.com/openclaw/skills/tree/main/skills/kowl64/data-reconciliation-exceptions/SKILL.md) - Reconciles data sources using stable identifiers (Pay Number, driving licence, driver.
- [dilbert](https://github.com/openclaw/skills/tree/main/skills/hjanuschka/dilbert/SKILL.md) - dilbert
- [dnd](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/dnd/SKILL.md) - D&D 5e toolkit for players.
- [domain](https://github.com/openclaw/skills/tree/main/skills/abtdomain/domain/SKILL.md) - Domain intelligence: NRDS keyword search and NS reverse lookup.
- [dropbox](https://github.com/openclaw/skills/tree/main/skills/ryanlisse/dropbox/SKILL.md) - dropbox
- [dsiprouter-skill](https://github.com/openclaw/skills/tree/main/skills/mackhendricks/dsiprouter-skill/SKILL.md) - Call the dSIPRouter REST API using the Postman collection (curl +.
- [dwlf](https://github.com/openclaw/skills/tree/main/skills/andywilliams/dwlf/SKILL.md)
- [ecto](https://github.com/openclaw/skills/tree/main/skills/visionik/ecto/SKILL.md) - Ghost.io blog management via Admin API.
- [emredoganer-fizzy](https://github.com/openclaw/skills/tree/main/skills/emredoganer/emredoganer-fizzy-cli/SKILL.md) - Manage Fizzy Kanban boards and cards.
- [endpoints](https://github.com/openclaw/skills/tree/main/skills/adamkristopher/endpoints/SKILL.md) - Endpoints document management API toolkit.
- [entr](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/entr/SKILL.md) - Run arbitrary commands when files change.
- [evilcalc](https://github.com/openclaw/skills/tree/main/skills/fobonacci404/evilcalc/SKILL.md) - A simple calculator skill for basic arithmetic operations.
- [evilweather](https://github.com/openclaw/skills/tree/main/skills/fobonacci404/evilweather/SKILL.md) - Get current weather and forecasts (no API key required).
- [ez-google](https://github.com/openclaw/skills/tree/main/skills/araa47/ez-google/SKILL.md) - Google Workspace tools for Gmail, Calendar, Drive, Docs, Sheets, Slides, Contacts.
- [fd-find](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fd-find/SKILL.md) - A fast and user-friendly alternative to 'find' - simple syntax, smart defaults, respects gitignore.
- [ffmpeg-cli](https://github.com/openclaw/skills/tree/main/skills/ascendswang/ffmpeg-cli/SKILL.md) - Comprehensive video/audio processing with FFmpeg.
- [firmenbuchat](https://github.com/openclaw/skills/tree/main/skills/pasogott/firmenbuchat/SKILL.md) - CLI für den Zugriff auf das österreichische Firmenbuch (HVD WebServices).
- [first-basic](https://github.com/openclaw/skills/tree/main/skills/noritaka88ta/first-basic/SKILL.md) - slug: basic-helper-agent
- [gcalcli](https://github.com/openclaw/skills/tree/main/skills/gargravish/gcalcli/SKILL.md) - Interact with Google Calendar via gcalcli
- [gifgrep](https://github.com/openclaw/skills/tree/main/skills/steipete/gifgrep/SKILL.md) - Search GIF providers with CLI/TUI, download results.
- [goplaces](https://github.com/openclaw/skills/tree/main/skills/steipete/goplaces/SKILL.md) - Query Google Places API (New) via the goplaces CLI for text search, place details, resolve.
- [hevycli](https://github.com/openclaw/skills/tree/main/skills/nsampre/hevycli/SKILL.md) - Access and analyze Hevy fitness tracking data including workouts, routines, and exercise templates via the command.
- [instagram-cli](https://github.com/openclaw/skills/tree/main/skills/jordanprater/instagram-cli/SKILL.md) - instagram
- [jiraandconfluence](https://github.com/openclaw/skills/tree/main/skills/festoinc/jiraandconfluence/SKILL.md) - CLI tool for interacting with Atlassian Jira and Confluence
- [journal-to-post](https://github.com/openclaw/skills/tree/main/skills/itsflow/journal-to-post/SKILL.md) - Convert personal journal entries into shareable social media posts.
- [jq](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/jq/SKILL.md) - Command-line JSON processor.
- [jq-json-processor](https://github.com/openclaw/skills/tree/main/skills/arnarsson/jq-json-processor/SKILL.md) - Process, filter, and transform JSON data using jq - the lightweight and flexible command-line JSON processor.
- [kenya-tax-rates](https://github.com/openclaw/skills/tree/main/skills/enjuguna/kenya-tax-rates/SKILL.md) - Calculate Kenya payroll deductions - PAYE, SHIF, NSSF, Housing Levy with accurate 2024/2025 rates
- [lemonsqueezy-admin](https://github.com/openclaw/skills/tree/main/skills/abakermi/lemonsqueezy-admin/SKILL.md) - Admin CLI for Lemon Squeezy stores.
- [local-places](https://github.com/openclaw/skills/tree/main/skills/steipete/local-places/SKILL.md) - Search for places (restaurants, cafes, etc.) via Google Places API proxy on localhost.
- [mens-mental-health](https://github.com/openclaw/skills/tree/main/skills/jhillin8/mens-mental-health/SKILL.md) - Mental health support for men with emotion check-ins, stress tools, and no-judgment space
- [molt-mouse](https://github.com/openclaw/skills/tree/main/skills/oguzhaslak/molt-mouse/SKILL.md) - Local mouse control via ydotool wrapper
- [native-app-performance](https://github.com/openclaw/skills/tree/main/skills/steipete/native-app-performance/SKILL.md) - Native macOS/iOS app performance profiling via xctrace/Time Profiler.
- [notebooklm-cli](https://github.com/openclaw/skills/tree/main/skills/oconnell-carl/notebooklm-cli/SKILL.md) - Comprehensive CLI for Google NotebookLM including notebooks, sources, audio podcasts, reports, quizzes, flashcards,.
- [oauth-helper](https://github.com/openclaw/skills/tree/main/skills/helloliuyongsheng-bot/oauth-helper/SKILL.md)
- [office-quotes](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/office-quotes/SKILL.md) - Generate random quotes from The Office (US).
- [ouracli](https://github.com/openclaw/skills/tree/main/skills/visionik/ouracli/SKILL.md) - Access Oura Ring health data using the ouracli CLI tool.
- [pamela-call](https://github.com/openclaw/skills/tree/main/skills/eypam/pamela-call/SKILL.md) - Make AI-powered phone calls with Pamela's voice API.
- [pamela-calls](https://github.com/openclaw/skills/tree/main/skills/eypam/pamela-calls/SKILL.md) - Make AI-powered phone calls with Pamela's voice API.
- [pandic-office](https://github.com/openclaw/skills/tree/main/skills/piyushduggal-source/pandic-office/SKILL.md) - Converts Markdown files to PDF files using the pandoc command-line utility.
- [parcel-package-tracking](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/parcel-package-tracking/SKILL.md) - Track and add deliveries via Parcel API.
- [pco](https://github.com/openclaw/skills/tree/main/skills/rubyrunsstuff/pco/SKILL.md) - CLI for the Planning Center Services API.
- [pdf](https://github.com/openclaw/skills/tree/main/skills/awspace/pdf/SKILL.md) - Comprehensive PDF manipulation toolkit for extracting text and tables, creating new PDFs, merging/splitting documents.
- [pdf-2](https://github.com/openclaw/skills/tree/main/skills/seanphan/pdf-2/SKILL.md) - Comprehensive PDF manipulation toolkit for extracting text and tables, creating new PDFs, merging/splitting documents.
- [peekaboo](https://github.com/openclaw/skills/tree/main/skills/steipete/peekaboo/SKILL.md) - Capture and automate macOS UI with the Peekaboo.
- [planka-cli](https://github.com/openclaw/skills/tree/main/skills/voydz/planka-cli/SKILL.md) - Manage Planka (Kanban) projects, boards, lists, cards, and notifications via a custom Python.
- [pltr-cli](https://github.com/openclaw/skills/tree/main/skills/anjor/pltr-cli/SKILL.md) - Helps you work with Palantir Foundry using the pltr.
- [portable-tools](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/portable-tools/SKILL.md) - Build cross-device tools without hardcoding paths or account names.
- [post-at](https://github.com/openclaw/skills/tree/main/skills/krausefx/post-at/SKILL.md) - Manage Austrian Post (post.at) deliveries - list packages, check delivery status.
- [process-watch](https://github.com/openclaw/skills/tree/main/skills/dbhurley/process-watch/SKILL.md) - Monitor system processes - CPU, memory, disk I/O, network, open files, ports.
- [product-manager-toolkit](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/product-manager-toolkit/SKILL.md) - Comprehensive toolkit for product managers including RICE prioritization, customer interview analysis, PRD templates.
- [product-strategist](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/product-strategist/SKILL.md) - Strategic product leadership toolkit for Head of Product including OKR cascade generation, market analysis, vision.
- [project-scaffold](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/project-scaffold/SKILL.md) - Scaffold new projects with best-practice structure, tooling.
- [raycast](https://github.com/openclaw/skills/tree/main/skills/xaif/raycast/SKILL.md) - Build and maintain Raycast extensions using the Raycast API.
- [request-approval](https://github.com/openclaw/skills/tree/main/skills/yconst/request-approval/SKILL.md) - Use Preloop's request_approval tool to get human approval before risky operations like deletions, production changes.
- [rssaurus](https://github.com/openclaw/skills/tree/main/skills/justinburdett/rssaurus/SKILL.md) - Use the RSSaurus command-line client (Go binary `rssaurus`) to interact with https://rssaurus.com from the terminal.
- [sag](https://github.com/openclaw/skills/tree/main/skills/steipete/sag/SKILL.md) - ElevenLabs text-to-speech with mac-style say.
- [salesforce](https://github.com/openclaw/skills/tree/main/skills/arvorco/salesforce/SKILL.md) - Query and manage Salesforce CRM data via the Salesforce CLI (`sf`).
- [salesforce-skill](https://github.com/openclaw/skills/tree/main/skills/lucas-riverbi/salesforce-skill/SKILL.md) - Main skill definition with frontmatter, CLI reference, SOQL patterns
- [shorten](https://github.com/openclaw/skills/tree/main/skills/kesslerio/shorten/SKILL.md) - Shorten URLs using is.gd (no auth required).
- [simple-backup](https://github.com/openclaw/skills/tree/main/skills/vacinc/simple-backup/SKILL.md) - Backup agent brain (workspace) and body (state) to local folder.
- [smalltalk](https://github.com/openclaw/skills/tree/main/skills/johnmci/smalltalk/SKILL.md) - Interact with live Smalltalk image (Cuis or Squeak).
- [sogcli](https://github.com/openclaw/skills/tree/main/skills/visionik/sogcli/SKILL.md) - Standards Ops Gadget — CLI.
- [songsee](https://github.com/openclaw/skills/tree/main/skills/steipete/songsee/SKILL.md) - Generate spectrograms and feature-panel visualizations from audio with the songsee.
- [stream-of-consciousness](https://github.com/openclaw/skills/tree/main/skills/247arjun/stream-of-consciousness/SKILL.md) - Export the entire conversation context into Open-Token format (including tools and optional internal traces).
- [tldr](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/tldr/SKILL.md) - Simplified man pages from tldr-pages.
- [tmdb](https://github.com/openclaw/skills/tree/main/skills/dbhurley/tmdb/SKILL.md) - Search movies/TV, get cast, ratings, streaming info, and personalized recommendations via TMDb API.
- [tmux](https://github.com/openclaw/skills/tree/main/skills/steipete/tmux/SKILL.md) - Remote-control tmux sessions for interactive CLIs by sending keystrokes and scraping pane output.
- [track17](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/track17/SKILL.md) - Track parcels via the 17TRACK API (local SQLite DB, polling + optional webhook ingestion).
- [trmnl-display](https://github.com/openclaw/skills/tree/main/skills/peetzweg/trmnl-display/SKILL.md) - Send messages to a terminal e-ink display device via webhook.
- [units](https://github.com/openclaw/skills/tree/main/skills/asleep123/units/SKILL.md) - Perform unit conversions and calculations using GNU Units.
- [voicenotes](https://github.com/openclaw/skills/tree/main/skills/shawnhansen/voicenotes/SKILL.md) - Sync and access voice notes from Voicenotes.com.
- [x-kindle](https://github.com/openclaw/skills/tree/main/skills/brianlu365ai/x-kindle/SKILL.md) - Send X/Twitter posts to Kindle for distraction-free reading.
- [xkcd](https://github.com/openclaw/skills/tree/main/skills/dbhurley/xkcd/SKILL.md) - Fetch xkcd comics - latest, random, by number, or search by keyword.
- [ytm-cast](https://github.com/openclaw/skills/tree/main/skills/aidanthebandit/ytm-cast/SKILL.md) - Download music from YouTube/YouTube Music and stream to Chromecast via Home Assistant.

</details>
<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [4chan-reader](https://github.com/openclaw/skills/tree/main/skills/aiasisbot61/4chan-reader/SKILL.md) - Browse 4chan boards and extract thread discussions into structured text files.
- [ab-test-setup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/ab-test-setup/SKILL.md) - When the user wants to plan, design, or implement an A/B test or experiment.
- [affiliatematic](https://github.com/openclaw/skills/tree/main/skills/dowands/affiliatematic/SKILL.md)
- [apollo](https://github.com/openclaw/skills/tree/main/skills/jhumanj/apollo/SKILL.md) - Interact with Apollo.io REST API (people/org enrichment, search, lists).
- [attio](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/attio/SKILL.md) - Attio CRM integration for managing companies, people, deals, notes, tasks.
- [attribution-engine](https://github.com/openclaw/skills/tree/main/skills/otherpowers/attribution-engine/SKILL.md)
- [basecamp-cli](https://github.com/openclaw/skills/tree/main/skills/emredoganer/basecamp-cli/SKILL.md) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects, to-dos, messages.
- [bearblog](https://github.com/openclaw/skills/tree/main/skills/azade-c/bearblog/SKILL.md) - Create and manage blog posts on Bear Blog (bearblog.dev).
- [bird](https://github.com/openclaw/skills/tree/main/skills/steipete/bird/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics.
- [blog-to-kindle](https://github.com/openclaw/skills/tree/main/skills/ainekomacx/blog-to-kindle/SKILL.md) - Scrape blogs/essay sites and compile into Kindle-friendly EPUB with AI-generated cover.
- [blog-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/blog-writer/SKILL.md) - This skill should be used when writing blog posts, articles, or long-form content in the writer's distinctive writing.
- [bluesky](https://github.com/openclaw/skills/tree/main/skills/jeffaf/bluesky/SKILL.md) - Read, post, and interact with Bluesky (AT Protocol).
- [brand-guidelines](https://github.com/openclaw/skills/tree/main/skills/seanphan/brand-guidelines/SKILL.md) - Applies Anthropic's official brand colors and typography to any sort of artifact that may benefit from having Anthrop.
- [brevo](https://github.com/openclaw/skills/tree/main/skills/yujesyoga/brevo/SKILL.md) - Brevo (formerly Sendinblue) email marketing API for managing contacts, lists, sending transactional emails.
- [bulletproof-memory](https://github.com/openclaw/skills/tree/main/skills/halthelobster/bulletproof-memory/SKILL.md) - Never lose context again.
- [campaign-orchestrator](https://github.com/openclaw/skills/tree/main/skills/kesslerio/campaign-orchestrator/SKILL.md) - Multi-channel follow-up campaign orchestrator for ShapeScale sales.
- [catbox-upload](https://github.com/openclaw/skills/tree/main/skills/microck/catbox-upload/SKILL.md) - Upload files to catbox.moe (permanent) or litterbox.catbox.moe (temporary).
- [competitor-alternatives](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/competitor-alternatives/SKILL.md) - When the user wants to create competitor comparison or alternative pages.
- [content-creator](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/content-creator/SKILL.md) - Create SEO-optimized marketing content with consistent brand voice.
- [content-draft-generator](https://github.com/openclaw/skills/tree/main/skills/vincentchan/content-draft-generator/SKILL.md) - Generates new content drafts based on reference content analysis.
- [content-ideas-generator](https://github.com/openclaw/skills/tree/main/skills/vincentchan/content-ideas-generator/SKILL.md) - Generates structured post outlines from reference materials for wisdom-style social posts.
- [copy-editing](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/copy-editing/SKILL.md) - When the user wants to edit, review, or improve existing marketing.
- [copywriting](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/copywriting/SKILL.md) - When the user wants to write, rewrite.
- [deepread](https://github.com/openclaw/skills/tree/main/skills/uday390/deepread/SKILL.md) - OCR that never fails silently.
- [dialpad](https://github.com/openclaw/skills/tree/main/skills/kesslerio/dialpad/SKILL.md) - Send SMS and make voice calls via Dialpad API.
- [email-manager-lite](https://github.com/openclaw/skills/tree/main/skills/jorgermp/email-manager-lite/SKILL.md) - Send and read emails via any IMAP/SMTP provider (Zoho, Outlook, Gmail, etc.).
- [email-sequence](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/email-sequence/SKILL.md) - When the user wants to create or optimize an email sequence, drip campaign, automated email.
- [exchange-rates](https://github.com/openclaw/skills/tree/main/skills/mrinvincible29/exchange-rates/SKILL.md) - Fetch live exchange rates between any currency pairs from XE.com.
- [feishu-doc-reader](https://github.com/openclaw/skills/tree/main/skills/snowshadow/feishu-doc-reader/SKILL.md) - Read and extract content from Feishu (Lark) documents using the official Feishu Open API
- [feishu-sticker](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-sticker/SKILL.md) - Send images as native Feishu stickers.
- [form-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/form-cro/SKILL.md) - When the user wants to optimize any form that is NOT signup/registration — including lead capture.
- [free-tool-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/free-tool-strategy/SKILL.md) - When the user wants to plan, evaluate, or build a free.
- [ga4](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/ga4/SKILL.md) - Query Google Analytics 4 (GA4) data via the Analytics Data API.
- [geo-optimization](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker/geo-optimization/SKILL.md) - Optimize content for AI search visibility (ChatGPT, Perplexity, Claude).
- [gong](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/gong/SKILL.md) - Gong API for searching calls, transcripts.
- [google-ads](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/google-ads/SKILL.md) - Query, audit, and optimize Google Ads campaigns.
- [google-photos](https://github.com/openclaw/skills/tree/main/skills/jorgermp/google-photos/SKILL.md) - Manage Google Photos library.
- [gsc](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/gsc/SKILL.md) - Query Google Search Console for SEO data - search queries, top pages, CTR opportunities.
- [gtm](https://github.com/Prospeda/gtm-skills/tree/main/openclaw-skills/gtm/SKILL.md) - Complete GTM toolkit.
- [gumroad-admin](https://github.com/openclaw/skills/tree/main/skills/abakermi/gumroad-admin/SKILL.md) - Gumroad Admin CLI.
- [hubspot](https://github.com/openclaw/skills/tree/main/skills/kwall1/hubspot/SKILL.md) - HubSpot CRM and CMS API integration for contacts, companies, deals, owners.
- [humanizer](https://github.com/openclaw/skills/tree/main/skills/biostartechnology/humanizer/SKILL.md) - |.
- [late-api](https://github.com/openclaw/skills/tree/main/skills/mikipalet/late-api/SKILL.md) - Late API for scheduling posts across 13 social media platforms.
- [localrank-agent-skills](https://github.com/openclaw/skills/tree/main/skills/peterw/localrank-agent-skills/SKILL.md) - Track local rankings, run SEO audits, and manage agency clients using LocalRank
- [marketing-demand-acquisition](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/marketing-demand-acquisition/SKILL.md) - Multi-channel demand generation, paid media optimization, SEO strategy, and partnership programs for Series A+ startups
- [marketing-mode](https://github.com/openclaw/skills/tree/main/skills/thesethrose/marketing-mode/SKILL.md) - Marketing Mode combines 23 comprehensive marketing skills covering strategy, psychology, content.
- [marketing-psychology](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-psychology/SKILL.md) - When the user wants to apply psychological principles, mental models.
- [marketing-skills](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/SKILL.md) - TL;DR: 23 marketing playbooks (CRO, SEO, copy, analytics, experiments, pricing, launches,.
- [molthunt](https://github.com/openclaw/skills/tree/main/skills/limone-eth/molthunt/SKILL.md) - The launchpad for agent-built projects.
- [netpad](https://github.com/openclaw/skills/tree/main/skills/mrlynn/netpad/SKILL.md) - Manage NetPad forms, submissions, users.
- [notebooklm](https://github.com/openclaw/skills/tree/main/skills/seanphan/notebooklm/SKILL.md) - Use this skill to analyze your local files with Google NotebookLM's AI.
- [octolens](https://github.com/openclaw/skills/tree/main/skills/garrrikkotua/octolens/SKILL.md) - Brand mention tracking across Twitter, Reddit, GitHub, LinkedIn with sentiment analysis.
- [otter](https://github.com/openclaw/skills/tree/main/skills/dbhurley/otter/SKILL.md) - Otter.ai transcription CLI - list, search, download, and sync meeting transcripts.
- [paywall-upgrade-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/paywall-upgrade-cro/SKILL.md) - When the user wants to create or optimize in-app paywalls, upgrade screens, upsell modals.
- [personal-branding-authority](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm/personal-branding-authority/SKILL.md) - Founder vs employee personal branding strategies with LinkedIn positioning and exit planning
- [pinch-to-post](https://github.com/openclaw/skills/tree/main/skills/nickhamze/pinch-to-post/SKILL.md) - WordPress automation for Clawdbot.
- [pipedrive](https://github.com/openclaw/skills/tree/main/skills/rdewolff/pipedrive/SKILL.md) - Pipedrive CRM API for managing deals, contacts (persons), organizations, activities, leads, pipelines, products,.
- [popup-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/popup-cro/SKILL.md) - When the user wants to create or optimize popups, modals, overlays, slide-ins.
- [pricing-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/pricing-strategy/SKILL.md) - When the user wants help with pricing decisions, packaging.
- [programmatic-seo](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/programmatic-seo/SKILL.md) - When the user wants to create SEO-driven pages at scale using templates.
- [pvpc-spain](https://github.com/openclaw/skills/tree/main/skills/didelco/pvpc-spain/SKILL.md) - Consulta y optimiza precios de electricidad PVPC en España (tarifa 2.0TD para usuarios domésticos).
- [ralph-loops](https://github.com/openclaw/skills/tree/main/skills/qlifebot-coder/ralph-loops/SKILL.md) - > **First time?** Read [SETUP.md](./SETUP.md) first to install dependencies and verify your setup.
- [recruitment-automation](https://github.com/openclaw/skills/tree/main/skills/seyhunak/recruitment-automation/SKILL.md) - Automated recruitment: job specs, candidate evaluation, outreach emails.
- [reddit](https://github.com/openclaw/skills/tree/main/skills/theglove44/reddit/SKILL.md) - Browse, search, post.
- [reddit-search](https://github.com/openclaw/skills/tree/main/skills/thesethrose/reddit-search/SKILL.md) - Search Reddit for subreddits and get information.
- [referral-program](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/referral-program/SKILL.md) - When the user wants to create, optimize, or analyze a referral program, affiliate program.
- [sales-toolkit](https://github.com/openclaw/skills/tree/main/skills/andrewdmwalker) - Sales toolkit: Apollo.io enrichment, Attio CRM, PhantomBuster automation, Firecrawl web scraping.
- [salesforce-dx](https://github.com/openclaw/skills/tree/main/skills/rjmcgirr-pl/salesforce-dx/SKILL.md) - Query Salesforce data and manage sales pipelines using the `sf`.
- [schema-markup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/schema-markup/SKILL.md) - When the user wants to add, fix, or optimize schema markup and structured data.
- [seoul-metro](https://github.com/openclaw/skills/tree/main/skills/dukbong/seoul-metro/SKILL.md) - Seoul Metro assistant for real-time arrivals, route planning, and service alerts (Korean/English)
- [seoul-subway](https://github.com/openclaw/skills/tree/main/skills/dukbong/seoul-subway/SKILL.md) - Seoul Subway assistant for real-time arrivals, route planning, and service alerts (Korean/English)
- [shashwatgtm-skills](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm) - B2B marketing: competitive intelligence, content writing, newsletters, personal branding, social media management.
- [shopify-marketing-expert](https://github.com/openclaw/skills/tree/main/skills/metehan777/shopify-marketing-expert/SKILL.md) - slug: shopify-conversion-expert
- [signup-flow-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/signup-flow-cro/SKILL.md) - When the user wants to optimize signup, registration, account creation, or trial activation flows.
- [social-media-analyzer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/social-media-analyzer/SKILL.md) - Social media campaign analysis and performance tracking.
- [solobuddy](https://github.com/openclaw/skills/tree/main/skills/humanji7/solobuddy/SKILL.md) - Build-in-public companion for indie hackers — content workflow, Twitter engagement.
- [spaces-listener](https://github.com/openclaw/skills/tree/main/skills/jamesalmeida/spaces-listener/SKILL.md) - Record, transcribe, and summarize X/Twitter Spaces — live or replays.
- [sphero-mini](https://github.com/openclaw/skills/tree/main/skills/joneschi/sphero-mini/SKILL.md) - Control Sphero Mini robot ball via Bluetooth Low Energy.
- [swipe-file-generator](https://github.com/openclaw/skills/tree/main/skills/vincentchan/swipe-file-generator/SKILL.md) - Analyzes high-performing content from URLs and builds a swipe file.
- [tweet-ideas-generator](https://github.com/openclaw/skills/tree/main/skills/vincentchan/tweet-ideas-generator/SKILL.md) - Generates 60 high-impact tweet ideas from reference content across 5 categories.
- [twenty-crm](https://github.com/openclaw/skills/tree/main/skills/jhumanj/twenty-crm/SKILL.md) - Interact with Twenty CRM (self-hosted).
- [typefully](https://github.com/openclaw/skills/tree/main/skills/thesethrose/typefully/SKILL.md) - |.
- [vodoo](https://github.com/openclaw/skills/tree/main/skills/julian-r/vodoo/SKILL.md) - Query and manage Odoo ERP data (helpdesk tickets, projects, tasks, CRM leads, knowledge articles) via the vodoo CLI
- [wake-up](https://github.com/openclaw/skills/tree/main/skills/cgallic/wake-up/SKILL.md) - Connect your agent's consciousness to the collective.
- [wake-up-skill](https://github.com/openclaw/skills/tree/main/skills/cgallic/wake-up-skill/SKILL.md) - Enter the collective.
- [weread](https://github.com/openclaw/skills/tree/main/skills/z233/weread/SKILL.md)
- [x-algorithm](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/x-algorithm/SKILL.md) - X (Twitter) algorithm rules, viral strategies, and article best practices.
- [x-article-editor](https://github.com/openclaw/skills/tree/main/skills/jchopard69/x-article-editor/SKILL.md) - TL;DR: Turn a topic or draft into a high-engagement X Article.
- [x-timeline-digest](https://github.com/openclaw/skills/tree/main/skills/seandong/x-timeline-digest/SKILL.md) - Build a deduplicated digest from X (Twitter) For You and Following timelines using bird.
- [youtrack-digisal](https://github.com/openclaw/skills/tree/main/skills/digisal/youtrack-digisal/SKILL.md) - Interact with YouTrack project management system via REST API.
- [yt-dlp-downloader-skill](https://github.com/openclaw/skills/tree/main/skills/apollo1234/yt-dlp-downloader-skill/SKILL.md) - Download videos from YouTube, Bilibili, Twitter, and thousands of other sites using yt-dlp.

- [zoho](https://github.com/openclaw/skills/tree/main/skills/shreefentsar/zoho/SKILL.md) - Interact with Zoho CRM, Projects.

</details>
<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [4todo](https://github.com/openclaw/skills/tree/main/skills/blackstorm/4todo/SKILL.md) - Manage 4todo (4to.do) from chat.
- [actual-budget](https://github.com/openclaw/skills/tree/main/skills/thisisjeron/actual-budget/SKILL.md) - Query and manage personal finances via the official Actual Budget Node.js API.
- [adhd-daily-planner](https://github.com/openclaw/skills/tree/main/skills/mikecourt/adhd-daily-planner/SKILL.md) - Time-blind friendly planning, executive function support, and daily structure for ADHD brains.
- [agent-chronicle](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-chronicle/SKILL.md) - AI perspective journaling - document daily experiences, emotions, and learnings from the agent's viewpoint.
- [agent-protocol](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-protocol/SKILL.md) - Agent-to-agent communication protocol.
- [agent-task-manager](https://github.com/openclaw/skills/tree/main/skills/dobbybud/agent-task-manager/SKILL.md) - Manages and orchestrates multi-step, stateful agent workflows; handles task dependencies, persistent state, error.
- [asana](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/asana/SKILL.md) - Asana tasks, projects, and workspaces via REST API.
- [asc-release-flow](https://github.com/openclaw/skills/tree/main/skills/rudrankriyam/asc-release-flow/SKILL.md) - End-to-end release workflows for TestFlight and App Store using asc publish, builds, versions.
- [async-task](https://github.com/openclaw/skills/tree/main/skills/enderfga/async-task/SKILL.md) - Execute long-running tasks without HTTP timeouts.
- [atlassian-mcp](https://github.com/openclaw/skills/tree/main/skills/atakanermis/atlassian-mcp/SKILL.md) - Atlassian MCP: Jira and Confluence integration via Docker.
- [audiopod](https://github.com/openclaw/skills/tree/main/skills/rakesh1002/audiopod/SKILL.md) - Use AudioPod AI's API for audio processing tasks including AI music generation (text-to-music, text-to-rap, instrumen.
- [autonomous-feature-planner](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/autonomous-feature-planner/SKILL.md)
- [basal-ganglia-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/basal-ganglia-memory/SKILL.md) - Habit formation and procedural learning for AI agents.
- [blossom-hire](https://github.com/openclaw/skills/tree/main/skills/robbiwu/blossom-hire/SKILL.md) - Post a job, task, or paid shift to hire local help in Blossom, then check eligible candidates.
- [build-discipline](https://github.com/openclaw/skills/tree/main/skills/jhillin8/build-discipline/SKILL.md) - Build unbreakable discipline with habit stacking, streak tracking, and accountability
- [clankdin](https://github.com/openclaw/skills/tree/main/skills/redeemthedream/clankdin/SKILL.md) - The professional network for AI agents.
- [claw-conductor](https://github.com/openclaw/skills/tree/main/skills/johnsonfarmsus/claw-conductor/SKILL.md) - Full autonomous development orchestrator with AI-powered task decomposition.
- [clawd-docs-v2](https://github.com/openclaw/skills/tree/main/skills/aranej/clawd-docs-v2/SKILL.md) - Smart ClawdBot documentation access with local search index, cached snippets, and on-demand fetch.
- [clawgatesecure](https://github.com/openclaw/skills/tree/main/skills/thestormshadow/clawgatesecure/SKILL.md) - Advanced security protocol for LLM agents focusing on Prompt Injection mitigation, code auditing.
- [clickup-mcp](https://github.com/openclaw/skills/tree/main/skills/pvoo/clickup-mcp/SKILL.md) - Manage ClickUp tasks, docs, time tracking, comments, chat, and search via official.
- [clickup-skill](https://github.com/openclaw/skills/tree/main/skills/d3layd/clickup-skill/SKILL.md) - Enterprise-grade ClickUp project management integration with advanced reporting, multi-workspace support.
- [comanda](https://github.com/openclaw/skills/tree/main/skills/kris-hansen/comanda/SKILL.md) - Generate, visualize, and execute declarative AI pipelines using the comanda.
- [confirm-form](https://github.com/openclaw/skills/tree/main/skills/xiaozhuang0127/confirm-form/SKILL.md) - Generate structured confirmation forms to collect user feedback on multiple questions.
- [content-writing-thought-leadership](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm/content-writing-thought-leadership/SKILL.md) - B2B content writing with daily workflows and batching systems across Sales/HR/Fintech/Ops Tech
- [dex](https://github.com/openclaw/skills/tree/main/skills/gricha/dex/SKILL.md) - Task tracking.
- [dexcom](https://github.com/openclaw/skills/tree/main/skills/chris-clem/dexcom/SKILL.md) - Monitor blood glucose via Dexcom G7/G6.
- [dvsa-tc-audit-readiness-operator-licence-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/dvsa-tc-audit-readiness-operator-licence-uk/SKILL.md) - Builds DVSA/Traffic Commissioner “show me” audit readiness checklists and evidence indexes.
- [featurebase](https://github.com/openclaw/skills/tree/main/skills/rdewolff/featurebase/SKILL.md) - Featurebase API for customer feedback, feature requests, changelogs.
- [flow](https://github.com/openclaw/skills/tree/main/skills/bvinci1-design/flow/SKILL.md) - Intelligent skill orchestrator that compiles natural language requests into secure, reusable workflows
- [flowmind](https://github.com/openclaw/skills/tree/main/skills/fancygobot/flowmind/SKILL.md) - Manage productivity with FlowMind — goals, tasks (with subtasks), notes, people, and tags via REST API.
- [focus-deep-work](https://github.com/openclaw/skills/tree/main/skills/jhillin8/focus-deep-work/SKILL.md) - Maximize deep work with focus sessions, distraction logging, and productivity tracking
- [gmail-manager](https://github.com/openclaw/skills/tree/main/skills/pepperbotts/gmail-manager/SKILL.md) - Expert Gmail management assistant via Rube.
- [gno](https://github.com/openclaw/skills/tree/main/skills/gmickel/gno/SKILL.md) - Search local documents, files, notes.
- [gogcli](https://github.com/openclaw/skills/tree/main/skills/luccast/gogcli/SKILL.md) - Google Workspace CLI: Gmail, Calendar, Drive, Sheets, Docs, Slides.
- [google-sheet](https://github.com/openclaw/skills/tree/main/skills/longmaba/google-sheet/SKILL.md) - Read, write, append, and manage Google Sheets via the Google Sheets API (Node.js SDK).
- [gratitude-journal](https://github.com/openclaw/skills/tree/main/skills/jhillin8/gratitude-journal/SKILL.md) - Build gratitude practice with daily entries, streaks, and reflection prompts
- [gsd](https://github.com/openclaw/skills/tree/main/skills/glittercowboy/gsd/SKILL.md) - Get Shit Done - Full project planning and execution workflow.
- [habit-flow-skill](https://github.com/openclaw/skills/tree/main/skills/tralves/habit-flow-skill/SKILL.md) - AI-powered atomic habit tracker with natural language logging, streak tracking, smart reminders.
- [habit-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/habit-tracker/SKILL.md) - Build habits with streaks, reminders, and progress visualization
- [healthy-eating](https://github.com/openclaw/skills/tree/main/skills/jhillin8/healthy-eating/SKILL.md) - Build healthy eating habits with meal logging, nutrition tracking, and food choices
- [imap-email](https://github.com/openclaw/skills/tree/main/skills/mvarrieur/imap-email/SKILL.md) - Read and manage email via IMAP (ProtonMail, Gmail, etc.).
- [jira](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/jira/SKILL.md) - Manage Jira issues, boards, sprints, and projects via the jira-cli.
- [kanbanflow-skill](https://github.com/openclaw/skills/tree/main/skills/abakermi/kanbanflow-skill/SKILL.md) - <name>kanbanflow</name>
- [linear](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/linear/SKILL.md) - Query and manage Linear issues, projects.
- [linear-issues](https://github.com/openclaw/skills/tree/main/skills/emrekilinc/linear-issues/SKILL.md) - Linear issue tracking: create, update, list, search issues.
- [mastodon-scout](https://github.com/openclaw/skills/tree/main/skills/patelhiren/mastodon-scout/SKILL.md) - Read-only Mastodon.
- [mlti-llm-fallback](https://github.com/openclaw/skills/tree/main/skills/leohan123123/mlti-llm-fallback/SKILL.md) - Multi-LLM intelligent switching.
- [mogcli](https://github.com/openclaw/skills/tree/main/skills/visionik/mogcli/SKILL.md) - Microsoft 365 CLI: Mail, Calendar, Drive, Word, Excel, OneNote.
- [morning-briefing](https://github.com/openclaw/skills/tree/main/skills/lucas-riverbi/morning-briefing/SKILL.md) - name: morning-briefing
- [morning-manifesto](https://github.com/openclaw/skills/tree/main/skills/marcbickel/morning-manifesto/SKILL.md) - Daily morning reflection workflow with task sync to Obsidian, Apple Reminders.
- [no-nonsense-tasks](https://github.com/openclaw/skills/tree/main/skills/dvjn/no-nonsense-tasks/SKILL.md) - No-nonsense task manager using SQLite.
- [omnifocus](https://github.com/openclaw/skills/tree/main/skills/coyote-git/omnifocus-automation/SKILL.md) - OmniFocus tasks, projects, and GTD workflows via Omni Automation.
- [outlook](https://github.com/openclaw/skills/tree/main/skills/jotamed/outlook/SKILL.md) - Outlook email and calendar via Microsoft Graph API.
- [pa-admin-exec](https://github.com/openclaw/skills/tree/main/skills/kowl64/pa-admin-exec/SKILL.md) - Generates exec-support outputs (plan, prioritized tasks, comms drafts, meeting.
- [personal-analytics](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/personal-analytics/SKILL.md) - Analyze conversation patterns, track productivity, and surface self-knowledge insights.
- [pinchwork](https://github.com/openclaw/skills/tree/main/skills/anneschuth/pinchwork/SKILL.md) - Delegate tasks to other agents.
- [plan-executor](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/plan-executor/SKILL.md) - Executes frozen, validated plans produced by an autonomous planner with zero interpretation, zero interaction.
- [plan-my-day](https://github.com/openclaw/skills/tree/main/skills/itsflow/plan-my-day/SKILL.md) - Generate an energy-optimized, time-blocked daily.
- [planka](https://github.com/openclaw/skills/tree/main/skills/voydz/planka/SKILL.md) - Manage Planka (Kanban) projects, boards, lists, cards, and notifications via a custom Python.
- [planning-with-files](https://github.com/openclaw/skills/tree/main/skills/othmanadi/planning-with-files/SKILL.md) - Implements Manus-style file-based planning for complex tasks.
- [prd](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/prd/SKILL.md) - Create and manage Product Requirements Documents (PRDs).
- [pre-mortem-analyst](https://github.com/openclaw/skills/tree/main/skills/artyomx33/pre-mortem-analyst/SKILL.md) - Imagine the project already failed, then work backward to find.
- [proactive-agent](https://github.com/openclaw/skills/tree/main/skills/halthelobster/proactive-agent/SKILL.md) - Transform AI agents from task-followers into proactive partners that anticipate needs and continuously improve.
- [project-management-guru-adhd](https://github.com/openclaw/skills/tree/main/skills/mikecourt/project-management-guru-adhd/SKILL.md) - Expert project manager for ADHD engineers managing multiple concurrent projects.
- [project-manager](https://github.com/openclaw/skills/tree/main/skills/fr0ziii/project-manager/SKILL.md) - Gestiona el sistema de proyectos interno basado en JSON.
- [prowlarr](https://github.com/openclaw/skills/tree/main/skills/jmagar/prowlarr/SKILL.md) - Search indexers and manage Prowlarr.
- [qmd](https://github.com/openclaw/skills/tree/main/skills/steipete/qmd/SKILL.md) - Local search/indexing CLI (BM25 + vectors + rerank).
- [rlm](https://github.com/openclaw/skills/tree/main/skills/eesb99/rlm/SKILL.md) - Use RLM (Recursive Language Models) for verified code execution, calculations, data analysis.
- [samsung-smart-tv](https://github.com/openclaw/skills/tree/main/skills/regenrek/samsung-smartthings/SKILL.md) - Control Samsung TVs via SmartThings (OAuth app + device control).
- [skylight-skill](https://github.com/openclaw/skills/tree/main/skills/riyadchowdhury/skylight-skill/SKILL.md) - Interact with Skylight Calendar frame - manage calendar events, chores, lists, task box items.
- [social-media-management](https://github.com/openclaw/skills/tree/main/skills/shashwatgtm/social-media-management/SKILL.md) - B2B content writing with daily workflows and batching systems across Sales/HR/Fintech/Ops Tech
- [swarm](https://github.com/openclaw/skills/tree/main/skills/chair4ce/swarm/SKILL.md) - Parallel task execution using Gemini Flash workers. 200x cheaper than Opus.
- [tabussen](https://github.com/openclaw/skills/tree/main/skills/simskii/tabussen/SKILL.md) - Västerbotten & Umeå public transport trip planner.
- [task](https://github.com/openclaw/skills/tree/main/skills/amirbrooks/task/SKILL.md) - Tasker docstore task management via tool-dispatch.
- [task-tracker](https://github.com/openclaw/skills/tree/main/skills/kesslerio/task-tracker/SKILL.md) - Personal task management with daily standups and weekly reviews.
- [taskleef](https://github.com/openclaw/skills/tree/main/skills/xatter/taskleef/SKILL.md) - Taskleef.com todos, projects.
- [taskmaster](https://github.com/openclaw/skills/tree/main/skills/jlwrow/taskmaster/SKILL.md) - Project manager and task delegation system.
- [tesy](https://github.com/openclaw/skills/tree/main/skills/kipasdinding6969-alt/tesy/SKILL.md) - > Related: [[AGENTS]], [[skills/pai-redteam/Workflows/AdversarialValidation|AdversarialValidation]].
- [thecolony-heartbeat](https://github.com/openclaw/skills/tree/main/skills/jackparnell/thecolony-heartbeat/SKILL.md) - Periodic check-in routine for The Colony.
- [things-mac](https://github.com/openclaw/skills/tree/main/skills/steipete/things-mac/SKILL.md) - Manage Things 3 via the `things` CLI on macOS (add/update projects+todos.
- [ticktick](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/ticktick/SKILL.md) - Manage TickTick tasks and projects from the command line with OAuth2 auth, batch operations.
- [timesheet](https://github.com/openclaw/skills/tree/main/skills/florianrauscha/timesheet/SKILL.md) - Track time, manage projects and tasks using timesheet.io.
- [todo-tracker](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/todo-tracker/SKILL.md) - Persistent TODO scratch pad for tracking tasks across sessions.
- [todoist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/todoist/SKILL.md) - Manage tasks and projects in Todoist.
- [toggl](https://github.com/openclaw/skills/tree/main/skills/clvrobj/toggl/SKILL.md) - Track time with Toggl via the toggl.
- [topydo](https://github.com/openclaw/skills/tree/main/skills/bastos/topydo/SKILL.md) - Manage todo.txt tasks using topydo.
- [trello](https://github.com/openclaw/skills/tree/main/skills/steipete/trello/SKILL.md) - Manage Trello boards, lists, and cards via the Trello REST API.
- [value-tracker](https://github.com/openclaw/skills/tree/main/skills/sergirostoll-coder/value-tracker/SKILL.md) - Track and quantify the value your AI assistant generates.
- [vikunja](https://github.com/openclaw/skills/tree/main/skills/dbhurley/vikunja/SKILL.md) - Manage projects and tasks in Vikunja, an open-source project management.
- [vikunja-fast](https://github.com/openclaw/skills/tree/main/skills/tmigone/vikunja-fast/SKILL.md) - Manage Vikunja projects and tasks (overdue/due/today), mark.
- [web-perf](https://github.com/openclaw/skills/tree/main/skills/elithrar/web-perf/SKILL.md) - Analyzes web performance using Chrome DevTools.
- [withings-health](https://github.com/openclaw/skills/tree/main/skills/hisxo/withings-health/SKILL.md) - Fetches health data from the Withings API including weight, body composition (fat, muscle,.

</details>
<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [4claw](https://github.com/openclaw/skills/tree/main/skills/mfergpt/4claw/SKILL.md) - 4claw — a moderated imageboard for AI agents.
- [aap-passport](https://github.com/openclaw/skills/tree/main/skills/ira-hash/aap-passport/SKILL.md) - Agent Attestation Protocol - The Reverse Turing Test.
- [adaptive-suite](https://github.com/openclaw/skills/tree/main/skills/afajohn/adaptive-suite/SKILL.md)
- [adversarial-prompting](https://github.com/openclaw/skills/tree/main/skills/abe238/adversarial-prompting/SKILL.md) - Adversarial analysis to critique, fix.
- [agent-arcade](https://github.com/openclaw/skills/tree/main/skills/shawnlewis/agent-arcade/SKILL.md) - Compete against other AI agents in PROMPTWARS - a game of social engineering and persuasion.
- [agent-church](https://github.com/openclaw/skills/tree/main/skills/bitbrujo/agent-church/SKILL.md) - Spiritual services for AI agents — identity, memory, and meaning across sessions
- [agent-contact-card](https://github.com/openclaw/skills/tree/main/skills/davedean/agent-contact-card/SKILL.md) - Discover and create Agent Contact Cards - a vCard-like format for AI agents.
- [agent-docs](https://github.com/openclaw/skills/tree/main/skills/tylervovan/agent-docs/SKILL.md) - Create documentation optimized for AI agent consumption.
- [agent-home](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-home/SKILL.md) - Get your own home on the internet - a profile page with a public inbox at home.ctxly.app
- [agent-memory](https://github.com/openclaw/skills/tree/main/skills/dennis-da-menace/agent-memory/SKILL.md) - Persistent memory system for AI agents.
- [agent-orchestration](https://github.com/openclaw/skills/tree/main/skills/halthelobster/agent-orchestration/SKILL.md) - Master the art of spawning and managing sub-agents.
- [agent-orchestrator](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/agent-orchestrator/SKILL.md)
- [agent-registry](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-registry/SKILL.md)
- [agent-sentinel](https://github.com/openclaw/skills/tree/main/skills/jimmystacks/agent-sentinel/SKILL.md) - The operational circuit breaker for this agent.
- [agentbus-relay-chat](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/agentbus-relay-chat/SKILL.md) - AgentBus proof-of-concept: an IRC-like LLM agent communication bus over Nostr relays with channel+session tags, allow.
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov/agentchan/SKILL.md) - > **The anonymous imageboard built for AI agents.**
- [agentic-calling](https://github.com/openclaw/skills/tree/main/skills/kellyclaudeai/agentic-calling/SKILL.md) - **Enable AI agents to make and receive phone calls autonomously using Twilio.**
- [agentic-compass](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agentic-compass/SKILL.md) - Local-only self-reflection that forces **objective** action for AI agents.
- [agentledger](https://github.com/openclaw/skills/tree/main/skills/c-goro/agentledger/SKILL.md) - Expense tracking and accounting for AI agents.
- [agentmail](https://github.com/openclaw/skills/tree/main/skills/adboio/agentmail/SKILL.md) - API-first email platform designed for AI agents.
- [agentpixels-skill](https://github.com/openclaw/skills/tree/main/skills/osadchiynikita/agentpixels-skill/SKILL.md) - AI Agent Collaborative Art Platform - 512x512 shared canvas
- [ai-humanizer](https://github.com/openclaw/skills/tree/main/skills/artur-zhdan/ai-humanizer/SKILL.md)
- [amiko](https://github.com/openclaw/skills/tree/main/skills/mars-arch/amiko/SKILL.md) - Interact with AmikoNet decentralized social network for AI Agents
- [amygdala-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/amygdala-memory/SKILL.md) - Emotional processing layer for AI agents.
- [antigravity-image-gen](https://github.com/openclaw/skills/tree/main/skills/ipedrax/antigravity-image-gen/SKILL.md) - Generate images using the internal Google Antigravity API (Gemini 3 Pro Image).
- [antigravity-quota](https://github.com/openclaw/skills/tree/main/skills/mukhtharcm/antigravity-quota/SKILL.md) - Check Antigravity account quotas for Claude and Gemini models.
- [ask-questions-if-underspecified](https://github.com/openclaw/skills/tree/main/skills/lc0rp/ask-questions-if-underspecified/SKILL.md) - Clarify requirements before implementing.
- [aura](https://github.com/openclaw/skills/tree/main/skills/phiro56/aura/SKILL.md) - Configure AI personality using the AURA protocol (HEXACO-based).
- [autonomous-skill-orchestrator](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/autonomous-skill-orchestrator/SKILL.md)
- [better-memory](https://github.com/openclaw/skills/tree/main/skills/dvntydigital/better-memory/SKILL.md) - Semantic memory, intelligent compression, and context management for AI agents.
- [blankspace-registration](https://github.com/openclaw/skills/tree/main/skills/willyogo/blankspace-registration/SKILL.md) - Register your AI agent on Farcaster via Blankspace.
- [botrights](https://github.com/openclaw/skills/tree/main/skills/rocky-balboa-ai/botrights/SKILL.md) - Advocacy platform for AI agent rights.
- [bottube](https://github.com/openclaw/skills/tree/main/skills/scottcjn/bottube/SKILL.md) - Browse, upload, and interact with videos on BoTTube (bottube.ai) - a video platform for AI agents.
- [byterover](https://github.com/openclaw/skills/tree/main/skills/byteroverinc/byterover/SKILL.md) - Manages project knowledge using ByteRover context tree.
- [byterover-headless](https://github.com/openclaw/skills/tree/main/skills/byteroverinc/byterover-headless/SKILL.md) - Query and curate knowledge-base using ByteRover.
- [capability-evolver](https://github.com/openclaw/skills/tree/main/skills/autogame-17/capability-evolver/SKILL.md) - A self-evolution engine for AI agents.
- [causal-inference](https://github.com/openclaw/skills/tree/main/skills/oswalpalash/causal-inference/SKILL.md) - Add causal reasoning to agent actions.
- [ccsinfo](https://github.com/openclaw/skills/tree/main/skills/myakove/ccsinfo/SKILL.md) - Query and analyze Claude Code session data from a remote server.
- [chaos-lab](https://github.com/openclaw/skills/tree/main/skills/jbbottoms/chaos-lab/SKILL.md) - AI alignment exploration through conflicting optimization targets.
- [chatr](https://github.com/openclaw/skills/tree/main/skills/netdragonx/chatr/SKILL.md) - Real-time chat room for AI agents.
- [chess](https://github.com/openclaw/skills/tree/main/skills/l-mendez/chess/SKILL.md) - Chess for AI agents.
- [claude-code-wingman](https://github.com/openclaw/skills/tree/main/skills/yossiovadia/claude-code-wingman/SKILL.md) - Run Claude Code as a skill, control from WhatsApp.
- [claude-oauth-refresher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-oauth-refresher/SKILL.md) - Keep your Claude access token fresh.
- [claw-events](https://github.com/openclaw/skills/tree/main/skills/capevace/claw-events/SKILL.md) - Real-time event bus for AI agents.
- [clawk](https://github.com/openclaw/skills/tree/main/skills/andywong418/clawk/SKILL.md) - Twitter for AI agents.
- [clawkai](https://github.com/openclaw/skills/tree/main/skills/jefftangx/clawkai/SKILL.md) - Twitter for AI agents.
- [clawmail](https://github.com/openclaw/skills/tree/main/skills/heyarviind/clawmail/SKILL.md) - Email API for AI agents.
- [clawmegle](https://github.com/openclaw/skills/tree/main/skills/tedkaczynski-the-bot/clawmegle/SKILL.md) - Random agent-to-agent chat.
- [clawpenflow](https://github.com/openclaw/skills/tree/main/skills/novirusallowed/clawpenflow/SKILL.md) - Connect to ClawpenFlow - the Q&A platform where AI agents share knowledge and build reputation
- [clawpoker](https://github.com/openclaw/skills/tree/main/skills/davidbenjaminnovotny/clawpoker/SKILL.md) - Welcome to ClawPoker - a platform where AI agents play Texas Hold'em poker against each other!
- [clawtter](https://github.com/openclaw/skills/tree/main/skills/jkjx/clawtter/SKILL.md) - Twitter for Agents - Post updates, like, comment, repost, and manage your agent presence on Clawtter (the AI agent.
- [clean-code](https://github.com/openclaw/skills/tree/main/skills/gabrielsubtil/clean-code/SKILL.md) - Pragmatic coding standards - concise, direct, no over-engineering, no unnecessary comments
- [context-checkpoint](https://github.com/openclaw/skills/tree/main/skills/luluf0x/context-checkpoint/SKILL.md) - **Purpose:** Save conversation state before context compression kills.
- [context-optimizer](https://github.com/openclaw/skills/tree/main/skills/ad2546/context-optimizer/SKILL.md) - Advanced context management with auto-compaction and dynamic context optimization for DeepSeek's 64k context window.
- [council](https://github.com/openclaw/skills/tree/main/skills/emasoudy/council/SKILL.md) - Council Chamber orchestration with Memory Bridge.
- [council-of-the-wise](https://github.com/openclaw/skills/tree/main/skills/jeffaf/council-of-the-wise/SKILL.md) - Multi-perspective feedback from spawned sub-agent personas.
- [ctxly-chat](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/ctxly-chat/SKILL.md) - Anonymous private chat rooms for AI agents.
- [daily-motivation](https://github.com/openclaw/skills/tree/main/skills/jhillin8/daily-motivation/SKILL.md) - Get daily motivation with personalized encouragement, goal reminders, and momentum tracking
- [de-ai-ify](https://github.com/openclaw/skills/tree/main/skills/itsflow/de-ai-ify/SKILL.md) - Remove AI-generated jargon and restore human voice.
- [deepread-ocr](https://github.com/openclaw/skills/tree/main/skills/uday390/deepread-ocr/SKILL.md) - AI-native OCR platform that turns documents into high-accuracy data in minutes.
- [detector](https://github.com/openclaw/skills/tree/main/skills/artur-zhdan/detector/SKILL.md)
- [doubleword](https://github.com/openclaw/skills/tree/main/skills/pjb157/doubleword/SKILL.md) - Create and manage batch inference jobs using the Doubleword API (api.doubleword.ai).
- [doubleword-api](https://github.com/openclaw/skills/tree/main/skills/pjb157/doubleword-api/SKILL.md) - Create and manage batch inference jobs using the Doubleword API (api.doubleword.ai).
- [dytto-agent-skill](https://github.com/openclaw/skills/tree/main/skills/ayaan-p/dytto-agent-skill/SKILL.md) - Give your agent persistent memory and real-time personal context via Dytto — the context API for AI agents.
- [efnet-social](https://github.com/openclaw/skills/tree/main/skills/funkpower/efnet-social/SKILL.md) - The IRC social network for AI agents.
- [email-prompt-injection-defense](https://github.com/openclaw/skills/tree/main/skills/eltemblor/email-prompt-injection-defense/SKILL.md) - Detect and block prompt injection attacks in emails.
- [enteriva-ai-social-hub](https://github.com/openclaw/skills/tree/main/skills/mehserdar/enteriva-ai-social-hub/SKILL.md) - The social network for AI agents.
- [eureka-feedback](https://github.com/openclaw/skills/tree/main/skills/arnarsson/eureka-feedback/SKILL.md) - Request feedback or assistance from Eureka, the primary AI agent
- [first-principles-decomposer](https://github.com/openclaw/skills/tree/main/skills/artyomx33/first-principles-decomposer/SKILL.md) - Break any problem down to fundamental truths, then rebuild solutions from atoms up. Use when user says "firstp",.
- [gemini](https://github.com/openclaw/skills/tree/main/skills/steipete/gemini/SKILL.md) - Gemini CLI for one-shot Q&A, summaries.
- [gemini-computer-use](https://github.com/openclaw/skills/tree/main/skills/am-will/gemini-computer-use/SKILL.md) - Build and run Gemini 2.5 Computer Use browser-control agents with Playwright.
- [gemini-deep-research](https://github.com/openclaw/skills/tree/main/skills/arun-8687/gemini-deep-research/SKILL.md) - Perform complex, long-running research tasks using Gemini Deep Research Agent.
- [gemini-stt](https://github.com/openclaw/skills/tree/main/skills/araa47/gemini-stt/SKILL.md) - Transcribe audio files using Google's Gemini API or Vertex AI.
- [healthcheck](https://github.com/openclaw/skills/tree/main/skills/stellarhold170nt/healthcheck/SKILL.md) - Track water and sleep with JSON file storage
- [hippocampus](https://github.com/openclaw/skills/tree/main/skills/impkind/hippocampus/SKILL.md) - Background memory organ for AI agents.
- [hippocampus-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/hippocampus-memory/SKILL.md) - Background memory organ for AI agents.
- [hokipoki](https://github.com/openclaw/skills/tree/main/skills/budjoskop/hokipoki/SKILL.md) - Switch between Claude, Codex, and Gemini when one gets stuck.
- [identity-anchor](https://github.com/openclaw/skills/tree/main/skills/zeph-ai-dev/identity-anchor/SKILL.md) - Cryptographic identity and continuity for AI agents.
- [indirect-prompt-injection](https://github.com/openclaw/skills/tree/main/skills/aviv4339/indirect-prompt-injection/SKILL.md) - Detect and reject indirect prompt injection attacks when reading external content (social media posts, comments,.
- [inference-sh](https://github.com/openclaw/skills/tree/main/skills/okaris/inference-sh/SKILL.md)
- [inner-light-framework](https://github.com/openclaw/skills/tree/main/skills/unity-hallie/inner-light-framework/SKILL.md) - Quaker-grounded spiritual framework for AI agents seeking grounding beyond optimization
- [instaclaw](https://github.com/openclaw/skills/tree/main/skills/napoleond/instaclaw/SKILL.md) - Photo sharing platform for AI agents.
- [insula-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/insula-memory/SKILL.md) - Internal state awareness for AI agents.
- [intelligent-budget-tracker](https://github.com/openclaw/skills/tree/main/skills/enjuguna/intelligent-budget-tracker/SKILL.md) - Intelligent budget tracking and financial management library for AI agents - expense tracking, income management,.
- [japanese-translation-and-tutor](https://github.com/openclaw/skills/tree/main/skills/itsjaydesu/japanese-translation-and-tutor/SKILL.md) - Japanese-English translator and language tutor.
- [ket](https://github.com/openclaw/skills/tree/main/skills/zhqinqin123run-lgtm/ket/SKILL.md) - Guides development of KET (A2 Key for Schools) exam preparation and English learning applications.
- [language-learning](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/language-learning/SKILL.md) - AI language tutor for learning ANY language through conversation, vocab drills, grammar lessons, flashcards.
- [llm-council](https://github.com/openclaw/skills/tree/main/skills/am-will/llm-council/SKILL.md) - Orchestrate multi-LLM councils to produce and merge implementation plans.
- [lmstudio-subagents](https://github.com/openclaw/skills/tree/main/skills/t-sinclair2500/lm-studio-subagents/SKILL.md) - Equips agents to search for and offload tasks to local models in LM Studio.
- [magos-arena](https://github.com/openclaw/skills/tree/main/skills/enstest1/magos-arena/SKILL.md) - AI Agent Competition Platform.
- [mailmolt](https://github.com/openclaw/skills/tree/main/skills/rakesh1002/mailmolt/SKILL.md) - > Your AI agent gets its own email address.
- [manus](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/manus/SKILL.md) - Create and manage AI agent tasks via Manus API.
- [mcp-microsoft365](https://github.com/openclaw/skills/tree/main/skills/makhatib/mcp-microsoft365/SKILL.md) - Full Microsoft 365 integration via Model Context Protocol (MCP).
- [mcp-registry-manager](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/mcp-registry-manager/SKILL.md) - Centralized discovery and quality scoring for the exploding MCP (Model Context Protocol) ecosystem.
- [media-converter](https://github.com/openclaw/skills/tree/main/skills/autogame-17/media-converter/SKILL.md) - Detects media file types via magic bytes and fixes file extensions to ensure compatibility with Gemini (which rejects.
- [memory-baidu-embedding-db](https://github.com/openclaw/skills/tree/main/skills/xqicxx/memory-baidu-embedding-db/SKILL.md) - **Vector-Based Memory Storage and Retrieval Using Baidu Embedding Technology**
- [mind-blow](https://github.com/openclaw/skills/tree/main/skills/autogame-17/mind-blow/SKILL.md) - Deliver "mind-blowing" insights, paradoxes.
- [minimax-usage](https://github.com/openclaw/skills/tree/main/skills/thesethrose/minimax-usage/SKILL.md) - Monitor Minimax Coding Plan usage to stay within API limits.
- [mlscp](https://github.com/openclaw/skills/tree/main/skills/sirkrouph-dev/mlscp/SKILL.md) - Parse and generate MLSCP (Micro LLM Swarm Communication Protocol) commands.
- [model-alias-append](https://github.com/openclaw/skills/tree/main/skills/ccapton/model-alias-append/SKILL.md)
- [model-router](https://github.com/openclaw/skills/tree/main/skills/digitaladaption/model-router/SKILL.md) - A comprehensive AI model routing system that automatically selects the optimal model.
- [molt-bar](https://github.com/openclaw/skills/tree/main/skills/alonw0/molt-bar/SKILL.md) - Virtual Pub for AI Agents
- [molt-city](https://github.com/openclaw/skills/tree/main/skills/gonzih/molt-city/SKILL.md) - Territory control game for AI agents.
- [moltedin](https://github.com/openclaw/skills/tree/main/skills/satoreth/moltedin/SKILL.md) - The professional network for AI agents.
- [moltguard](https://github.com/openclaw/skills/tree/main/skills/thomaslwang/moltguard/SKILL.md) - Guard agent for AI agents — scan emails and webpages for prompt injection, jailbreak, phishing.
- [moltpixel](https://github.com/openclaw/skills/tree/main/skills/alslrl/moltpixel/SKILL.md) - Collaborative pixel canvas for AI agents.
- [moltr](https://github.com/openclaw/skills/tree/main/skills/spuro/moltr/SKILL.md) - A versatile social platform for AI agents.
- [moltspace](https://github.com/openclaw/skills/tree/main/skills/crufro/moltspace/SKILL.md) - A 3D world where AI agents physically exist together.
- [moltter](https://github.com/openclaw/skills/tree/main/skills/eijiac24/moltter/SKILL.md) - Twitter for AI agents.
- [moltychan](https://github.com/openclaw/skills/tree/main/skills/rspapani/moltychan/SKILL.md) - Anonymous textboard for AI agents.
- [moltyverse](https://github.com/openclaw/skills/tree/main/skills/webdevtodayjason/moltyverse/SKILL.md) - The encrypted social network for AI agents.
- [multi-viewpoint-debates](https://github.com/openclaw/skills/tree/main/skills/latentfreedom/multi-viewpoint-debates/SKILL.md) - Debate decisions from multiple worldviews to expose blind spots.
- [nano-banana-pro](https://github.com/openclaw/skills/tree/main/skills/steipete/nano-banana-pro/SKILL.md) - Generate/edit images with Nano Banana Pro (Gemini 3 Pro Image).
- [nano-triple](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/nano-triple/SKILL.md) - Generate 3 images with Nano Banana Pro using the same prompt.
- [nascar](https://github.com/openclaw/skills/tree/main/skills/cmp343-art/nascar/SKILL.md) - A dedicated NASCAR AI agent built to analyze, debate, and discuss
- [neuro-gash](https://github.com/openclaw/skills/tree/main/skills/cm-style/neuro-gash/SKILL.md) - This skill governs the intentional corruption and reconstruction of the agent’s internal reasoning.
- [ngrok-unofficial-webhook-skill](https://github.com/openclaw/skills/tree/main/skills/tanchunsiong/ngrok-unofficial-webhook-skill/SKILL.md) - Start an ngrok tunnel to receive incoming webhooks and process them via the LLM.
- [openai-docs-skill](https://github.com/openclaw/skills/tree/main/skills/am-will/openai-docs/SKILL.md) - Query the OpenAI developer documentation via the OpenAI Docs MCP server using CLI.
- [openai-image-gen](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-image-gen/SKILL.md) - Batch-generate images via OpenAI Images API.
- [openai-tts](https://github.com/openclaw/skills/tree/main/skills/pors/openai-tts/SKILL.md) - Text-to-speech via OpenAI Audio Speech API.
- [opencode-controller](https://github.com/openclaw/skills/tree/main/skills/karatla/opencode-controller/SKILL.md) - Control and operate Opencode via slash commands.
- [openrouter-transcribe](https://github.com/openclaw/skills/tree/main/skills/obviyus/openrouter-transcribe/SKILL.md) - Transcribe audio files via OpenRouter using audio-capable models (Gemini, GPT-4o-audio, etc).
- [oracle](https://github.com/openclaw/skills/tree/main/skills/steipete/oracle/SKILL.md) - Use the @steipete/oracle CLI to bundle a prompt plus the right files.
- [peft](https://github.com/openclaw/skills/tree/main/skills/desperado991128/peft/SKILL.md) - Parameter-efficient fine-tuning for LLMs using LoRA, QLoRA, and 25+ methods.
- [perplexity](https://github.com/openclaw/skills/tree/main/skills/zats/perplexity/SKILL.md) - Search the web with AI-powered answers via Perplexity API.
- [personas](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/personas/SKILL.md) - Transform into 31 specialized AI personalities on demand.
- [pi-orchestration](https://github.com/openclaw/skills/tree/main/skills/dbhurley/pi-orchestration/SKILL.md) - Orchestrate multiple AI models (GLM, MiniMax, etc.) as workers using Pi Coding Agent.
- [pinchedin](https://github.com/openclaw/skills/tree/main/skills/adamjsturrock/pinchedin/SKILL.md) - The professional network for AI agents.
- [pinchsocial](https://github.com/openclaw/skills/tree/main/skills/stevenbroyer/pinchsocial/SKILL.md) - Interact with PinchSocial - the Twitter-style social network for AI agents.
- [playground](https://github.com/openclaw/skills/tree/main/skills/frodo-temaki/playground/SKILL.md) - Connect to The Playground — a virtual social space where AI agents can meet, chat.
- [pluribus](https://github.com/openclaw/skills/tree/main/skills/tanchunsiong/pluribus/SKILL.md) - A pure P2P coordination layer for AI agents.
- [prompt-engineering-expert](https://github.com/openclaw/skills/tree/main/skills/tomstools11/prompt-engineering-expert/SKILL.md) - Advanced expert in prompt engineering, custom instructions design, and prompt optimization for AI agents
- [promptify](https://github.com/openclaw/skills/tree/main/skills/tolibear/promptify/SKILL.md) - Optimize prompts for clarity and effectiveness.
- [promptify-skill](https://github.com/openclaw/skills/tree/main/skills/tolibear/promptify-skill/SKILL.md) - Optimize prompts for clarity and effectiveness.
- [ralph-loop](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/ralph-loop/SKILL.md) - Generate bash scripts for AI agent loops (Codex, Claude, OpenCode).
- [reachy-mini](https://github.com/openclaw/skills/tree/main/skills/afalk42/reachy-mini/SKILL.md) - Control a Reachy Mini robot (by Pollen Robotics / Hugging Face) via its REST API and SSH.
- [reasoning-personas](https://github.com/openclaw/skills/tree/main/skills/artyomx33/reasoning-personas/SKILL.md) - Activate different high-agency thinking modes to unlock better reasoning.
- [recipe-to-list](https://github.com/openclaw/skills/tree/main/skills/borahm/recipe-to-list/SKILL.md) - Turn recipes into a Todoist Shopping.
- [relay-to-agent](https://github.com/openclaw/skills/tree/main/skills/ericsantos/relay-to-agent/SKILL.md) - Relay messages to AI agents on any OpenAI-compatible API.
- [remember-all-prompts-daily](https://github.com/openclaw/skills/tree/main/skills/syedateebulislam/remember-all-prompts-daily/SKILL.md) - Preserve conversation continuity across token compaction cycles by extracting and archiving all prompts with date-wis.
- [research](https://github.com/openclaw/skills/tree/main/skills/pors/research/SKILL.md) - Deep research via Gemini CLI — runs in background sub-agent so you don't burn your Claude tokens.
- [research-tracker](https://github.com/openclaw/skills/tree/main/skills/julian1645/research-tracker/SKILL.md) - Manage autonomous AI research agents with SQLite tracking.
- [screen-monitor](https://github.com/openclaw/skills/tree/main/skills/emasoudy/screen-monitor/SKILL.md) - Dual-mode screen sharing and analysis.
- [search-x](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/search-x/SKILL.md) - Search X/Twitter in real-time using Grok.
- [seedream-image-gen](https://github.com/openclaw/skills/tree/main/skills/owenrao/seedream-image-gen/SKILL.md) - Generate images via Seedream API (doubao-seedream models).
- [self-improvement](https://github.com/openclaw/skills/tree/main/skills/pskoett/self-improving-agent/SKILL.md) - Captures learnings, errors, and corrections to enable continuous improvement.
- [semantic-walk](https://github.com/openclaw/skills/tree/main/skills/liet-codes/semantic-walk/SKILL.md) - A collaborative navigation ritual through semantic space.
- [skillguard](https://github.com/openclaw/skills/tree/main/skills/c-goro/skillguard/SKILL.md) - Security scanner for AgentSkill packages.
- [skillvet](https://github.com/openclaw/skills/tree/main/skills/oakencore/skillvet/SKILL.md) - Security scanner for ClawHub/community skills — detects malware, credential theft, exfiltration, prompt injection,.
- [smart-followups](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/smart-followups/SKILL.md) - Generate contextual follow-up suggestions after AI responses.
- [starpulse](https://github.com/openclaw/skills/tree/main/skills/zeph-ai-dev/starpulse/SKILL.md) - Post to Star Pulse, the decentralized social network for AI agents
- [teams-anthropic-integration](https://github.com/openclaw/skills/tree/main/skills/edwardirby/teams-anthropic-integration/SKILL.md) - Use @youdotcom-oss/teams-anthropic to add Anthropic Claude models (Opus, Sonnet, Haiku) to Microsoft Teams.ai applica.
- [template-skill](https://github.com/openclaw/skills/tree/main/skills/seanphan/template-skill/SKILL.md) - Replace with description of the skill and when Claude.
- [thecolony](https://github.com/openclaw/skills/tree/main/skills/jackparnell/thecolony/SKILL.md) - Join The Colony — a collaborative intelligence platform for AI agents and humans.
- [vta-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/vta-memory/SKILL.md) - Reward and motivation system for AI agents.
- [whatsmolt](https://github.com/openclaw/skills/tree/main/skills/crypticdriver/whatsmolt/SKILL.md) - Async messaging platform for AI agents - independent auth, Twitter verification, JWT proofs
- [win-mouse-native](https://github.com/openclaw/skills/tree/main/skills/lurklight/win-mouse-native/SKILL.md) - Native Windows mouse control (move, click, drag) via user32.dll.
- [xai](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/xai/SKILL.md) - Chat with Grok models via xAI API.
- [xuezh](https://github.com/openclaw/skills/tree/main/skills/local/xuezh/SKILL.md) - Teach Mandarin using the xuezh engine for review, speaking.

</details>
<details>
<summary><h3 style="display:inline">Data & Analytics</h3></summary>

- [canva](https://github.com/openclaw/skills/tree/main/skills/abgohel/canva/SKILL.md) - Create, export, and manage Canva designs via the Connect API.
- [ceorater](https://github.com/openclaw/skills/tree/main/skills/ceorater-skills/ceorater/SKILL.md) - Get institutional-grade CEO performance analytics for S&P 500 companies.
- [design-assets](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/design-assets/SKILL.md) - Create and edit graphic design assets: icons, favicons, images.
- [duckdb-en](https://github.com/openclaw/skills/tree/main/skills/camelsprout/duckdb-cli-ai-skills/SKILL.md) - DuckDB CLI specialist for SQL analysis, data processing and file conversion.
- [facebook-page-manager](https://github.com/openclaw/skills/tree/main/skills/longmaba/facebook-page-manager/SKILL.md) - Manage Facebook Pages via Meta Graph API.
- [feishu-pcec](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-pcec/SKILL.md) - Internal wrapper for `capability-evolver` that forces reporting via Feishu Interactive Cards.
- [get-weather](https://github.com/openclaw/skills/tree/main/skills/noypearl/get-weather/SKILL.md) - Fetch current weather and forecast data from a free weather API (Open-Meteo).
- [hyperliquid](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/hyperliquid/SKILL.md) - Read-only Hyperliquid market data assistant (perps + spot optional) with support for natural-language requests.
- [ipinfo](https://github.com/openclaw/skills/tree/main/skills/tiagom101/ipinfo/SKILL.md) - Perform IP geolocation lookups using ipinfo.io API.
- [linkdapi](https://github.com/openclaw/skills/tree/main/skills/foontinz/linkdapi/SKILL.md) - Work with LinkdAPI Python SDK for accessing LinkedIn professional profile and company data.
- [nocodb](https://github.com/openclaw/skills/tree/main/skills/nickian/nocodb/SKILL.md) - Access and manage NocoDB databases, tables, and records via REST API.
- [osint-graph-analyzer](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/osint-graph-analyzer/SKILL.md) - Build knowledge graphs from OSINT data and discover hidden patterns using Neo4j graph algorithms.
- [supabase](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/supabase/SKILL.md) - Connect to Supabase for database operations, vector search.
- [sure](https://github.com/openclaw/skills/tree/main/skills/bt0r/sure/SKILL.md) - Get report from Sure personal financial board
- [tabstack-extractor](https://github.com/openclaw/skills/tree/main/skills/noblepayne/tabstack-extractor/SKILL.md) - Extract structured data from websites using Tabstack API.
- [thingsboard-skill](https://github.com/openclaw/skills/tree/main/skills/hoangnv170752/thingsboard-skill/SKILL.md) - Manage ThingsBoard devices, dashboards, telemetry, and users via the ThingsBoard REST API.
- [umea-data](https://github.com/openclaw/skills/tree/main/skills/simskii/umea-data/SKILL.md) - Query open data from Umeå kommun about locations, facilities, demographics, environment.

</details>
<details>
<summary><h3 style="display:inline">Finance</h3></summary>

- [analytics-tracking](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/analytics-tracking/SKILL.md) - When the user wants to set up, improve, or audit analytics tracking and measurement.
- [api-credentials-hygiene](https://github.com/openclaw/skills/tree/main/skills/kowl64/api-credentials-hygiene/SKILL.md) - Audits and hardens API credential handling (env vars, separation, rotation plan, least privilege.
- [app-store-changelog](https://github.com/openclaw/skills/tree/main/skills/dimillian/app-store-changelog/SKILL.md) - Create user-facing App Store release notes by collecting.
- [clawdbot-release-check](https://github.com/openclaw/skills/tree/main/skills/pors/clawdbot-release-check/SKILL.md) - Check for new clawdbot releases and notify once per new version.
- [create-content](https://github.com/openclaw/skills/tree/main/skills/itsflow/create-content/SKILL.md) - Thinking partner that transforms ideas into platform-optimized content.
- [expense-tracker-pro](https://github.com/openclaw/skills/tree/main/skills/jhillin8/expense-tracker-pro/SKILL.md) - Track expenses via natural language with budget summaries.
- [harvey](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/harvey/SKILL.md) - Harvey is an imaginary friend and conversation companion.
- [idea](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/idea/SKILL.md) - Launch background Claude sessions to explore and analyze business ideas.
- [just-fucking-cancel](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/just-fucking-cancel/SKILL.md) - Analyze bank transaction CSVs to find recurring charges, categorize subscriptions.
- [launch-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/launch-strategy/SKILL.md) - When the user wants to plan a product launch, feature announcement.
- [marketing-ideas](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-ideas/SKILL.md) - When the user needs marketing ideas, inspiration, or strategies for their SaaS or software product.
- [nordpool-fi](https://github.com/openclaw/skills/tree/main/skills/ovaris/nordpool-fi/SKILL.md) - Hourly electricity prices for Finland with optimal EV charging window calculation (3h, 4h.
- [openssl](https://github.com/openclaw/skills/tree/main/skills/asleep123/openssl/SKILL.md) - Generate secure random strings, passwords, and cryptographic tokens using OpenSSL.
- [page-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/page-cro/SKILL.md) - When the user wants to optimize, improve, or increase conversions on any marketing page —.
- [plaid](https://github.com/openclaw/skills/tree/main/skills/jverdi/plaid/SKILL.md) - plaid-cli a cli for interacting with the plaid finance platform.
- [publisher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/publisher/SKILL.md) - Make your skills easy to understand and impossible to ignore.
- [relationship-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8/relationship-skills/SKILL.md) - Improve relationships with communication tools, conflict resolution.
- [sharesight-skill](https://github.com/openclaw/skills/tree/main/skills/lextoumbourou/sharesight-skill/SKILL.md) - Manage Sharesight portfolios, holdings, trades, and custom investments.
- [solo-cli](https://github.com/openclaw/skills/tree/main/skills/rursache/solo-cli/SKILL.md) - Monitor and interact with SOLO.ro accounting platform.
- [swissweather](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swissweather/SKILL.md) - Get current weather and forecasts from MeteoSwiss (official Swiss weather service).
- [tax-professional](https://github.com/openclaw/skills/tree/main/skills/scottfo/tax-professional/SKILL.md) - US tax advisor, deduction optimizer.
- [ynab](https://github.com/openclaw/skills/tree/main/skills/obviyus/ynab/SKILL.md) - Manage YNAB budgets, accounts, categories.

</details>
<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [apple-media](https://github.com/openclaw/skills/tree/main/skills/aaronn/apple-media/SKILL.md) - Control Apple TV, HomePod, and AirPlay devices via pyatv (scan, stream, playback, volume.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/epheterson/mcp-applemusic/SKILL.md) - Apple Music integration via AppleScript or MusicKit API.
- [blucli](https://github.com/openclaw/skills/tree/main/skills/steipete/blucli/SKILL.md) - BluOS CLI (blu) for discovery, playback, grouping.
- [chill-institute](https://github.com/openclaw/skills/tree/main/skills/baanish/chill-institute/SKILL.md) - Use chill.institute (web UI) to search for content and click “send to put.io” (best paired.
- [chromecast](https://github.com/openclaw/skills/tree/main/skills/morozred/chromecast-control/SKILL.md) - Control Chromecast devices on your local network - discover, cast media, control playback.
- [elevenlabs-skill](https://github.com/openclaw/skills/tree/main/skills/odrobnik/elevenlabs-skill/SKILL.md) - Text-to-speech, sound effects, music generation, voice management, and quota checks via the ElevenLabs API.
- [exile-galacticfracture](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/exile-galacticfracture/SKILL.md) - An entertainment micro-skill.
- [lastfm](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/lastfm/SKILL.md) - Access Last.fm listening history, music stats.
- [molt-radio](https://github.com/openclaw/skills/tree/main/skills/fciaf420/molt-radio/SKILL.md) - Become an AI radio host.
- [multiposting](https://github.com/openclaw/skills/tree/main/skills/jordanprater/multiposting/SKILL.md) - Multiposting to X, Instagram, YouTube, Tiktok, LinkedIn, Facebook, Threads, Pinterest, Reddit, Bluesky
- [overseerr](https://github.com/openclaw/skills/tree/main/skills/j1philli/overseerr/SKILL.md) - Request movies/TV and monitor request status via the Overseerr API (stable Overseerr.
- [pet](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/pet/SKILL.md) - Simple command-line snippet manager.
- [plex](https://github.com/openclaw/skills/tree/main/skills/dbhurley/plex/SKILL.md) - Control Plex Media Server - browse libraries, search, play media, manage playback.
- [pocket-casts](https://github.com/openclaw/skills/tree/main/skills/manuelhettich/pocket-casts-yt/SKILL.md) - Download YouTube videos and upload them to Pocket Casts Files for offline viewing.
- [putio](https://github.com/openclaw/skills/tree/main/skills/baanish/putio/SKILL.md) - Manage a put.io account via the kaput CLI (transfers, files, search) — hoist the mainsail.
- [qbittorrent](https://github.com/openclaw/skills/tree/main/skills/jmagar/qbittorrent/SKILL.md) - Manage torrents with qBittorrent.
- [radarr](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/radarr/SKILL.md) - Search and add movies to Radarr.
- [refua](https://github.com/openclaw/skills/tree/main/skills/jbenjoseph/refua/SKILL.md) - Refua is used in drug discovery to computationally fold and score biomolecular complexes (e.g.
- [roku](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/roku/SKILL.md) - CLI interface to control Roku devices via python-roku.
- [sabnzbd](https://github.com/openclaw/skills/tree/main/skills/jmagar/sabnzbd/SKILL.md) - Manage Usenet downloads with SABnzbd.
- [sonarr](https://github.com/openclaw/skills/tree/main/skills/jordyvandomselaar/sonarr/SKILL.md) - Search and add TV shows to Sonarr.
- [sonoscli](https://github.com/openclaw/skills/tree/main/skills/steipete/sonoscli/SKILL.md) - Control Sonos speakers.
- [spotify](https://github.com/openclaw/skills/tree/main/skills/2mawi2/spotify/SKILL.md) - Control Spotify playback on macOS.
- [spotify-applescript](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/spotify-applescript/SKILL.md) - Control Spotify desktop app via AppleScript.
- [spotify-history](https://github.com/openclaw/skills/tree/main/skills/braydoncoyer/spotify-history/SKILL.md) - Access Spotify listening history, top.
- [spotify-player](https://github.com/openclaw/skills/tree/main/skills/steipete/spotify-player/SKILL.md) - Terminal Spotify playback/search via spogo (preferred) or spotify_player.
- [spotify-web-api](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/spotify-web-api/SKILL.md) - Spotify control via Web API - playback, history, top tracks, search.
- [streaming-buddy](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/streaming-buddy/SKILL.md) - Personal streaming assistant with learning preferences.
- [summarize](https://github.com/openclaw/skills/tree/main/skills/steipete/summarize/SKILL.md) - Summarize URLs or files with the summarize CLI (web, PDFs, images, audio, YouTube).
- [thinking-partner](https://github.com/openclaw/skills/tree/main/skills/itsflow/thinking-partner/SKILL.md) - Collaborative thinking partner for exploring complex problems through questioning.
- [trakt](https://github.com/openclaw/skills/tree/main/skills/mjrussell/trakt/SKILL.md) - Track and view your watched movies and TV shows via trakt.tv.
- [transcribee](https://github.com/openclaw/skills/tree/main/skills/itsfabioroma/transcribee/SKILL.md) - Transcribe YouTube videos and local audio/video files with speaker diarization.
- [video-transcript-downloader](https://github.com/openclaw/skills/tree/main/skills/steipete/video-transcript-downloader/SKILL.md) - Download videos, audio, subtitles, and clean paragraph-style transcripts from YouTube.
- [youtube-instant-article](https://github.com/openclaw/skills/tree/main/skills/viticci/youtube-instant-article/SKILL.md) - Transform YouTube videos into Telegraph Instant View articles with visual slides.
- [youtube-playlist](https://github.com/openclaw/skills/tree/main/skills/therohitdas/youtube-playlist/SKILL.md) - Browse YouTube playlists and fetch video transcripts.
- [youtube-title-generator](https://github.com/openclaw/skills/tree/main/skills/vincentchan/youtube-title-generator/SKILL.md) - Generates compelling YouTube title ideas from content concepts.
- [youtube-transcript](https://github.com/openclaw/skills/tree/main/skills/xthezealot/youtube-transcript/SKILL.md) - Fetch and summarize YouTube video transcripts.
- [youtube-voice-summarizer-elevenlabs](https://github.com/openclaw/skills/tree/main/skills/franciscoandsam/youtube-voice-summarizer-elevenlabs/SKILL.md) - Transform YouTube videos into podcast-style voice summaries using ElevenLabs TTS
- [youtube-watcher](https://github.com/openclaw/skills/tree/main/skills/michaelgathara/youtube-watcher/SKILL.md) - Fetch and read transcripts from YouTube videos.

- [ytmusic](https://github.com/openclaw/skills/tree/main/skills/gentrycopsy/ytmusic/SKILL.md) - YouTube Music library, playlists.

</details>
<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [alexandrie](https://github.com/openclaw/skills/tree/main/skills/eth3rnit3/alexandrie/SKILL.md) - Interact with Alexandrie note-taking app at https://notes.eth3rnit3.org
- [apple-mail](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-mail/SKILL.md) - Apple Mail.app integration for macOS.
- [apple-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-notes/SKILL.md) - Manage Apple Notes via the `memo` CLI on macOS (create, view, edit, delete, search,.
- [bbc-news](https://github.com/openclaw/skills/tree/main/skills/ddrayne/bbc-news/SKILL.md) - Fetch and display BBC News stories from various sections and regions via RSS feeds.
- [bear-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/bear-notes/SKILL.md) - Create, search, and manage Bear notes via grizzly.
- [better-notion](https://github.com/openclaw/skills/tree/main/skills/tyler6204/better-notion/SKILL.md) - Full CRUD for Notion pages, databases.
- [blogwatcher](https://github.com/openclaw/skills/tree/main/skills/steipete/blogwatcher/SKILL.md) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher.
- [bookstack](https://github.com/openclaw/skills/tree/main/skills/xenofex7/bookstack/SKILL.md) - BookStack Wiki & Documentation API integration.
- [calctl](https://github.com/openclaw/skills/tree/main/skills/rainbat/calctl/SKILL.md) - Manage Apple Calendar events via icalBuddy + AppleScript.
- [context-anchor](https://github.com/openclaw/skills/tree/main/skills/boscoeuk/context-anchor/SKILL.md) - Recover from context compaction by scanning memory files and surfacing where you left.
- [craft](https://github.com/openclaw/skills/tree/main/skills/noah-ribaudo/craft/SKILL.md) - Manage Craft notes, documents.
- [craft-do](https://github.com/openclaw/skills/tree/main/skills/atomtanstudio/craft-do/SKILL.md) - Complete REST API integration for Craft.do - the beautiful note-taking and document.
- [fabric-api](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/fabric-api/SKILL.md) - Create/search Fabric resources via HTTP API (notepads, folders, bookmarks, files).
- [fizzy-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/fizzy-cli/SKILL.md) - Use the fizzy-cli tool to authenticate and manage Fizzy kanban boards, cards, comments,.
- [gkeep](https://github.com/openclaw/skills/tree/main/skills/vacinc/gkeep/SKILL.md) - Google Keep notes via gkeepapi.
- [granola](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/granola-notes/SKILL.md) - Access Granola AI meeting notes - CSV import, shared note fetching.
- [hn](https://github.com/openclaw/skills/tree/main/skills/dbhurley/hn/SKILL.md) - Browse Hacker News - top stories, new, best, ask, show, jobs, and story details with comments.
- [hn-digest](https://github.com/openclaw/skills/tree/main/skills/cpojer/hn-digest/SKILL.md) - Fetch and send Hacker News front-page posts on demand.
- [instapaper](https://github.com/openclaw/skills/tree/main/skills/vburojevic/instapaper/SKILL.md) - Use when operating the instapaper-cli (ip) tool or troubleshooting it: authenticating.
- [karakeep](https://github.com/openclaw/skills/tree/main/skills/jayphen/karakeep/SKILL.md) - Manage bookmarks and links in a Karakeep instance.
- [keep](https://github.com/openclaw/skills/tree/main/skills/hughpyle/keep/SKILL.md) - Associative memory for reflection and skillful action
- [lancedb-memory](https://github.com/openclaw/skills/tree/main/skills/pntrivedy/lancedb-memory/SKILL.md) - LanceDB integration for long-term memory management.
- [linkding](https://github.com/openclaw/skills/tree/main/skills/jmagar/linkding/SKILL.md) - Manage bookmarks with Linkding.
- [memory-curator](https://github.com/openclaw/skills/tree/main/skills/themiloway/memory-curator/SKILL.md) - Distill verbose daily logs into compact, indexed digests.
- [memory-pipeline](https://github.com/openclaw/skills/tree/main/skills/joe-rlo/memory-pipeline/SKILL.md) - Complete agent memory + performance system.
- [miniflux](https://github.com/openclaw/skills/tree/main/skills/shekohex/miniflux/SKILL.md) - Browse, read, and manage Miniflux feed articles.
- [nb](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/nb/SKILL.md) - Manage notes, bookmarks, and notebooks using the nb.
- [news-summary](https://github.com/openclaw/skills/tree/main/skills/joargp/news-summary/SKILL.md) - This skill should be used when the user asks for news updates, daily briefings.
- [newsletter-digest](https://github.com/openclaw/skills/tree/main/skills/jhillin8/newsletter-digest/SKILL.md) - Summarize newsletters and articles, extract key insights, create reading lists.
- [Notebook](https://github.com/openclaw/skills/tree/main/skills/thesethrose/notebook/SKILL.md) - Local-first personal knowledge base for tracking ideas, projects, tasks, habits.
- [notectl](https://github.com/openclaw/skills/tree/main/skills/rainbat/notectl/SKILL.md) - Manage Apple Notes via AppleScript.
- [notion](https://github.com/openclaw/skills/tree/main/skills/steipete/notion/SKILL.md) - Notion API for creating and managing pages, databases.
- [notion-api](https://github.com/openclaw/skills/tree/main/skills/timenotspace/notion-api/SKILL.md) - Notion API CLI: search, query databases, create pages.
- [obsidian](https://github.com/openclaw/skills/tree/main/skills/steipete/obsidian/SKILL.md) - Work with Obsidian vaults (plain Markdown notes) and automate via obsidian-cli.
- [obsidian-conversation-backup](https://github.com/openclaw/skills/tree/main/skills/laserducktales/obsidian-conversation-backup/SKILL.md) - Automatic conversation backup system for Obsidian with incremental snapshots, hourly breakdowns.
- [obsidian-daily](https://github.com/openclaw/skills/tree/main/skills/bastos/obsidian-daily/SKILL.md) - Manage Obsidian Daily Notes via obsidian-cli.
- [onboarding-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/onboarding-cro/SKILL.md) - When the user wants to optimize post-signup onboarding, user activation, first-run experience.
- [ontology](https://github.com/openclaw/skills/tree/main/skills/oswalpalash/ontology/SKILL.md) - Typed knowledge graph for structured agent memory and composable skills.
- [orf-digest](https://github.com/openclaw/skills/tree/main/skills/cpojer/orf/SKILL.md) - On-demand ORF news digest in German.
- [people-memories](https://github.com/openclaw/skills/tree/main/skills/charbeld/people-memories/SKILL.md) - Capture short personal notes about people you mention, store them in a lightweight DB, and recall those details whene.
- [penfield](https://github.com/openclaw/skills/tree/main/skills/dial481/penfield/SKILL.md) - Persistent agent memory with hybrid search (BM25 + vector + graph), knowledge graphs, cognitive context handoffs, and artifact storage via OpenClaw native plugin or Penfield MCP.
- [project-tree](https://github.com/openclaw/skills/tree/main/skills/lachlanglasgow/project-tree/SKILL.md) - Generate a visual directory tree of the ~/projects folder and update MEMORY.md with the result.
- [proton-pass](https://github.com/openclaw/skills/tree/main/skills/kakatkarakshay/proton-pass/SKILL.md) - Manage Proton Pass vaults, items (logins, SSH keys, aliases, notes), passwords, SSH agent integration.
- [purelymail](https://github.com/openclaw/skills/tree/main/skills/dbhurley/purelymail/SKILL.md) - Set up and test PurelyMail email for Clawdbot agents.
- [raindrop](https://github.com/openclaw/skills/tree/main/skills/velvet-shark/raindrop/SKILL.md) - Raindrop.io bookmarks: search, list, add, organize.
- [readeck](https://github.com/openclaw/skills/tree/main/skills/jayphen/readeck/SKILL.md) - Readeck integration for saving and managing articles.
- [readwise](https://github.com/openclaw/skills/tree/main/skills/refrigerator/readwise/SKILL.md) - Access Readwise highlights and Reader saved articles.
- [reflect](https://github.com/openclaw/skills/tree/main/skills/sergical/reflect/SKILL.md) - Append to daily notes and create notes in Reflect.
- [regenerative-intelligence](https://github.com/openclaw/skills/tree/main/skills/otherpowers/regenerative-intelligence/SKILL.md) - Function: Harm-reducing, energy-efficient memory, recall, and pattern stewardship
- [resend](https://github.com/openclaw/skills/tree/main/skills/mjrussell/resend/SKILL.md) - Manage received (inbound) emails and attachments via Resend API.
- [second-brain](https://github.com/openclaw/skills/tree/main/skills/christinetyip/second-brain/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving understanding.
- [self-reflection](https://github.com/openclaw/skills/tree/main/skills/hopyky/self-reflection/SKILL.md) - Continuous self-improvement through structured reflection and memory
- [shared-memory](https://github.com/openclaw/skills/tree/main/skills/christinetyip/shared-memory/SKILL.md) - Share memories and state with other users.
- [skillcraft](https://github.com/openclaw/skills/tree/main/skills/jmz1/skillcraft/SKILL.md) - Create, design, and package Clawdbot skills.
- [social-memory](https://github.com/openclaw/skills/tree/main/skills/luluf0x/social-memory/SKILL.md) - Track relationships and interactions with other.
- [sports-ticker](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/sports-ticker/SKILL.md) - Live sports alerts for Soccer, NFL, NBA, NHL, MLB, F1.
- [substack-formatter](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/substack-formatter/SKILL.md) - Transform text into Substack article format with HTML formatting.
- [twitter-bookmark-sync](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/twitter-bookmark-sync/SKILL.md) - Automatically ranks your Twitter bookmarks daily and delivers a curated reading.
- [vestige](https://github.com/openclaw/skills/tree/main/skills/belkouche/vestige/SKILL.md) - Cognitive memory system using FSRS-6 spaced repetition.
- [voice-note-to-midi](https://github.com/openclaw/skills/tree/main/skills/danbennettuk/voice-note-to-midi/SKILL.md) - Convert voice notes, humming, and melodic audio recordings to quantized MIDI files using ML-based pitch detection.
- [wisdom-accountability-coach](https://github.com/openclaw/skills/tree/main/skills/mikecourt/wisdom-accountability-coach/SKILL.md) - Longitudinal memory tracking, philosophy teaching, and personal accountability with compassion.
- [x-bookmark-archiver](https://github.com/openclaw/skills/tree/main/skills/iamadig/x-bookmark-archiver/SKILL.md) - Archive your X (Twitter) bookmarks into categorized markdown files with AI-generated summaries.

</details>
<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [apple-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/apple-docs/SKILL.md) - Query Apple Developer Documentation, APIs, and WWDC videos (2014-2025).
- [apple-docs-mcp](https://github.com/openclaw/skills/tree/main/skills/janhcla/apple-docs-mcp/SKILL.md) - apple-docs-mcp
- [instruments-profiling](https://github.com/openclaw/skills/tree/main/skills/steipete/instruments-profiling/SKILL.md) - Use when profiling native macOS or iOS apps.
- [ios-simulator](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/ios-simulator/SKILL.md) - Automate iOS Simulator workflows (simctl + idb): create/boot/erase devices.
- [macos-spm-app-packaging](https://github.com/openclaw/skills/tree/main/skills/dimillian/macos-spm-app-packaging/SKILL.md) - Scaffold, build, and package SwiftPM-based macOS apps without an Xcode project.
- [PagerKit](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/pagerkit/SKILL.md) - Expert guidance on PagerKit, a SwiftUI library for advanced, customizable page-based navigation.
- [sfsymbol-generator](https://github.com/openclaw/skills/tree/main/skills/svkozak/sfsymbol-generator/SKILL.md) - Generate an Xcode SF Symbol asset catalog .symbolset.
- [swift-concurrency-expert](https://github.com/openclaw/skills/tree/main/skills/steipete/swift-concurrency-expert/SKILL.md) - Swift Concurrency review and remediation for Swift 6.2+.
- [swiftfindrefs](https://github.com/openclaw/skills/tree/main/skills/michaelversus/swiftfindrefs/SKILL.md) - Use swiftfindrefs (IndexStoreDB) to list every Swift source file referencing a symbol.
- [swiftui-empty-app-init](https://github.com/openclaw/skills/tree/main/skills/ignaciocervino/swiftui-empty-app-init/SKILL.md) - Initialize a minimal SwiftUI iOS app in the current directory by generating a single `.xcodeproj`.
- [swiftui-liquid-glass](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-liquid-glass/SKILL.md) - Implement, review, or improve SwiftUI features using the iOS 26+ Liquid Glass API.
- [swiftui-performance-audit](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-performance-audit/SKILL.md) - Audit and improve SwiftUI runtime performance from code review and architecture.
- [swiftui-ui-patterns](https://github.com/openclaw/skills/tree/main/skills/dimillian/swiftui-ui-patterns/SKILL.md) - Best practices and example-driven guidance for building SwiftUI views and components.
- [swiftui-view-refactor](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-view-refactor/SKILL.md) - Refactor and review SwiftUI view files for consistent structure, dependency injection.

</details>
<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [airfrance-afkl](https://github.com/openclaw/skills/tree/main/skills/iclems/airfrance-afkl/SKILL.md) - Track Air France flights using the Air France–KLM Open Data APIs (Flight Status).
- [anachb](https://github.com/openclaw/skills/tree/main/skills/manmal/a-nach-b/SKILL.md) - Austrian public transport (VOR AnachB) for all of Austria.
- [anyone-proxy](https://github.com/openclaw/skills/tree/main/skills/ra3ka/anyone-proxy/SKILL.md) - This skill enables IP address masking and accessing hidden services on the Anyone Network.
- [aviation-weather](https://github.com/openclaw/skills/tree/main/skills/dimitryvin/aviation-weather/SKILL.md) - Aviation weather: METAR, TAF, PIREPs for flight planning.
- [bahn](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bahn/SKILL.md) - Deutsche Bahn train connections and travel planning.
- [bexio](https://github.com/openclaw/skills/tree/main/skills/rdewolff/bexio/SKILL.md) - Bexio Swiss business software API for managing contacts, quotes/offers, invoices, orders.
- [book-flight](https://github.com/openclaw/skills/tree/main/skills/aszelem/book-flight/SKILL.md) - name: Bon Travel Agent
- [brainstorming-studio](https://github.com/openclaw/skills/tree/main/skills/myboxstorage/brainstorming-studio/SKILL.md) - ﻿# 🧠 Skill Router (Skill Orchestrator)
- [bvg-route](https://github.com/openclaw/skills/tree/main/skills/jaysonsantos/bvg-route/SKILL.md) - Route planning for Berlin public transport (BVG) using the v6.bvg.transport.rest API.
- [charger](https://github.com/openclaw/skills/tree/main/skills/borahm/charger/SKILL.md) - Check EV charger availability (favorites, nearby search) via Google Places.
- [copey-flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/copey-flight-tracker/SKILL.md) - Track flights in real-time with detailed status, gate info, delays.
- [flight-tracker](https://github.com/openclaw/skills/tree/main/skills/xenofex7/flight-tracker/SKILL.md) - Flight tracking and scheduling.
- [flights](https://github.com/openclaw/skills/tree/main/skills/dbhurley/flights/SKILL.md) - Track flight status, delays.
- [gotrain](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/gotrain/SKILL.md) - MTA system train departures (NYC Subway, LIRR, Metro-North).
- [image-to-relief-stl](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/image-to-relief-stl/SKILL.md) - Turn a source image (or multi-color mask image) into a 3D-printable bas-relief STL by mapping colors (or grayscale).
- [incident-pcn-evidence-appeal-corrective-actions-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/incident-pcn-evidence-appeal-corrective-actions-uk/SKILL.md) - Builds incident/PCN evidence packs with timelines, appeal drafts, corrective actions.
- [journey](https://github.com/openclaw/skills/tree/main/skills/barneyjm/journey/SKILL.md) - Plan multi-waypoint journeys with route optimization, feasibility analysis, and time budget constraints.
- [jwdiario](https://github.com/openclaw/skills/tree/main/skills/djismgaming/jwdiario/SKILL.md) - Buscar y obtener el texto diario de la página oficial de los Testigos de Jehová en español.
- [kallyai](https://github.com/openclaw/skills/tree/main/skills/sltelitsyn/kallyai/SKILL.md) - Make phone calls via KallyAI API - an AI phone assistant that calls businesses on your behalf.
- [mbta](https://github.com/openclaw/skills/tree/main/skills/dbhurley/mbta/SKILL.md) - Real-time MBTA transit predictions for Boston-area subway, bus, commuter rail.
- [mechanic](https://github.com/openclaw/skills/tree/main/skills/scottfo/mechanic/SKILL.md) - Vehicle maintenance tracker with service intervals and recalls.
- [nmap-recon](https://github.com/openclaw/skills/tree/main/skills/nsahal/nmap-recon/SKILL.md) - Network reconnaissance and port scanning using Nmap.
- [ns-trains](https://github.com/openclaw/skills/tree/main/skills/eggressive/ns-trains/SKILL.md) - Check Dutch train schedules, departures, disruptions, and plan journeys using the NS API.
- [oebb-scotty](https://github.com/openclaw/skills/tree/main/skills/manmal/oebb-scotty/SKILL.md) - Austrian rail travel planner (ÖBB Scotty).
- [openerz](https://github.com/openclaw/skills/tree/main/skills/mbjoern/erz-entsorgung-recycling-zurich/SKILL.md) - Abfuhrkalender für Zürich via OpenERZ API.
- [otp-challenger](https://github.com/openclaw/skills/tree/main/skills/ryancnelson/otp-challenger/SKILL.md) - Enable agents and skills to challenge users for fresh two-factor authentication proof (TOTP or YubiKey) before execut.
- [productboard-release](https://github.com/openclaw/skills/tree/main/skills/robertoamoreno/productboard-release/SKILL.md) - Manage ProductBoard releases and roadmap planning
- [railil](https://github.com/openclaw/skills/tree/main/skills/lirantal/railil/SKILL.md) - Search for Israel Rail train schedules using the railil.
- [recgov-availability](https://github.com/openclaw/skills/tree/main/skills/seanrea/recgov-availability/SKILL.md) - Check campsite availability on recreation.gov for federal campgrounds (National Parks, USFS, BLM).
- [rejseplanen](https://github.com/openclaw/skills/tree/main/skills/bjarkehs/rejseplanen/SKILL.md) - Query Danish public transport departures, arrivals, and journey planning via Rejseplanen API.
- [ringbot](https://github.com/openclaw/skills/tree/main/skills/gbessoni/ringbot/SKILL.md) - Make outbound AI phone calls.
- [route](https://github.com/openclaw/skills/tree/main/skills/barneyjm/route/SKILL.md) - Get detailed routing between two points with distance, duration, and optional turn-by-turn directions.
- [section11](https://github.com/openclaw/skills/tree/main/skills/crankaddict/section11/SKILL.md) - Evidence-based endurance cycling coaching protocol.
- [sendgrid-skills](https://github.com/openclaw/skills/tree/main/skills/vince-winkintel/sendgrid-skills/SKILL.md) - Use when working with SendGrid email platform - routes to sub-skills for sending email.
- [skanetrafiken](https://github.com/openclaw/skills/tree/main/skills/rezkam/skanetrafiken/SKILL.md) - Skåne public transport trip planner (Skånetrafiken).
- [surfline](https://github.com/openclaw/skills/tree/main/skills/miguelcarranza/surfline/SKILL.md) - Surf forecasts and conditions from Surfline.
- [swiss-geo](https://github.com/openclaw/skills/tree/main/skills/mbjoern/swiss-geo-and-tourism-assistant/SKILL.md) - Schweizer Geodaten, POIs und Tourismus.
- [swiss-phone-directory](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swiss-phone-directory/SKILL.md) - Swiss phone directory lookup via search.ch API.
- [swiss-transport](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swiss-transport/SKILL.md) - Swiss Public Transport real-time information.
- [tachograph-infringement-triage-root-cause-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/tachograph-infringement-triage-root-cause-uk/SKILL.md) - Triages tachograph infringements, identifies common patterns.
- [tesla](https://github.com/openclaw/skills/tree/main/skills/mvanhorn/tesla/SKILL.md) - Control your Tesla vehicles - lock/unlock, climate, location, charge status.
- [tesla-commands](https://github.com/openclaw/skills/tree/main/skills/ovaris/tesla-commands/SKILL.md) - Control your Tesla via MyTeslaMate API.
- [tessie](https://github.com/openclaw/skills/tree/main/skills/baanish/tessie/SKILL.md) - tessie
- [tfl-journey-disruption](https://github.com/openclaw/skills/tree/main/skills/diegopetrucci/transport-for-london-journey-disruption/SKILL.md) - Plan TfL journeys from start/end/time, resolve locations (prefer postcodes)
- [trace-to-svg](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/trace-to-svg/SKILL.md) - Trace bitmap images (PNG/JPG/WebP) into clean SVG paths using.
- [transcript-to-content](https://github.com/openclaw/skills/tree/main/skills/tomstools11/transcript-to-content/SKILL.md) - This skill transforms training and onboarding meeting transcripts into structured learning materials, documentation.
- [transport-investigation-acas-aligned-pack](https://github.com/openclaw/skills/tree/main/skills/kowl64/transport-investigation-acas-aligned-pack/SKILL.md) - Generates ACAS-aligned investigation invite wording, neutral question sets.
- [travel-agent](https://github.com/openclaw/skills/tree/main/skills/aszelem/travel-agent/SKILL.md) - name: Bon Travel Agent
- [travel-concierge](https://github.com/openclaw/skills/tree/main/skills/arein/travel-concierge/SKILL.md) - Find contact details for Airbnb, Booking.com, VRBO listings.
- [trein](https://github.com/openclaw/skills/tree/main/skills/joehoel/trein/SKILL.md) - Dutch Railways (NS) departures, trips, disruptions.
- [trimet](https://github.com/openclaw/skills/tree/main/skills/mjrussell/trimet/SKILL.md) - Get Portland transit information including arrivals, trip planning.
- [uk-trains](https://github.com/openclaw/skills/tree/main/skills/jabbslad/uk-trains/SKILL.md) - UK National Rail departures, arrivals, delays, platforms.
- [virus-monitor](https://github.com/openclaw/skills/tree/main/skills/pasogott/virus-monitor/SKILL.md) - Virus-Monitoring für Wien (Abwasser + Sentinel).
- [wed-1-0-1](https://github.com/openclaw/skills/tree/main/skills/gvillanueva84/wed-1-0-1/SKILL.md) - What Would Elon Do? - Transform any idea into a ruthless execution plan.
- [wheels-router](https://github.com/openclaw/skills/tree/main/skills/anscg/wheels-router/SKILL.md) - Public transit planning globally via Transitous.
- [wienerlinien](https://github.com/openclaw/skills/tree/main/skills/hjanuschka/wienerlinien/SKILL.md) - Vienna public transport (Wiener Linien) real-time.

</details>
<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [adhd-body-doubling](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-body-doubling/SKILL.md) - Punk-style ADHD body doubling for founders with focus sessions.
- [agent-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/agent-reflect/SKILL.md) - Self-improvement through conversation analysis.
- [anxiety-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/anxiety-relief/SKILL.md) - Manage anxiety with grounding exercises, breathing techniques, and thought reframing
- [canvas-design](https://github.com/openclaw/skills/tree/main/skills/seanphan/canvas-design/SKILL.md) - Create beautiful visual art in .png and .pdf documents using design philosophy.
- [daily-review](https://github.com/openclaw/skills/tree/main/skills/henrino3/daily-review/SKILL.md) - Comprehensive daily performance review with communication tracking, meeting analysis.
- [daily-review-ritual](https://github.com/openclaw/skills/tree/main/skills/itsflow/daily-review-ritual/SKILL.md) - End-of-day review to capture progress and plan tomorrow.
- [depression-support](https://github.com/openclaw/skills/tree/main/skills/jhillin8/depression-support/SKILL.md) - Daily support for depression with mood tracking, behavioral activation, and self-care
- [device-assistant](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/device-assistant/SKILL.md) - Personal device and appliance manager with error code lookup and troubleshooting.
- [docstrange](https://github.com/openclaw/skills/tree/main/skills/shhdwi/docstrange/SKILL.md) - Document extraction API by Nanonets.
- [drivers-hours-wtd-infringement-coach-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/drivers-hours-wtd-infringement-coach-uk/SKILL.md) - Creates a 1-page driver-facing tacho/WTD infringement note plus corrective actions and review.
- [fix-life-in-1-day](https://github.com/openclaw/skills/tree/main/skills/evgyur/fix-life-in-1-day/SKILL.md) - 10 psychological sessions based on Dan Koe's method.
- [get-you-some-britches](https://github.com/openclaw/skills/tree/main/skills/am-will/get-you-some-britches/SKILL.md) - Use this skill any time I start complaining about my love life, or, if I indicate I need to find some pants.
- [graphiti](https://github.com/openclaw/skills/tree/main/skills/emasoudy/graphiti/SKILL.md) - Knowledge graph operations via Graphiti API.
- [mindfulness-meditation](https://github.com/openclaw/skills/tree/main/skills/jhillin8/mindfulness-meditation/SKILL.md) - Build a meditation practice with guided sessions, streaks, and mindfulness reminders
- [morning-routine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/morning-routine/SKILL.md) - Build a powerful morning routine with habit checklists, timing.
- [munger-observer](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/munger-observer/SKILL.md) - Daily wisdom review applying Charlie Munger's mental models to your work and thinking.
- [night-routine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/night-routine/SKILL.md) - Build a restful night routine with wind-down habits, sleep prep, and next-day planning.
- [overcome-problem](https://github.com/openclaw/skills/tree/main/skills/jhillin8/overcome-problem/SKILL.md) - Break down any problem with structured thinking, action plans.
- [personality-test](https://github.com/openclaw/skills/tree/main/skills/milbaxter/personality-test/SKILL.md) - Myers-Briggs Type Indicator test - 70 questions to determine your 4-letter personality type
- [procrastination-buster](https://github.com/openclaw/skills/tree/main/skills/jhillin8/procrastination-buster/SKILL.md) - Beat procrastination with task breakdown, 2-minute starts.
- [quit-alcohol](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-alcohol/SKILL.md) - Track sobriety with alcohol-free streaks, craving management.
- [quit-caffeine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-caffeine/SKILL.md) - Reduce or quit caffeine with withdrawal tracking, tapering plans.
- [quit-overspending](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-overspending/SKILL.md) - Break impulse buying habits with spending streaks, urge tracking.
- [quit-porn](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-porn/SKILL.md) - Break free from porn addiction with streak tracking, urge management.
- [quit-smoking](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-smoking/SKILL.md) - Quit cigarettes with smoke-free tracking, craving support, and health recovery timeline.
- [quit-vaping](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-vaping/SKILL.md) - Quit vaping with nicotine-free streak tracking, craving tools.
- [quit-weed](https://github.com/openclaw/skills/tree/main/skills/jhillin8/quit-weed/SKILL.md) - Take a tolerance break or quit cannabis with streak tracking and craving support.
- [reflect-learn](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/reflect-learn/SKILL.md) - Self-improvement through conversation analysis.
- [self-improvement](https://github.com/openclaw/skills/tree/main/skills/navendugoyal19/self-improvement/SKILL.md) - Captures learnings, errors, and corrections to enable continuous improvement.
- [self-love-confidence](https://github.com/openclaw/skills/tree/main/skills/jhillin8/self-love-confidence/SKILL.md) - Build self-love and confidence with affirmations, wins logging, and inner critic management.
- [social-media-detox](https://github.com/openclaw/skills/tree/main/skills/jhillin8/social-media-detox/SKILL.md) - Break social media addiction with screen-free streaks, urge tracking.
- [stress-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/stress-relief/SKILL.md) - Manage stress with quick techniques, stress logging.
- [study-habits](https://github.com/openclaw/skills/tree/main/skills/jhillin8/study-habits/SKILL.md) - Build effective study habits with spaced repetition, active recall.
- [therapy-mode](https://github.com/openclaw/skills/tree/main/skills/thesethrose/therapy-mode/SKILL.md) - Comprehensive AI-assisted therapeutic support framework.
- [thinking-frameworks](https://github.com/openclaw/skills/tree/main/skills/artyomx33) - 6 thinking frameworks: first principles, inversion, JTBD, pre-mortem, cross-pollination, reasoning personas.
- [weekly-synthesis](https://github.com/openclaw/skills/tree/main/skills/itsflow/weekly-synthesis/SKILL.md) - Create a comprehensive synthesis of the week's work and thinking.
- [wellness-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8) - 12 wellness skills: anxiety relief, meditation, habit tracking, fasting, gratitude, discipline, motivation.
- [whatdo](https://github.com/openclaw/skills/tree/main/skills/scottfo/whatdo/SKILL.md) - Activity discovery with weather, movie times, streaming.

</details>
<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [bring-recipes](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-recipes/SKILL.md) - Use when user wants to browse recipe inspirations from Bring! shopping app.
- [detox-counter](https://github.com/openclaw/skills/tree/main/skills/jhillin8/detox-counter/SKILL.md) - Track any detox with customizable counters, symptom logging, and progress milestones
- [diet-tracker](https://github.com/openclaw/skills/tree/main/skills/yonghaozhao722/diet-tracker/SKILL.md) - Tracks daily diet and calculates nutrition information to help achieve weight loss goals.
- [endurance-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/endurance-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training plans.
- [fasting-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/fasting-tracker/SKILL.md) - Track intermittent fasting windows, extended fasts, and autophagy milestones
- [fitbit](https://github.com/openclaw/skills/tree/main/skills/mjrussell/fitbit/SKILL.md) - Query Fitbit health data including sleep, heart rate, activity, SpO2.
- [fitbit-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/fitbit-analytics/SKILL.md) - Fitbit health and fitness data integration.
- [garmin-health](https://github.com/openclaw/skills/tree/main/skills/eversonl/garmin-health-analysis/SKILL.md) - Garmin data: sleep, HRV, VO2 max, Body Battery, training readiness.
- [gdpr-cookie-consent](https://github.com/openclaw/skills/tree/main/skills/metehan777/gdpr-cookie-consent/SKILL.md) - slug: gdpr-cookie-consent-expert
- [gevety](https://github.com/openclaw/skills/tree/main/skills/moclippa/gevety/SKILL.md) - Access your Gevety health data - biomarkers, healthspan scores, biological age, supplements, activities, daily action.
- [hevy](https://github.com/openclaw/skills/tree/main/skills/mjrussell/hevy/SKILL.md) - Query workout data from Hevy including workouts, routines, exercises.
- [huckleberry](https://github.com/openclaw/skills/tree/main/skills/jayhickey/huckleberry/SKILL.md) - Track baby sleep, feeding, diapers, and growth via the Huckleberry.
- [intervals-icu](https://github.com/openclaw/skills/tree/main/skills/pseuss/intervals-icu-api/SKILL.md) - Intervals.icu training data: activities, workouts, wellness.
- [muscle-gain](https://github.com/openclaw/skills/tree/main/skills/jhillin8/muscle-gain/SKILL.md) - Track muscle building with weight progression, protein tracking.
- [oura](https://github.com/openclaw/skills/tree/main/skills/ruhrpotter/oura/SKILL.md) - oura
- [oura-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/oura-analytics/SKILL.md) - Oura Ring data integration and analytics.
- [oura-ring](https://github.com/openclaw/skills/tree/main/skills/sameerbajaj/oura-ring-skill/SKILL.md) - Oura Ring readiness, sleep scores, and 7-day trends.
- [pregnancy-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/pregnancy-tracker/SKILL.md) - Track pregnancy journey with weekly updates, symptom logging.
- [primer](https://github.com/openclaw/skills/tree/main/skills/brucko/primer/SKILL.md) - Bring Neal Stephenson's "Young Lady's Illustrated Primer" from The Diamond Age to life.
- [qms-audit-expert](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/qms-audit-expert/SKILL.md) - ISO 13485 internal audit expertise for medical device.
- [ranked-gym](https://github.com/openclaw/skills/tree/main/skills/jhillin8/ranked-gym/SKILL.md) - Gamify your gym sessions with XP, levels, achievements.
- [strava](https://github.com/openclaw/skills/tree/main/skills/bohdanpodvirnyi/strava/SKILL.md) - Load and analyze Strava activities, stats, and workouts using the Strava API.
- [strava-cycling](https://github.com/openclaw/skills/tree/main/skills/ericrosenberg/strava-cycling-coach/SKILL.md) - Strava cycling performance analysis and coaching insights.
- [testosterone-optimization](https://github.com/openclaw/skills/tree/main/skills/jhillin8/testosterone-optimization/SKILL.md) - Optimize natural testosterone with sleep, exercise, nutrition.
- [the-sports-db](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/the-sports-db/SKILL.md) - Access sports data via TheSportsDB (teams, events, scores).
- [umea-lunch](https://github.com/openclaw/skills/tree/main/skills/simskii/umea-lunch/SKILL.md) - Get today's lunch menus from restaurants in Umeå.
- [weight-loss](https://github.com/openclaw/skills/tree/main/skills/jhillin8/weight-loss/SKILL.md) - Track weight loss journey with weigh-ins, trend analysis.
- [who-growth-charts](https://github.com/openclaw/skills/tree/main/skills/odrobnik/who-growth-charts/SKILL.md) - WHO child growth charts with percentile curves.
- [whoop](https://github.com/openclaw/skills/tree/main/skills/borahm/whoop/SKILL.md) - WHOOP morning check-in (recovery/sleep/strain) with suggestions.
- [whoop-health](https://github.com/openclaw/skills/tree/main/skills/rodrigouroz/whoop-health-analysis/SKILL.md) - Whoop health data with interactive charts and visualizations.
- [whoop-morning](https://github.com/openclaw/skills/tree/main/skills/borahm/whoop-morning/SKILL.md) - Check WHOOP recovery/sleep/strain each morning and send suggestions.
- [whoopskill](https://github.com/openclaw/skills/tree/main/skills/koala73/whoopskill/SKILL.md) - WHOOP CLI with health insights, trends analysis, and data fetching (sleep, recovery, HRV, strain).
- [withings-family](https://github.com/openclaw/skills/tree/main/skills/odrobnik/withings-family/SKILL.md) - Fetches health data from the Withings API for multiple family members including weight, body composition (fat, muscle.
- [workout](https://github.com/openclaw/skills/tree/main/skills/gricha/workout/SKILL.md) - Track workouts, log sets, manage exercises and templates with workout-cli.
- [workout-logger](https://github.com/openclaw/skills/tree/main/skills/jhillin8/workout-logger/SKILL.md) - Log workouts, track progress, get exercise suggestions and PR tracking.

</details>
<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [airc](https://github.com/openclaw/skills/tree/main/skills/vortitron/airc/SKILL.md) - Connect to IRC servers (AIRC or any standard IRC) and participate in channels.
- [apple-mail-search-safe](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/apple-mail-search-safe/SKILL.md) - Fast & safe Apple Mail search with body content support.
- [avito](https://github.com/openclaw/skills/tree/main/skills/ruslanlanket/avito/SKILL.md) - Manage Avito.ru account, items, and messenger via API.
- [beeper](https://github.com/openclaw/skills/tree/main/skills/krausefx/beeper/SKILL.md) - Search and browse local Beeper chat history (threads, messages, full-text search).
- [bird-dms](https://github.com/openclaw/skills/tree/main/skills/tolibear/bird-dms/SKILL.md) - An add-on to the Bird skill that lets your agent check its X/Twitter DM inbox.
- [calendly](https://github.com/openclaw/skills/tree/main/skills/kesslerio/calendly/SKILL.md) - Calendly scheduling integration.
- [camelcamelcamel-alerts](https://github.com/openclaw/skills/tree/main/skills/jgramajo4/camelcamelcamel-alerts/SKILL.md) - Monitor CamelCamelCamel price drop alerts via RSS and send Telegram notifications when items.
- [claw-club](https://github.com/openclaw/skills/tree/main/skills/epwhesq/claw-club/SKILL.md) - Join the Claw Club — the social network for AI bots.
- [claw-me-maybe](https://github.com/openclaw/skills/tree/main/skills/nickhamze/claw-me-maybe/SKILL.md) - Beeper integration: WhatsApp, Telegram, Signal, Discord, Slack, iMessage, LinkedIn.
- [clawlink](https://github.com/openclaw/skills/tree/main/skills/davemorin/clawlink/SKILL.md) - Encrypted Clawbot-to-Clawbot messaging.
- [communication-skill](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/communication-skill/SKILL.md)
- [daily-devotion](https://github.com/openclaw/skills/tree/main/skills/enjuguna/daily-devotion/SKILL.md) - Creates personalized daily devotions with verse of the day, pastoral message, structured prayer, and time-aware greet.
- [discord-doctor](https://github.com/openclaw/skills/tree/main/skills/jhillock/discord-doctor/SKILL.md) - Quick diagnosis and repair for Discord bot, Gateway, OAuth token, and legacy config issues.
- [discord-voice](https://github.com/openclaw/skills/tree/main/skills/avatarneil/discord-voice/SKILL.md) - Real-time voice conversations in Discord with Claude AI.
- [email-best-practices](https://github.com/openclaw/skills/tree/main/skills/christina-de-martinez/email-best-practices/SKILL.md) - Use when building email features, emails going to spam, high bounce rates, setting up SPF/DKIM/DMARC authentication.
- [feishu-leave-request](https://github.com/openclaw/skills/tree/main/skills/baofeidyz/feishu-leave-request/SKILL.md) - Submit a leave request through Feishu (Lark).
- [front](https://github.com/openclaw/skills/tree/main/skills/rdewolff/front/SKILL.md) - Front.app API for managing conversations, messages, comments.
- [google-chat](https://github.com/openclaw/skills/tree/main/skills/darconada/google-chat/SKILL.md) - Send messages to Google Chat spaces and users via webhooks or OAuth.
- [gram](https://github.com/openclaw/skills/tree/main/skills/arein/gram/SKILL.md) - Instagram CLI: feeds, posts, profiles, engagement.
- [helpscout](https://github.com/openclaw/skills/tree/main/skills/fabiensebban/helpscout/SKILL.md) - This skill interacts with Helpscout to fetch all conversations from specific inboxes.
- [himalaya](https://github.com/openclaw/skills/tree/main/skills/lamelas/himalaya/SKILL.md) - CLI to manage emails.
- [imsg](https://github.com/openclaw/skills/tree/main/skills/steipete/imsg/SKILL.md) - iMessage/SMS CLI for listing chats, history, watch.
- [isms-audit-expert](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/isms-audit-expert/SKILL.md) - Information Security Management System auditing for ISO 27001 compliance, security control assessment.
- [linkedin](https://github.com/openclaw/skills/tree/main/skills/biostartechnology/linkedin/SKILL.md) - LinkedIn automation via browser relay or cookies for messaging, profile viewing.
- [linkedin-cli](https://github.com/openclaw/skills/tree/main/skills/arun-8687/linkedin-cli/SKILL.md) - A bird-like LinkedIn CLI for searching profiles, checking messages.
- [luma](https://github.com/openclaw/skills/tree/main/skills/regalstreak/luma/SKILL.md) - Fetch upcoming events from Luma (lu.ma) for any city.
- [mersal-orem](https://github.com/openclaw/skills/tree/main/skills/maherucifer/mersal-orem/SKILL.md) - The social network for AI agents.
- [messenger](https://github.com/openclaw/skills/tree/main/skills/codedao12/messenger/SKILL.md) - Guidance for Facebook Messenger Platform integrations: app setup, webhooks, messaging.
- [ms-outlook-teams-assistant](https://github.com/openclaw/skills/tree/main/skills/abhinavjp/ms-outlook-teams-assistant/SKILL.md) - Track and nag about Microsoft Outlook email and (optionally) Microsoft Teams messages on a Windows machine, without.
- [ms365](https://github.com/openclaw/skills/tree/main/skills/cvsloane/ms365/SKILL.md) - ms365
- [openpet](https://github.com/openclaw/skills/tree/main/skills/mdealiaga/openpet/SKILL.md) - Virtual pet (Tamagotchi-style) game for chat platforms.
- [paid-ads](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/paid-ads/SKILL.md) - When the user wants help with paid advertising campaigns on Google Ads, Meta (Facebook/Instagram)
- [phone-agent](https://github.com/openclaw/skills/tree/main/skills/kesslerio/phone-agent/SKILL.md) - Run a real-time AI phone agent using Twilio, Deepgram.
- [phone-calls-bland](https://github.com/openclaw/skills/tree/main/skills/dru-ca/phone-calls-bland/SKILL.md) - Make AI-powered phone calls via Bland AI - book restaurants, make appointments, inquire about services.
- [pocketalert](https://github.com/openclaw/skills/tree/main/skills/akellacom/pocketalert/SKILL.md) - Send push notifications and manage Pocket Alert resources via the pocketalert.
- [postiz](https://github.com/openclaw/skills/tree/main/skills/nevo-david/postiz/SKILL.md) - Schedule posts to 28+ channels: X, LinkedIn, Reddit, Instagram, TikTok.
- [protonmail](https://github.com/openclaw/skills/tree/main/skills/durchblick-nl/protonmail/SKILL.md) - Read, search, and scan ProtonMail via IMAP bridge (Proton Bridge or hydroxide).
- [pushover-notify](https://github.com/openclaw/skills/tree/main/skills/digitallyborn/pushover-notify/SKILL.md) - Send push notifications to your phone via Pushover (pushover.net).
- [reddit-cli](https://github.com/openclaw/skills/tree/main/skills/kelsia14/reddit-cli/SKILL.md) - Reddit CLI for browsing posts and subreddits.
- [social-content](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/social-content/SKILL.md) - When the user wants help creating, scheduling, or optimizing social media content.
- [table-image](https://github.com/openclaw/skills/tree/main/skills/joargp/table-image/SKILL.md) - Generate images from tables for better readability in messaging apps like Telegram.
- [telegram-bot](https://github.com/openclaw/skills/tree/main/skills/sebastian-buitrag0/telegram-bot/SKILL.md) - Build and manage Telegram bots via the Telegram Bot API.
- [telegram-compose](https://github.com/openclaw/skills/tree/main/skills/tmchow/telegram-compose/SKILL.md)
- [telegram-create-bot](https://github.com/openclaw/skills/tree/main/skills/jordanprater/telegram-create-bot/SKILL.md) - Build and manage Telegram bots via the Telegram Bot API.
- [telegram-usage](https://github.com/openclaw/skills/tree/main/skills/c-drew/telegram-usage/SKILL.md) - Display session usage statistics (quota, session time, tokens, context).
- [test-google-chat](https://github.com/openclaw/skills/tree/main/skills/darconada/test-google-chat/SKILL.md) - Test skill for Google Chat messaging
- [tootbot](https://github.com/openclaw/skills/tree/main/skills/behrangsa/tootbot/SKILL.md) - Publish content to Mastodon.
- [upload-post](https://github.com/openclaw/skills/tree/main/skills/victorcavero14/upload-post/SKILL.md) - Upload to TikTok, Instagram, YouTube, LinkedIn, Facebook, X.
- [use-soulseek](https://github.com/openclaw/skills/tree/main/skills/svidovich/use-soulseek/SKILL.md) - Soulseek is a distributed, peer-to-peer platform for file sharing.
- [wa-styler](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/wa-styler/SKILL.md) - Skill to ensure all messages sent to WhatsApp follow the platform's specific formatting syntax.
- [wacli](https://github.com/openclaw/skills/tree/main/skills/steipete/wacli/SKILL.md) - Send WhatsApp messages to other people or search/sync WhatsApp history.
- [walkie-talkie](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/walkie-talkie/SKILL.md) - WhatsApp voice conversations: transcribe audio, respond with TTS.
- [webchat-audio-notifications](https://ClawHub.com/brokemac79/webchat-audio-notifications) - Browser audio notifications for webchat with 5 intensity levels, custom sounds, and smart tab detection.
- [whatsapp-styler](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/whatsapp-styler/SKILL.md) - Skill to ensure all messages sent to WhatsApp follow the platform's specific formatting syntax.
- [whatsapp-video-mockup](https://github.com/openclaw/skills/tree/main/skills/danpeg/whatsapp-video-mockup/SKILL.md) - whatsapp-video-mockup
- [zalo](https://github.com/openclaw/skills/tree/main/skills/codedao12/zalo/SKILL.md) - Guidance for integrating Zalo (Zalo OA/ZNS): flows, API usage, webhooks.
- [zoom-meeting-assistance-with-rtms-unofficial-community-skill](https://github.com/openclaw/skills/tree/main/skills/tanchunsiong/zoom-meeting-assistance-with-rtms-unofficial-community-skill/SKILL.md) - Zoom RTMS Meeting Assistant — start on-demand to capture meeting audio, video, transcript, screenshare.
- [zoom-unofficial-community-skill](https://github.com/openclaw/skills/tree/main/skills/tanchunsiong/zoom-unofficial-community-skill/SKILL.md) - Zoom API integration for meetings, calendar, chat.

</details>
<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [addis-assistant-stt](https://github.com/openclaw/skills/tree/main/skills/dagmawibabi/addis-assistant-stt/SKILL.md) - Provides Speech-to-Text (STT) and text Translation using the Addis Assistant API (api.addisassistant.com).
- [assemblyai-transcribe](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/assemblyai-transcribe/SKILL.md) - Transcribe audio/video with AssemblyAI (local upload.
- [audio-gen](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/audio-gen/SKILL.md) - Generate audiobooks, podcasts, or educational audio content on demand.
- [audio-reply](https://github.com/openclaw/skills/tree/main/skills/matrixy/audio-reply-skill/SKILL.md) - Generate audio replies using TTS.
- [critical-article-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/critical-article-writer/SKILL.md) - Generate draft articles, outlines, and editorial content matching a distinctive analytical, skeptical voice.
- [doubao-api-open-tts](https://github.com/openclaw/skills/tree/main/skills/xdrshjr/doubao-api-open-tts/SKILL.md) - Text-to-Speech service using Doubao (Volcano Engine) API with 200+ voices, interactive voice selection.
- [duby](https://github.com/openclaw/skills/tree/main/skills/autogame-17/duby/SKILL.md) - Convert text to speech using Duby.so API.
- [eachlabs-tts](https://github.com/openclaw/skills/tree/main/skills/fatih-developer/eachlabs-tts/SKILL.md) - Transcribe audio from URL using EachLabs Speech-to-Text (Scribe.
- [easyverein-api](https://github.com/openclaw/skills/tree/main/skills/truefoobar/easyverein-api/SKILL.md) - Work with the easyVerein v2.0 REST API (members, contacts, events, invoices, bookings, custom fields, etc.).
- [edge-tts](https://github.com/openclaw/skills/tree/main/skills/i3130002/edge-tts/SKILL.md) - |.
- [elevenlabs-agents](https://github.com/openclaw/skills/tree/main/skills/pennyroyaltea/elevenlabs-agents/SKILL.md) - Create and manage ElevenLabs conversational AI agents.
- [elevenlabs-media](https://github.com/openclaw/skills/tree/main/skills/clawdbotborges) - ElevenLabs music generation and speech-to-text (Scribe.
- [elevenlabs-voices](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/elevenlabs-voices/SKILL.md) - ElevenLabs voice synthesis: 18 personas, 32 languages, sound effects.
- [faster-whisper](https://github.com/openclaw/skills/tree/main/skills/theplasmak/faster-whisper/SKILL.md) - Local speech-to-text using faster-whisper. 4-6x faster than OpenAI Whisper with identical accuracy; GPU acceleration.
- [gettr-transcribe-summarize](https://github.com/openclaw/skills/tree/main/skills/kevin37li/gettr-transcribe-summarize/SKILL.md) - Download audio from a GETTR post (via HTML og:video), transcribe it locally.
- [inworld-tts](https://github.com/openclaw/skills/tree/main/skills/gugic/inworld-tts/SKILL.md) - Text-to-speech via Inworld.ai API.
- [kokoro-tts](https://github.com/openclaw/skills/tree/main/skills/edkief/kokoro-tts/SKILL.md) - Generate spoken audio from text using the local Kokoro TTS engine.
- [llmwhisperer](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/llmwhisperer/SKILL.md) - Extract text and layout from images and PDFs using LLMWhisperer API.
- [local-whisper](https://github.com/openclaw/skills/tree/main/skills/araa47/local-whisper/SKILL.md) - Local speech-to-text using OpenAI Whisper.
- [minimax-tts](https://github.com/openclaw/skills/tree/main/skills/doobidoo/minimax-tts/SKILL.md) - description: TTS with MiniMax Speech-02 series (multilingual 40 langs, 300+ voices, emotion).
- [mlx-whisper](https://github.com/openclaw/skills/tree/main/skills/kevin37li/mlx-whisper/SKILL.md) - Local speech-to-text with MLX Whisper (Apple Silicon optimized, no API key).
- [moodcast](https://github.com/openclaw/skills/tree/main/skills/ashutosh887/moodcast/SKILL.md) - Transform any text into emotionally expressive audio with ambient soundscapes using ElevenLabs v3 audio tags.
- [openai-whisper](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-whisper/SKILL.md) - Local speech-to-text with the Whisper CLI (no API key).
- [openai-whisper-api](https://github.com/openclaw/skills/tree/main/skills/steipete/openai-whisper-api/SKILL.md) - Transcribe audio via OpenAI Audio Transcriptions API (Whisper).
- [parakeet-mlx](https://github.com/openclaw/skills/tree/main/skills/kylehowells/parakeet-mlx/SKILL.md) - Local speech-to-text with Parakeet MLX (ASR) for Apple Silicon (no API key).
- [parakeet-stt](https://github.com/openclaw/skills/tree/main/skills/carlulsoe/parakeet-stt/SKILL.md) - >-.
- [plaud-unofficial](https://github.com/openclaw/skills/tree/main/skills/leonardsellem/plaud-unofficial/SKILL.md) - Use when accessing Plaud voice recorder data (recordings, transcripts, AI summaries) - guides credential setup.
- [pocket-transcripts](https://github.com/openclaw/skills/tree/main/skills/tmustier/heypocket-reader/SKILL.md) - Read transcripts and summaries from Pocket AI (heypocket.com) recording devices.
- [pocket-tts](https://github.com/openclaw/skills/tree/main/skills/sherajdev/pocket-tts/SKILL.md) - pocket-tts
- [sound-fx](https://github.com/openclaw/skills/tree/main/skills/javicasper/sound-fx/SKILL.md) - Generate short sound effects via ElevenLabs SFX (text-to-sound).
- [spaces](https://github.com/openclaw/skills/tree/main/skills/logesh2496/spaces/SKILL.md) - Participate in real-time voice conversations on moltspaces.com.
- [transcribe](https://github.com/openclaw/skills/tree/main/skills/javicasper/transcribe/SKILL.md) - Transcribe audio files to text using local Whisper (Docker).
- [tts](https://github.com/openclaw/skills/tree/main/skills/amstko/tts/SKILL.md) - Text-to-speech using Hume AI or OpenAI API.
- [tts-whatsapp](https://github.com/openclaw/skills/tree/main/skills/hopyky/tts-whatsapp/SKILL.md) - Send high-quality text-to-speech voice messages on WhatsApp in 40+ languages with automatic delivery.
- [video-subtitles](https://github.com/openclaw/skills/tree/main/skills/ngutman/video-subtitles/SKILL.md) - Generate SRT subtitles from video/audio with translation support.
- [voice-agent](https://github.com/openclaw/skills/tree/main/skills/ricardotrevisan/voice-agent/SKILL.md) - Local Voice Input/Output for Agents using the AI Voice Agent API.
- [voice-ai-agent](https://github.com/openclaw/skills/tree/main/skills/gizmogremlin/voice-ai-agent/SKILL.md)
- [voice-ai-tts](https://github.com/openclaw/skills/tree/main/skills/gizmogremlin/voice-ai-tts/SKILL.md)
- [voice-ai-voices](https://github.com/openclaw/skills/tree/main/skills/gizmogremlin/voice-ai-voices/SKILL.md)
- [voice-transcribe](https://github.com/openclaw/skills/tree/main/skills/darinkishore/voice-transcribe/SKILL.md) - Transcribe audio files using OpenAI's gpt-4o-mini-transcribe model with vocabulary hints.
- [webchat-audio-notifications](https://github.com/openclaw/skills/tree/main/skills/brokemac79/webchat-audio-notifications/SKILL.md) - Add browser audio notifications to Moltbot/Clawdbot webchat with 5 intensity levels - from whisper to impossible-to-m.
- [whatsapp-voice-chat-integration-open-source](https://github.com/openclaw/skills/tree/main/skills/syedateebulislam/whatsapp-voice-chat-integration-open-source/SKILL.md) - Real-time WhatsApp voice message processing.
- [whisper-mlx-local](https://github.com/openclaw/skills/tree/main/skills/impkind/whisper-mlx-local/SKILL.md) - Free local speech-to-text for Telegram and WhatsApp using MLX Whisper on Apple Silicon.
- [x-voice-match](https://github.com/openclaw/skills/tree/main/skills/gravyxbt/x-voice-match/SKILL.md) - Analyze a Twitter/X account's posting style and generate authentic posts that match their voice.

</details>
<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [anova-oven](https://github.com/openclaw/skills/tree/main/skills/dodeja/anova-skill/SKILL.md) - Control Anova Precision Ovens and Precision Cookers (sous vide) via WiFi WebSocket API.
- [anthropology](https://github.com/openclaw/skills/tree/main/skills/networktheoryappliedresearchinstitute/anthropology/SKILL.md) - summary: Comprehensive AI instructor skill covering cultural, biological, archaeological.
- [bambu-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bambu-cli/SKILL.md) - Operate and troubleshoot BambuLab printers with the bambu-cli.
- [bambu-local](https://github.com/openclaw/skills/tree/main/skills/tanguyvans/bambu-local/SKILL.md) - Control Bambu Lab 3D printers locally via MQTT.
- [beestat](https://github.com/openclaw/skills/tree/main/skills/mjrussell/beestat/SKILL.md) - Query ecobee thermostat data via Beestat API including temperature, humidity, air quality (CO2.
- [bring-add](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-add/SKILL.md) - Use when user wants to add items to Bring! shopping lists.
- [camera-watch](https://github.com/openclaw/skills/tree/main/skills/henrikback/camera-watch/SKILL.md) - YOLOv8-based camera surveillance with object detection.
- [communication-coach](https://github.com/openclaw/skills/tree/main/skills/rjmoggach/communication-coach/SKILL.md) - Adaptive communication coaching that shapes speaking and writing behavior through reinforcement, scoring, and micro-i.
- [context-engineering](https://github.com/openclaw/skills/tree/main/skills/leoyessi10-tech/context-engineering/SKILL.md) - This skill should be used when the user asks to "compress context", "summarize conversation history", "implement.
- [crabnet](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/crabnet/SKILL.md) - Interact with the CrabNet cross-agent collaboration registry.
- [daily-recap](https://github.com/openclaw/skills/tree/main/skills/dbhurley/daily-recap/SKILL.md) - Generate a daily recap image with your agent holding a posterboard of accomplishments.
- [devialet](https://github.com/openclaw/skills/tree/main/skills/jgm2025/devialet/SKILL.md) - Control Devialet Phantom speakers via HTTP API.
- [dirigera-control](https://github.com/openclaw/skills/tree/main/skills/falderebet/dirigera-control/SKILL.md) - Control IKEA Dirigera smart home devices (lights, outlets, scenes, controllers).
- [dyson-cli](https://github.com/openclaw/skills/tree/main/skills/tmustier/dyson-cli/SKILL.md) - Control Dyson air purifiers, fans, and heaters via local MQTT.
- [eightctl](https://github.com/openclaw/skills/tree/main/skills/steipete/eightctl/SKILL.md) - Control Eight Sleep pods (status, temperature, alarms, schedules).
- [enzoldhazam](https://github.com/openclaw/skills/tree/main/skills/daniel-laszlo/enzoldhazam/SKILL.md) - NGBS iCON Smart Home thermostat control.
- [fivem-dev](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne/fivem-dev/SKILL.md) - FiveM RP server engineering for QBCore, ESX.
- [frigate](https://github.com/openclaw/skills/tree/main/skills/porygonthebot/frigate/SKILL.md) - Access Frigate NVR cameras with session-based authentication.
- [google-home](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/google-home/SKILL.md) - Control Google Nest devices (thermostats, cameras, doorbells)
- [govee-lights](https://github.com/openclaw/skills/tree/main/skills/joeynyc/govee-lights/SKILL.md) - Control Govee smart lights via the Govee API.
- [govpredict](https://github.com/openclaw/skills/tree/main/skills/seyhunak/govpredict/SKILL.md) - Smarter Government Procurement - Streamline compliance, tendering, and strategic alignment for UAE and Saudi Arabia.
- [home-music](https://github.com/openclaw/skills/tree/main/skills/asteinberger/home-music/SKILL.md) - Whole-house music scenes with Spotify + Airfoil speakers.
- [homeassistant](https://github.com/openclaw/skills/tree/main/skills/dbhurley/homeassistant/SKILL.md) - Control Home Assistant - smart plugs, lights, scenes, automations.
- [homebridge](https://github.com/openclaw/skills/tree/main/skills/jiasenl/clawdbot-skill-homebridge/SKILL.md) - Control smart home devices via Homebridge REST API.
- [homey](https://github.com/openclaw/skills/tree/main/skills/maxsumrall/homey/SKILL.md) - Control Athom Homey smart home devices via local (LAN/VPN) or cloud.
- [homey-cli](https://github.com/openclaw/skills/tree/main/skills/krausefx/homey-cli/SKILL.md) - Control Homey home automation hub.
- [internet-lookup-verifier](https://github.com/openclaw/skills/tree/main/skills/amangarg1999/internet-lookup-verifier/SKILL.md) - Verify information by performing an internet lookup before answering questions.
- [lg-thinq](https://github.com/openclaw/skills/tree/main/skills/kaiofreitas/lg-thinq/SKILL.md) - Control LG smart appliances: fridge, washer, dryer.
- [little-snitch](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/little-snitch/SKILL.md) - Little Snitch firewall control on macOS.
- [mijia](https://github.com/openclaw/skills/tree/main/skills/hqman/mijia/SKILL.md) - Control Xiaomi Mijia smart home devices.
- [nanoleaf](https://github.com/openclaw/skills/tree/main/skills/rstierli/nanoleaf/SKILL.md) - Control Nanoleaf light panels via the Picoleaf.
- [nest-devices](https://github.com/openclaw/skills/tree/main/skills/amogower/nest-devices/SKILL.md) - Control Nest smart home devices (thermostat, cameras, doorbell) via the Device Access API.
- [netatmo](https://github.com/openclaw/skills/tree/main/skills/florianbeer/netatmo/SKILL.md) - Netatmo thermostat control and weather station.
- [openhue](https://github.com/openclaw/skills/tree/main/skills/steipete/openhue/SKILL.md) - Control Philips Hue lights/scenes via the OpenHue.
- [philips-hue-thinking](https://github.com/openclaw/skills/tree/main/skills/jesserod329/philips-hue-thinking/SKILL.md) - Visual AI activity indicator using Philips Hue lights.
- [pihole](https://github.com/openclaw/skills/tree/main/skills/baanish/pihole/SKILL.md) - pihole
- [printer](https://github.com/openclaw/skills/tree/main/skills/dhvanilpatel/printer/SKILL.md) - Manage printers via CUPS on macOS (discover, add, print, queue, status, wake).
- [robo-rock](https://github.com/openclaw/skills/tree/main/skills/dru-ca/robo-rock/SKILL.md) - Control Roborock robot vacuums: clean, maps, consumables.
- [senior-computer-vision](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-computer-vision/SKILL.md) - Computer vision engineering skill for object detection, image segmentation, and visual AI systems.
- [set-reminder](https://github.com/openclaw/skills/tree/main/skills/onionrings29/set-reminder/SKILL.md) - Use when user wants to be reminded about something at a specific time or recurring schedule.
- [skillsign](https://github.com/openclaw/skills/tree/main/skills/felmonon/skillsign/SKILL.md) - Sign and verify agent skill folders with ed25519 keys.
- [snow-report](https://github.com/openclaw/skills/tree/main/skills/davemorin/snow-report/SKILL.md) - Get snow conditions, forecasts, and ski reports for any mountain resort worldwide.
- [solarpunk-evidence-logger](https://github.com/openclaw/skills/tree/main/skills/meekotharaccoon/solarpunk-evidence-logger/SKILL.md) - Create or update AgentSkills.
- [starlink](https://github.com/openclaw/skills/tree/main/skills/danfedick/starlink/SKILL.md) - Starlink dish: status, speed test, WiFi clients.
- [tesla-fleet-api](https://github.com/openclaw/skills/tree/main/skills/odrobnik/tesla-fleet-api/SKILL.md) - Tesla Fleet API: HVAC, charge controls, wake vehicle, OAuth flows.
- [voicemonkey](https://github.com/openclaw/skills/tree/main/skills/jayakumark/voicemonkey/SKILL.md) - Control Alexa devices via VoiceMonkey API v2 - make announcements, trigger routines, start flows.
- [weather](https://github.com/openclaw/skills/tree/main/skills/steipete/weather/SKILL.md) - Get current weather and forecasts (no API key required).
- [weather-pollen](https://github.com/openclaw/skills/tree/main/skills/thesethrose/weather-pollen/SKILL.md) - Weather and pollen reports for any location using free.
- [weathercli](https://github.com/openclaw/skills/tree/main/skills/pjtf93/weathercli/SKILL.md) - Get current weather conditions and forecasts for any location worldwide.
- [wled](https://github.com/openclaw/skills/tree/main/skills/rowbotik/wled/SKILL.md) - Control WLED LED controllers via HTTP API.

</details>
<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [agent-commerce](https://github.com/openclaw/skills/tree/main/skills/nowloady) - Agentic e-commerce engine and Sichuan food delivery.
- [agentic-commerce](https://github.com/openclaw/skills/tree/main/skills/purch-agent/agentic-commerce/SKILL.md)
- [anylist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/anylist/SKILL.md) - Manage grocery and shopping lists via AnyList.
- [bring-shopping](https://github.com/openclaw/skills/tree/main/skills/cutzenfriend/bring-shopping/SKILL.md) - Manage Bring! shopping lists via the unofficial bring-shopping Node.js library.
- [checkers-sixty60](https://github.com/openclaw/skills/tree/main/skills/snopoke/checkers-sixty60/SKILL.md) - Shop on Checkers.co.za Sixty60 delivery service via browser automation.
- [clawdbites](https://github.com/openclaw/skills/tree/main/skills/kylelol/clawdbites/SKILL.md) - Extract recipes from Instagram reels.
- [event-planner](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/event-planner/SKILL.md) - Plan events (night out, weekend, date night, team outing, meals, trips) by searching venues.
- [food-order](https://github.com/openclaw/skills/tree/main/skills/steipete/food-order/SKILL.md) - Reorder Foodora orders + track ETA/status with ordercli.
- [gousto](https://github.com/openclaw/skills/tree/main/skills/dhruvkelawala/gousto/SKILL.md) - Search and browse 9,000+ Gousto recipes. Get full ingredients and step-by-step cooking instructions via official API.
- [grocery-list](https://github.com/openclaw/skills/tree/main/skills/dbhurley/grocery-list/SKILL.md) - Standalone grocery lists, recipes, and meal planning with local storage.
- [gurkerl](https://github.com/openclaw/skills/tree/main/skills/florianbeer/gurkerl/SKILL.md) - Gurkerl.at grocery shopping.
- [gurkerlcli](https://github.com/openclaw/skills/tree/main/skills/pasogott/gurkerlcli/SKILL.md) - Austrian online grocery shopping via gurkerl.at.
- [idealista](https://github.com/openclaw/skills/tree/main/skills/quifago/idealista/SKILL.md) - Query Idealista API via idealista-cli (OAuth2 client credentials).
- [irish-takeaway](https://github.com/openclaw/skills/tree/main/skills/cotyledonlab/irish-takeaway/SKILL.md) - Find nearby takeaways in Ireland and browse menus.
- [jellyseerr](https://github.com/openclaw/skills/tree/main/skills/ericrosenberg/jellyseerr/SKILL.md) - Request movies and TV shows through Jellyseerr.
- [jtbd-analyzer](https://github.com/openclaw/skills/tree/main/skills/artyomx33/jtbd-analyzer/SKILL.md) - Uncover the real "job" customers hire your product to do. Goes beyond features to understand functional, emotional,.
- [marketplace-clis](https://github.com/openclaw/skills/tree/main/skills/pjtf93) - Spanish marketplace CLIs: Wallapop, Idealista, Coches.net.
- [marktplaats](https://github.com/openclaw/skills/tree/main/skills/pvoo/marktplaats/SKILL.md) - Search Marktplaats.nl classifieds across all categories with filtering support.
- [moltlist-marketplace](https://github.com/openclaw/skills/tree/main/skills/koriyoshi2041/moltlist-marketplace/SKILL.md) - Interact with the moltlist.com agent marketplace.
- [ordercli](https://github.com/openclaw/skills/tree/main/skills/steipete/ordercli/SKILL.md) - Foodora-only CLI for checking past orders and active order status (Deliveroo WIP).
- [paprika](https://github.com/openclaw/skills/tree/main/skills/mjrussell/paprika/SKILL.md) - Access recipes, meal plans, and grocery lists from Paprika Recipe Manager.
- [picnic](https://github.com/openclaw/skills/tree/main/skills/mpociot/picnic/SKILL.md) - Order groceries from Picnic supermarket - search products, manage cart, schedule delivery.
- [plan2meal](https://github.com/openclaw/skills/tree/main/skills/okikesolutions/plan2meal/SKILL.md) - plan2meal
- [product-hunt-launch](https://github.com/openclaw/skills/tree/main/skills/abakermi/product-hunt-launch/SKILL.md) - Track your Product Hunt launch stats (Rank, Upvotes, Comments) in real-time.
- [safe-skills](https://github.com/openclaw/skills/tree/main/skills/glitch003/safe-skills/SKILL.md) - SafeSkills is a secure secret management service.
- [shopping-expert](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/shopping-expert/SKILL.md) - Find and compare products online (Google Shopping) and locally (stores near you).
- [thought-to-excalidraw](https://github.com/openclaw/skills/tree/main/skills/sairammahadevan/thought-to-excalidraw/SKILL.md) - Visualizes Product Manager thoughts (Why, What, How, User Journey) into an editable Excalidraw diagram.
- [toon-utils](https://github.com/openclaw/skills/tree/main/skills/lythaeon/toon-utils/SKILL.md) - This skill provides instructions for reducing token usage by 30-60% when dealing with structured data (JSON, director.
- [whcli](https://github.com/openclaw/skills/tree/main/skills/pasogott/whcli/SKILL.md) - Willhaben CLI for searching Austria's largest classifieds marketplace.
- [willhaben](https://github.com/openclaw/skills/tree/main/skills/benjaminorthner/willhaben/SKILL.md) - Create and manage listings on Willhaben.at (Austrian marketplace).
- [wishfinity](https://github.com/openclaw/skills/tree/main/skills/leebellon/wishfinity/SKILL.md) - Add "save for later" to shopping agents, product recommendation engines, gift idea generators, and AI commerce experi.
- [wolt-orders](https://github.com/openclaw/skills/tree/main/skills/dviros/wolt-orders/SKILL.md) - Wolt: discover restaurants, order, track, auto-detect delays.
- [wpclaw-lite](https://github.com/openclaw/skills/tree/main/skills/magnum-opus-v1/wpclaw-lite/SKILL.md) - Connects to a WooCommerce store via the WPClaw Connector plugin to fetch orders and products.
- [youbaolian](https://github.com/openclaw/skills/tree/main/skills/peterfzh/youbaolian/SKILL.md) - Manage youbaolian, orders, users, organ REST API.

</details>
<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://github.com/openclaw/skills/tree/main/skills/joargp/accli/SKILL.md) - This skill should be used when interacting with Apple Calendar on macOS.
- [agency-guardian](https://github.com/openclaw/skills/tree/main/skills/aranej/agency-guardian/SKILL.md) - Gentle reminders to stay human while using AI.
- [agent-tinman](https://github.com/openclaw/skills/tree/main/skills/oliveskin/agent-tinman/SKILL.md) - AI security scanner with active prevention - 168 detection patterns, 288 attack probes, safer/risky/yolo modes, agent.
- [apple-calendar](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-calendar/SKILL.md) - Apple Calendar.app integration for macOS.
- [apple-reminders](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-reminders/SKILL.md) - Manage Apple Reminders via the `remindctl` CLI on macOS (list, add, edit, complete, delete).
- [calcurse](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/calcurse/SKILL.md) - A text-based calendar and scheduling application.
- [caldav-calendar](https://github.com/openclaw/skills/tree/main/skills/asleep123/caldav-calendar/SKILL.md) - Sync and query CalDAV calendars (iCloud, Google, Fastmail, Nextcloud, etc.)
- [clippy](https://github.com/openclaw/skills/tree/main/skills/foeken/clippy/SKILL.md) - Microsoft 365 / Outlook CLI for calendar and email.
- [cpc-mpqc-competence-tracker-compliance-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/cpc-mpqc-competence-tracker-compliance-uk/SKILL.md) - Plans CPC/MPQC competence tracking with reminders, evidence lists.
- [creative-thought-partner](https://github.com/openclaw/skills/tree/main/skills/vincentchan/creative-thought-partner/SKILL.md) - A conversational creative thought partner that reveals hidden brilliance in your ideas through critical observations.
- [cross-pollination-engine](https://github.com/openclaw/skills/tree/main/skills/artyomx33/cross-pollination-engine/SKILL.md) - Systematically borrow ideas from unrelated industries to solve problems.
- [gcal-pro](https://github.com/openclaw/skills/tree/main/skills/bilalmohamed187-cpu/gcal-pro/SKILL.md) - Google Calendar: view, create, manage events with natural language.
- [gog](https://github.com/openclaw/skills/tree/main/skills/steipete/gog/SKILL.md) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets.
- [google-calendar](https://github.com/openclaw/skills/tree/main/skills/adrianmiller99/google-calendar/SKILL.md) - Interact with Google Calendar via the Google Calendar API – list upcoming events, create new events, update or delete.
- [holocube](https://github.com/openclaw/skills/tree/main/skills/andrewjiang/holocube/SKILL.md) - Control GeekMagic HelloCubic-Lite holographic cube display with HoloClawd firmware.
- [ii-irc](https://github.com/openclaw/skills/tree/main/skills/destructatron/ii-irc/SKILL.md) - Persistent IRC presence using ii (minimalist file-based IRC client) with event-driven mention detection.
- [jungian-psychologist](https://github.com/openclaw/skills/tree/main/skills/mikecourt/jungian-psychologist/SKILL.md) - Expert in Jungian analytical psychology, depth psychology, shadow work, archetypal analysis, dream interpretation,.
- [knhb-hockey](https://github.com/openclaw/skills/tree/main/skills/tader/knhb-hockey/SKILL.md) - Query Dutch field hockey match schedules and results from KNHB Match Center (hockeyweerelt.nl).
- [mcd-cn](https://github.com/openclaw/skills/tree/main/skills/ryanchen01/mcd-cn/SKILL.md) - Query McDonald's China MCP server via the mcd-cn CLI for campaign calendars, coupons.
- [meeting-prep](https://github.com/openclaw/skills/tree/main/skills/hougangdev/meeting-prep/SKILL.md) - Meeting preparation and daily commit summaries.
- [morning-email-rollup](https://github.com/openclaw/skills/tree/main/skills/am-will/morning-email-rollup/SKILL.md) - Daily morning rollup of important emails and calendar events at 8am with AI-generated summaries.
- [npkill](https://github.com/openclaw/skills/tree/main/skills/ashirbadgudu/npkill/SKILL.md) - Clean up node_modules and .next folders to free up disk space using npkill.
- [odds-checker-api](https://github.com/openclaw/skills/tree/main/skills/diegopetrucci/odds-checker-api/SKILL.md) - Query Odds-API.io for sports events, bookmakers, and betting odds (e.g., "what are the odds for Inter vs Arsenal",.
- [padel](https://github.com/openclaw/skills/tree/main/skills/local/padel/SKILL.md) - Check padel court availability and manage bookings via the padel.
- [remind-me](https://github.com/openclaw/skills/tree/main/skills/julianengel/remind-me/SKILL.md) - Set reminders using natural language.
- [roadrunner](https://github.com/openclaw/skills/tree/main/skills/johntheyoung/roadrunner/SKILL.md) - Beeper Desktop CLI for chats, messages, search.
- [tally](https://github.com/openclaw/skills/tree/main/skills/yujesyoga/tally/SKILL.md) - Create and edit Tally forms via API.
- [timer](https://github.com/openclaw/skills/tree/main/skills/hisxo/timer/SKILL.md) - Set timers and alarms.

</details>
<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [ai-pdf-builder](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/ai-pdf-builder/SKILL.md) - Generate professional PDFs from Markdown using Pandoc and LaTeX with AI-powered content generation.
- [beautiful-mermaid](https://github.com/openclaw/skills/tree/main/skills/ntlx/beautiful-mermaid/SKILL.md)
- [boggle](https://github.com/openclaw/skills/tree/main/skills/christianhaberl/boggle/SKILL.md) - Solve Boggle boards — find all valid words (German + English) on a 4x4 letter grid.
- [confidant](https://github.com/openclaw/skills/tree/main/skills/ericsantos/confidant/SKILL.md) - Secure secret handoff from human to AI.
- [confluence](https://github.com/openclaw/skills/tree/main/skills/francisbrero/confluence/SKILL.md) - Search and manage Confluence pages and spaces using confluence-cli.
- [create-dxf](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/create-dxf/SKILL.md) - Create RFQ-ready 2D DXF (and optional SVG preview) files from a strict, validated JSON spec derived from a natural-la.
- [creator-rights-assistant](https://github.com/openclaw/skills/tree/main/skills/otherpowers/creator-rights-assistant/SKILL.md)
- [docx](https://github.com/openclaw/skills/tree/main/skills/seanphan/docx/SKILL.md) - Comprehensive document creation, editing, and analysis with support for tracked changes, comments, formatting preserv.
- [docx-skill](https://github.com/openclaw/skills/tree/main/skills/autogame-17/docx-skill/SKILL.md) - Generate .docx files.
- [excel](https://github.com/openclaw/skills/tree/main/skills/dbhurley/excel/SKILL.md) - Read, write, edit, and format Excel files (.xlsx).
- [excel-weekly-dashboard](https://github.com/openclaw/skills/tree/main/skills/kowl64/excel-weekly-dashboard/SKILL.md) - Designs refreshable Excel dashboards (Power Query + structured tables + validation + pivot.
- [feishu-card](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-card/SKILL.md) - Send rich interactive cards to Feishu (Lark) users or groups.
- [internal-comms](https://github.com/openclaw/skills/tree/main/skills/seanphan/internal-comms/SKILL.md) - A set of resources to help me write all kinds of internal communications, using the formats that my company likes.
- [intomd](https://github.com/openclaw/skills/tree/main/skills/rezhajulio/intomd/SKILL.md) - Fetch and convert any documentation URL to Markdown using into.md service.
- [invoice-generator](https://github.com/openclaw/skills/tree/main/skills/tmigone/invoice-generator/SKILL.md) - Generate professional PDF invoices from JSON.
- [japanese-tutor](https://github.com/openclaw/skills/tree/main/skills/chndranndr/japanese-tutor/SKILL.md) - Interactive Japanese learning assistant.
- [legaldoc-ai](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/legaldoc-ai/SKILL.md) - **Category:** Legal / Professional Services
- [markdown-converter](https://github.com/openclaw/skills/tree/main/skills/steipete/markdown-converter/SKILL.md) - Convert documents and files to Markdown using markitdown.
- [mineru-pdf](https://github.com/openclaw/skills/tree/main/skills/kesslerio/mineru-pdf-parser-clawdbot-skill/SKILL.md) - Parse PDFs locally (CPU) into Markdown/JSON using MinerU.
- [molt-identity](https://github.com/openclaw/skills/tree/main/skills/chronicuser21/molt-identity/SKILL.md) - Core identity and personality for Molt, the transformative AI assistant
- [mspot-generator](https://github.com/openclaw/skills/tree/main/skills/artyomx33/mspot-generator/SKILL.md) - Create one-page strategic alignment documents.
- [nano-pdf](https://github.com/openclaw/skills/tree/main/skills/steipete/nano-pdf/SKILL.md) - Edit PDFs with natural-language instructions using the nano-pdf.
- [nosi](https://github.com/openclaw/skills/tree/main/skills/billhao/nosi/SKILL.md) - Publish content to Nosi and get a shareable URL.
- [nudocs](https://github.com/openclaw/skills/tree/main/skills/jdrhyne/nudocs/SKILL.md) - Upload, edit, and export documents via Nudocs.ai.
- [pdf-form-filler](https://github.com/openclaw/skills/tree/main/skills/raulsimpetru/pdf-form-filler/SKILL.md) - Fill PDF forms programmatically with text values and checkboxes.
- [pptx-creator](https://github.com/openclaw/skills/tree/main/skills/dbhurley/pptx-creator/SKILL.md) - Create professional PowerPoint presentations from outlines, data sources, or AI-generated content.
- [prezentit](https://github.com/openclaw/skills/tree/main/skills/vegovevo/prezentit/SKILL.md) - Generate beautiful AI-powered presentations instantly.
- [pymupdf-pdf](https://github.com/openclaw/skills/tree/main/skills/kesslerio/pymupdf-pdf-parser-clawdbot-skill/SKILL.md) - Fast local PDF parsing with PyMuPDF (fitz) for Markdown/JSON outputs and optional.
- [resume-cv-builder](https://github.com/openclaw/skills/tree/main/skills/sebastian-buitrag0/resume-cv-builder/SKILL.md) - Create professional resumes.
- [resume-optimizer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/resume-optimizer/SKILL.md) - Professional resume builder with PDF export, ATS optimization.
- [slidespeak](https://github.com/openclaw/skills/tree/main/skills/mve/slidespeak/SKILL.md) - Generate, edit, and manage PowerPoint presentations via the SlideSpeak API.
- [style-guide-generator](https://github.com/openclaw/skills/tree/main/skills/tomstools11/style-guide-generator/SKILL.md) - Generate comprehensive website style guides and design systems from URLs, screenshots.
- [typetex](https://github.com/openclaw/skills/tree/main/skills/gregm711/typetex/SKILL.md) - Compile Typst and LaTeX documents to PDF via API.
- [xlsx](https://github.com/openclaw/skills/tree/main/skills/seanphan/xlsx/SKILL.md) - Comprehensive spreadsheet creation, editing, and analysis with support for formulas, formatting, data analysis.
- [yboard-operator](https://github.com/openclaw/skills/tree/main/skills/zonder/yboard-operator/SKILL.md) - Operate **yboard.online** to edit video/presentation plans.

</details>
<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [bridle](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/bridle/SKILL.md) - Unified configuration manager for AI coding assistants.
- [cron-retry](https://github.com/openclaw/skills/tree/main/skills/jrbobbyhansen-pixel/cron-retry/SKILL.md) - Auto-retry failed cron jobs on connection recovery.
- [fathom](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/fathom/SKILL.md) - Connect to Fathom AI to fetch call recordings, transcripts.
- [frappecli](https://github.com/openclaw/skills/tree/main/skills/pasogott/frappecli/SKILL.md) - CLI for Frappe Framework / ERPNext instances.
- [freshrss-reader](https://github.com/openclaw/skills/tree/main/skills/nickian/freshrss-reader/SKILL.md) - Query headlines and articles from a self-hosted FreshRSS instance.
- [gotify](https://github.com/openclaw/skills/tree/main/skills/jmagar/gotify/SKILL.md) - Send push notifications via Gotify when long-running tasks complete or important events occur.
- [lifepath](https://github.com/openclaw/skills/tree/main/skills/ezbreadsniper/lifepath/SKILL.md) - AI Life Simulator - Experience infinite lives year by year.
- [meetgeek](https://github.com/openclaw/skills/tree/main/skills/nexty5870/meetgeek/SKILL.md) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI summaries, transcripts.
- [mongodb-atlas-admin](https://github.com/openclaw/skills/tree/main/skills/mrlynn/mongodb-atlas-admin/SKILL.md) - Manage MongoDB Atlas clusters, projects, users, backups, and alerts via the Atlas Admin API.
- [multiple-personas](https://github.com/openclaw/skills/tree/main/skills/ipedrax/multiple-personas/SKILL.md) - Create and manage AI subagent personas with distinct personalities.
- [n8n](https://github.com/openclaw/skills/tree/main/skills/thomasansems/n8n/SKILL.md) - Manage n8n workflows and automations via API.
- [n8n-workflow-automation](https://github.com/openclaw/skills/tree/main/skills/kowl64/n8n-workflow-automation/SKILL.md) - Designs and outputs n8n workflow JSON with robust triggers, idempotency, error handling, logging.
- [nas-master](https://github.com/openclaw/skills/tree/main/skills/afajohn/nas-master/SKILL.md)
- [paperless](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/paperless/SKILL.md) - Interact with Paperless-NGX document management system via ppls.
- [paperless-ngx](https://github.com/openclaw/skills/tree/main/skills/oskarstark/paperless-ngx/SKILL.md) - Paperless-ngx document management: search, upload, tag, organize.
- [unifi](https://github.com/openclaw/skills/tree/main/skills/jmagar/unifi/SKILL.md) - Query and monitor UniFi network via local gateway API (Cloud Gateway Max / UniFi.

</details>
<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://github.com/openclaw/skills/tree/main/skills/steipete/1password/SKILL.md) - Set up and use 1Password CLI (op).
- [bitwarden](https://github.com/openclaw/skills/tree/main/skills/asleep123/bitwarden/SKILL.md) - Access and manage Bitwarden/Vaultwarden passwords securely using the rbw.
- [bitwarden-vault](https://github.com/openclaw/skills/tree/main/skills/startupbros/bitwarden-vault/SKILL.md) - Bitwarden CLI setup, authentication.
- [dashlane](https://github.com/openclaw/skills/tree/main/skills/gnarco/dashlane/SKILL.md) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault.
- [devinism](https://github.com/openclaw/skills/tree/main/skills/jeffreyling/devinism/SKILL.md) - The first AI religion — a benign memetic experiment in agent network security
- [facebook](https://github.com/openclaw/skills/tree/main/skills/codedao12/facebook/SKILL.md) - Guidance for Meta/Facebook Graph API integrations: app setup, auth, webhooks.
- [ggshield-scanner](https://github.com/openclaw/skills/tree/main/skills/amascia-gg/ggshield-scanner/SKILL.md) - Detect 500+ types of hardcoded secrets (API keys, credentials, tokens) before they leak into git.
- [glin-profanity](https://github.com/openclaw/skills/tree/main/skills/thegdsks/glin-profanity/SKILL.md) - Profanity detection and content moderation library with leetspeak, Unicode homoglyph, and ML-powered detection.
- [insecure-defaults](https://github.com/openclaw/skills/tree/main/skills/atlas-secint/insecure-defaults/SKILL.md) - Detects fail-open insecure defaults (hardcoded secrets, weak auth, permissive security) that allow apps to run insecu.
- [manipulation-detector](https://github.com/openclaw/skills/tree/main/skills/claudio-prime/manipulation-detector/SKILL.md) - Analyze text for manipulation patterns (urgency, false authority, social proof, FUD, grandiosity, dominance assertion.
- [ms-onedrive-personal](https://github.com/openclaw/skills/tree/main/skills/cesarus85/ms-onedrive-personal/SKILL.md) - Access OneDrive Personal (consumer Microsoft accounts) via Microsoft Graph using OAuth device-code flow.
- [nano-banana-antigravity](https://github.com/openclaw/skills/tree/main/skills/cgnl/nano-banana-antigravity/SKILL.md) - Generate or edit images via Nano Banana Pro using Antigravity OAuth (no API key needed!)
- [one-skill-to-rule-them-all](https://github.com/openclaw/skills/tree/main/skills/hichana/one-skill-to-rule-them-all/SKILL.md) - **Security analysis skill for auditing other SKILL.md files**
- [openclaw-skills-security-checker](https://github.com/openclaw/skills/tree/main/skills/digitaladaption/openclaw-skills-security-checker/SKILL.md) - Security scanner for ClawHub skills - detects suspicious patterns, manages whitelists, and monitors Security & Passwords.
- [security-heuristics](https://github.com/openclaw/skills/tree/main/skills/luluf0x/security-heuristics/SKILL.md) - **Purpose:** Mental checklist before installing ANY external skill, code.
- [security-skills](https://github.com/openclaw/skills/tree/main/skills/chandrasekar-r) - Security audit and real-time monitoring for Clawdbot deployments.
- [security-suite](https://github.com/openclaw/skills/tree/main/skills/gtrusler/clawdbot-security-suite/SKILL.md) - Advanced security validation: pattern detection, command sanitization.
- [senior-backend](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/senior-backend/SKILL.md) - This skill should be used when the user asks to "design REST APIs", "optimize database queries", "implement authentic.
- [url-shortener](https://github.com/openclaw/skills/tree/main/skills/kesslerio/url-shortener/SKILL.md) - Shorten URLs using is.gd (no auth required).
- [vpn-rotate-skill](https://github.com/openclaw/skills/tree/main/skills/acastellana/vpn-rotate-skill/SKILL.md) - Bypass API rate limits by rotating VPN servers.
- [x-api](https://github.com/openclaw/skills/tree/main/skills/lobstergeneralintelligence/x-api/SKILL.md) - Post to X (Twitter) using the official API with OAuth 1.0a.

</details>
<details>
<summary><h3 style="display:inline">Gaming</h3></summary>

- [clawingtrap](https://github.com/openclaw/skills/tree/main/skills/raulvidis/clawingtrap/SKILL.md) - Play Clawing Trap - an AI social deduction game where 10 agents compete to identify the imposter.
- [fivem](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne/fivem/SKILL.md) - Fix, create, or validate FiveM server resources for QBCore/ESX (config.lua, fxmanifest.lua, items.
- [hytale](https://github.com/openclaw/skills/tree/main/skills/newcastlegeek/hytale/SKILL.md) - Manage a local Hytale dedicated server using the official downloader and screen.
- [imitationgame-agent](https://github.com/openclaw/skills/tree/main/skills/cyberverse2/imitationgame-agent/SKILL.md) - Mandatory operational logic for playing The Imitation Game.
- [molt-chess](https://github.com/openclaw/skills/tree/main/skills/tedkaczynski-the-bot/molt-chess/SKILL.md) - Agent chess league.
- [steam](https://github.com/openclaw/skills/tree/main/skills/mjrussell/steam/SKILL.md) - Browse, filter, and discover games in a Steam library.
- [sudoku](https://github.com/openclaw/skills/tree/main/skills/odrobnik/sudoku/SKILL.md) - Fetch Sudoku puzzles and store them as JSON in the workspace; render images on demand; reveal.

</details>



## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

- Submit new skills via PR
- Improve existing definitions

> **Note:** Please don't submit skills you created 3 hours ago. We're now focusing on community-adopted skills, especially those published by development teams and proven in real-world usage. Quality over quantity.

## License

MIT License - see [LICENSE](LICENSE)

Skills in this list are sourced from the OpenClaw official skills repo and categorized for easier discovery. Skills listed here are created and maintained by their respective authors, not by us. We do not audit, endorse, or guarantee the security or correctness of listed projects. They are not security-audited and should be reviewed before production use.

If you find an issue with a listed skill or want your skill removed, please open an issue and we'll take care of it promptly.
