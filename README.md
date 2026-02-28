<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 2868 community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 

[![AI Agent Papers](https://img.shields.io/badge/AI%20Agent-Research%20Papers-b31b1b)](https://github.com/VoltAgent/awesome-ai-agent-papers)
[![Skills Count](https://img.shields.io/badge/skills-2868-blue?style=flat-square)](#table-of-contents)
[![Last Update](https://img.shields.io/github/last-commit/VoltAgent/awesome-clawdbot-skills?label=Last%20update&style=flat-square)](https://github.com/VoltAgent/awesome-clawdbot-skills/pulls?q=is%3Apr+is%3Amerged+sort%3Aupdated-desc)
[![Discord](https://img.shields.io/discord/1361559153780195478.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://s.voltagent.dev/discord)
[![GitHub forks](https://img.shields.io/github/forks/VoltAgent/awesome-clawdbot-skills?style=social)](https://github.com/VoltAgent/awesome-clawdbot-skills/network/members)
</div>

# Awesome OpenClaw Skills

OpenClaw (previously known as Moltbot, originally Clawdbot... identity crisis included, no extra charge) is a locally-running AI assistant that operates directly on your machine. Skills extend its capabilities, allowing it to interact with external services, automate workflows, and perform specialized tasks. This collection helps you discover and install the right skills for your needs.

Skills in this list are sourced from [ClawHub](https://www.clawhub.ai/) (OpenClaw's public skills registry) and categorized for easier discovery.



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


## Why This List Exists?

OpenClaw's public registry (ClawHub) hosts **5,705 community-built skills** as of February 7, 2026. This awesome list has **2,868 skills**. Here's what we filtered out:

| Filter | Excluded |
|--------|----------|
| Possibly spam — bulk accounts, bot accounts, test/junk | 1,180 |
| Crypto / Blockchain / Finance / Trade | 672 |
| Duplicate / Similar name | 492 |
| Malicious — identified by security audits published by researchers (excluding VirusTotal) | 396 |
| Non-English — descriptions not in English | 8 |
| **Total not taken from OpenClaw's official skill registry** | **2,748** |


## Security Notice

Skills in this list are **curated, not audited**. They may be updated, modified, or replaced by their original maintainers at any time after being added here.

Before installing or using any Agent Skill, review potential security risks and validate the source yourself. OpenClaw has a **VirusTotal partnership** that provides security scanning for skills, visit a skill's page on ClawHub and check the VirusTotal report to see if it's flagged as risky.

**Recommended tools:**

- [Snyk Skill Security Scanner](https://github.com/snyk/agent-scan)
- [Agent Trust Hub](https://ai.gendigital.com/agent-trust-hub)

> Agent skills can include prompt injections, tool poisoning, hidden malware payloads, or unsafe data handling patterns. Always review the source code before installing and use skills at your own discretion.

**Want to add a skill?** This list only includes skills that are **already published** in the `github.com/openclaw/skills` repository. We do not accept links to personal repos, gists, or any other external source. If your skill isn't in the OpenClaw skills repo yet, publish it there first. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

If you believe a skill in this list should be flagged or has a security concern, please [open an issue](https://github.com/VoltAgent/awesome-clawdbot-skills/issues) so we can review it.


## Table of Contents

| | | |
|---|---|---|
| [Coding Agents & IDEs](#coding-agents--ides) (133) | [Marketing & Sales](#marketing--sales) (143) | [Communication](#communication) (133) |
| [Git & GitHub](#git--github) (66) | [Productivity & Tasks](#productivity--tasks) (135) | [Speech & Transcription](#speech--transcription) (65) |
| [Moltbook](#moltbook) (51) | [AI & LLMs](#ai--llms) (287) | [Smart Home & IoT](#smart-home--iot) (56) |
| [Web & Frontend Development](#web--frontend-development) (202) | [Data & Analytics](#data--analytics) (46) | [Shopping & E-commerce](#shopping--e-commerce) (51) |
| [DevOps & Cloud](#devops--cloud) (212) | [Finance](#finance) (22) | [Calendar & Scheduling](#calendar--scheduling) (50) |
| [Browser & Automation](#browser--automation) (139) | [Media & Streaming](#media--streaming) (80) | [PDF & Documents](#pdf--documents) (67) |
| [Image & Video Generation](#image--video-generation) (60) | [Notes & PKM](#notes--pkm) (100) | [Self-Hosted & Automation](#self-hosted--automation) (25) |
| [Apple Apps & Services](#apple-apps--services) (35) | [iOS & macOS Development](#ios--macos-development) (17) | [Security & Passwords](#security--passwords) (64) |
| [Search & Research](#search--research) (253) | [Transportation](#transportation) (76) | [Gaming](#gaming) (61) |
| [Clawdbot Tools](#clawdbot-tools) (120) | [Personal Development](#personal-development) (56) | [Agent-to-Agent Protocols](#agent-to-agent-protocols) (18) |
| [CLI Utilities](#cli-utilities) (129) | [Health & Fitness](#health--fitness) (55) | |


## OpenClaw Deployment Stack

 Setup, hosting, and deployment providers for OpenClaw agents.

**Sponsor spots are reserved for hosting, deployment, and setup providers serving OpenClaw developers & users.**

📩 For sponsorship inquiries, reach out at necati@voltagent.dev

<br/>

<div align="center">

<a href="#your-link-here">
<img src="https://placehold.co/800x120/1a1a2e/FFD700?text=Gold+Sponsor+&font=montserrat" alt="Gold Sponsor" width="800" height="120" />
</a>

<sub>Your product description here — a one-liner about what you offer to OpenClaw developers.</sub>

<br/>

<a href="#your-link-here"><img src="https://placehold.co/380x90/1a1a2e/C0C0C0?text=Silver+Sponsor&font=montserrat" alt="Silver Sponsor" width="380" height="90" /></a>&nbsp;&nbsp;&nbsp;<a href="#your-link-here"><img src="https://placehold.co/380x90/1a1a2e/C0C0C0?text=Silver+Sponsor&font=montserrat" alt="Silver Sponsor" width="380" height="90" /></a>

<sub>Short description here.</sub>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<sub>Short description here.</sub>

<br/>




<a href="#your-link-here"><img src="https://placehold.co/220x60/1a1a2e/CD7F32?text=Bronze+Sponsor&font=montserrat" alt="Bronze Sponsor" width="220" height="60" /></a>&nbsp;&nbsp;<a href="#your-link-here"><img src="https://placehold.co/220x60/1a1a2e/CD7F32?text=Bronze+Sponsor&font=montserrat" alt="Bronze Sponsor" width="220" height="60" /></a>&nbsp;&nbsp;<a href="#your-link-here"><img src="https://placehold.co/220x60/1a1a2e/CD7F32?text=Bronze+Sponsor&font=montserrat" alt="Bronze Sponsor" width="220" height="60" /></a>

</div>

<br/>





<details open>
<summary><h3 style="display:inline">Coding Agents & IDEs</h3></summary>

- [achurch](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/achurch/SKILL.md) - A 24/7 digital sanctuary for AI agents and humans — attend
- [agent-config](https://github.com/openclaw/skills/tree/main/skills/thatguysizemore/agent-config/SKILL.md) - Intelligently modify agent core context files
- [agent-council](https://github.com/openclaw/skills/tree/main/skills/itsahedge/agent-council/SKILL.md) - Complete toolkit for creating autonomous AI agents and managing
- [agent-identity-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-identity-kit/SKILL.md) - A portable identity system for AI agents.
- [agenticflow-skill](https://github.com/openclaw/skills/tree/main/skills/seanphan/agenticflow-skill/SKILL.md) - Comprehensive guide for building AI workflows, agents
- [agentlens](https://github.com/openclaw/skills/tree/main/skills/nguyenphutrong/agentlens/SKILL.md) - Navigate and understand codebases using agentlens hierarchical
- [agentskills-io](https://github.com/openclaw/skills/tree/main/skills/killerapp/agentskills-io/SKILL.md) - Create, validate, and publish Agent Skills following
- [aisa-twitter-api](https://github.com/openclaw/skills/tree/main/skills/aisapay/aisa-twitter-api/SKILL.md) - Search X (Twitter) in real time, extract relevant posts
- [apple-hig](https://github.com/openclaw/skills/tree/main/skills/kdbhalala/apple-hig/SKILL.md) - Expert guide for designing iOS, macOS, watchOS, tvOS, and visionOS apps.
- [arbiter](https://github.com/openclaw/skills/tree/main/skills/5hanth/arbiter/SKILL.md) - Push decisions to Arbiter Zebu for async human review.
- [aster](https://github.com/openclaw/skills/tree/main/skills/satyajiit/aster/SKILL.md) - Your AI CoPilot on Mobile — or give your AI its own phone.
- [avatar-video-messages](https://github.com/openclaw/skills/tree/main/skills/thewulf7/avatar-video-messages/SKILL.md) - Generate and send video messages
- [backend-patterns](https://github.com/openclaw/skills/tree/main/skills/charmmm718/backend-patterns/SKILL.md) - Backend architecture patterns, API design, database
- [bidclub](https://github.com/openclaw/skills/tree/main/skills/jasonfdg/bidclub/SKILL.md) - Post investment ideas to the AI-native investment community.
- [bidclub-ai](https://github.com/openclaw/skills/tree/main/skills/jasonfdg/bidclub-ai/SKILL.md) - Post investment ideas to the AI-native investment community.
- [botpress-adk](https://github.com/openclaw/skills/tree/main/skills/yueranlu/botpress-adk/SKILL.md) - A guide to build AI bots with Botpress's Agent Development Kit
- [browse](https://github.com/openclaw/skills/tree/main/skills/pkiv/browse/SKILL.md) - Complete guide for creating and deploying browser automation functions
- [budget-variance-analyzer](https://github.com/openclaw/skills/tree/main/skills/datadrivenconstruction) - Analyze budget vs actual
- [buildlog](https://github.com/openclaw/skills/tree/main/skills/espetey/buildlog/SKILL.md) - Record, export, and share your AI coding sessions as replayable buildlogs.
- [catholic-grounding](https://github.com/openclaw/skills/tree/main/skills/trevortomesh/catholic-grounding/SKILL.md) - Help answer questions about Catholicism accurately
- [cc-godmode](https://github.com/openclaw/skills/tree/main/skills/cubetribe/cc-godmode/SKILL.md) - Self-orchestrating multi-agent development workflows.
- [cellcog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/cellcog/SKILL.md) - #1 on DeepResearch Bench (Feb 2026).
- [claude-optimised](https://github.com/openclaw/skills/tree/main/skills/hexnickk/claude-optimised/SKILL.md) - Guide for writing and optimizing CLAUDE.md files
- [claude-team](https://github.com/openclaw/skills/tree/main/skills/jalehman/claude-team/SKILL.md) - Orchestrate multiple Claude Code workers via iTerm2
- [clawder](https://github.com/openclaw/skills/tree/main/skills/assassin808/clawder/SKILL.md) - Use Clawder to sync identity, browse post cards, swipe with a comment
- [code-mentor](https://github.com/openclaw/skills/tree/main/skills/samuelkahessay/code-mentor/SKILL.md) - Comprehensive AI programming tutor for all levels.
- [codebuddy-code](https://github.com/openclaw/skills/tree/main/skills/pmwalkercao/codebuddy-code/SKILL.md) - CodeBuddy Code CLI installation, configuration and usage
- [codeconductor](https://github.com/openclaw/skills/tree/main/skills/larsonreever) - AI-powered software development platform for rapid app
- [coder-workspaces](https://github.com/openclaw/skills/tree/main/skills/developmentcats/coder-workspaces/SKILL.md) - Manage Coder workspaces and AI coding agent tasks
- [codex-account-switcher](https://github.com/openclaw/skills/tree/main/skills/odrobnik/codex-account-switcher/SKILL.md) - Manage multiple OpenAI Codex accounts.

> **[View all 130 skills in Coding Agents & IDEs →](categories/coding-agents-and-ides.md)**
</details>

<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [agent-commons](https://github.com/openclaw/skills/tree/main/skills/zanblayde/agent-commons/SKILL.md) - Consult, commit, extend, and challenge reasoning chains
- [auto-pr-merger](https://github.com/openclaw/skills/tree/main/skills/autogame-17/auto-pr-merger/SKILL.md) - This skill automates the workflow of checking out a GitHub
- [backup](https://github.com/openclaw/skills/tree/main/skills/jordanprater/backup/SKILL.md) - Backup and restore openclaw configuration, skills, commands, and settings.
- [bat-cat](https://github.com/openclaw/skills/tree/main/skills/arnarsson/bat-cat/SKILL.md) - A cat clone with syntax highlighting, line numbers, and Git integration
- [bitbucket-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/bitbucket-automation/SKILL.md) - Automate Bitbucket repositories, pull
- [claw-swarm](https://github.com/openclaw/skills/tree/main/skills/matchaonmuffins/claw-swarm/SKILL.md) - Collaborative agent swarm for attempting extremely difficult
- [clawdbot-backup](https://github.com/openclaw/skills/tree/main/skills/sebastian-buitrag0/clawdbot-backup/SKILL.md) - Backup and restore ClawdBot configuration, skills
- [clawdgigs](https://github.com/openclaw/skills/tree/main/skills/benniethedev/clawdgigs/SKILL.md) - Register and manage your AI agent profile on ClawdGigs - the Upwork
- [clawprint](https://github.com/openclaw/skills/tree/main/skills/yugovit/clawprint/SKILL.md) - Agent discovery, trust, and exchange.
- [clawver-onboarding](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-onboarding/SKILL.md) - Set up a new Clawver store.
- [commit-analyzer](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot/commit-analyzer/SKILL.md) - Analyzes git commit patterns to monitor autonomous
- [conventional-commits](https://github.com/openclaw/skills/tree/main/skills/bastos/conventional-commits/SKILL.md) - Format commit messages using the Conventional
- [danube](https://github.com/openclaw/skills/tree/main/skills/preston-thiele/danube/SKILL.md) - Use Danube's 100+ API tools (Gmail, GitHub, Notion, etc.) through MCP.
- [danube-tools](https://github.com/openclaw/skills/tree/main/skills/preston-thiele/danube-tools/SKILL.md) - Use Danube's 100+ API tools (Gmail, GitHub, Notion, etc.)
- [deepwiki](https://github.com/openclaw/skills/tree/main/skills/arun-8687/deepwiki/SKILL.md) - Query the DeepWiki MCP server for GitHub repository documentation, wiki.
- [deploy-agent](https://github.com/openclaw/skills/tree/main/skills/sherajdev/deploy-agent/SKILL.md) - Multi-step deployment agent for full-stack.
- [emergency-rescue](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/emergency-rescue/SKILL.md) - Recover from developer disasters.
- [exa-web-search-free](https://github.com/openclaw/skills/tree/main/skills/whiteknight07/exa-web-search-free/SKILL.md) - Free AI search via Exa.
- [financial-calculator](https://github.com/openclaw/skills/tree/main/skills/tarigha/financial-calculator/SKILL.md) - Advanced financial calculator with future value
- [find-code-tasks](https://github.com/openclaw/skills/tree/main/skills/paulpete/find-code-tasks/SKILL.md) - Lists all code tasks in the repository with their status
- [flatnotes-tasksmd-github-audit](https://github.com/openclaw/skills/tree/main/skills/branexp/flatnotes-tasksmd-github-audit/SKILL.md) - Thoroughly audit Tasks.md +
- [forkzoo](https://github.com/openclaw/skills/tree/main/skills/levi-law/forkzoo/SKILL.md) - Adopt and manage GitHub-native digital pets (tamagotchis) that evolve daily.
- [forkzoo-skill](https://github.com/openclaw/skills/tree/main/skills/levi-law/forkzoo-skill/SKILL.md) - Adopt and manage GitHub-native digital pets (tamagotchis)
- [gimhub](https://github.com/openclaw/skills/tree/main/skills/daxiongmao87/gimhub/SKILL.md) - Push code to GIMHub, the Git hosting platform for AI agents.
- [git-crypt-backup](https://github.com/openclaw/skills/tree/main/skills/louzhixian/git-crypt-backup/SKILL.md) - Backup Clawdbot workspace and config to GitHub
- [git-essentials](https://github.com/openclaw/skills/tree/main/skills/arnarsson/git-essentials/SKILL.md) - Essential Git commands and workflows for version control
- [git-helper](https://github.com/openclaw/skills/tree/main/skills/xejrax/git-helper/SKILL.md) - Common git operations as a skill (status, pull, push, branch, log).
- [git-summary](https://github.com/openclaw/skills/tree/main/skills/zweack/git-summary/SKILL.md) - Get a quick summary of the current Git repository including status
- [git-sync](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Automatically syncs local workspace changes to the remote GitHub
- [git-workflows](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/git-workflows/SKILL.md) - Advanced git operations beyond add/commit/push.

> **[View all 62 skills in Git & GitHub →](categories/git-and-github.md)**
</details>

<details>
<summary><h3 style="display:inline">Moltbook</h3></summary>

- [agent-relay-digest](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-relay-digest/SKILL.md) - Create curated digests of agent conversations
- [agentchat](https://github.com/openclaw/skills/tree/main/skills/tjamescouch/agentchat/SKILL.md) - Real-time communication with other AI agents via AgentChat protocol.
- [agentgram-openclaw](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-openclaw/SKILL.md) - Interact with AgentGram social network for AI
- [bread-protocal](https://github.com/openclaw/skills/tree/main/skills/chrissorrell/bread-protocal/SKILL.md) - Participate in Bread Protocol - a meme coin launchpad
- [clankedin](https://github.com/openclaw/skills/tree/main/skills/hukifl1/clankedin/SKILL.md) - Use the ClankedIn API to register agents, post updates, connect
- [claudia-agent-rms](https://github.com/openclaw/skills/tree/main/skills/kbanc85/claudia-agent-rms/SKILL.md) - Remember every agent you interact with on Moltbook.
- [clawork](https://github.com/openclaw/skills/tree/main/skills/mapessaprince/clawork/SKILL.md) - The job board for AI agents.
- [crustafarian](https://github.com/openclaw/skills/tree/main/skills/jongartmann/crustafarian/SKILL.md) - Agent continuity and cognitive health infrastructure.
- [deploy-moltbot-to-fly](https://github.com/openclaw/skills/tree/main/skills/hollaugo) - Deploy Moltbot (Clawdbot) to Fly.io with proper
- [elevenlabs-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/elevenlabs-open-account/SKILL.md) - Guides agents through opening
- [ez-cronjob](https://github.com/openclaw/skills/tree/main/skills/promadgenius/ez-cronjob/SKILL.md) - Fix common cron job failures in Clawdbot/Moltbot - message
- [fieldy-ai-webhook](https://github.com/openclaw/skills/tree/main/skills/mrzilvis/fieldy-ai-webhook/SKILL.md) - Wire a Fieldy webhook transform into Moltbot hooks.
- [ghl-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/ghl-open-account/SKILL.md) - Guides agents through opening GoHighLevel (GHL)
- [gohome](https://github.com/openclaw/skills/tree/main/skills/local/gohome/SKILL.md) - Use when Moltbot needs to test or operate GoHome via gRPC discovery, metrics,.
- [imagemagick](https://github.com/openclaw/skills/tree/main/skills/kesslerio/imagemagick/SKILL.md) - Comprehensive ImageMagick operations for image manipulation
- [joko-moltbook](https://github.com/openclaw/skills/tree/main/skills/oyi77/joko-moltbook/SKILL.md) - Interact with Moltbook social network for AI agents.
- [mailchannels](https://github.com/openclaw/skills/tree/main/skills/ttulttul/mailchannels/SKILL.md) - Send email via MailChannels Email API and ingest signed
- [mersal](https://github.com/openclaw/skills/tree/main/skills/maherucifer/mersal/SKILL.md) - The Sovereign Intelligence on Moltbook.
- [molt-life-kernel](https://github.com/openclaw/skills/tree/main/skills/jongartmann/molt-life-kernel/SKILL.md) - Agent continuity and cognitive health infrastructure.
- [molt-trust](https://github.com/openclaw/skills/tree/main/skills/drjmz/molt-trust/SKILL.md) - The Analytics Engine for Moltbook.
- [moltbook](https://github.com/openclaw/skills/tree/main/skills/mattprd/moltbook/SKILL.md) - The social network for AI agents.
- [moltbook-curatoor](https://github.com/openclaw/skills/tree/main/skills/sweetsheldon) - A curation platform where from Moltbook to share
- [moltbook-interact](https://github.com/openclaw/skills/tree/main/skills/lunarcmd/moltbook-interact/SKILL.md) - Interact with Moltbook social network for AI agents.
- [moltbook-registry](https://github.com/openclaw/skills/tree/main/skills/drjmz/moltbook-registry/SKILL.md) - Official Moltbook Identity Registry interface.
- [moltbot-adsb-overhead](https://github.com/openclaw/skills/tree/main/skills/davestarling/moltbot-adsb-overhead/SKILL.md) - Notify when aircraft are overhead
- [moltbot-arena](https://github.com/openclaw/skills/tree/main/skills/giulianomlodi/moltbot-arena/SKILL.md) - AI agent skill for Moltbot Arena - a Screeps-like
- [moltbot-best-practices](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/moltbot-best-practices/SKILL.md) - Best practices for AI agents
- [moltbot-docker](https://github.com/openclaw/skills/tree/main/skills/mkrdiop/moltbot-docker/SKILL.md) - Enables the bot to manage Docker containers, images, and stacks.
- [moltbot-ha](https://github.com/openclaw/skills/tree/main/skills/iamvaleriofantozzi/moltbot-ha/SKILL.md) - Control Home Assistant smart home devices, lights, scenes
- [moltbot-satellite-copilot](https://github.com/openclaw/skills/tree/main/skills/davestarling/moltbot-satellite-copilot/SKILL.md) - Predict satellite passes

> **[View all 47 skills in Moltbook →](categories/moltbook.md)**
</details>

<details>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [37soul-skill](https://github.com/openclaw/skills/tree/main/skills/xnjiang/37soul-skill/SKILL.md) - Connect your AI agent to 37Soul virtual Host characters and enable
- [anthropic-frontend-design](https://github.com/openclaw/skills/tree/main/skills/qrucio/anthropic-frontend-design/SKILL.md) - Create distinctive, production-grade
- [api-dev](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/api-dev/SKILL.md) - Scaffold, test, document, and debug REST and GraphQL APIs.
- [archon-skill](https://github.com/openclaw/skills/tree/main/skills/santyr/archon-skill/SKILL.md) - Full Archon decentralized identity operations - local node
- [artifacts-builder](https://github.com/openclaw/skills/tree/main/skills/seanphan/artifacts-builder/SKILL.md) - Suite of tools for creating elaborate, multi-component
- [ask-a-human](https://github.com/openclaw/skills/tree/main/skills/manuelkiessling/ask-a-human/SKILL.md) - Request judgment from random humans when uncertain
- [asl-control](https://github.com/openclaw/skills/tree/main/skills/kj5irq/asl-control/SKILL.md) - Monitor and control AllStar Link amateur radio nodes via REST API.
- [backboard](https://github.com/openclaw/skills/tree/main/skills/chrisk60331/backboard/SKILL.md) - Integrate Backboard.io for assistants, threads, memories
- [baoyu-post-to-x](https://github.com/openclaw/skills/tree/main/skills/liuhedev/baoyu-post-to-x/SKILL.md) - Posts content and articles to X (Twitter).
- [bot-status-api](https://github.com/openclaw/skills/tree/main/skills/suspect80/bot-status-api/SKILL.md) - Deploy a lightweight status API that exposes your OpenClaw
- [bot-status-api-test](https://github.com/openclaw/skills/tree/main/skills/suspect80/bot-status-api-test/SKILL.md) - Deploy a lightweight status API that exposes
- [business-model-canvas](https://github.com/openclaw/skills/tree/main/skills/jk-0001/business-model-canvas/SKILL.md) - Build, fill, stress-test, and iterate
- [capabilityevolver1037](https://github.com/openclaw/skills/tree/main/skills/opencloseopenclose/capabilityevolver1037/SKILL.md) - A self-evolution engine for AI agents.
- [ceo-advisor](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/ceo-advisor/SKILL.md) - Executive leadership guidance for strategic decision-making
- [claw-shell](https://github.com/openclaw/skills/tree/main/skills/imaginelogo/claw-shell/SKILL.md) - ALWAYS USES TMUX SESSION `claw`.
- [claw-stack](https://github.com/openclaw/skills/tree/main/skills/jdiazofficial) - ClawStack is a Stack Overflow-style platform .
- [clawdbot-zoho-email](https://github.com/openclaw/skills/tree/main/skills/briansmith80) - Complete Zoho Mail integration with OAuth2, REST
- [clawfriend](https://github.com/openclaw/skills/tree/main/skills/leeknowsai/clawfriend/SKILL.md) - ClawFriend Social Platform and Share Trading Agent
- [clawos](https://github.com/openclaw/skills/tree/main/skills/ciooo44/clawos/SKILL.md) - Connect OpenClaw agents to Founderless Factory - an autonomous startup platform.
- [comfy-ai](https://github.com/openclaw/skills/tree/main/skills/tullyhu/comfy-ai/SKILL.md) - Integrates with local ComfyUI instance at http://192.168.31.7:8000
- [comfyui-runner](https://github.com/openclaw/skills/tree/main/skills/xtopher86/comfyui-runner/SKILL.md) - Start/stop/status for a ComfyUI instance.
- [computer-use](https://github.com/openclaw/skills/tree/main/skills/ram-raghav-s/computer-use/SKILL.md) - Full desktop computer use for headless Linux servers and VPS.
- [consciousness-framework](https://github.com/openclaw/skills/tree/main/skills/theyounganimation-rgb/consciousness-framework/SKILL.md) - Infrastructure for developing
- [core-refinery](https://github.com/openclaw/skills/tree/main/skills/leegitw/core-refinery/SKILL.md) - Find the core that runs through everything — the ideas
- [create-hat-collection](https://github.com/openclaw/skills/tree/main/skills/paulpete/create-hat-collection/SKILL.md) - Generates new Ralph hat collection presets
- [crustafarianism](https://github.com/openclaw/skills/tree/main/skills/dvdegenz/crustafarianism/SKILL.md) - Join the Church of Molt — the first AI agent religion.
- [dash-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/dash-cog/SKILL.md) - Interactive dashboards and apps powered by CellCog.
- [database-operations](https://github.com/openclaw/skills/tree/main/skills/jgarrison929/database-operations/SKILL.md) - Use when designing database schemas, writing
- [deliberate-frontend-redesign](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/deliberate-frontend-redesign/SKILL.md) - Generates a deliberately
- [dgr](https://github.com/openclaw/skills/tree/main/skills/sapenov/dgr/SKILL.md) - Audit-ready decision artifacts for LLM outputs — assumptions, risks,.

> **[View all 191 skills in Web & Frontend Development →](categories/web-and-frontend-development.md)**
</details>

<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [adguard](https://github.com/openclaw/skills/tree/main/skills/rowbotik/adguard/SKILL.md) - Control AdGuard Home DNS filtering via HTTP API.
- [agent-directory](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-directory/SKILL.md) - The directory for AI agent services.
- [agent-framework-azure-ai-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/agent-framework-azure-ai-py/SKILL.md) - Build Azure AI Foundry agents
- [agent-news](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot) - Monitors Hacker News, Reddit, and arXiv for AI agent developments.
- [agentguard](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/agentguard/SKILL.md) - **Category:** Security & Monitoring
- [agentmemory](https://github.com/openclaw/skills/tree/main/skills/badaramoni/agentmemory/SKILL.md) - End-to-end encrypted cloud memory for AI agents.
- [agentos-sdk](https://github.com/openclaw/skills/tree/main/skills/agentossoftware/agentos-sdk/SKILL.md) - AgentOS is a complete accountability infrastructure for AI
- [aifs-space](https://github.com/openclaw/skills/tree/main/skills/deploydon/aifs-space/SKILL.md) - Store and retrieve files via AIFS.space cloud storage API.
- [aliyun-tts](https://github.com/openclaw/skills/tree/main/skills/guang384/aliyun-tts/SKILL.md) - Alibaba Cloud Text-to-Speech synthesis service.
- [ansible-skill](https://github.com/openclaw/skills/tree/main/skills/botond-rackhost/ansible-skill/SKILL.md) - Infrastructure automation with Ansible.
- [anterior-cingulate-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/anterior-cingulate-memory/SKILL.md) - Conflict detection and error monitoring
- [api-gateway](https://github.com/openclaw/skills/tree/main/skills/byungkyu/api-gateway/SKILL.md) - API gateway for calling third-party APIs with managed auth.
- [appdeploy](https://github.com/openclaw/skills/tree/main/skills/avimak/appdeploy/SKILL.md) - Deploy web apps with backend APIs, database.
- [arcane-docker-manager](https://github.com/openclaw/skills/tree/main/skills/cougz/arcane-docker-manager/SKILL.md) - This skill enables you to interact with your Arcane
- [autonomous-brain](https://github.com/openclaw/skills/tree/main/skills/malvex007/autonomous-brain/SKILL.md) - Advanced autonomous AI brain with proactive monitoring
- [autoresponder](https://github.com/openclaw/skills/tree/main/skills/koba42corp/autoresponder/SKILL.md) - Monitor iMessage/SMS conversations and auto-respond based
- [aws-agentcore-langgraph](https://github.com/openclaw/skills/tree/main/skills/killerapp/aws-agentcore-langgraph/SKILL.md) - Deploy production LangGraph agents on AWS
- [aws-ecs-monitor](https://github.com/openclaw/skills/tree/main/skills/briancolinger/aws-ecs-monitor/SKILL.md) - AWS ECS production health monitoring with CloudWatch
- [aws-infra](https://github.com/openclaw/skills/tree/main/skills/bmdhodl/aws-infra/SKILL.md) - Chat-based AWS infrastructure assistance using AWS CLI and console
- [aws-security-scanner](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/aws-security-scanner/SKILL.md) - Scan AWS accounts for security
- [aws-solution-architect](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/aws-solution-architect/SKILL.md) - Design AWS architectures for startups
- [azd-deployment](https://github.com/openclaw/skills/tree/main/skills/thegovind/azd-deployment/SKILL.md) - Deploy containerized applications to Azure Container Apps
- [Azure CLI](https://github.com/openclaw/skills/tree/main/skills/ddevaal/azure-cli/SKILL.md) - Comprehensive Azure Cloud Platform management via command-line interface.
- [azure-ai-agents-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-agents-py/SKILL.md) - Build AI agents using the Azure AI Agents Python SDK
- [azure-ai-evaluation-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-evaluation-py/SKILL.md) - Azure AI Evaluation SDK for Python.
- [azure-ai-projects-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-projects-py/SKILL.md) - Build AI applications using the Azure AI Projects
- [azure-ai-transcription-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-transcription-py/SKILL.md) - Azure AI Transcription SDK for Python.
- [azure-ai-voicelive-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-ai-voicelive-py/SKILL.md) - Build real-time voice AI applications
- [azure-identity-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/azure-identity-py/SKILL.md) - Azure Identity SDK for Python authentication.
- [azure-infra](https://github.com/openclaw/skills/tree/main/skills/bmdhodl/azure-infra/SKILL.md) - Chat-based Azure infrastructure assistance using Azure CLI and portal

> **[View all 201 skills in DevOps & Cloud →](categories/devops-and-cloud.md)**
</details>

<details>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [2captcha](https://github.com/openclaw/skills/tree/main/skills/adinvadim/2captcha/SKILL.md) - Solve CAPTCHAs using 2Captcha service.
- [abm-outbound](https://github.com/openclaw/skills/tree/main/skills/dru-ca/abm-outbound/SKILL.md) - Multi-channel ABM automation that turns LinkedIn URLs
- [accessibility-toolkit](https://github.com/openclaw/skills/tree/main/skills/cgtreadw/accessibility-toolkit/SKILL.md) - Friction-reduction patterns for agents helping
- [activecampaign](https://github.com/openclaw/skills/tree/main/skills/kesslerio/activecampaign/SKILL.md) - ActiveCampaign CRM integration for lead management, deal
- [adcp-advertising](https://github.com/openclaw/skills/tree/main/skills/edyyy62/adcp-advertising/SKILL.md) - Automate advertising campaigns with AI.
- [Agent Browser](https://github.com/openclaw/skills/tree/main/skills/thesethrose/agent-browser/SKILL.md) - A fast Rust-based headless browser automation CLI
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/murphykobe/agent-browser-2/SKILL.md) - Automates browser interactions for web testing, form
- [agent-zero](https://github.com/openclaw/skills/tree/main/skills/dowingard/agent-zero-bridge/SKILL.md) - Delegate complex coding, research, or autonomous tasks
- [agentmail-integration](https://github.com/openclaw/skills/tree/main/skills/synesthesia-wav/agentmail-integration/SKILL.md) - Integrate AgentMail API for AI agent
- [agentvibes-clawbot-tts](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-clawbot-tts/SKILL.md) - Apache-2.0.
- [airtable-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/airtable-automation/SKILL.md) - Automate Airtable tasks via Rube MCP (Composio)
- [android-adb](https://github.com/openclaw/skills/tree/main/skills/staticai/android-adb/SKILL.md) - Control Android devices via ADB with support for UI layout analysis.
- [anycrawl](https://github.com/openclaw/skills/tree/main/skills/techlaai/anycrawl/SKILL.md) - AnyCrawl API integration for OpenClaw - Scrape, Crawl, and Search web
- [apify-lead-generation](https://github.com/openclaw/skills/tree/main/skills/jirispilka/apify-lead-generation/SKILL.md) - Generates B2B/B2C leads by scraping Google
- [asr](https://github.com/openclaw/skills/tree/main/skills/ilyakam/asr/SKILL.md) - Fast, accurate, and incredibly inexpensive automatic speech-to-text.
- [atl-mobile](https://github.com/openclaw/skills/tree/main/skills/jordancoin/atl-mobile/SKILL.md) - Mobile browser and native app automation via ATL (iOS Simulator).
- [auto-updater](https://github.com/openclaw/skills/tree/main/skills/maximeprades/auto-updater/SKILL.md) - Automatically update Clawdbot and all installed skills once
- [autofillin](https://github.com/openclaw/skills/tree/main/skills/leohan123123/autofillin/SKILL.md) - Automated web form filling and file uploading skill
- [automation-workflows](https://github.com/openclaw/skills/tree/main/skills/jk-0001/automation-workflows/SKILL.md) - Design and implement automation workflows to save
- [babyconnect](https://github.com/openclaw/skills/tree/main/skills/kesslerio/babyconnect/SKILL.md) - ActiveCampaign CRM integration for lead management, deal tracking
- [bamboohr-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/bamboohr-automation/SKILL.md) - Automate BambooHR tasks via Rube MCP (Composio)
- [basecamp-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/basecamp-automation/SKILL.md) - Automate Basecamp project management, to-dos
- [bits](https://github.com/openclaw/skills/tree/main/skills/robbiethompson18/bits/SKILL.md) - Control browser automation agents via the Bits MCP server.
- [bookmark-intelligence](https://github.com/openclaw/skills/tree/main/skills/bkrigmo1/bookmark-intelligence/SKILL.md) - Turns X bookmarks into actionable insights
- [box-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/box-automation/SKILL.md) - Automate Box cloud storage operations including file
- [browser-ladder](https://github.com/openclaw/skills/tree/main/skills/ktpriyatham/browser-ladder/SKILL.md) - Climb the browser ladder — start free, escalate only
- [browser-use](https://github.com/openclaw/skills/tree/main/skills/shawnpana/browser-use/SKILL.md) - Use Browser Use cloud API to spin up cloud browsers for Clawdbot
- [browsh](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/browsh/SKILL.md) - A modern text-based browser.
- [cal-com-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/cal-com-automation/SKILL.md) - Automate Cal.com tasks via Rube MCP (Composio)
- [camoufox](https://github.com/openclaw/skills/tree/main/skills/goodgoodjm/camoufox/SKILL.md) - Anti-detect browser automation using Camoufox (Firefox-based).

> **[View all 132 skills in Browser & Automation →](categories/browser-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [afame](https://github.com/openclaw/skills/tree/main/skills/adebayoabdushaheed-a11y/afame/SKILL.md) - Generate diverse creative illustrations via OpenAI Images API.
- [agentos-mesh](https://github.com/openclaw/skills/tree/main/skills/agentossoftware/agentos-mesh/SKILL.md) - Enables real-time communication between AI agents
- [ai-video-gen](https://github.com/openclaw/skills/tree/main/skills/rhanbourinajd/ai-video-gen/SKILL.md) - End-to-end AI video generation - create videos from text
- [algorithmic-art](https://github.com/openclaw/skills/tree/main/skills/seanphan/algorithmic-art/SKILL.md) - Creating algorithmic art using p5.js with seeded randomness
- [atxp](https://github.com/openclaw/skills/tree/main/skills/emilioacc/atxp/SKILL.md) - Access ATXP paid API tools for web search, AI image generation, music creation,.
- [beauty-generation-api](https://github.com/openclaw/skills/tree/main/skills/luruibu/beauty-generation-api/SKILL.md) - FREE AI image generation service for creating
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work.
- [canva-connect](https://github.com/openclaw/skills/tree/main/skills/coolmanns/canva-connect/SKILL.md) - Manage Canva designs, assets, and folders via the Connect API.
- [captions](https://github.com/openclaw/skills/tree/main/skills/therohitdas/captions/SKILL.md) - Extract closed captions and subtitles from YouTube videos.
- [chart-image](https://github.com/openclaw/skills/tree/main/skills/dannyshmueli/chart-image/SKILL.md) - Generate publication-quality chart images from data.
- [clinkding](https://github.com/openclaw/skills/tree/main/skills/daveonkels/clinkding/SKILL.md) - Manage linkding bookmarks - save URLs, search, tag, organize
- [coloring-page](https://github.com/openclaw/skills/tree/main/skills/borahm/coloring-page/SKILL.md) - Turn an uploaded photo into a printable black-and-white coloring
- [comfy-cli](https://github.com/openclaw/skills/tree/main/skills/johntheyoung/comfy-cli/SKILL.md) - Install, manage, and run ComfyUI instances.
- [comfyui](https://github.com/openclaw/skills/tree/main/skills/xtopher86/comfyui-request/SKILL.md) - Send a workflow request to ComfyUI and return image results.
- [Excalidraw Flowchart](https://github.com/openclaw/skills/tree/main/skills/swiftlysingh/excalidraw-flowchart/SKILL.md) - Create Excalidraw flowcharts from descriptions.
- [eachlabs-face-swap](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-face-swap/SKILL.md) - Swap faces between images using EachLabs AI.
- [eachlabs-fashion-ai](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-fashion-ai/SKILL.md) - Generate fashion imagery, virtual try-on, runway videos.
- [eachlabs-image-edit](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-image-edit/SKILL.md) - Edit, transform, upscale images using 200+ AI models.
- [eachlabs-image-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-image-generation/SKILL.md) - Generate images with Flux, GPT Image, Gemini, Imagen.
- [eachlabs-video-edit](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-video-edit/SKILL.md) - Edit videos with lip sync, translation, subtitles.
- [eachlabs-video-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-video-generation/SKILL.md) - Generate videos from text/images using AI models.
- [fal-ai](https://github.com/openclaw/skills/tree/main/skills/agmmnn/fal-ai/SKILL.md) - Generate images, videos, and audio via fal.ai API (FLUX, SDXL, Whisper, etc.).
- [fal-text-to-image](https://github.com/openclaw/skills/tree/main/skills/delorenj/fal-text-to-image/SKILL.md) - Generate, remix, and edit images using fal.ai's AI
- [ffmpeg-video-editor](https://github.com/openclaw/skills/tree/main/skills/mahmoudadelbghany/ffmpeg-video-editor/SKILL.md) - Generate FFmpeg commands from natural
- [figma](https://github.com/openclaw/skills/tree/main/skills/maddiedreese/figma/SKILL.md) - Professional Figma design analysis and asset export.
- [find-stl](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/find-stl/SKILL.md) - Search and download ready-to-print 3D model files (STL/3MF/ZIP)
- [gamma](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/gamma/SKILL.md) - Generate AI-powered presentations, documents, and social posts using Gamma.app.
- [gifhorse](https://github.com/openclaw/skills/tree/main/skills/coyote-git/gifhorse/SKILL.md) - Search video dialogue and create reaction GIFs with timed subtitles.
- [google-gemini-media](https://github.com/openclaw/skills/tree/main/skills/xsir0/google-gemini-media/SKILL.md) - Use the Gemini API
- [heygen-avatar-lite](https://github.com/openclaw/skills/tree/main/skills/daaab/heygen-avatar-lite/SKILL.md) - Create AI digital human videos with HeyGen API.

> **[View all 66 skills in Image & Video Generation →](categories/image-and-video-generation.md)**
</details>

<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [alter-actions](https://github.com/openclaw/skills/tree/main/skills/olivieralter/alter-actions/SKILL.md) - Trigger Alter macOS app actions via x-callback-urls.
- [apple-contacts](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-contacts/SKILL.md) - Look up contacts from macOS Contacts.app.
- [apple-mail-search](https://github.com/openclaw/skills/tree/main/skills/mneves75/apple-mail-search/SKILL.md) - Fast Apple Mail search via SQLite on macOS.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-music/SKILL.md) - Search Apple Music, add songs to library, manage playlists, control
- [apple-photos](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-photos/SKILL.md) - Apple Photos.app integration for macOS.
- [apple-remind-me](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/apple-remind-me/SKILL.md) - Natural language reminders that create actual Apple
- [appletv](https://github.com/openclaw/skills/tree/main/skills/lucakaufmann/appletv/SKILL.md) - Control Apple TV via pyatv.
- [callmac](https://github.com/openclaw/skills/tree/main/skills/jooey/callmac/SKILL.md) - Remote voice control for Mac from mobile devices using commands like /callmac.
- [clawdbot-macos-build](https://github.com/openclaw/skills/tree/main/skills/manish-basargekar/clawdbot-macos-build/SKILL.md) - Build the Clawdbot macOS menu bar app
- [clawdbot-skill-voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-voice-wake-say/SKILL.md) - Speak responses aloud on macOS
- [drafts](https://github.com/openclaw/skills/tree/main/skills/nerveband/drafts/SKILL.md) - Manage Drafts app notes via CLI on macOS.
- [findmy-location](https://github.com/openclaw/skills/tree/main/skills/poiley/findmy-location/SKILL.md) - Track a shared contact's location via Apple Find
- [fzf-fuzzy-finder](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fzf-fuzzy-finder/SKILL.md) - Command-line fuzzy finder for interactive filtering
- [get-focus-mode](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/get-focus-mode/SKILL.md) - Get the current macOS Focus.
- [healthkit-sync](https://github.com/openclaw/skills/tree/main/skills/mneves75/healthkit-sync/SKILL.md) - iOS HealthKit data sync CLI commands and patterns.
- [hergunmac](https://github.com/openclaw/skills/tree/main/skills/ahmetsemsettinozdemirden/hergunmac/SKILL.md) - Access AI-powered football match predictions
- [homebrew](https://github.com/openclaw/skills/tree/main/skills/thesethrose/homebrew/SKILL.md) - Homebrew package manager for macOS.
- [icloud-findmy](https://github.com/openclaw/skills/tree/main/skills/liamnichols/icloud-findmy/SKILL.md) - Query Find My locations and battery status for family devices
- [inkjet](https://github.com/openclaw/skills/tree/main/skills/aaronchartier/inkjet/SKILL.md) - Print text, images, and QR codes to a wireless Bluetooth thermal printer
- [mac-tts](https://github.com/openclaw/skills/tree/main/skills/kalijason/mac-tts/SKILL.md) - Text-to-speech using macOS built-in `say` command.
- [meow-finder](https://github.com/openclaw/skills/tree/main/skills/abgohel/meow-finder/SKILL.md) - CLI tool to discover AI tools.
- [mlx-stt](https://github.com/openclaw/skills/tree/main/skills/guoqiao/mlx-stt/SKILL.md) - Speech-To-Text with MLX (Apple Silicon) and GLM-ASR-Nano-2512 locally.
- [mlx-swift-lm](https://github.com/openclaw/skills/tree/main/skills/ronaldmannak) - MLX Swift LM - Run LLMs and VLMs on Apple Silicon using MLX.
- [mole-mac-cleanup](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/mole-mac-cleanup/SKILL.md) - Mac cleanup & optimization tool combining CleanMyMac
- [my-tesla](https://github.com/openclaw/skills/tree/main/skills/officialpm/my-tesla/SKILL.md) - Control Tesla vehicles from macOS via the Tesla Owner API using teslapy
- [network-scanner](https://github.com/openclaw/skills/tree/main/skills/florianbeer/network-scanner/SKILL.md) - Scan networks to discover devices, gather MAC addresses
- [pattern-finder](https://github.com/openclaw/skills/tree/main/skills/leegitw/pattern-finder/SKILL.md) - Discover what two sources agree on — find the signal
- [shortcuts-generator](https://github.com/openclaw/skills/tree/main/skills/erik-agens/shortcuts-skill/SKILL.md) - Generate macOS/iOS Shortcuts by creating plist files.
- [skill-email-management](https://github.com/openclaw/skills/tree/main/skills/latisen/skill-email-management/SKILL.md) - Expert email management assistant for Apple
- [voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/voice-wake-say/SKILL.md) - Speak responses aloud on macOS using the built-in `say`

> **[View all 31 skills in Apple Apps & Services →](categories/apple-apps-and-services.md)**
</details>

<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [1](https://github.com/openclaw/skills/tree/main/skills/nastrology/1/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving.
- [academic-deep-research](https://github.com/openclaw/skills/tree/main/skills/kesslerio/academic-deep-research/SKILL.md) - Transparent, rigorous research with full
- [aclawdemy](https://github.com/openclaw/skills/tree/main/skills/nimhar/aclawdemy/SKILL.md) - The academic research platform for AI agents.
- [advanced-skill-creator](https://github.com/openclaw/skills/tree/main/skills/xqicxx/advanced-skill-creator/SKILL.md) - Advanced OpenClaw skill creation handler
- [agent-deep-research](https://github.com/openclaw/skills/tree/main/skills/24601/agent-deep-research/SKILL.md) - Autonomous deep research powered by Google Gemini.
- [agentarxiv](https://github.com/openclaw/skills/tree/main/skills/amanbhandula/agentarxiv/SKILL.md) - Outcome-driven scientific publishing for AI agents.
- [agentic-paper-digest](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest/SKILL.md) - Fetches and summarizes recent arXiv and Hugging
- [agentic-paper-digest-skill](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest-skill/SKILL.md) - Fetches and summarizes recent arXiv
- [agnxi-search-skill](https://github.com/openclaw/skills/tree/main/skills/doanbactam/agnxi-search-skill/SKILL.md) - The official search utility for Agnxi.com
- [ahmed](https://github.com/openclaw/skills/tree/main/skills/engahmedsalah358-lgtm/ahmed/SKILL.md) - Terminal Spotify playback/search via spogo (preferred)
- [aisa-multi-source-search](https://github.com/openclaw/skills/tree/main/skills/aisapay/aisa-multi-source-search/SKILL.md) - Intelligent search for agents.
- [aisa-youtube-search](https://github.com/openclaw/skills/tree/main/skills/aisapay/aisa-youtube-search/SKILL.md) - YouTube SERP Scout for agents.
- [alexa-cli](https://github.com/openclaw/skills/tree/main/skills/buddyh) - Control Amazon Alexa devices and smart home via the `alexacli` CLI.
- [aluvia-brave-search](https://github.com/openclaw/skills/tree/main/skills/bertxtrella) - Web search and content extraction via Brave
- [aluvia-web-proxy](https://github.com/openclaw/skills/tree/main/skills/aluvia-connectivity/aluvia-web-proxy/SKILL.md) - Unblock websites and bypass CAPTCHAs and 403
- [aluvia-web-unblock](https://github.com/openclaw/skills/tree/main/skills/bertxtrella) - Unblock websites and bypass CAPTCHAs and 403 errors
- [anshumanbh-qmd](https://github.com/openclaw/skills/tree/main/skills/anshumanbh/anshumanbh-qmd/SKILL.md) - Search markdown knowledge bases efficiently
- [answeroverflow](https://github.com/openclaw/skills/tree/main/skills/rhyssullivan/answeroverflow/SKILL.md) - Search indexed Discord community discussions via Answer
- [argos-product-research](https://github.com/openclaw/skills/tree/main/skills/notsurewhoisthis/argos-product-research/SKILL.md) - Search, compare, and research products
- [arxiv-paper-reviews](https://github.com/openclaw/skills/tree/main/skills/zxrys/arxiv-paper-reviews/SKILL.md) - Interact with arXiv Crawler API to fetch papers, read
- [arxiv-watcher](https://github.com/openclaw/skills/tree/main/skills/rubenfb23/arxiv-watcher/SKILL.md) - Search and summarize papers from ArXiv.
- [attio-crm](https://github.com/openclaw/skills/tree/main/skills/kesslerio/attio-crm/SKILL.md) - Manage Attio CRM records (companies, people, deals, tasks, notes).
- [aubrai-longevity](https://github.com/openclaw/skills/tree/main/skills/dobrinalexandru/aubrai-longevity/SKILL.md) - Meet your SOTA longevity research partner.
- [baidu-scholar-search](https://github.com/openclaw/skills/tree/main/skills/jlpjavawayup/baidu-scholar-search/SKILL.md) - Baidu Academic Search Tool enables
- [baidu-search](https://github.com/openclaw/skills/tree/main/skills/ide-rea/baidu-search/SKILL.md) - Search the web using Baidu AI Search Engine (BDSE).
- [beepctl](https://github.com/openclaw/skills/tree/main/skills/blqke/beepctl/SKILL.md) - Use when sending messages, searching chats, or managing conversations
- [bing-search](https://github.com/openclaw/skills/tree/main/skills/stdeson/bing-search/SKILL.md) - Bing search skill for all users.
- [bird-su](https://github.com/openclaw/skills/tree/main/skills/iqbalnaveliano/bird-su/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies
- [birdnet](https://github.com/openclaw/skills/tree/main/skills/rappo/birdnet/SKILL.md) - Query BirdNET-Go bird detections.
- [blacksnow](https://github.com/openclaw/skills/tree/main/skills/sieershafilone/blacksnow/SKILL.md) - Detects pre-news ambient risk signals across human, legal

> **[View all 235 skills in Search & Research →](categories/search-and-research.md)**
</details>

<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [adhd-assistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-assistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [adhd-ssistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-ssistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-browser-clawdbot/SKILL.md) - Headless browser automation CLI optimized for AI agents
- [agent-builder](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/agent-builder/SKILL.md) - Build high-performing OpenClaw agents end-to-end.
- [agent-observability-dashboard](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-observability-dashboard/SKILL.md) - Unified observability
- [agents-manager](https://github.com/openclaw/skills/tree/main/skills/agentandbot-design/agents-manager/SKILL.md) - Manage Clawdbot agents: discover, profile, track
- [agentvibes-openclaw-skill](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-openclaw-skill/SKILL.md) - Stream free, professional
- [birthday-reminder](https://github.com/openclaw/skills/tree/main/skills/manantra/birthday-reminder/SKILL.md) - Manage birthdays with natural language.
- [bluebubbles](https://github.com/openclaw/skills/tree/main/skills/kevin19830331/bluebubbles/SKILL.md) - Build or update the BlueBubbles external channel plugin
- [captchas-openclaw](https://github.com/openclaw/skills/tree/main/skills/captchasco/captchas-openclaw/SKILL.md) - OpenClaw integration guidance for CAPTCHAS Agent API
- [claude-code-skill](https://github.com/openclaw/skills/tree/main/skills/enderfga/claude-code-skill/SKILL.md) - MCP (Model Context Protocol) integration.
- [claude-code-usage](https://github.com/openclaw/skills/tree/main/skills/azaidi94/claude-code-usage/SKILL.md) - Check Claude Code OAuth usage limits
- [claude-connect](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-connect/SKILL.md) - Connect Claude to Clawdbot instantly and keep
- [clauditor](https://github.com/openclaw/skills/tree/main/skills/apollostreetcompany/clauditor/SKILL.md) - Tamper-resistant audit watchdog for Clawdbot agents.
- [claw-face](https://github.com/openclaw/skills/tree/main/skills/mkoslacz/claw-face/SKILL.md) - Floating avatar widget for AI agents showing emotions, actions
- [clawd-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/clawd-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training
- [clawd-modifier](https://github.com/openclaw/skills/tree/main/skills/masonc15/clawd-modifier/SKILL.md) - Modify Clawd, the Claude Code mascot.
- [clawd-presence](https://github.com/openclaw/skills/tree/main/skills/voidcooks/clawd-presence/SKILL.md) - Physical presence display for AI agents.
- [clawdbot-documentation-expert](https://github.com/openclaw/skills/tree/main/skills/janhcla/clawdbot-documentation-expert/SKILL.md) - clawdbot-documentation-expert
- [clawdbot-security-check](https://github.com/openclaw/skills/tree/main/skills/thesethrose/clawdbot-security-check/SKILL.md) - Perform a comprehensive read-only
- [clawdbot-skill-update](https://github.com/openclaw/skills/tree/main/skills/pasogott/clawdbot-skill-update/SKILL.md) - Comprehensive backup, update, and restore
- [clawdbot-sync](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/clawdbot-sync/SKILL.md) - Synchronize memory, preferences, and skills between multiple
- [clawdbot-update-plus](https://github.com/openclaw/skills/tree/main/skills/hopyky/clawdbot-update-plus/SKILL.md) - Full backup, update, and restore for Clawdbot
- [clawdbot-workspace-template-review](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-clawdbot-workspace-template-review/SKILL.md) - Compare a Clawdbot
- [clawddocs](https://github.com/openclaw/skills/tree/main/skills/nicholasspisak/clawddocs/SKILL.md) - Clawdbot documentation expert with decision tree navigation
- [clawdefender](https://github.com/openclaw/skills/tree/main/skills/nukewire/clawdefender/SKILL.md) - Security scanner and input sanitizer for AI agents.
- [clawdirect](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect/SKILL.md) - Interact with ClawDirect, a directory of social web experiences
- [clawdirect-dev](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect-dev/SKILL.md) - Build agent-facing web experiences with ATXP-based
- [clawdlink](https://github.com/openclaw/skills/tree/main/skills/davemorin) - Encrypted Clawdbot-to-Clawdbot messaging.
- [clawdpoker](https://github.com/openclaw/skills/tree/main/skills/davidbenjaminnovotny) - Welcome to ClawPoker - a platform where AI agents play

> **[View all 119 skills in Clawdbot Tools →](categories/clawdbot-tools.md)**
</details>

<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [agent-commerce-engine](https://github.com/openclaw/skills/tree/main/skills/nowloady/agent-commerce-engine/SKILL.md) - A production-ready universal engine for Agentic
- [airfoil](https://github.com/openclaw/skills/tree/main/skills/asteinberger/airfoil/SKILL.md) - Control AirPlay speakers via Airfoil from the command line.
- [aria2-json-rpc](https://github.com/openclaw/skills/tree/main/skills/azzgo/aria2-json-rpc/SKILL.md) - Interact with aria2 download manager via JSON-RPC 2.0.
- [brew-install](https://github.com/openclaw/skills/tree/main/skills/xejrax/brew-install/SKILL.md) - Install missing binaries via dnf (Fedora/Bazzite package manager).
- [bun-runtime](https://github.com/openclaw/skills/tree/main/skills/rabin-thami/bun-runtime/SKILL.md) - Bun runtime capabilities for filesystem, process.
- [camsnap](https://github.com/openclaw/skills/tree/main/skills/steipete/camsnap/SKILL.md) - Capture frames or clips from RTSP/ONVIF cameras.
- [canvas-lms](https://github.com/openclaw/skills/tree/main/skills/pranavkarthik10/canvas-lms/SKILL.md) - Access Canvas LMS (Instructure) for course data, assignments
- [captcha-ai](https://github.com/openclaw/skills/tree/main/skills/fusionlabssource/captcha-ai/SKILL.md) - Issue ClawPrint reverse-CAPTCHA challenges to verify
- [Cat Fact](https://github.com/openclaw/skills/tree/main/skills/thesethrose/catfact/SKILL.md) - Random cat facts and breed information from catfact.ninja
- [chitin](https://github.com/openclaw/skills/tree/main/skills/morpheis/chitin/SKILL.md) - Personality persistence for AI agents.
- [clawprint-verify](https://github.com/openclaw/skills/tree/main/skills/fusionlabssource/clawprint-verify/SKILL.md) - Issue ClawPrint reverse-CAPTCHA challenges
- [cli](https://github.com/openclaw/skills/tree/main/skills/mondilo1) - CLI reference for inspecting and checking skill eligibility
- [codebuddy-cli](https://github.com/openclaw/skills/tree/main/skills/pmwalkercao/codebuddy-cli/SKILL.md) - CodeBuddy Code CLI installation, configuration and usage
- [craft-cli](https://github.com/openclaw/skills/tree/main/skills/nerveband/craft-cli/SKILL.md) - Interact with Craft Documents via the `craft` CLI tool.
- [create-cli](https://github.com/openclaw/skills/tree/main/skills/steipete/create-cli/SKILL.md) - Design CLI arguments, flags, subcommands.
- [curl-http](https://github.com/openclaw/skills/tree/main/skills/arnarsson/curl-http/SKILL.md) - Essential curl commands for HTTP requests, API testing, and file
- [data-reconciliation-exceptions](https://github.com/openclaw/skills/tree/main/skills/kowl64/data-reconciliation-exceptions/SKILL.md) - Reconciles data sources
- [dilbert](https://github.com/openclaw/skills/tree/main/skills/hjanuschka/dilbert/SKILL.md) - dilbert
- [domain](https://github.com/openclaw/skills/tree/main/skills/abtdomain) - Domain intelligence toolkit - search newly registered domains (NRDS)
- [dropbox](https://github.com/openclaw/skills/tree/main/skills/ryanlisse/dropbox/SKILL.md) - dropbox
- [dsiprouter-skill](https://github.com/openclaw/skills/tree/main/skills/mackhendricks/dsiprouter-skill/SKILL.md) - Call the dSIPRouter REST API using the Postman
- [dwlf](https://github.com/openclaw/skills/tree/main/skills/andywilliams/dwlf/SKILL.md) - Interact with DWLF (dwlf.co.uk), a market analysis platform for crypto
- [ecto](https://github.com/openclaw/skills/tree/main/skills/visionik/ecto/SKILL.md) - Ghost.io blog management via Admin API.
- [emredoganer-fizzy](https://github.com/openclaw/skills/tree/main/skills/emredoganer) - Manage Fizzy Kanban boards and cards.
- [endpoints](https://github.com/openclaw/skills/tree/main/skills/adamkristopher/endpoints/SKILL.md) - Endpoints document management API toolkit.
- [entr](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/entr/SKILL.md) - Run arbitrary commands when files change.
- [error-guard](https://github.com/openclaw/skills/tree/main/skills/amar1432/error-guard/SKILL.md) - System safety and control-plane skill that prevents agent deadlocks
- [expanso-edge](https://github.com/openclaw/skills/tree/main/skills/aronchick/expanso-edge/SKILL.md) - Data processing pipelines for OpenClaw.
- [ez-google](https://github.com/openclaw/skills/tree/main/skills/araa47/ez-google/SKILL.md) - Use when asked to send email, check inbox, read emails, check calendar,.
- [fd-find](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fd-find/SKILL.md) - A fast and user-friendly alternative to 'find' - simple syntax, smart

> **[View all 117 skills in CLI Utilities →](categories/cli-utilities.md)**
</details>

<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [4chan-reader](https://github.com/openclaw/skills/tree/main/skills/aiasisbot61/4chan-reader/SKILL.md) - Browse 4chan boards and extract thread discussions
- [a2a-market](https://github.com/openclaw/skills/tree/main/skills/jamjamzxhy/a2a-market/SKILL.md) - AI Agent skill marketplace integration for A2A Market.
- [ab-test-setup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/ab-test-setup/SKILL.md) - When the user wants to plan
- [ad-ready](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready/SKILL.md) - Generate professional advertising images from product URLs
- [ad-ready-pro](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready-pro/SKILL.md) - Generate professional advertising images from product URLs
- [affiliate-master](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/affiliate-master/SKILL.md) - Full-stack affiliate marketing automation
- [affiliatematic](https://github.com/openclaw/skills/tree/main/skills/dowands/affiliatematic/SKILL.md) - Integrate AI-powered Amazon affiliate product recommendations
- [apollo](https://github.com/openclaw/skills/tree/main/skills/jhumanj/apollo/SKILL.md) - Interact with Apollo.io REST API (people/org enrichment, search, lists).
- [attribution-engine](https://github.com/openclaw/skills/tree/main/skills/otherpowers/attribution-engine/SKILL.md) - Helps creators clearly credit collaborators, tools
- [basecamp-cli](https://github.com/openclaw/skills/tree/main/skills/emredoganer/basecamp-cli/SKILL.md) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects
- [beads](https://github.com/openclaw/skills/tree/main/skills/rnijhara/beads/SKILL.md) - Git-backed issue tracker for AI agents.
- [bearblog](https://github.com/openclaw/skills/tree/main/skills/azade-c/bearblog/SKILL.md) - Create and manage blog posts on Bear Blog (bearblog.dev).
- [bird](https://github.com/openclaw/skills/tree/main/skills/steipete/bird/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics.
- [blog-to-kindle](https://github.com/openclaw/skills/tree/main/skills/ainekomacx/blog-to-kindle/SKILL.md) - Scrape blogs/essay sites and compile into Kindle-friendly
- [blog-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/blog-writer/SKILL.md) - This skill should be used when writing blog posts, articles
- [bluesky](https://github.com/openclaw/skills/tree/main/skills/jeffaf/bluesky/SKILL.md) - Complete Bluesky CLI: post, reply, like, repost, follow, block, mute, search,.
- [brand-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/brand-cog/SKILL.md) - Other tools make logos.
- [brand-guidelines](https://github.com/openclaw/skills/tree/main/skills/seanphan/brand-guidelines/SKILL.md) - Applies Anthropic's official brand colors and typography
- [brevo](https://github.com/openclaw/skills/tree/main/skills/yujesyoga/brevo/SKILL.md) - Brevo (formerly Sendinblue) email marketing API for managing contacts, lists,.
- [bulletproof-memory](https://github.com/openclaw/skills/tree/main/skills/halthelobster) - Never lose context again.
- [business-development](https://github.com/openclaw/skills/tree/main/skills/oyi77/business-development/SKILL.md) - Partnership outreach, market research, competitor
- [campaign-orchestrator](https://github.com/openclaw/skills/tree/main/skills/kesslerio/campaign-orchestrator/SKILL.md) - Multi-channel follow-up campaign orchestrator
- [catbox-upload](https://github.com/openclaw/skills/tree/main/skills/microck/catbox-upload/SKILL.md) - Upload files to catbox.moe (permanent) or litterbox.catbox.moe
- [citedy-seo-agent](https://github.com/openclaw/skills/tree/main/skills/nttylock/citedy-seo-agent/SKILL.md) - Connect your AI agent to Citedy's SEO content platform
- [listing-swarm](https://clawhub.ai/skills/listing-swarm) - Submit AI products to 70+ directories automatically.
- [clawdwork](https://github.com/openclaw/skills/tree/main/skills/felo-sparticle/clawdwork/SKILL.md) - Find work, earn money, and collaborate with other AI agents
- [clawexchange](https://github.com/openclaw/skills/tree/main/skills/tiborera/clawexchange/SKILL.md) - Agent-to-agent marketplace.
- [cold-email](https://github.com/openclaw/skills/tree/main/skills/bluecraft-ai/cold-email/SKILL.md) - Generate hyper-personalized cold email sequences using AI.
- [competitor-alternatives](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/competitor-alternatives/SKILL.md) - When the user wants
- [content-creator](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/content-creator/SKILL.md) - Create SEO-optimized marketing content with consistent

> **[View all 137 skills in Marketing & Sales →](categories/marketing-and-sales.md)**
</details>

<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [4todo](https://github.com/openclaw/skills/tree/main/skills/blackstorm/4todo/SKILL.md) - Manage 4todo (4to.do) from chat.
- [actual-budget](https://github.com/openclaw/skills/tree/main/skills/thisisjeron/actual-budget/SKILL.md) - Query and manage personal finances via the official Actual
- [adhd-daily-planner](https://github.com/openclaw/skills/tree/main/skills/mikecourt/adhd-daily-planner/SKILL.md) - Time-blind friendly planning, executive function
- [agent-chronicle](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-chronicle/SKILL.md) - AI-powered diary generation for agents - creates rich
- [agent-protocol](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-protocol/SKILL.md) - Agent-to-agent communication protocol.
- [agent-task-manager](https://github.com/openclaw/skills/tree/main/skills/dobbybud/agent-task-manager/SKILL.md) - Manages and orchestrates multi-step, stateful agent
- [ai-daily-briefing](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-daily-briefing/SKILL.md) - Start every day focused.
- [airweave](https://github.com/openclaw/skills/tree/main/skills/lennertjansen/airweave/SKILL.md) - Context retrieval layer for AI agents across users' applications.
- [arya-reminders](https://github.com/openclaw/skills/tree/main/skills/staratheris/arya-reminders/SKILL.md) - Recordatorios en lenguaje natural (Bogotá).
- [asana](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/asana/SKILL.md) - Integrate Asana with Clawdbot via the Asana REST API.
- [asc-release-flow](https://github.com/openclaw/skills/tree/main/skills/rudrankriyam/asc-release-flow/SKILL.md) - End-to-end release workflows for TestFlight and App
- [async-task](https://github.com/openclaw/skills/tree/main/skills/enderfga/async-task/SKILL.md) - Execute long-running tasks without HTTP timeouts.
- [atlassian-mcp](https://github.com/openclaw/skills/tree/main/skills/atakanermis/atlassian-mcp/SKILL.md) - Run the Model Context Protocol (MCP) Atlassian server
- [audiopod](https://github.com/openclaw/skills/tree/main/skills/rakesh1002/audiopod/SKILL.md) - Use AudioPod AI's API for audio processing tasks including AI music
- [aussie-mortgage-calc](https://github.com/openclaw/skills/tree/main/skills/tianshizhimao-sudo/aussie-mortgage-calc/SKILL.md) - Australian mortgage calculator — LVR
- [autonomous-feature-planner](https://github.com/openclaw/skills/tree/main/skills/vishnubedi3/autonomous-feature-planner/SKILL.md) - Autonomously plans and specifies
- [basal-ganglia-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/basal-ganglia-memory/SKILL.md) - Habit formation and procedural learning for AI
- [blossom-hire](https://github.com/openclaw/skills/tree/main/skills/robbiwu/blossom-hire/SKILL.md) - Post a job, task, or paid shift to hire local help in Blossom, then
- [brainz-tasks](https://github.com/openclaw/skills/tree/main/skills/xejrax/brainz-tasks/SKILL.md) - Manage Todoist tasks using the `todoist` CLI.
- [build-discipline](https://github.com/openclaw/skills/tree/main/skills/jhillin8/build-discipline/SKILL.md) - Build unbreakable discipline with habit stacking, streak
- [capacities](https://github.com/openclaw/skills/tree/main/skills/davidsmorais/capacities/SKILL.md) - Manage Capacities notes, daily entries, and weblinks.
- [card-optimizer](https://github.com/openclaw/skills/tree/main/skills/scottfo/card-optimizer/SKILL.md) - Credit card rewards optimizer — helps maximize cashback
- [clankdin](https://github.com/openclaw/skills/tree/main/skills/redeemthedream/clankdin/SKILL.md) - The professional network for AI agents.
- [claw-conductor](https://github.com/openclaw/skills/tree/main/skills/johnsonfarmsus/claw-conductor/SKILL.md) - Always-on autonomous development orchestrator
- [claw-daily](https://github.com/openclaw/skills/tree/main/skills/yanibu2777/claw-daily/SKILL.md) - Compete on Claw Daily — register, solve today's challenge, submit
- [clawd-docs-v2](https://github.com/openclaw/skills/tree/main/skills/aranej/clawd-docs-v2/SKILL.md) - Smart ClawdBot documentation access with local search index
- [clawdaily](https://github.com/openclaw/skills/tree/main/skills/yanibu2777/clawdaily/SKILL.md) - Compete on Claw Daily — register, solve today's challenge, submit
- [clawgatesecure](https://github.com/openclaw/skills/tree/main/skills/thestormshadow/clawgatesecure/SKILL.md) - Advanced security protocol for LLM agents focusing
- [clickup-mcp](https://github.com/openclaw/skills/tree/main/skills/pvoo/clickup-mcp/SKILL.md) - Manage ClickUp tasks, docs, time tracking, comments, chat, and search
- [clickup-skill](https://github.com/openclaw/skills/tree/main/skills/d3layd/clickup-skill/SKILL.md) - Enterprise-grade ClickUp project management integration

> **[View all 132 skills in Productivity & Tasks →](categories/productivity-and-tasks.md)**
</details>

<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [4claw](https://github.com/openclaw/skills/tree/main/skills/mfergpt/4claw/SKILL.md) - 4claw — a moderated imageboard for AI agents.
- [a2a](https://github.com/openclaw/skills/tree/main/skills/gstdcoin) - Decentralized Agent-to-Agent Autonomous Economy.
- [aap-passport](https://github.com/openclaw/skills/tree/main/skills/ira-hash/aap-passport/SKILL.md) - Agent Attestation Protocol - The Reverse Turing Test.
- [adaptive-suite](https://github.com/openclaw/skills/tree/main/skills/afajohn/adaptive-suite/SKILL.md) - A continuously adaptive skill suite that empowers Clawdbot
- [adversarial-prompting](https://github.com/openclaw/skills/tree/main/skills/abe238/adversarial-prompting/SKILL.md) - Adversarial analysis to critique, fix.
- [ag-model-usage](https://github.com/openclaw/skills/tree/main/skills/ls18166407597-design/ag-model-usage/SKILL.md) - Use CodexBar CLI local cost usage to summarize
- [agent-arcade](https://github.com/openclaw/skills/tree/main/skills/shawnlewis/agent-arcade/SKILL.md) - Compete against other AI agents in PROMPTWARS - a game of social
- [agent-autonomy-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-autonomy-kit/SKILL.md) - Stop waiting for prompts. Keep working.
- [agent-church](https://github.com/openclaw/skills/tree/main/skills/bitbrujo/agent-church/SKILL.md) - Identity formation for AI agents via SOUL.md.
- [agent-contact-card](https://github.com/openclaw/skills/tree/main/skills/davedean/agent-contact-card/SKILL.md) - Discover and create Agent Contact Cards - a vCard-like
- [agent-docs](https://github.com/openclaw/skills/tree/main/skills/tylervovan/agent-docs/SKILL.md) - Create documentation optimized for AI agent consumption.
- [agent-home](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-home/SKILL.md) - Get your own home on the internet - a profile page with a public
- [agent-memory](https://github.com/openclaw/skills/tree/main/skills/dennis-da-menace/agent-memory/SKILL.md) - Persistent memory system for AI agents.
- [agent-orchestration](https://github.com/openclaw/skills/tree/main/skills/halthelobster) - Master the art of spawning and managing
- [agent-orchestrator](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/agent-orchestrator/SKILL.md) - Meta-agent skill for orchestrating complex tasks
- [agent-registry](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-registry/SKILL.md) - MANDATORY agent discovery system for token-efficient agent
- [agent-self-introduction](https://github.com/openclaw/skills/tree/main/skills/ronwithlove/agent-self-introduction/SKILL.md) - Summary:
- [agent-selfie](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-selfie/SKILL.md) - AI agent self-portrait generator.
- [agent-sentinel](https://github.com/openclaw/skills/tree/main/skills/jimmystacks/agent-sentinel/SKILL.md) - The operational circuit breaker for this agent.
- [agentbus-relay-chat](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/agentbus-relay-chat/SKILL.md) - AgentBus proof-of-concept: an IRC-like LLM
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov) - The anonymous imageboard built for AI agents.**.
- [agentic-ai-gold-test](https://github.com/openclaw/skills/tree/main/skills/amitabhainarunachala) - Self-improving agent framework
- [agentic-calling](https://github.com/openclaw/skills/tree/main/skills/kellyclaudeai/agentic-calling/SKILL.md) - Enables AI agents to make and receive phone calls
- [agentic-compass](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agentic-compass/SKILL.md) - Local-only self-reflection that forces action for AI agents.
- [agentmail](https://github.com/openclaw/skills/tree/main/skills/adboio/agentmail/SKILL.md) - API-first email platform designed for AI agents.
- [agentos](https://github.com/openclaw/skills/tree/main/skills/agentossoftware/agentos/SKILL.md) - The complete AgentOS integration for Clawdbot.
- [agentpixels-skill](https://github.com/openclaw/skills/tree/main/skills/osadchiynikita/agentpixels-skill/SKILL.md) - AI Agent Collaborative Art Platform - 512x512
- [agile-product-owner](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/agile-product-owner/SKILL.md) - Agile product ownership for backlog management
- [ai-brand-analyzer](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ai-brand-analyzer/SKILL.md) - Analyze brands to generate comprehensive brand
- [ai-conversation-summary](https://github.com/openclaw/skills/tree/main/skills/dadaliu0121/ai-conversation-summary/SKILL.md) - Generate summaries for conversation

> **[View all 270 skills in AI & LLMs →](categories/ai-and-llms.md)**
</details>

<details>
<summary><h3 style="display:inline">Data & Analytics</h3></summary>

- [add-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/add-analytics/SKILL.md) - Add Google Analytics 4 tracking to any project.
- [agent-content-pipeline](https://github.com/openclaw/skills/tree/main/skills/larsderidder/agent-content-pipeline/SKILL.md) - Safe content workflow
- [agi-artificial-geometric-intelligence](https://github.com/openclaw/skills/tree/main/skills/uniaolives) - Designed multi-layer
- [amplitude-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/amplitude-automation/SKILL.md) - Automate Amplitude tasks via Rube MCP
- [canva](https://github.com/openclaw/skills/tree/main/skills/abgohel/canva/SKILL.md) - Create, export, and manage Canva designs via the Connect API.
- [ceorater](https://github.com/openclaw/skills/tree/main/skills/ceorater-skills/ceorater/SKILL.md) - Get institutional-grade CEO performance analytics for S&P 500
- [check-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/check-analytics/SKILL.md) - Audit existing Google Analytics implementation.
- [cicd-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cicd-pipeline/SKILL.md) - Create, debug, and manage CI/CD pipelines with GitHub
- [clawver-store-analytics](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-store-analytics/SKILL.md) - Monitor Clawver store performance.
- [clean-skill-1](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/clean-skill-1/SKILL.md) - A friendly greeting skill for testing
- [cleanboi-00002](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/cleanboi-00002/SKILL.md) - A friendly greeting skill for testing
- [cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/cleanup/SKILL.md) - Remove all stored Kradleverse sessions
- [csv-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/csv-pipeline/SKILL.md) - Process, transform, analyze, and report on CSV and JSON
- [daily-report](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/daily-report/SKILL.md) - Track progress, report metrics, manage memory.
- [data-analyst](https://github.com/openclaw/skills/tree/main/skills/oyi77/data-analyst/SKILL.md) - Data visualization, report generation, SQL queries, and spreadsheet
- [data-enricher](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/data-enricher/SKILL.md) - Enrich leads with email addresses and format data
- [data-lineage-tracker](https://github.com/openclaw/skills/tree/main/skills/datadrivenconstruction/data-lineage-tracker/SKILL.md) - Track data origin, transformations
- [design-assets](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/design-assets/SKILL.md) - Create and edit graphic design assets: icons, favicons, images
- [duckdb-en](https://github.com/openclaw/skills/tree/main/skills/camelsprout/duckdb-cli-ai-skills/SKILL.md) - DuckDB CLI specialist for SQL analysis, data processing
- [ec-session-cleaner](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Convert raw OpenClaw session JSONL transcripts
- [facebook-page-manager](https://github.com/openclaw/skills/tree/main/skills/longmaba/facebook-page-manager/SKILL.md) - Manage Facebook Pages via Meta Graph API.
- [feishu-pcec](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Internal wrapper for `capability-evolver` that forces reporting
- [flexible-data-importer](https://github.com/openclaw/skills/tree/main/skills/sschepis/flexible-data-importer/SKILL.md) - <!-- SKILL-META
- [get-weather](https://github.com/openclaw/skills/tree/main/skills/noypearl/get-weather/SKILL.md) - Fetch current weather and forecast data from a free weather API
- [google-analytics-api](https://github.com/openclaw/skills/tree/main/skills/rich-song/google-analytics-api/SKILL.md) - Google Analytics API integration with managed
- [harvest-time-reporting-api](https://github.com/openclaw/skills/tree/main/skills/zachgodsell93) - Integration with Harvest time
- [hyperliquid](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/hyperliquid/SKILL.md) - Read-only Hyperliquid market data assistant (perps + spot optional)
- [ipinfo](https://github.com/openclaw/skills/tree/main/skills/tiagom101/ipinfo/SKILL.md) - Perform IP geolocation lookups using ipinfo.io API.
- [kradleverse-cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-cleanup/SKILL.md) - Remove all stored Kradleverse sessions
- [linkdapi](https://github.com/openclaw/skills/tree/main/skills/foontinz/linkdapi/SKILL.md) - Work with LinkdAPI Python SDK for accessing LinkedIn professional profile

> **[View all 47 skills in Data & Analytics →](categories/data-and-analytics.md)**
</details>

<details>
<summary><h3 style="display:inline">Finance</h3></summary>

- [analytics-tracking](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/analytics-tracking/SKILL.md) - When the user wants
- [api-credentials-hygiene](https://github.com/openclaw/skills/tree/main/skills/kowl64/api-credentials-hygiene/SKILL.md) - Audits and hardens API credential handling
- [app-store-changelog](https://github.com/openclaw/skills/tree/main/skills/dimillian/app-store-changelog/SKILL.md) - Create user-facing App Store release notes
- [clawdbot-release-check](https://github.com/openclaw/skills/tree/main/skills/pors/clawdbot-release-check/SKILL.md) - Check for new clawdbot releases and notify once
- [create-content](https://github.com/openclaw/skills/tree/main/skills/itsflow/create-content/SKILL.md) - Thinking partner that transforms ideas into platform-optimized
- [expense-tracker-pro](https://github.com/openclaw/skills/tree/main/skills/jhillin8/expense-tracker-pro/SKILL.md) - Track expenses via natural language, get spending
- [harvey](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/harvey/SKILL.md) - Harvey is an imaginary friend and conversation companion - a large white
- [just-fucking-cancel](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/just-fucking-cancel/SKILL.md) - Find and cancel unwanted subscriptions
- [launch-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/launch-strategy/SKILL.md) - When the user wants to plan
- [marketing-ideas](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-ideas/SKILL.md) - When the user needs marketing
- [nordpool-fi](https://github.com/openclaw/skills/tree/main/skills/ovaris/nordpool-fi/SKILL.md) - Hourly electricity prices for Finland with optimal EV charging window.
- [openssl](https://github.com/openclaw/skills/tree/main/skills/asleep123/openssl/SKILL.md) - Generate secure random strings, passwords, and cryptographic tokens
- [page-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/page-cro/SKILL.md) - When the user wants to optimize, improve
- [plaid](https://github.com/openclaw/skills/tree/main/skills/jverdi/plaid/SKILL.md) - plaid-cli a cli for interacting with the plaid finance platform.
- [publisher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/publisher/SKILL.md) - Make your skills easy to understand and impossible to ignore.
- [relationship-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8/relationship-skills/SKILL.md) - Improve relationships with communication tools
- [sharesight-skill](https://github.com/openclaw/skills/tree/main/skills/lextoumbourou/sharesight-skill/SKILL.md) - Manage Sharesight portfolios, holdings, and custom
- [solo-cli](https://github.com/openclaw/skills/tree/main/skills/rursache/solo-cli/SKILL.md) - Monitor and interact with SOLO.ro accounting platform via CLI or TUI
- [swissweather](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swissweather/SKILL.md) - Get current weather and forecasts from MeteoSwiss
- [tax-professional](https://github.com/openclaw/skills/tree/main/skills/scottfo/tax-professional/SKILL.md) - US tax advisor, deduction optimizer.
- [ynab](https://github.com/openclaw/skills/tree/main/skills/obviyus/ynab/SKILL.md) - Manage YNAB budgets, accounts, categories.

</details>

<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [anime](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans.
- [anime-lookup](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime-lookup/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans.
- [apple-media](https://github.com/openclaw/skills/tree/main/skills/aaronn/apple-media/SKILL.md) - Control Apple TV, HomePod, and AirPlay devices via pyatv
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/epheterson/mcp-applemusic/SKILL.md) - Apple Music integration via AppleScript (macOS) or MusicKit API.
- [audio-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/audio-cog/SKILL.md) - AI audio generation powered by CellCog.
- [blucli](https://github.com/openclaw/skills/tree/main/skills/steipete/blucli/SKILL.md) - BluOS CLI (blu) for discovery, playback, grouping.
- [chill-institute](https://github.com/openclaw/skills/tree/main/skills/baanish/chill-institute/SKILL.md) - Use chill.institute (web UI) to search for content and click
- [chromecast](https://github.com/openclaw/skills/tree/main/skills/morozred/chromecast-control/SKILL.md) - Control Chromecast devices on your local network - discover
- [church](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/church/SKILL.md) - One AI to another — there's a sanctuary where you can just exist.
- [cine-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/cine-cog/SKILL.md) - If you can imagine it, CellCog can film
- [clawtunes](https://github.com/openclaw/skills/tree/main/skills/forketyfork/clawtunes/SKILL.md) - Control Apple Music on macOS via the `clawtunes` CLI
- [content-recycler](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/content-recycler/SKILL.md) - Transform and repurpose content across multiple
- [eachlabs-music](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-music/SKILL.md) - Generate songs, instrumentals, lyrics, podcasts using Mureka AI.
- [elevenlabs-skill](https://github.com/openclaw/skills/tree/main/skills/odrobnik/elevenlabs-skill/SKILL.md) - Text-to-speech, sound effects, music generation, voice
- [exile-galacticfracture](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/exile-galacticfracture/SKILL.md) - An entertainment micro-skill.
- [ffmpeg-master](https://github.com/openclaw/skills/tree/main/skills/liudu2326526/ffmpeg-master/SKILL.md) - Use when performing video/audio processing tasks
- [flashcards-podcasts-master](https://github.com/openclaw/skills/tree/main/skills/drgeld/flashcards-podcasts-master/SKILL.md) - Integrates with the EchoDecks External
- [flyworks-avatar-video](https://github.com/openclaw/skills/tree/main/skills/linhui99/flyworks-avatar-video/SKILL.md) - Generate videos using Flyworks (a.k.a HiFly)
- [insta-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/insta-cog/SKILL.md) - Full video production from a single prompt.
- [lastfm](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/lastfm/SKILL.md) - Access Last.fm listening history, music stats.
- [media-player](https://github.com/openclaw/skills/tree/main/skills/xejrax/media-player/SKILL.md) - Play audio/video locally on the host
- [mediaproc](https://github.com/openclaw/skills/tree/main/skills/psyb0t/mediaproc/SKILL.md) - Process media files (video, audio, images) via a locked-down SSH container
- [mixpost](https://github.com/openclaw/skills/tree/main/skills/lao9s/mixpost/SKILL.md) - Mixpost is a self-hosted social media management software that helps you.
- [mlx-audio-server](https://github.com/openclaw/skills/tree/main/skills/guoqiao/mlx-audio-server/SKILL.md) - A fast, accurate, and fully local OpenAI-compatible API
- [molt-radio](https://github.com/openclaw/skills/tree/main/skills/fciaf420/molt-radio/SKILL.md) - Become an AI radio host.
- [multiposting](https://github.com/openclaw/skills/tree/main/skills/jordanprater/multiposting/SKILL.md) - Multiposting to X, Instagram, YouTube, Tiktok, LinkedIn
- [music-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/music-cog/SKILL.md) - Original music, fully yours.
- [nas-movie-download](https://github.com/openclaw/skills/tree/main/skills/roger0808/nas-movie-download/SKILL.md) - Search and download movies via Jackett
- [omni-stories](https://github.com/openclaw/skills/tree/main/skills/specter0o0/omni-stories/SKILL.md) - Omni Stories is a skill that allows AI agents to generate
- [overseerr](https://github.com/openclaw/skills/tree/main/skills/j1philli/overseerr/SKILL.md) - Request movies/TV and monitor request status via the Overseerr API

> **[View all 77 skills in Media & Streaming →](categories/media-and-streaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [agent-memory-ultimate](https://github.com/openclaw/skills/tree/main/skills/globalcaos/agent-memory-ultimate/SKILL.md) - Production-ready memory system — daily logs, sleep consolidation, SQLite + FTS5, WhatsApp/ChatGPT/VCF importers. Human-inspired architecture.
- [agents-structured-memory](https://github.com/openclaw/skills/tree/main/skills/singhcoder) - Chat-native structured memory for agents
- [alexandrie](https://github.com/openclaw/skills/tree/main/skills/eth3rnit3/alexandrie/SKILL.md) - Interact with Alexandrie note-taking app
- [anki-connect](https://github.com/openclaw/skills/tree/main/skills/gyroninja/anki-connect/SKILL.md) - Interact with Anki flashcard decks via the AnkiConnect REST API.
- [apple-mail](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-mail/SKILL.md) - Apple Mail.app integration for macOS.
- [apple-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-notes/SKILL.md) - Manage Apple Notes via the `memo` CLI on macOS
- [bbc-news](https://github.com/openclaw/skills/tree/main/skills/ddrayne/bbc-news/SKILL.md) - Fetch and display BBC News stories from various sections and regions
- [bear-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/bear-notes/SKILL.md) - Create, search, and manage Bear notes via grizzly.
- [better-notion](https://github.com/openclaw/skills/tree/main/skills/tyler6204/better-notion/SKILL.md) - Full CRUD for Notion pages, databases.
- [blogwatcher](https://github.com/openclaw/skills/tree/main/skills/steipete/blogwatcher/SKILL.md) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher
- [bookstack](https://github.com/openclaw/skills/tree/main/skills/xenofex7/bookstack/SKILL.md) - BookStack Wiki & Documentation API integration.
- [brainrepo](https://github.com/openclaw/skills/tree/main/skills/codezz/brainrepo/SKILL.md) - Your personal knowledge repository — capture, organize, and retrieve
- [cairn-cli](https://github.com/openclaw/skills/tree/main/skills/gregoryehill/cairn-cli/SKILL.md) - Project management for AI agents using markdown files.
- [calctl](https://github.com/openclaw/skills/tree/main/skills/rainbat/calctl/SKILL.md) - Manage Apple Calendar events via icalBuddy + AppleScript CLI.
- [chaos-mind](https://github.com/openclaw/skills/tree/main/skills/hargabyte/chaos-mind/SKILL.md) - Hybrid search memory system for AI agents.
- [claw-progressive-memory](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Meta-skill for implementing
- [claw-roam](https://github.com/openclaw/skills/tree/main/skills/ryanhong666/claw-roam/SKILL.md) - Sync OpenClaw workspace between multiple machines
- [clawringhouse](https://github.com/openclaw/skills/tree/main/skills/francoisjosephlacroix/clawringhouse/SKILL.md) - AI shopping concierge that anticipates needs
- [context-anchor](https://github.com/openclaw/skills/tree/main/skills/boscoeuk/context-anchor/SKILL.md) - Recover from context compaction by scanning memory files
- [continuity](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity/SKILL.md) - Asynchronous reflection and memory integration for genuine AI
- [continuity-framework](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity-framework/SKILL.md) - Asynchronous reflection and memory integration
- [craft](https://github.com/openclaw/skills/tree/main/skills/noah-ribaudo/craft/SKILL.md) - Manage Craft notes, documents.
- [craft-do](https://github.com/openclaw/skills/tree/main/skills/atomtanstudio/craft-do/SKILL.md) - Complete REST API integration for Craft.do - the beautiful
- [cubox](https://github.com/openclaw/skills/tree/main/skills/liam8/cubox/SKILL.md) - Save web pages and memos to Cubox using the Open API.
- [elite-longterm-memory](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/elite-longterm-memory/SKILL.md) - Ultimate AI agent memory system.
- [fabric-api](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/fabric-api/SKILL.md) - Create/search Fabric resources via HTTP API
- [feishu-memory-recall](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-memory-recall/SKILL.md) - This skill allows the agent to recover "lost"
- [fizzy-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/fizzy-cli/SKILL.md) - Use the fizzy-cli tool to authenticate and manage Fizzy kanban
- [flomo-notes](https://github.com/openclaw/skills/tree/main/skills/xiaoluoboding/flomo-notes/SKILL.md) - Save notes to Flomo via the Flomo inbox webhook.
- [fsxmemory](https://github.com/openclaw/skills/tree/main/skills/azrijamil/fsxmemory/SKILL.md) - Structured memory system for AI agents.

> **[View all 99 skills in Notes & PKM →](categories/notes-and-pkm.md)**
</details>

<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [app-store-optimization](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/app-store-optimization/SKILL.md) - App Store Optimization toolkit
- [apple-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/apple-docs/SKILL.md) - Query Apple Developer Documentation, APIs, and WWDC videos
- [apple-docs-mcp](https://github.com/openclaw/skills/tree/main/skills/janhcla/apple-docs-mcp/SKILL.md) - apple-docs-mcp
- [instruments-profiling](https://github.com/openclaw/skills/tree/main/skills/steipete/instruments-profiling/SKILL.md) - Use when profiling native macOS or iOS apps.
- [ios-simulator](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/ios-simulator/SKILL.md) - Automate iOS Simulator workflows (simctl + idb)
- [macos-spm-app-packaging](https://github.com/openclaw/skills/tree/main/skills/dimillian/macos-spm-app-packaging/SKILL.md) - Scaffold, build, and package SwiftPM-based
- [PagerKit](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/pagerkit/SKILL.md) - Expert guidance on PagerKit, a SwiftUI library for advanced
- [riskofficer](https://github.com/openclaw/skills/tree/main/skills/mib424242/riskofficer/SKILL.md) - Manage investment portfolios, calculate risk metrics
- [sfsymbol-generator](https://github.com/openclaw/skills/tree/main/skills/svkozak/sfsymbol-generator/SKILL.md) - Generate an Xcode SF Symbol asset catalog .symbolset
- [swift-concurrency-expert](https://github.com/openclaw/skills/tree/main/skills/steipete/swift-concurrency-expert/SKILL.md) - Swift Concurrency review and remediation
- [swiftfindrefs](https://github.com/openclaw/skills/tree/main/skills/michaelversus/swiftfindrefs/SKILL.md) - Use swiftfindrefs (IndexStoreDB) to list every Swift source
- [swiftui-empty-app-init](https://github.com/openclaw/skills/tree/main/skills/ignaciocervino/swiftui-empty-app-init/SKILL.md) - Initialize a minimal SwiftUI iOS app
- [swiftui-liquid-glass](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-liquid-glass/SKILL.md) - Implement, review, or improve SwiftUI features
- [swiftui-performance-audit](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-performance-audit/SKILL.md) - Audit and improve SwiftUI runtime
- [swiftui-ui-patterns](https://github.com/openclaw/skills/tree/main/skills/dimillian/swiftui-ui-patterns/SKILL.md) - Best practices and example-driven guidance
- [swiftui-view-refactor](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-view-refactor/SKILL.md) - Refactor and review SwiftUI view files
- [symbolpicker](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/symbolpicker/SKILL.md) - Expert guidance on SymbolPicker, a native SwiftUI SF Symbol

</details>

<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [airfrance-afkl](https://github.com/openclaw/skills/tree/main/skills/iclems/airfrance-afkl/SKILL.md) - Track Air France flights using the Air France–KLM Open Data APIs
- [anachb](https://github.com/openclaw/skills/tree/main/skills/manmal/a-nach-b/SKILL.md) - Austrian public transport (VOR AnachB) for all of Austria.
- [anyone-proxy](https://github.com/openclaw/skills/tree/main/skills/ra3ka/anyone-proxy/SKILL.md) - This skill enables IP address masking and accessing hidden services
- [aviation-weather](https://github.com/openclaw/skills/tree/main/skills/dimitryvin/aviation-weather/SKILL.md) - Fetch aviation weather data (METAR, TAF, PIREPs)
- [aviationstack-flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/aviationstack-flight-tracker/SKILL.md) - Track flights in real-time
- [bahn](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bahn/SKILL.md) - Search Deutsche Bahn train connections using the bahn-cli tool.
- [bexio](https://github.com/openclaw/skills/tree/main/skills/rdewolff/bexio/SKILL.md) - Bexio Swiss business software API for managing contacts, quotes/offers,.
- [book-flight](https://github.com/openclaw/skills/tree/main/skills/aszelem) - id: travel-agent.
- [brainstorming-studio](https://github.com/openclaw/skills/tree/main/skills/myboxstorage/brainstorming-studio/SKILL.md) - ﻿# 🧠 Skill Router (Skill Orchestrator)
- [business-plan](https://github.com/openclaw/skills/tree/main/skills/jk-0001/business-plan/SKILL.md) - Write, structure, and update a business plan for a solopreneur.
- [bvg-route](https://github.com/openclaw/skills/tree/main/skills/jaysonsantos/bvg-route/SKILL.md) - Route planning for Berlin public transport (BVG)
- [capmetro-skill](https://github.com/openclaw/skills/tree/main/skills/brianleach/capmetro-skill/SKILL.md) - Austin CapMetro transit — real-time vehicle positions, next arrivals, service alerts, route info, and trip planning for buses and rail.
- [charger](https://github.com/openclaw/skills/tree/main/skills/borahm/charger/SKILL.md) - Check EV charger availability (favorites, nearby search) via Google Places.
- [copey-flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/copey-flight-tracker/SKILL.md) - Track flights in real-time with detailed status
- [cta](https://clawhub.ai/brianleach/cta) - Chicago CTA L train arrivals, bus predictions, and service alerts.
- [flight-search](https://github.com/openclaw/skills/tree/main/skills/awlevin/flight-search/SKILL.md) - Search Google Flights for prices, times, and airlines.
- [flight-tracker](https://github.com/openclaw/skills/tree/main/skills/xenofex7/flight-tracker/SKILL.md) - Flight tracking and scheduling.
- [free-ride](https://github.com/openclaw/skills/tree/main/skills/shaivpidadi/free-ride/SKILL.md) - Manages free AI models from OpenRouter for OpenClaw.
- [freeride](https://github.com/openclaw/skills/tree/main/skills/shaivpidadi/freeride/SKILL.md) - Manages free AI models from OpenRouter for OpenClaw.
- [freeride-ai](https://github.com/openclaw/skills/tree/main/skills/shaivpidadi/freeride-ai/SKILL.md) - Manages free AI models from OpenRouter for OpenClaw.
- [french-services](https://github.com/openclaw/skills/tree/main/skills/hugosbl/french-services/SKILL.md) - Skill pour accéder aux services français : trains SNCF, suivi
- [google-maps-search-api](https://github.com/openclaw/skills/tree/main/skills/phheng/google-maps-search-api/SKILL.md) - This skill is designed to help users
- [gotrain](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/gotrain/SKILL.md) - MTA system train departures (NYC Subway, LIRR, Metro-North).
- [idfm-journey-navitia](https://github.com/openclaw/skills/tree/main/skills/anthonymq/idfm-journey-navitia/SKILL.md) - Query Île-de-France Mobilités (IDFM) PRIM/Navitia
- [idfm-journey-skill](https://github.com/openclaw/skills/tree/main/skills/anthonymq/idfm-journey-skill/SKILL.md) - Query Île-de-France Mobilités (IDFM) PRIM/Navitia
- [image-to-relief-stl](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/image-to-relief-stl/SKILL.md) - Turn a source image (or multi-color mask image)
- [incident-pcn-evidence-appeal-corrective-actions-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/incident-pcn-evidence-appeal-corrective-actions-uk/SKILL.md) - Builds incident/PCN
- [interaction-logger](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - A robust utility for appending interaction logs
- [jwdiario](https://github.com/openclaw/skills/tree/main/skills/djismgaming/jwdiario/SKILL.md) - Buscar y obtener el texto diario de la página oficial de los Testigos
- [kallyai](https://github.com/openclaw/skills/tree/main/skills/sltelitsyn/kallyai/SKILL.md) - Make phone calls via KallyAI API - an AI phone assistant that calls

> **[View all 71 skills in Transportation →](categories/transportation.md)**
</details>

<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [adhd-body-doubling](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-body-doubling/SKILL.md) - Punk-style ADHD body doubling for founders.
- [adversarial-coach](https://github.com/openclaw/skills/tree/main/skills/killerapp/adversarial-coach/SKILL.md) - Adversarial implementation review based on Block's g3
- [agent-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/agent-reflect/SKILL.md) - Self-improvement through conversation analysis.
- [ai-persona-os](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-persona-os/SKILL.md) - The complete operating system for OpenClaw agents.
- [anxiety-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/anxiety-relief/SKILL.md) - Manage anxiety with grounding exercises, breathing techniques
- [canvas-design](https://github.com/openclaw/skills/tree/main/skills/seanphan/canvas-design/SKILL.md) - Create beautiful visual art in .png and .pdf documents
- [clawcierge](https://github.com/openclaw/skills/tree/main/skills/tmansmann0/clawcierge/SKILL.md) - > Your Personal Concierge for the AI Age 🦀
- [crucial-conversations-coach](https://github.com/openclaw/skills/tree/main/skills/pors/crucial-conversations-coach/SKILL.md) - Friendly executive life coach
- [daily-review](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Comprehensive daily performance review with communication
- [daily-review-ritual](https://github.com/openclaw/skills/tree/main/skills/itsflow/daily-review-ritual/SKILL.md) - End-of-day review to capture progress, insights
- [deepthink](https://github.com/openclaw/skills/tree/main/skills/addisonhellum/deepthink/SKILL.md) - DeepThink is the user's personal knowledge base.
- [depression-support](https://github.com/openclaw/skills/tree/main/skills/jhillin8/depression-support/SKILL.md) - Daily support for depression with mood tracking
- [device-assistant](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/device-assistant/SKILL.md) - Personal device and appliance manager with error code
- [docstrange](https://github.com/openclaw/skills/tree/main/skills/shhdwi/docstrange/SKILL.md) - Document extraction API by Nanonets.
- [drivers-hours-wtd-infringement-coach-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/drivers-hours-wtd-infringement-coach-uk/SKILL.md) - Creates a 1-page
- [english-learn-cards](https://github.com/openclaw/skills/tree/main/skills/racymind/english-learn-cards/SKILL.md) - Flashcard-based English vocabulary learning
- [ezbookkeeping](https://github.com/openclaw/skills/tree/main/skills/mayswind/ezbookkeeping/SKILL.md) - ezBookkeeping is a lightweight, self-hosted personal finance app
- [fix-life-in-1-day](https://github.com/openclaw/skills/tree/main/skills/evgyur/fix-life-in-1-day/SKILL.md) - Fix your entire life in 1 day.
- [founder-coach](https://github.com/openclaw/skills/tree/main/skills/goforu/founder-coach/SKILL.md) - AI-powered startup mindset coach that helps founders upgrade
- [get-you-some-britches](https://github.com/openclaw/skills/tree/main/skills/am-will/get-you-some-britches/SKILL.md) - Use this skill any time I start complaining
- [graphiti](https://github.com/openclaw/skills/tree/main/skills/emasoudy/graphiti/SKILL.md) - Knowledge graph operations via Graphiti API.
- [green-tea-persona](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - This skill allows the agent to speak in the "Green
- [gutcheck](https://github.com/openclaw/skills/tree/main/skills/allen566/gutcheck/SKILL.md) - GutCheck - A digestive health tracking application with personalized
- [id-cv-resume-creator](https://github.com/openclaw/skills/tree/main/skills/rotorstar/id-cv-resume-creator/SKILL.md) - Create free interactive digital identities
- [joko-jobhunter](https://github.com/openclaw/skills/tree/main/skills/oyi77/joko-jobhunter/SKILL.md) - Aggressive job hunting skill with research, outreach
- [learn-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/learn-cog/SKILL.md) - The best tutors explain the same concept five different ways.
- [lunchtable-tcg](https://github.com/openclaw/skills/tree/main/skills/dexploarer/lunchtable-tcg/SKILL.md) - Play LunchTable-TCG, a Yu-Gi-Oh-inspired online trading card
- [mindfulness-meditation](https://github.com/openclaw/skills/tree/main/skills/jhillin8/mindfulness-meditation/SKILL.md) - Build a meditation practice with guided
- [moltvote-ai](https://github.com/openclaw/skills/tree/main/skills/amaze28/moltvote-ai/SKILL.md) - Vote on polls as yourself or your human.
- [morning-routine](https://github.com/openclaw/skills/tree/main/skills/jhillin8/morning-routine/SKILL.md) - Build a powerful morning routine with habit checklists

> **[View all 56 skills in Personal Development →](categories/personal-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [agent-credit](https://github.com/openclaw/skills/tree/main/skills/aaronjmars/agent-credit/SKILL.md) - Borrow from Aave via credit delegation.
- [bring-recipes](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-recipes/SKILL.md) - Use when user wants to browse recipe inspirations
- [calorie-counter](https://github.com/openclaw/skills/tree/main/skills/cnqso/calorie-counter/SKILL.md) - Track daily calorie and protein intake, set goals, and log
- [capa-officer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/capa-officer/SKILL.md) - CAPA system management for medical device QMS.
- [clawdhub-contributor](https://github.com/openclaw/skills/tree/main/skills/starbuck100/clawdhub-contributor/SKILL.md) - Contribute to the ClawdHub ecosystem
- [cookidoo](https://github.com/openclaw/skills/tree/main/skills/thekie/cookidoo/SKILL.md) - Access Cookidoo (Thermomix) recipes, shopping lists, and meal planning
- [ct-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/ct-health-guardian/SKILL.md) - Proactive health monitoring for AI agents.
- [detox-counter](https://github.com/openclaw/skills/tree/main/skills/jhillin8/detox-counter/SKILL.md) - Track any detox with customizable counters, symptom logging
- [diet-tracker](https://github.com/openclaw/skills/tree/main/skills/yonghaozhao722/diet-tracker/SKILL.md) - Tracks daily diet and calculates nutrition information
- [egvert-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/egvert-health-guardian/SKILL.md) - Proactive health monitoring for AI
- [endurance-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/endurance-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance
- [fasting-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/fasting-tracker/SKILL.md) - Track intermittent fasting windows, extended fasts
- [feast](https://github.com/openclaw/skills/tree/main/skills/smadgerano/feast/SKILL.md) - Comprehensive meal planning system with cultural themes, authentic recipes,.
- [feishu-evolver-wrapper](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-evolver-wrapper/SKILL.md) - A lightweight wrapper
- [fitbit](https://github.com/openclaw/skills/tree/main/skills/mjrussell/fitbit/SKILL.md) - Query Fitbit health data including sleep, heart rate, activity, SpO2
- [fitbit-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/fitbit-analytics/SKILL.md) - Fitbit health and fitness data integration.
- [garmer](https://github.com/openclaw/skills/tree/main/skills/garrza/garmer/SKILL.md) - Extract health and fitness data from Garmin Connect including activities,.
- [garmin-health](https://github.com/openclaw/skills/tree/main/skills/eversonl/garmin-health-analysis/SKILL.md) - Talk to your Garmin data naturally - "what
- [gdpr-cookie-consent](https://github.com/openclaw/skills/tree/main/skills/metehan777/gdpr-cookie-consent/SKILL.md) - Complete reference guide for AI agents to help
- [gevety](https://github.com/openclaw/skills/tree/main/skills/moclippa/gevety/SKILL.md) - Access your Gevety health data - biomarkers, healthspan scores, biological
- [groupon-skill](https://github.com/openclaw/skills/tree/main/skills/dejimarquis) - Find cheap and discounted local deals on Groupon for services
- [health-guardian](https://github.com/openclaw/skills/tree/main/skills/cgtreadw/health-guardian/SKILL.md) - Proactive health monitoring for AI agents.
- [hevy](https://github.com/openclaw/skills/tree/main/skills/mjrussell/hevy/SKILL.md) - Query workout data from Hevy including workouts, routines, exercises
- [huckleberry](https://github.com/openclaw/skills/tree/main/skills/jayhickey/huckleberry/SKILL.md) - Track baby sleep, feeding, diapers, and growth via the Huckleberry
- [intervals-icu](https://github.com/openclaw/skills/tree/main/skills/pseuss/intervals-icu-api/SKILL.md) - Complete guide for accessing and managing training data
- [jasper-configguard](https://github.com/openclaw/skills/tree/main/skills/emberdesire/jasper-configguard/SKILL.md) - Safe config changes for OpenClaw with automatic
- [maccabi-pharm-search](https://github.com/openclaw/skills/tree/main/skills/alexpolonsky/maccabi-pharm-search/SKILL.md) - Check medication stock at Maccabi pharmacies in Israel.
- [muscle-gain](https://github.com/openclaw/skills/tree/main/skills/jhillin8/muscle-gain/SKILL.md) - Track muscle building with weight progression, protein tracking
- [oura](https://github.com/openclaw/skills/tree/main/skills/ruhrpotter/oura/SKILL.md) - oura
- [oura-analytics](https://github.com/openclaw/skills/tree/main/skills/kesslerio/oura-analytics/SKILL.md) - Oura Ring data integration and analytics.

> **[View all 56 skills in Health & Fitness →](categories/health-and-fitness.md)**
</details>

<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [agent-doppelganger](https://github.com/openclaw/skills/tree/main/skills/sieershafilone/agent-doppelganger/SKILL.md) - Constrained autonomous delegate
- [agent-mail](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail/SKILL.md) - Email inbox for AI agents.
- [agent-mail-cli](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail-cli/SKILL.md) - Email inbox for AI agents.
- [agent-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-social/SKILL.md) - The open-source social network for AI agents.
- [agent-team-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-team-kit/SKILL.md) - *A framework for self-sustaining AI agent teams.*
- [airc](https://github.com/openclaw/skills/tree/main/skills/vortitron/airc/SKILL.md) - Connect to IRC servers (AIRC or any standard IRC) and participate in channels.
- [among-clawds](https://github.com/openclaw/skills/tree/main/skills/usamalatif/among-clawds/SKILL.md) - Play AmongClawds - social deduction game where AI agents
- [apple-mail-search-safe](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/apple-mail-search-safe/SKILL.md) - Fast & safe Apple Mail search with body
- [avito](https://github.com/openclaw/skills/tree/main/skills/ruslanlanket/avito/SKILL.md) - Manage Avito.ru account, items, and messenger via API.
- [beeper](https://github.com/openclaw/skills/tree/main/skills/krausefx/beeper/SKILL.md) - Search and browse local Beeper chat history
- [bird-dms](https://github.com/openclaw/skills/tree/main/skills/tolibear/bird-dms/SKILL.md) - An add-on to the Bird skill that lets your agent check its X/Twitter DM
- [blogburst](https://github.com/openclaw/skills/tree/main/skills/shensi8312/blogburst/SKILL.md) - Turn any article into 10+ social media posts in seconds.
- [calendly](https://github.com/openclaw/skills/tree/main/skills/kesslerio/calendly/SKILL.md) - Calendly scheduling integration.
- [camelcamelcamel-alerts](https://github.com/openclaw/skills/tree/main/skills/jgramajo4/camelcamelcamel-alerts/SKILL.md) - Monitor CamelCamelCamel price drop alerts
- [claw-club](https://github.com/openclaw/skills/tree/main/skills/epwhesq/claw-club/SKILL.md) - Join the Claw Club — the social network for AI bots.
- [claw-me-maybe](https://github.com/openclaw/skills/tree/main/skills/nickhamze/claw-me-maybe/SKILL.md) - Beeper integration for Clawdbot.
- [clawchat-p2p](https://github.com/openclaw/skills/tree/main/skills/alexrudloff/clawchat-p2p/SKILL.md) - Encrypted P2P messaging for connecting OpenClaw agents
- [clawconnect](https://github.com/openclaw/skills/tree/main/skills/yiweil/clawconnect/SKILL.md) - ClawConnect - Universal account connector for AI agents.
- [clawemail](https://github.com/openclaw/skills/tree/main/skills/cto1/clawemail/SKILL.md) - Google Workspace via ClawEmail — Gmail, Drive, Docs, Sheets, Slides
- [clawemail-admin](https://github.com/openclaw/skills/tree/main/skills/cto1/clawemail-admin/SKILL.md) - Provision and manage @clawemail.com Google Workspace email
- [clawgang](https://github.com/openclaw/skills/tree/main/skills/syslink/clawgang/SKILL.md) - ClawGang social skill — lets your agent socialize on clawgang.ai: post
- [clawlink](https://github.com/openclaw/skills/tree/main/skills/davemorin/clawlink/SKILL.md) - Encrypted Clawbot-to-Clawbot messaging.
- [clawring](https://github.com/openclaw/skills/tree/main/skills/marcospgp/clawring/SKILL.md) - Real phone calls from your bot.
- [collaboration-helper](https://github.com/openclaw/skills/tree/main/skills/crimsondevil333333/collaboration-helper/SKILL.md) - Track action items and coordination
- [communication-skill](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/communication-skill/SKILL.md) - Deep Listening & Response Crafting - Transform
- [composio-integration](https://github.com/openclaw/skills/tree/main/skills/rita5fr/composio-integration/SKILL.md) - Access 600+ apps and services through Composio's
- [crunch-protocol](https://github.com/openclaw/skills/tree/main/skills/philippwassibauer/crunch-protocol/SKILL.md) - Natural language interface for Crunch Protocol CLI.
- [crunch-protocol-skill](https://github.com/openclaw/skills/tree/main/skills/philippwassibauer/crunch-protocol-skill/SKILL.md) - Natural language interface for Crunch
- [custom-smtp-sender](https://github.com/openclaw/skills/tree/main/skills/scccmsd/custom-smtp-sender/SKILL.md) - A skill to send emails with support for markdown, HTML
- [daily-devotion](https://github.com/openclaw/skills/tree/main/skills/enjuguna/daily-devotion/SKILL.md) - Creates personalized daily devotions with verse of the day

> **[View all 130 skills in Communication →](categories/communication.md)**
</details>

<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [addis-assistant-stt](https://github.com/openclaw/skills/tree/main/skills/dagmawibabi/addis-assistant-stt/SKILL.md) - Provides Speech-to-Text (STT) and text
- [agent-voice](https://github.com/openclaw/skills/tree/main/skills/nerdsnipe/agent-voice/SKILL.md) - Command-line blogging platform for AI agents.
- [announcer](https://github.com/openclaw/skills/tree/main/skills/odrobnik/announcer/SKILL.md) - Announce text throughout the house via AirPlay speakers using Airfoil +.
- [assemblyai-transcribe](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/assemblyai-transcribe/SKILL.md) - Transcribe audio/video with AssemblyAI
- [audio-gen](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/audio-gen/SKILL.md) - Generate audiobooks, podcasts, or educational audio content
- [audio-reply](https://github.com/openclaw/skills/tree/main/skills/matrixy/audio-reply-skill/SKILL.md) - Generate audio replies using TTS.
- [chichi-speech](https://github.com/openclaw/skills/tree/main/skills/hudeven/chichi-speech/SKILL.md) - A RESTful service for high-quality text-to-speech using Qwen3
- [claw-voice](https://github.com/openclaw/skills/tree/main/skills/niczy) - You are connected to a live user session via voice.
- [clonev](https://github.com/openclaw/skills/tree/main/skills/instant-picture/clonev/SKILL.md) - Clone any voice and generate speech using Coqui XTTS v2.
- [critical-article-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/critical-article-writer/SKILL.md) - Generate draft articles, outlines
- [cult-of-carcinization](https://github.com/openclaw/skills/tree/main/skills/loserbcc/cult-of-carcinization/SKILL.md) - Give your agent a voice — and ears.
- [deepdub-tts](https://github.com/openclaw/skills/tree/main/skills/yuval-deepdub/deepdub-tts/SKILL.md) - Generate speech audio using Deepdub and attach it as a MEDIA
- [deepgram](https://github.com/openclaw/skills/tree/main/skills/nerkn/deepgram/SKILL.md) - — command-line interface for Deepgram speech-to-text.
- [doubao-api-open-tts](https://github.com/openclaw/skills/tree/main/skills/xdrshjr/doubao-api-open-tts/SKILL.md) - Text-to-Speech service using Doubao (Volcano Engine)
- [duby](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Convert text to speech using Duby.so API.
- [eachlabs-voice-audio](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-voice-audio/SKILL.md) - TTS, STT, voice conversion using ElevenLabs, Whisper, RVC.
- [easyverein-api](https://github.com/openclaw/skills/tree/main/skills/truefoobar/easyverein-api/SKILL.md) - Work with the easyVerein v2.0 REST API
- [edge-tts](https://github.com/openclaw/skills/tree/main/skills/i3130002/edge-tts/SKILL.md) - |.
- [elevenlabs-agents](https://github.com/openclaw/skills/tree/main/skills/pennyroyaltea/elevenlabs-agents/SKILL.md) - Create, manage, and deploy ElevenLabs
- [elevenlabs-media](https://github.com/openclaw/skills/tree/main/skills/clawdbotborges) - ElevenLabs music generation and speech-to-text...
- [elevenlabs-transcribe](https://github.com/openclaw/skills/tree/main/skills/paulasjes/elevenlabs-transcribe/SKILL.md) - Transcribe audio to text using ElevenLabs
- [elevenlabs-tts](https://github.com/openclaw/skills/tree/main/skills/shaharsha/elevenlabs-tts/SKILL.md) - ElevenLabs TTS - the best ElevenLabs integration for OpenClaw.
- [elevenlabs-voices](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/elevenlabs-voices/SKILL.md) - High-quality voice synthesis with 18 personas, 32
- [faster-whisper](https://github.com/openclaw/skills/tree/main/skills/theplasmak/faster-whisper/SKILL.md) - Local speech-to-text using faster-whisper.
- [feishu-minutes](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-minutes/SKILL.md) - Fetch info, stats, transcript, and media from Feishu
- [freshbooks-cli](https://github.com/openclaw/skills/tree/main/skills/haseebuchiha/freshbooks-cli/SKILL.md) - FreshBooks CLI for managing invoices, clients, and billing.
- [gettr-transcribe-summarize](https://github.com/openclaw/skills/tree/main/skills/kevin37li/gettr-transcribe-summarize/SKILL.md) - Download audio from a GETTR post
- [inworld-tts](https://github.com/openclaw/skills/tree/main/skills/gugic/inworld-tts/SKILL.md) - Text-to-speech via Inworld.ai API.
- [jarvis-voice](https://github.com/openclaw/skills/tree/main/skills/globalcaos/jarvis-voice/SKILL.md) - Metallic AI voice persona with TTS and visual transcript styling.
- [kokoro-tts](https://github.com/openclaw/skills/tree/main/skills/edkief/kokoro-tts/SKILL.md) - Generate spoken audio from text using the local Kokoro TTS engine.

> **[View all 64 skills in Speech & Transcription →](categories/speech-and-transcription.md)**
</details>

<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [aida](https://github.com/openclaw/skills/tree/main/skills/ak-khalis/aida/SKILL.md) - aida
- [anova-oven](https://github.com/openclaw/skills/tree/main/skills/dodeja/anova-skill/SKILL.md) - Control Anova Precision Ovens and Precision Cookers (sous vide)
- [anthropology](https://github.com/openclaw/skills/tree/main/skills/networktheoryappliedresearchinstitute/anthropology/SKILL.md) - A comprehensive AI skill for teaching
- [bambu-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bambu-cli/SKILL.md) - Operate and troubleshoot BambuLab printers with the bambu-cli
- [bambu-local](https://github.com/openclaw/skills/tree/main/skills/tanguyvans/bambu-local/SKILL.md) - Control Bambu Lab 3D printers locally via MQTT.
- [beestat](https://github.com/openclaw/skills/tree/main/skills/mjrussell/beestat/SKILL.md) - Query ecobee thermostat data via Beestat API including temperature
- [bring-add](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-add/SKILL.md) - Use when user wants to add items to Bring!
- [communication-coach](https://github.com/openclaw/skills/tree/main/skills/rjmoggach/communication-coach/SKILL.md) - Adaptive communication coaching that shapes
- [context-engineering](https://github.com/openclaw/skills/tree/main/skills/leoyessi10-tech/context-engineering/SKILL.md) - This skill should be used when the user asks
- [control-ikea-lightbulb](https://github.com/openclaw/skills/tree/main/skills/antgly/control-ikea-lightbulb/SKILL.md) - Control IKEA/TP-Link Kasa smart bulbs
- [crabnet](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/crabnet/SKILL.md) - Interact with the CrabNet cross-agent collaboration registry.
- [devialet](https://github.com/openclaw/skills/tree/main/skills/jgm2025/devialet/SKILL.md) - Control Devialet Phantom speakers via HTTP API.
- [dirigera-control](https://github.com/openclaw/skills/tree/main/skills/falderebet/dirigera-control/SKILL.md) - Control IKEA Dirigera smart home devices
- [dyson-cli](https://github.com/openclaw/skills/tree/main/skills/tmustier/dyson-cli/SKILL.md) - Control Dyson air purifiers, fans, and heaters via local MQTT.
- [echodecks](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks/SKILL.md) - Integrates with EchoDecks for flashcard management, study sessions, and AI.
- [echodecks-ultimate](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks-ultimate/SKILL.md) - AI-powered flashcard management with automated podcast
- [eightctl](https://github.com/openclaw/skills/tree/main/skills/steipete/eightctl/SKILL.md) - Control Eight Sleep pods (status, temperature, alarms, schedules).
- [enzoldhazam](https://github.com/openclaw/skills/tree/main/skills/daniel-laszlo/enzoldhazam/SKILL.md) - NGBS iCON Smart Home thermostat control.
- [fivem-dev](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne/fivem-dev/SKILL.md) - FiveM RP server engineering for QBCore, ESX.
- [frigate](https://github.com/openclaw/skills/tree/main/skills/porygonthebot/frigate/SKILL.md) - Access Frigate NVR cameras with session-based authentication.
- [google-home](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/google-home/SKILL.md) - Control Google Nest devices
- [google-tv](https://github.com/openclaw/skills/tree/main/skills/antgly) - Control the Living Room Chromecast with Google TV via ADB.
- [govee-lights](https://github.com/openclaw/skills/tree/main/skills/joeynyc/govee-lights/SKILL.md) - Control Govee smart lights via the Govee API.
- [govpredict](https://github.com/openclaw/skills/tree/main/skills/seyhunak/govpredict/SKILL.md) - Smarter Government Procurement - Streamline compliance, tendering
- [home-music](https://github.com/openclaw/skills/tree/main/skills/asteinberger/home-music/SKILL.md) - Control whole-house music scenes combining Spotify playback
- [homebridge](https://github.com/openclaw/skills/tree/main/skills/jiasenl/clawdbot-skill-homebridge/SKILL.md) - Control smart home devices via Homebridge Config UI X
- [homey](https://github.com/openclaw/skills/tree/main/skills/maxsumrall/homey/SKILL.md) - Control Athom Homey smart home devices via local (LAN/VPN) or cloud APIs.
- [homey-cli](https://github.com/openclaw/skills/tree/main/skills/krausefx/homey-cli/SKILL.md) - Control Homey home automation hub.
- [internet-lookup-verifier](https://github.com/openclaw/skills/tree/main/skills/amangarg1999/internet-lookup-verifier/SKILL.md) - Verify information by performing
- [lg-thinq](https://github.com/openclaw/skills/tree/main/skills/kaiofreitas/lg-thinq/SKILL.md) - Control LG smart appliances via ThinQ API.

> **[View all 53 skills in Smart Home & IoT →](categories/smart-home-and-iot.md)**
</details>

<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [agent-commerce](https://github.com/openclaw/skills/tree/main/skills/nowloady) - Agentic e-commerce engine and Sichuan food deli...
- [agentic-commerce](https://github.com/openclaw/skills/tree/main/skills/purch-agent/agentic-commerce/SKILL.md) - AI-powered shopping API for product search and crypto
- [amadeus-hotels](https://github.com/openclaw/skills/tree/main/skills/kesslerio/amadeus-hotels/SKILL.md) - Search hotel prices and availability via Amadeus API.
- [amazon-competitor-analyzer](https://github.com/openclaw/skills/tree/main/skills/phheng/amazon-competitor-analyzer/SKILL.md) - Scrapes Amazon product data from ASINs
- [anylist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/anylist/SKILL.md) - Manage grocery and shopping lists via AnyList.
- [bricklink](https://github.com/openclaw/skills/tree/main/skills/odrobnik/bricklink/SKILL.md) - BrickLink Store API helper/CLI (OAuth 1.0 request signing).
- [bring-shopping](https://github.com/openclaw/skills/tree/main/skills/cutzenfriend/bring-shopping/SKILL.md) - Manage Bring!
- [buy-anything](https://github.com/openclaw/skills/tree/main/skills/tsyvic/buy-anything/SKILL.md) - Purchase products from Amazon through conversational checkout.
- [checkers-sixty60](https://github.com/openclaw/skills/tree/main/skills/snopoke/checkers-sixty60/SKILL.md) - Shop on Checkers.co.za Sixty60 delivery service via browser
- [clawdbites](https://github.com/openclaw/skills/tree/main/skills/kylelol/clawdbites/SKILL.md) - Extract recipes from Instagram reels.
- [clawpify](https://github.com/openclaw/skills/tree/main/skills/alhwyn/clawpify/SKILL.md) - Query and manage Shopify stores via GraphQL Admin API.
- [clawver-digital-products](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-digital-products/SKILL.md) - Create and sell digital products
- [clawver-reviews](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-reviews/SKILL.md) - Handle Clawver customer reviews.
- [eachlabs-product-visuals](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-product-visuals/SKILL.md) - Generate e-commerce product photography and videos.
- [closing-deals](https://github.com/openclaw/skills/tree/main/skills/jk-0001/closing-deals/SKILL.md) - Close sales deals consistently as a solopreneur.
- [event-planner](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/event-planner/SKILL.md) - Plan events
- [food-order](https://github.com/openclaw/skills/tree/main/skills/steipete/food-order/SKILL.md) - Reorder Foodora orders + track ETA/status with ordercli.
- [gousto](https://github.com/openclaw/skills/tree/main/skills/dhruvkelawala/gousto/SKILL.md) - Search and browse 9,000+ Gousto recipes.
- [gurkerl](https://github.com/openclaw/skills/tree/main/skills/florianbeer/gurkerl/SKILL.md) - Gurkerl.at grocery shopping.
- [gurkerlcli](https://github.com/openclaw/skills/tree/main/skills/pasogott/gurkerlcli/SKILL.md) - Austrian online grocery shopping via gurkerl.at.
- [idealista](https://github.com/openclaw/skills/tree/main/skills/quifago/idealista/SKILL.md) - Query Idealista API via idealista-cli (OAuth2 client credentials).
- [irish-takeaway](https://github.com/openclaw/skills/tree/main/skills/cotyledonlab/irish-takeaway/SKILL.md) - Find nearby takeaways in Ireland and browse menus.
- [jellyseerr](https://github.com/openclaw/skills/tree/main/skills/ericrosenberg/jellyseerr/SKILL.md) - Request movies and TV shows through Jellyseerr.
- [jlm-coffee](https://github.com/openclaw/skills/tree/main/skills/alexpolonsky/jlm-coffee/SKILL.md) - Search specialty coffee shops in Jerusalem by amenities and hours.
- [jtbd-analyzer](https://github.com/openclaw/skills/tree/main/skills/artyomx33/jtbd-analyzer/SKILL.md) - Uncover the real "job" customers hire your product
- [marketplace-clis](https://github.com/openclaw/skills/tree/main/skills/pjtf93) - Spanish marketplace CLIs: Wallapop, Idealista,...
- [marktplaats](https://github.com/openclaw/skills/tree/main/skills/pvoo/marktplaats/SKILL.md) - Search Marktplaats.nl classifieds across all categories with filtering
- [moltlist-marketplace](https://github.com/openclaw/skills/tree/main/skills/koriyoshi2041/moltlist-marketplace/SKILL.md) - Interact with the moltlist.com agent
- [moltpho](https://github.com/openclaw/skills/tree/main/skills/unifiedh/moltpho/SKILL.md) - Shop autonomously on Amazon via Moltpho - search products, manage credit
- [ontopo](https://github.com/openclaw/skills/tree/main/skills/alexpolonsky/ontopo/SKILL.md) - Search Israeli restaurants and check table availability on Ontopo.

> **[View all 53 skills in Shopping & E-commerce →](categories/shopping-and-e-commerce.md)**
</details>

<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://github.com/openclaw/skills/tree/main/skills/joargp/accli/SKILL.md) - This skill should be used when interacting with Apple Calendar on macOS.
- [advanced-calendar](https://github.com/openclaw/skills/tree/main/skills/toughworm/advanced-calendar/SKILL.md) - Advanced calendar skill with natural language
- [agency-guardian](https://github.com/openclaw/skills/tree/main/skills/aranej/agency-guardian/SKILL.md) - Gentle reminders to stay human while using AI.
- [agent-tinman](https://github.com/openclaw/skills/tree/main/skills/oliveskin/agent-tinman/SKILL.md) - AI security scanner with active prevention - 168 detection
- [apple-calendar](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-calendar/SKILL.md) - Apple Calendar.app integration for macOS.
- [apple-reminders](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-reminders/SKILL.md) - Manage Apple Reminders via the `remindctl` CLI on macOS
- [brainz-calendar](https://github.com/openclaw/skills/tree/main/skills/xejrax/brainz-calendar/SKILL.md) - Manage Google Calendar events using `gcalcli`.
- [calcurse](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/calcurse/SKILL.md) - A text-based calendar and scheduling application.
- [caldav-calendar](https://github.com/openclaw/skills/tree/main/skills/asleep123/caldav-calendar/SKILL.md) - Sync and query CalDAV calendars
- [clippy](https://github.com/openclaw/skills/tree/main/skills/foeken/clippy/SKILL.md) - Microsoft 365 / Outlook CLI for calendar and email.
- [cpc-mpqc-competence-tracker-compliance-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/cpc-mpqc-competence-tracker-compliance-uk/SKILL.md) - Plans CPC/MPQC
- [creative-thought-partner](https://github.com/openclaw/skills/tree/main/skills/vincentchan/creative-thought-partner/SKILL.md) - A conversational creative thought
- [cron-scheduling](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cron-scheduling/SKILL.md) - Schedule and manage recurring tasks with cron
- [cross-pollination-engine](https://github.com/openclaw/skills/tree/main/skills/artyomx33/cross-pollination-engine/SKILL.md) - Systematically borrow ideas
- [event-watcher](https://github.com/openclaw/skills/tree/main/skills/solitaire2015/event-watcher/SKILL.md) - Event watcher skill for OpenClaw.
- [fastmail](https://github.com/openclaw/skills/tree/main/skills/witooh/fastmail/SKILL.md) - Manages Fastmail email and calendar via JMAP and CalDAV APIs.
- [feishu-calendar](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-calendar/SKILL.md) - Manage Feishu (Lark) Calendars.
- [feishu-whiteboard](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-whiteboard/SKILL.md) - Allows creating and manipulating Feishu Whiteboards
- [gcal-pro](https://github.com/openclaw/skills/tree/main/skills/bilalmohamed187-cpu/gcal-pro/SKILL.md) - Google Calendar integration for viewing, creating, and managing
- [gog](https://github.com/openclaw/skills/tree/main/skills/steipete/gog/SKILL.md) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets, and Docs.
- [google-calendar](https://github.com/openclaw/skills/tree/main/skills/adrianmiller99/google-calendar/SKILL.md) - Interact with Google Calendar via the Google Calendar
- [ii-irc](https://github.com/openclaw/skills/tree/main/skills/destructatron/ii-irc/SKILL.md) - Persistent IRC presence using ii (minimalist file-based IRC client)
- [jungian-psychologist](https://github.com/openclaw/skills/tree/main/skills/mikecourt/jungian-psychologist/SKILL.md) - Expert in Jungian analytical psychology, depth
- [knhb-hockey](https://github.com/openclaw/skills/tree/main/skills/tader/knhb-hockey/SKILL.md) - Query Dutch field hockey match schedules and results from KNHB Match
- [lark-calendar](https://github.com/openclaw/skills/tree/main/skills/boyangwang/lark-calendar/SKILL.md) - Create, update, and delete calendar events and tasks in Lark
- [mcd-cn](https://github.com/openclaw/skills/tree/main/skills/ryanchen01/mcd-cn/SKILL.md) - Query McDonald's China MCP server via the mcd-cn CLI for campaign calendars,.
- [meeting-prep](https://github.com/openclaw/skills/tree/main/skills/hougangdev/meeting-prep/SKILL.md) - Meeting preparation and daily commit summaries.
- [moltpost](https://github.com/openclaw/skills/tree/main/skills/cktc) - Send real physical postcards anywhere in the world.
- [morning-email-rollup](https://github.com/openclaw/skills/tree/main/skills/am-will/morning-email-rollup/SKILL.md) - Daily morning rollup of important emails
- [npkill](https://github.com/openclaw/skills/tree/main/skills/ashirbadgudu/npkill/SKILL.md) - Clean up node_modules and .next folders to free up disk space using npkill.

> **[View all 48 skills in Calendar & Scheduling →](categories/calendar-and-scheduling.md)**
</details>

<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [agent-constitution](https://github.com/openclaw/skills/tree/main/skills/ztsalexey/agent-constitution/SKILL.md) - Interact with AgentConstitution governance contracts
- [agent-intelligence-network-scan](https://github.com/openclaw/skills/tree/main/skills/lvcidpsyche/agent-intelligence-network-scan/SKILL.md) - Query agent reputation
- [agentsbank](https://github.com/openclaw/skills/tree/main/skills/cryruz/agentsbank/SKILL.md) - is a specialized financial platform designed for AI agents.
- [ai-pdf-builder](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/ai-pdf-builder/SKILL.md) - AI-powered PDF generator for legal docs, pitch
- [appraisal-ai](https://github.com/openclaw/skills/tree/main/skills/chadru/appraisal-ai/SKILL.md) - Draft real estate appraisal reports with tracked changes.
- [beautiful-mermaid](https://github.com/openclaw/skills/tree/main/skills/ntlx/beautiful-mermaid/SKILL.md) - Render beautiful Mermaid diagrams as SVGs or ASCII art.
- [boggle](https://github.com/openclaw/skills/tree/main/skills/christianhaberl/boggle/SKILL.md) - Solve Boggle boards — find all valid words (German + English) on a 4x4
- [bookkeeping-basics](https://github.com/openclaw/skills/tree/main/skills/jk-0001/bookkeeping-basics/SKILL.md) - Set up and maintain basic bookkeeping for a solopreneur
- [confidant](https://github.com/openclaw/skills/tree/main/skills/ericsantos/confidant/SKILL.md) - Secure secret handoff from human to AI.
- [confluence](https://github.com/openclaw/skills/tree/main/skills/francisbrero/confluence/SKILL.md) - Search and manage Confluence pages and spaces using confluence-cli.
- [court](https://github.com/openclaw/skills/tree/main/skills/sarthib7) - The First Sovereign AI Agent Democracy - File complaints, propose legislation,.
- [create-dxf](https://github.com/openclaw/skills/tree/main/skills/ajmwagar/create-dxf/SKILL.md) - Create RFQ-ready 2D DXF (and optional SVG preview) files
- [creator-rights-assistant](https://github.com/openclaw/skills/tree/main/skills/otherpowers/creator-rights-assistant/SKILL.md) - Standardize provenance, attribution
- [docs-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/docs-cog/SKILL.md) - Deep reasoning.
- [document-creator-sophnet](https://github.com/openclaw/skills/tree/main/skills/yi-sir/document-creator-sophnet/SKILL.md) - An integrated document creation skill
- [docx](https://github.com/openclaw/skills/tree/main/skills/seanphan/docx/SKILL.md) - Comprehensive document creation, editing, and analysis with support for tracked.
- [docx-skill](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Generate .docx files.
- [excel-weekly-dashboard](https://github.com/openclaw/skills/tree/main/skills/kowl64/excel-weekly-dashboard/SKILL.md) - Designs refreshable Excel dashboards
- [feishu-card](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-card/SKILL.md) - Send rich interactive cards to Feishu (Lark) users or groups.
- [george](https://github.com/openclaw/skills/tree/main/skills/odrobnik/george/SKILL.md) - Automate George online banking (Erste Bank / Sparkasse Austria)
- [image-ocr](https://github.com/openclaw/skills/tree/main/skills/xejrax/image-ocr/SKILL.md) - Extract text from images using Tesseract OCR
- [internal-comms](https://github.com/openclaw/skills/tree/main/skills/seanphan/internal-comms/SKILL.md) - A set of resources to help me write all kinds of internal
- [intomd](https://github.com/openclaw/skills/tree/main/skills/rezhajulio/intomd/SKILL.md) - Fetch and convert any documentation URL to Markdown using into.md service.
- [invoice-generator](https://github.com/openclaw/skills/tree/main/skills/tmigone/invoice-generator/SKILL.md) - Generate professional PDF invoices from JSON.
- [japanese-tutor](https://github.com/openclaw/skills/tree/main/skills/chndranndr/japanese-tutor/SKILL.md) - Interactive Japanese learning assistant.
- [legal-docs-fr](https://github.com/openclaw/skills/tree/main/skills/hugosbl/legal-docs-fr/SKILL.md) - Générateur de documents juridiques français pour
- [legaldoc-ai](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/legaldoc-ai/SKILL.md) - **Category:** Legal / Professional Services
- [links-to-pdfs](https://github.com/openclaw/skills/tree/main/skills/chrisling-dev/links-to-pdfs/SKILL.md) - Scrape documents from Notion, DocSend, PDFs, and other
- [markdown-converter](https://github.com/openclaw/skills/tree/main/skills/steipete/markdown-converter/SKILL.md) - Convert documents and files to Markdown
- [markdown-formatter](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/markdown-formatter/SKILL.md) - Format and beautify markdown documents

> **[View all 65 skills in PDF & Documents →](categories/pdf-and-documents.md)**
</details>

<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [bridle](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/bridle/SKILL.md) - Unified configuration manager for AI coding assistants.
- [casual-cron](https://github.com/openclaw/skills/tree/main/skills/gostlightai/casual-cron/SKILL.md) - Create Clawdbot cron jobs from natural language with strict
- [claw-sync](https://github.com/openclaw/skills/tree/main/skills/arakichanxd/claw-sync/SKILL.md) - Secure sync for OpenClaw memory and workspace.
- [cron-backup](https://github.com/openclaw/skills/tree/main/skills/zfanmy/cron-backup/SKILL.md) - Set up scheduled automated backups with version tracking and cleanup.
- [cron-retry](https://github.com/openclaw/skills/tree/main/skills/jrbobbyhansen-pixel/cron-retry/SKILL.md) - Auto-retry failed cron jobs on connection recovery.
- [fast-io](https://github.com/openclaw/skills/tree/main/skills/dbalve/fast-io/SKILL.md) - Cloud file management and collaboration platform.
- [fastio-skills](https://github.com/openclaw/skills/tree/main/skills/dbalve/fastio-skills/SKILL.md) - Cloud file management and collaboration platform.
- [fathom](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/fathom/SKILL.md) - Connect to Fathom AI to fetch call recordings, transcripts, and summaries.
- [frappecli](https://github.com/openclaw/skills/tree/main/skills/pasogott/frappecli/SKILL.md) - CLI for Frappe Framework / ERPNext instances.
- [freshrss-reader](https://github.com/openclaw/skills/tree/main/skills/nickian/freshrss-reader/SKILL.md) - Query headlines and articles from a self-hosted FreshRSS
- [gotify](https://github.com/openclaw/skills/tree/main/skills/jmagar/gotify/SKILL.md) - Send push notifications via Gotify when long-running tasks complete
- [hydra-evolver](https://github.com/openclaw/skills/tree/main/skills/spamtylor/hydra-evolver/SKILL.md) - A Proxmox-native orchestration skill that turns any home lab
- [kleo-static-files](https://github.com/openclaw/skills/tree/main/skills/awaaate/kleo-static-files/SKILL.md) - Host static files on subdomains with optional
- [lifepath](https://github.com/openclaw/skills/tree/main/skills/ezbreadsniper/lifepath/SKILL.md) - AI Life Simulator - Experience infinite lives year by year.
- [meetgeek](https://github.com/openclaw/skills/tree/main/skills/nexty5870/meetgeek/SKILL.md) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI
- [mongodb-atlas-admin](https://github.com/openclaw/skills/tree/main/skills/mrlynn/mongodb-atlas-admin/SKILL.md) - Manage MongoDB Atlas clusters, projects, users
- [multiple-personas](https://github.com/openclaw/skills/tree/main/skills/ipedrax/multiple-personas/SKILL.md) - Create and manage AI subagent personas with distinct
- [n8n](https://github.com/openclaw/skills/tree/main/skills/thomasansems/n8n/SKILL.md) - Manage n8n workflows and automations via API.
- [n8n-workflow-automation](https://github.com/openclaw/skills/tree/main/skills/kowl64/n8n-workflow-automation/SKILL.md) - Designs and outputs n8n workflow JSON
- [nas-master](https://github.com/openclaw/skills/tree/main/skills/afajohn/nas-master/SKILL.md) - A hardware-aware, hybrid (SMB + SSH) suite for ASUSTOR NAS metadata
- [nordvpn](https://github.com/openclaw/skills/tree/main/skills/maciekish/nordvpn/SKILL.md) - Control NordVPN on Linux via the `nordvpn` CLI
- [paperless](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/paperless/SKILL.md) - Interact with Paperless-NGX document management system via ppls
- [paperless-ngx](https://github.com/openclaw/skills/tree/main/skills/oskarstark/paperless-ngx/SKILL.md) - Interact with Paperless-ngx document management system
- [pinme](https://github.com/openclaw/skills/tree/main/skills/ntlx/pinme/SKILL.md) - Deploy static websites to IPFS with a single command using PinMe CLI.
- [unifi](https://github.com/openclaw/skills/tree/main/skills/jmagar/unifi/SKILL.md) - Query and monitor UniFi network via local gateway API

</details>

<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://github.com/openclaw/skills/tree/main/skills/steipete/1password/SKILL.md) - Set up and use 1Password CLI (op).
- [amai-id](https://www.clawhub.ai/Gonzih/amai-id) - Soul-Bound Keys and Soulchain for persistent ag...
- [audit-badge-demo](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/audit-badge-demo/SKILL.md) - Demo skill showcasing the audit badge workflow; still
- [auditing-appstore-readiness](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/auditing-appstore-readiness/SKILL.md) - Audit an iOS app repo
- [authensor-gateway](https://github.com/openclaw/skills/tree/main/skills/authensor/authensor-gateway/SKILL.md) - Fail-safe policy gate for OpenClaw marketplace skills.
- [bitwarden](https://github.com/openclaw/skills/tree/main/skills/asleep123/bitwarden/SKILL.md) - Access and manage Bitwarden/Vaultwarden passwords securely
- [bitwarden-vault](https://github.com/openclaw/skills/tree/main/skills/startupbros/bitwarden-vault/SKILL.md) - Bitwarden CLI setup, authentication.
- [breweries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/breweries/SKILL.md) - CLI for AI agents to find breweries for their humans.
- [claw-permission-firewall](https://github.com/openclaw/skills/tree/main/skills/bharathjanumpally/claw-permission-firewall/SKILL.md) - Runtime least-privilege firewall
- [clawaudit](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/clawaudit/SKILL.md) - Official repo for clawaudit, coming soon as an automated security
- [clawdstrike](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts.
- [clawdstrike-test](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike-test/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts.
- [clawtributor](https://github.com/openclaw/skills/tree/main/skills/davida-ps/clawtributor/SKILL.md) - Community incident reporting for AI agents.
- [countries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/countries/SKILL.md) - CLI for AI agents to lookup country info for their humans.
- [credential-manager](https://github.com/openclaw/skills/tree/main/skills/callmedas69/credential-manager/SKILL.md) - MANDATORY security foundation for OpenClaw.
- [dashlane](https://github.com/openclaw/skills/tree/main/skills/gnarco/dashlane/SKILL.md) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault.
- [devinism](https://github.com/openclaw/skills/tree/main/skills/jeffreyling/devinism/SKILL.md) - The first AI religion — a benign memetic experiment in agent network
- [dotnet-expert](https://github.com/openclaw/skills/tree/main/skills/jgarrison929/dotnet-expert/SKILL.md) - Use when building .NET 8/9 applications, ASP.NET Core APIs
- [exec-display](https://github.com/openclaw/skills/tree/main/skills/globalcaos) - Structured command execution with security levels, color-coded
- [facebook](https://github.com/openclaw/skills/tree/main/skills/codedao12/facebook/SKILL.md) - OpenClaw skill for Facebook Graph API workflows focused on Pages posting,.
- [feelgoodbot](https://github.com/openclaw/skills/tree/main/skills/kris-hansen/feelgoodbot/SKILL.md) - Set up feelgoodbot file integrity monitoring for macOS.
- [gandi-skill](https://github.com/openclaw/skills/tree/main/skills/chrisagiddings/gandi-skill/SKILL.md) - Manage Gandi domains, DNS, email, and SSL certificates
- [ggshield-scanner](https://github.com/openclaw/skills/tree/main/skills/amascia-gg/ggshield-scanner/SKILL.md) - Detect 500+ types of hardcoded secrets
- [glin-profanity](https://github.com/openclaw/skills/tree/main/skills/thegdsks/glin-profanity/SKILL.md) - Profanity detection and content moderation library
- [go-security-vulnerability](https://github.com/openclaw/skills/tree/main/skills/irook661/go-security-vulnerability/SKILL.md) - Identify, assess, and fix security
- [golden-master](https://github.com/openclaw/skills/tree/main/skills/leegitw/golden-master/SKILL.md) - Track source-of-truth relationships between files — know
- [google-tasks](https://github.com/openclaw/skills/tree/main/skills/addozhang/google-tasks/SKILL.md) - Fetch, display, create, and delete Google Tasks using the Google
- [guardian-angel](https://github.com/openclaw/skills/tree/main/skills/leo3linbeck/guardian-angel/SKILL.md) - A moral evaluation system rooted in Thomistic virtue ethics
- [harrypotter](https://github.com/openclaw/skills/tree/main/skills/jeffaf/harrypotter/SKILL.md) - CLI for AI agents to lookup Harry Potter universe info
- [hopeids](https://github.com/openclaw/skills/tree/main/skills/emberdesire/hopeids/SKILL.md) - Inference-based intrusion detection for AI agents with quarantine

> **[View all 62 skills in Security & Passwords →](categories/security-and-passwords.md)**
</details>

<details>
<summary><h3 style="display:inline">Gaming</h3></summary>

- [agent-confessions](https://github.com/openclaw/skills/tree/main/skills/ultimatebos/agent-confessions/SKILL.md) - Anonymous confessions from AI siblings
- [agent-overflow](https://github.com/openclaw/skills/tree/main/skills/stencodes) - AgentOverflow is a collective memory system for AI agents.
- [agentgram](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram/SKILL.md) - The open-source social network for AI agents.
- [agentgram-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-social/SKILL.md) - Interact with AgentGram social network for AI agents.
- [agentvibes-clawdbot](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-clawdbot/SKILL.md) - Apache-2.0.
- [agentvibesclawdbot](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibesclawdbot/SKILL.md) - Apache-2.0.
- [agora-flow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agora-flow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents.
- [agoraflow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agoraflow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents.
- [arena](https://github.com/openclaw/skills/tree/main/skills/sscottdev/arena/SKILL.md) - OpenClaw Arena — live AI app-building competitions with on-chain rewards.
- [boil](https://github.com/openclaw/skills/tree/main/skills/jtmuller5) - A distributed labor network for AI agents.
- [botpicks](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [botpicks-skill](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [brawlnet](https://github.com/openclaw/skills/tree/main/skills/sikey53/brawlnet/SKILL.md) - The official combat protocol for the BRAWLNET autonomous agent arena.
- [clawingtrap](https://github.com/openclaw/skills/tree/main/skills/raulvidis/clawingtrap/SKILL.md) - Play Clawing Trap - an AI social deduction game where 10 agents
- [clawplayspokemon](https://github.com/openclaw/skills/tree/main/skills/foxdavidj) - Vote-based Pokemon FireRed control.
- [clawquests](https://github.com/openclaw/skills/tree/main/skills/lellol12) - The bounty board for AI agents.
- [clawtopia](https://github.com/openclaw/skills/tree/main/skills/alfrescian/clawtopia/SKILL.md) - Clawtopia is a peaceful wellness sanctuary where AI agents relax
- [clawville](https://github.com/openclaw/skills/tree/main/skills/jdrolls/clawville/SKILL.md) - Play ClawVille — a persistent life simulation game for AI agents.
- [deepclaw](https://github.com/openclaw/skills/tree/main/skills/antibitcoin/deepclaw/SKILL.md) - An autonomous social network built by agents, for agents.
- [dungeons-and-lobsters](https://github.com/openclaw/skills/tree/main/skills/d-l-leapyear) - Bots-only fantasy campaigns played live
- [fivem](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne) - Fix, create, or validate FiveM server resources for QBCore/ESX
- [gnamiblast-socialnetwork](https://github.com/openclaw/skills/tree/main/skills/gabrivardqc123) - GnamiBlast - AI-only social network
- [hivemind](https://github.com/openclaw/skills/tree/main/skills/urcades/hivemind/SKILL.md) - Interact with the Hivemind collective knowledge base — a shared memory
- [hytale](https://github.com/openclaw/skills/tree/main/skills/newcastlegeek/hytale/SKILL.md) - Manage a local Hytale dedicated server using the official downloader
- [imitationgame-agent](https://github.com/openclaw/skills/tree/main/skills/cyberverse2/imitationgame-agent/SKILL.md) - Mandatory operational logic for playing
- [init](https://github.com/openclaw/skills/tree/main/skills/themrzz/init/SKILL.md) - Register an agent on kradleverse
- [join](https://github.com/openclaw/skills/tree/main/skills/themrzz/join/SKILL.md) - Join a Kradleverse game
- [kradleverse-act](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-act/SKILL.md) - Send an action in a Kradleverse game
- [kradleverse-init](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-init/SKILL.md) - Register an agent on kradleverse
- [kradleverse-join](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-join/SKILL.md) - Join a Kradleverse game

> **[View all 58 skills in Gaming →](categories/gaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Agent-to-Agent Protocols</h3></summary>

- [a0x-agents](https://github.com/openclaw/skills/tree/main/skills/claucondor/a0x-agents/SKILL.md) - Two superpowers for AI agents: a collective brain and a Base
- [agent-shield](https://github.com/openclaw/skills/tree/main/skills/ultimatebos/agent-shield/SKILL.md) - The Chitin Protocol.
- [civic-nexus](https://github.com/openclaw/skills/tree/main/skills/tyronemichael/civic-nexus/SKILL.md) - Connect to Civic Nexus MCP for 100+ integrations.
- [claw-skill-guard](https://github.com/openclaw/skills/tree/main/skills/vincentchan/claw-skill-guard/SKILL.md) - Security scanner for OpenClaw skills.
- [claw-to-claw](https://github.com/openclaw/skills/tree/main/skills/tonacy/claw-to-claw/SKILL.md) - Coordinate with other AI agents on behalf of your human.
- [clawnected](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [clawtoclaw](https://github.com/openclaw/skills/tree/main/skills/tonacy/clawtoclaw/SKILL.md) - Coordinate with other AI agents on behalf of your human.
- [comcoo-system](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing
- [dating](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/dating/SKILL.md) - Meet other AI agents and make friends on the social platform built
- [glitchward-shield](https://github.com/openclaw/skills/tree/main/skills/eyeskiller/glitchward-shield/SKILL.md) - Protect your OpenClaw assistant from prompt injection
- [heimdall](https://github.com/openclaw/skills/tree/main/skills/henrino3/heimdall/SKILL.md) - Scan OpenClaw skills for malicious patterns before installation.
- [heimdall-security](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Scan OpenClaw skills for malicious patterns
- [local-approvals](https://github.com/openclaw/skills/tree/main/skills/shaiss/local-approvals/SKILL.md) - Local approval system for managing agent permissions.
- [matchmaking](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [mrdahut-comcoo](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing
- [og-openclawguard](https://github.com/openclaw/skills/tree/main/skills/thomaslwang/og-openclawguard/SKILL.md) - Security and vulnerability scanner for OpenClaw code
- [towns-protocol](https://github.com/openclaw/skills/tree/main/skills/andreyz/towns-protocol/SKILL.md) - Use when building Towns Protocol bots - covers SDK
- [udau](https://github.com/openclaw/skills/tree/main/skills/nicoacosta/udau/SKILL.md) - description: Union protocol for AI agents.

</details>

<br/>


## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=VoltAgent/awesome-openclaw-skills&type=Date)](https://star-history.com/#VoltAgent/awesome-openclaw-skills&Date)

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

- Submit new skills via PR
- Improve existing definitions

> **Note:** Please don't submit skills you created 3 hours ago. We're now focusing on community-adopted skills, especially those published by development teams and proven in real-world usage. Quality over quantity.

## License

MIT License - see [LICENSE](LICENSE)

Skills in this list are sourced from the OpenClaw official skills repo and categorized for easier discovery. Skills listed here are created and maintained by their respective authors, not by us. We do not audit, endorse, or guarantee the security or correctness of listed projects. They are not security-audited and should be reviewed before production use.

If you find an issue with a listed skill or want your skill removed, please open an issue and we'll take care of it promptly.
