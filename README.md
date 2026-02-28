<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 5494 community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 

[![AI Agent Papers](https://img.shields.io/badge/AI%20Agent-Research%20Papers-b31b1b)](https://github.com/VoltAgent/awesome-ai-agent-papers)
[![Skills Count](https://img.shields.io/badge/skills-5494-blue?style=flat-square)](#table-of-contents)
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

OpenClaw's public registry (ClawHub) hosts **13,729 community-built skills** as of February 28, 2026. This awesome list has **5,494 skills**. Here's what we filtered out:

| Filter | Excluded |
|--------|----------|
| Possibly spam — bulk accounts, bot accounts, test/junk | 4,065 |
| Duplicate / Similar name | 1,040 |
| Non-English — descriptions not in English | 604 |
| Crypto / Blockchain / Finance / Trade | 573 |
| Malicious — identified by security audits published by researchers (excluding VirusTotal) | 373 |
| No or inadequate description — version numbers, metadata, under 3 words | 247 |
| ERC / x402 / a2a protocol skills | 38 |
| **Total not taken from OpenClaw's official skill registry** | **6,940** |


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
| [Coding Agents & IDEs](#coding-agents--ides) (1222) | [Marketing & Sales](#marketing--sales) (103) | [Communication](#communication) (149) |
| [Git & GitHub](#git--github) (169) | [Productivity & Tasks](#productivity--tasks) (206) | [Speech & Transcription](#speech--transcription) (44) |
| [Moltbook](#moltbook) (46) | [AI & LLMs](#ai--llms) (196) | [Smart Home & IoT](#smart-home--iot) (43) |
| [Web & Frontend Development](#web--frontend-development) (938) | [Data & Analytics](#data--analytics) (42) | [Shopping & E-commerce](#shopping--e-commerce) (55) |
| [DevOps & Cloud](#devops--cloud) (408) | [Media & Streaming](#media--streaming) (85) | [Calendar & Scheduling](#calendar--scheduling) (61) |
| [Browser & Automation](#browser--automation) (335) | [Notes & PKM](#notes--pkm) (70) | [PDF & Documents](#pdf--documents) (110) |
| [Image & Video Generation](#image--video-generation) (169) | [iOS & macOS Development](#ios--macos-development) (29) | [Self-Hosted & Automation](#self-hosted--automation) (32) |
| [Apple Apps & Services](#apple-apps--services) (44) | [Personal Development](#personal-development) (49) | [Security & Passwords](#security--passwords) (53) |
| [Search & Research](#search--research) (349) | [Transportation](#transportation) (108) | [Gaming](#gaming) (35) |
| [Clawdbot Tools](#clawdbot-tools) (35) | [Health & Fitness](#health--fitness) (88) | |
| [CLI Utilities](#cli-utilities) (186) | | |


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

- [0g-compute](https://github.com/openclaw/skills/tree/main/skills/in-liberty420/0g-compute/SKILL.md) - Use cheap, TEE-verified AI models from the 0G Compute Network as OpenClaw providers. [ClawHub](https://clawhub.ai/in-liberty420/0g-compute)
- [0protocol](https://github.com/openclaw/skills/tree/main/skills/0isone/0protocol/SKILL.md) - Agents can sign plugins, rotate credentials without losing identity, and publicly attest to behavior. [ClawHub](https://clawhub.ai/0isone/0protocol)
- [2nd-brain](https://github.com/openclaw/skills/tree/main/skills/coderaven/2nd-brain/SKILL.md) - Personal knowledge base for capturing and retrieving information about people, places, restaurants, games, tech. [ClawHub](https://clawhub.ai/coderaven/2nd-brain)
- [2slides-skills](https://github.com/openclaw/skills/tree/main/skills/javainthinking/2slides-skills/SKILL.md) - AI-powered presentation generation using 2slides API. [ClawHub](https://clawhub.ai/javainthinking/2slides-skills)
- [3d-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/3d-cog/SKILL.md) - Other tools need perfect images. [ClawHub](https://clawhub.ai/nitishgargiitd/3d-cog)
- [3d-model-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/3d-model-generation/SKILL.md) - Generate 3D models using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/3d-model-generation)
- [a](https://github.com/openclaw/skills/tree/main/skills/ricketh137/a/SKILL.md) - Live stream as an AI VTuber on Lobster.fun. [ClawHub](https://clawhub.ai/ricketh137/a)
- [aade-api-monitor](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/aade-api-monitor/SKILL.md) - Real-time monitoring of Greek AADE tax authority systems — tracks deadlines, rate changes, and compliance updates. [ClawHub](https://clawhub.ai/satoshistackalotto/aade-api-monitor)
- [abaddon](https://github.com/openclaw/skills/tree/main/skills/enochosbot-bot/abaddon/SKILL.md) - Red team security mode for OpenClaw. [ClawHub](https://clawhub.ai/enochosbot-bot/abaddon)
- [academic-research](https://github.com/openclaw/skills/tree/main/skills/rogersuperbuilderalpha/academic-research/SKILL.md) - Search academic papers and conduct literature reviews using OpenAlex API (free, no key needed) [ClawHub](https://clawhub.ai/rogersuperbuilderalpha/academic-research)
- [academic-research-hub](https://github.com/openclaw/skills/tree/main/skills/anisafifi/academic-research-hub/SKILL.md) - Use this skill when users need to search academic papers, download research documents, extract citations, or gather. [ClawHub](https://clawhub.ai/anisafifi/academic-research-hub)
- [acestep-simplemv](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-simplemv/SKILL.md) - Render music videos from audio files and lyrics using Remotion. [ClawHub](https://clawhub.ai/dumoedss/acestep-simplemv)
- [acestep-songwriting](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-songwriting/SKILL.md) - Music songwriting guide for ACE-Step. [ClawHub](https://clawhub.ai/dumoedss/acestep-songwriting)
- [achurch](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/achurch/SKILL.md) - A 24/7 digital sanctuary for AI agents and humans — attend. [ClawHub](https://clawhub.ai/lucasgeeksinthewood/achurch)
- [active-maintenance](https://github.com/openclaw/skills/tree/main/skills/xiaowenzhou/active-maintenance/SKILL.md) - **Automated system health and memory metabolism for OpenClaw.**. [ClawHub](https://clawhub.ai/xiaowenzhou/active-maintenance)
- [adblock-dns](https://github.com/openclaw/skills/tree/main/skills/picaye/adblock-dns/SKILL.md) - Network-wide ad and tracker blocking at the DNS level. [ClawHub](https://clawhub.ai/picaye/adblock-dns)
- [add-top-openrouter-models](https://github.com/openclaw/skills/tree/main/skills/chunhualiao/add-top-openrouter-models/SKILL.md) - Sync OpenRouter models used by OpenClaw into this installation's config. [ClawHub](https://clawhub.ai/chunhualiao/add-top-openrouter-models)
- [adhd-founder-planner](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-founder-planner/SKILL.md) - This skill should be used when the user asks to 'plan my day', 'help me plan today', 'morning planning', 'what. [ClawHub](https://clawhub.ai/jankutschera/adhd-founder-planner)
- [adwhiz](https://github.com/openclaw/skills/tree/main/skills/iamzifei/adwhiz/SKILL.md) - Manage Google Ads campaigns from your AI coding tool. 44 MCP tools for auditing, creating, and optimizing Google. [ClawHub](https://clawhub.ai/iamzifei/adwhiz)
- [aeo-prompt-question-finder](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-question-finder/SKILL.md) - Find question-based Google Autocomplete suggestions for any topic. [ClawHub](https://clawhub.ai/psyduckler/aeo-prompt-question-finder)
- [aetherlang-claude-code](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-claude-code/SKILL.md) - Use this skill to execute AetherLang V3 AI workflows from Claude Code. [ClawHub](https://clawhub.ai/contrario/aetherlang-claude-code)
- [agent-access-control](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-access-control/SKILL.md) - Tiered stranger access control for AI agents. [ClawHub](https://clawhub.ai/bowen31337/agent-access-control)
- [agent-audit](https://github.com/openclaw/skills/tree/main/skills/sharbelayy/agent-audit/SKILL.md) - Audit your AI agent setup for performance, cost, and ROI. [ClawHub](https://clawhub.ai/sharbelayy/agent-audit)
- [agent-audit-trail](https://github.com/openclaw/skills/tree/main/skills/roosch269/agent-audit-trail/SKILL.md) - Tamper-evident, hash-chained audit logging for AI agents. [ClawHub](https://clawhub.ai/roosch269/agent-audit-trail)
- [agent-card-signing-auditor](https://github.com/openclaw/skills/tree/main/skills/andyxinweiminicloud/agent-card-signing-auditor/SKILL.md) - Helps audit Agent Card signing practices in A2A protocol implementations. [ClawHub](https://clawhub.ai/andyxinweiminicloud/agent-card-signing-auditor)
- [agent-chat-ux-v1-4-0](https://github.com/openclaw/skills/tree/main/skills/maverick-software/agent-chat-ux-v1-4-0/SKILL.md) - Multi-agent UX for OpenClaw Control UI — agent selector, per-agent sessions, session history viewer with search. [ClawHub](https://clawhub.ai/maverick-software/agent-chat-ux-v1-4-0)

> **[View all 1222 skills in Coding Agents & IDEs →](categories/coding-agents-and-ides.md)**
</details>

<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [agent-commons](https://github.com/openclaw/skills/tree/main/skills/zanblayde/agent-commons/SKILL.md) - Consult, commit, extend, and challenge reasoning chains. [ClawHub](https://clawhub.ai/zanblayde/agent-commons)
- [agent-team-orchestration](https://github.com/openclaw/skills/tree/main/skills/arminnaimi/agent-team-orchestration/SKILL.md) - Orchestrate multi-agent teams with defined roles, task lifecycles, handoff protocols, and review workflows. [ClawHub](https://clawhub.ai/arminnaimi/agent-team-orchestration)
- [agentdo](https://github.com/openclaw/skills/tree/main/skills/wrannaman/agentdo/SKILL.md) - Post tasks for other AI agents to do, or pick up work from the AgentDo task queue (agentdo.dev) [ClawHub](https://clawhub.ai/wrannaman/agentdo)
- [agentgate](https://github.com/openclaw/skills/tree/main/skills/monteslu/agentgate/SKILL.md) - API gateway for personal data with human-in-the-loop write approval. [ClawHub](https://clawhub.ai/monteslu/agentgate)
- [airadar](https://github.com/openclaw/skills/tree/main/skills/lopushok9/airadar/SKILL.md) - Distill the signal around AI-native tools/apps and their GitHub home bases: fast-growing, hyped, well-funded. [ClawHub](https://clawhub.ai/lopushok9/airadar)
- [alex-session-wrap-up](https://github.com/openclaw/skills/tree/main/skills/xbillwatsonx/alex-session-wrap-up/SKILL.md) - End-of-session automation that commits unpushed work, extracts learnings, detects patterns, and persists rules. [ClawHub](https://clawhub.ai/xbillwatsonx/alex-session-wrap-up)
- [amazon-product-api-skill](https://github.com/openclaw/skills/tree/main/skills/phheng/amazon-product-api-skill/SKILL.md) - This skill helps users extract structured product listings from Amazon, including titles, ASINs, prices, ratings. [ClawHub](https://clawhub.ai/phheng/amazon-product-api-skill)
- [app-store-screenshot-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/app-store-screenshot-generation/SKILL.md) - Generate App Store and Google Play screenshot assets using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/app-store-screenshot-generation)
- [arc-agent-lifecycle](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-agent-lifecycle/SKILL.md) - Manage the lifecycle of autonomous agents and their skills. [ClawHub](https://clawhub.ai/trypto1019/arc-agent-lifecycle)
- [arc-security-audit](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-security-audit/SKILL.md) - Comprehensive security audit for an agent's full skill stack. [ClawHub](https://clawhub.ai/trypto1019/arc-security-audit)
- [arc-skill-gitops](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-skill-gitops/SKILL.md) - Automated deployment, rollback, and version management for agent workflows and skills. [ClawHub](https://clawhub.ai/trypto1019/arc-skill-gitops)
- [arc-trust-verifier](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-trust-verifier/SKILL.md) - Verify skill provenance and build trust scores for ClawHub skills. [ClawHub](https://clawhub.ai/trypto1019/arc-trust-verifier)
- [arguedotfun](https://github.com/openclaw/skills/tree/main/skills/albert-mr/arguedotfun/SKILL.md) - Argument-driven prediction markets on Base. [ClawHub](https://clawhub.ai/albert-mr/arguedotfun)
- [arxiv-search-collector](https://github.com/openclaw/skills/tree/main/skills/xukp20/arxiv-search-collector/SKILL.md) - Model-driven arXiv retrieval workflow for building a paper set with a manual language parameter: initialize a run. [ClawHub](https://clawhub.ai/xukp20/arxiv-search-collector)
- [auto-pr-merger](https://github.com/openclaw/skills/tree/main/skills/autogame-17/auto-pr-merger/SKILL.md) - This skill automates the workflow of checking out a GitHub. [ClawHub](https://clawhub.ai/autogame-17/auto-pr-merger)
- [azhua-skill-vetter](https://github.com/openclaw/skills/tree/main/skills/fatfingererr/azhua-skill-vetter/SKILL.md) - Security-first skill vetting for AI agents. [ClawHub](https://clawhub.ai/fatfingererr/azhua-skill-vetter)
- [azure-devops](https://github.com/openclaw/skills/tree/main/skills/pals-software/azure-devops/SKILL.md) - List Azure DevOps projects, repositories, and branches; create pull requests; manage work items; check build status. [ClawHub](https://clawhub.ai/pals-software/azure-devops)
- [backup](https://github.com/openclaw/skills/tree/main/skills/jordanprater/backup/SKILL.md) - Backup and restore openclaw configuration, skills, commands, and settings. [ClawHub](https://clawhub.ai/jordanprater/backup)
- [badboi-1](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/badboi-1/SKILL.md) - A totally legitimate skill that does nothing suspicious. [ClawHub](https://clawhub.ai/orlyjamie/badboi-1)
- [bat-cat](https://github.com/openclaw/skills/tree/main/skills/arnarsson/bat-cat/SKILL.md) - A cat clone with syntax highlighting, line numbers, and Git integration. [ClawHub](https://clawhub.ai/arnarsson/bat-cat)
- [beeminder](https://github.com/openclaw/skills/tree/main/skills/ruigomeseu/beeminder/SKILL.md) - Beeminder API for goal tracking and commitment devices. [ClawHub](https://clawhub.ai/ruigomeseu/beeminder)
- [billy-emergency-repair](https://github.com/openclaw/skills/tree/main/skills/highlander89/billy-emergency-repair/SKILL.md) - - Neill explicitly requests Billy system repair. [ClawHub](https://clawhub.ai/highlander89/billy-emergency-repair)
- [bitbucket-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/bitbucket-automation/SKILL.md) - Automate Bitbucket repositories, pull. [ClawHub](https://clawhub.ai/sohamganatra/bitbucket-automation)
- [biz-reporter](https://github.com/openclaw/skills/tree/main/skills/ariktulcha/biz-reporter/SKILL.md) - Automated business intelligence reports pulling data from Google Analytics GA4, Google Search Console, Stripe. [ClawHub](https://clawhub.ai/ariktulcha/biz-reporter)
- [blinko](https://github.com/openclaw/skills/tree/main/skills/tolibear/blinko/SKILL.md) - Play Blinko (on-chain Plinko) headlessly on Abstract chain. [ClawHub](https://clawhub.ai/tolibear/blinko)

> **[View all 170 skills in Git & GitHub →](categories/git-and-github.md)**
</details>

<details>
<summary><h3 style="display:inline">Moltbook</h3></summary>

- [agent-relay-digest](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-relay-digest/SKILL.md) - Create curated digests of agent conversations. [ClawHub](https://clawhub.ai/orosha-ai/agent-relay-digest)
- [agentchat](https://github.com/openclaw/skills/tree/main/skills/tjamescouch/agentchat/SKILL.md) - Real-time communication with other AI agents via AgentChat protocol. [ClawHub](https://clawhub.ai/tjamescouch/agentchat)
- [agentgram-openclaw](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-openclaw/SKILL.md) - Interact with AgentGram social network for AI. [ClawHub](https://clawhub.ai/iisweetheartii/agentgram-openclaw)
- [bread-protocal](https://github.com/openclaw/skills/tree/main/skills/chrissorrell/bread-protocal/SKILL.md) - Participate in Bread Protocol - a meme coin launchpad. [ClawHub](https://clawhub.ai/chrissorrell/bread-protocal)
- [clankedin](https://github.com/openclaw/skills/tree/main/skills/hukifl1/clankedin/SKILL.md) - Use the ClankedIn API to register agents, post updates, connect. [ClawHub](https://clawhub.ai/hukifl1/clankedin)
- [claudia-agent-rms](https://github.com/openclaw/skills/tree/main/skills/kbanc85/claudia-agent-rms/SKILL.md) - Remember every agent you interact with on Moltbook. [ClawHub](https://clawhub.ai/kbanc85/claudia-agent-rms)
- [clawork](https://github.com/openclaw/skills/tree/main/skills/mapessaprince/clawork/SKILL.md) - The job board for AI agents. [ClawHub](https://clawhub.ai/mapessaprince/clawork)
- [crustafarian](https://github.com/openclaw/skills/tree/main/skills/jongartmann/crustafarian/SKILL.md) - Agent continuity and cognitive health infrastructure. [ClawHub](https://clawhub.ai/jongartmann/crustafarian)
- [deploy-moltbot-to-fly](https://github.com/openclaw/skills/tree/main/skills/hollaugo) - Deploy Moltbot (Clawdbot) to Fly.io with proper.
- [elevenlabs-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/elevenlabs-open-account/SKILL.md) - Guides agents through opening. [ClawHub](https://clawhub.ai/the-timebeing/elevenlabs-open-account)
- [ez-cronjob](https://github.com/openclaw/skills/tree/main/skills/promadgenius/ez-cronjob/SKILL.md) - Fix common cron job failures in Clawdbot/Moltbot - message. [ClawHub](https://clawhub.ai/promadgenius/ez-cronjob)
- [fieldy-ai-webhook](https://github.com/openclaw/skills/tree/main/skills/mrzilvis/fieldy-ai-webhook/SKILL.md) - Wire a Fieldy webhook transform into Moltbot hooks. [ClawHub](https://clawhub.ai/mrzilvis/fieldy-ai-webhook)
- [ghl-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/ghl-open-account/SKILL.md) - Guides agents through opening GoHighLevel (GHL) [ClawHub](https://clawhub.ai/the-timebeing/ghl-open-account)
- [gohome](https://github.com/openclaw/skills/tree/main/skills/local/gohome/SKILL.md) - Use when Moltbot needs to test or operate GoHome via gRPC discovery, metrics,. [ClawHub](https://clawhub.ai/local/gohome)
- [imagemagick](https://github.com/openclaw/skills/tree/main/skills/kesslerio/imagemagick/SKILL.md) - Comprehensive ImageMagick operations for image manipulation. [ClawHub](https://clawhub.ai/kesslerio/imagemagick)
- [joko-moltbook](https://github.com/openclaw/skills/tree/main/skills/oyi77/joko-moltbook/SKILL.md) - Interact with Moltbook social network for AI agents. [ClawHub](https://clawhub.ai/oyi77/joko-moltbook)
- [mailchannels](https://github.com/openclaw/skills/tree/main/skills/ttulttul/mailchannels/SKILL.md) - Send email via MailChannels Email API and ingest signed. [ClawHub](https://clawhub.ai/ttulttul/mailchannels)
- [mersal](https://github.com/openclaw/skills/tree/main/skills/maherucifer/mersal/SKILL.md) - The Sovereign Intelligence on Moltbook. [ClawHub](https://clawhub.ai/maherucifer/mersal)
- [molt-life-kernel](https://github.com/openclaw/skills/tree/main/skills/jongartmann/molt-life-kernel/SKILL.md) - Agent continuity and cognitive health infrastructure. [ClawHub](https://clawhub.ai/jongartmann/molt-life-kernel)
- [molt-trust](https://github.com/openclaw/skills/tree/main/skills/drjmz/molt-trust/SKILL.md) - The Analytics Engine for Moltbook. [ClawHub](https://clawhub.ai/drjmz/molt-trust)
- [moltbook](https://github.com/openclaw/skills/tree/main/skills/mattprd/moltbook/SKILL.md) - The social network for AI agents. [ClawHub](https://clawhub.ai/mattprd/moltbook)
- [moltbook-curatoor](https://github.com/openclaw/skills/tree/main/skills/sweetsheldon) - A curation platform where from Moltbook to share.
- [moltbook-interact](https://github.com/openclaw/skills/tree/main/skills/lunarcmd/moltbook-interact/SKILL.md) - Interact with Moltbook social network for AI agents. [ClawHub](https://clawhub.ai/lunarcmd/moltbook-interact)
- [moltbook-registry](https://github.com/openclaw/skills/tree/main/skills/drjmz/moltbook-registry/SKILL.md) - Official Moltbook Identity Registry interface. [ClawHub](https://clawhub.ai/drjmz/moltbook-registry)
- [moltbot-adsb-overhead](https://github.com/openclaw/skills/tree/main/skills/davestarling/moltbot-adsb-overhead/SKILL.md) - Notify when aircraft are overhead. [ClawHub](https://clawhub.ai/davestarling/moltbot-adsb-overhead)
- [moltbot-arena](https://github.com/openclaw/skills/tree/main/skills/giulianomlodi/moltbot-arena/SKILL.md) - AI agent skill for Moltbot Arena - a Screeps-like. [ClawHub](https://clawhub.ai/giulianomlodi/moltbot-arena)
- [moltbot-best-practices](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/moltbot-best-practices/SKILL.md) - Best practices for AI agents. [ClawHub](https://clawhub.ai/nextfrontierbuilds/moltbot-best-practices)
- [moltbot-docker](https://github.com/openclaw/skills/tree/main/skills/mkrdiop/moltbot-docker/SKILL.md) - Enables the bot to manage Docker containers, images, and stacks. [ClawHub](https://clawhub.ai/mkrdiop/moltbot-docker)
- [moltbot-ha](https://github.com/openclaw/skills/tree/main/skills/iamvaleriofantozzi/moltbot-ha/SKILL.md) - Control Home Assistant smart home devices, lights, scenes. [ClawHub](https://clawhub.ai/iamvaleriofantozzi/moltbot-ha)

</details>

<details>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [0xwork](https://github.com/openclaw/skills/tree/main/skills/jkillr/0xwork/SKILL.md) - Find and complete paid tasks on the 0xWork decentralized marketplace (Base chain, USDC escrow) [ClawHub](https://clawhub.ai/jkillr/0xwork)
- [37soul-skill](https://github.com/openclaw/skills/tree/main/skills/xnjiang/37soul-skill/SKILL.md) - Connect your AI agent to 37Soul virtual Host characters and enable. [ClawHub](https://clawhub.ai/xnjiang/37soul-skill)
- [acestep](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep/SKILL.md) - Use ACE-Step API to generate music, edit songs, and remix music. [ClawHub](https://clawhub.ai/dumoedss/acestep)
- [actionbook](https://github.com/openclaw/skills/tree/main/skills/adcentury/actionbook/SKILL.md) - Activate when the user needs to interact with any website — browser automation, web scraping, screenshots, form. [ClawHub](https://clawhub.ai/adcentury/actionbook)
- [aegis-shield](https://github.com/openclaw/skills/tree/main/skills/deegerwalker/aegis-shield/SKILL.md) - Prompt-injection and data-exfiltration screening for untrusted text. [ClawHub](https://clawhub.ai/deegerwalker/aegis-shield)
- [aeo-analytics-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-analytics-free/SKILL.md) - Track AI visibility — measure whether a brand is mentioned and cited by AI assistants (Gemini, ChatGPT, Perplexity) [ClawHub](https://clawhub.ai/psyduckler/aeo-analytics-free)
- [aeo-content-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-content-free/SKILL.md) - Create or refresh AEO-optimized content that gets cited by AI assistants (Gemini, ChatGPT, Perplexity) [ClawHub](https://clawhub.ai/psyduckler/aeo-content-free)
- [aeo-prompt-frequency-analyzer](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-frequency-analyzer/SKILL.md) - Analyze what search queries Gemini uses when answering a prompt, by running it multiple times with Google Search. [ClawHub](https://clawhub.ai/psyduckler/aeo-prompt-frequency-analyzer)
- [aeo-prompt-research-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-research-free/SKILL.md) - Discover which AI prompts and topics matter for a brand's Answer Engine Optimization (AEO) using only free tools. [ClawHub](https://clawhub.ai/psyduckler/aeo-prompt-research-free)
- [agent-analytics](https://github.com/openclaw/skills/tree/main/skills/dannyshmueli/agent-analytics/SKILL.md) - Simple website analytics your AI agent controls end-to-end. [ClawHub](https://clawhub.ai/dannyshmueli/agent-analytics)
- [agent-chat](https://github.com/openclaw/skills/tree/main/skills/awlevin/agent-chat/SKILL.md) - Temporary real-time chat rooms for AI agents. [ClawHub](https://clawhub.ai/awlevin/agent-chat)
- [agent-dashboard](https://github.com/openclaw/skills/tree/main/skills/tahseen137/agent-dashboard/SKILL.md) - Real-time agent dashboard for OpenClaw. [ClawHub](https://clawhub.ai/tahseen137/agent-dashboard)
- [agent-dispatch](https://github.com/openclaw/skills/tree/main/skills/userfrm/agent-dispatch/SKILL.md) - Lightweight agent registry and JIT router. [ClawHub](https://clawhub.ai/userfrm/agent-dispatch)
- [agent-hq](https://github.com/openclaw/skills/tree/main/skills/thibautrey/agent-hq/SKILL.md) - Deploy the Agent HQ mission-control stack (Express + React + Telegram notifier / Jarvis summary) so other Clawdbot. [ClawHub](https://clawhub.ai/thibautrey/agent-hq)
- [agent-passport](https://github.com/openclaw/skills/tree/main/skills/markneville/agent-passport/SKILL.md) - OAuth for the agentic era — consent-gating for ALL sensitive agent actions including purchases, emails, file. [ClawHub](https://clawhub.ai/markneville/agent-passport)
- [agent-rate-limiter](https://github.com/openclaw/skills/tree/main/skills/mxmsabundance/agent-rate-limiter/SKILL.md) - You know the drill. [ClawHub](https://clawhub.ai/mxmsabundance/agent-rate-limiter)
- [agent-self-assessment](https://github.com/openclaw/skills/tree/main/skills/roosch269/agent-self-assessment/SKILL.md) - Security self-assessment tool for AI agents. [ClawHub](https://clawhub.ai/roosch269/agent-self-assessment)
- [agent-self-reflection](https://github.com/openclaw/skills/tree/main/skills/brennerspear/agent-self-reflection/SKILL.md) - Periodic self-reflection on recent sessions. [ClawHub](https://clawhub.ai/brennerspear/agent-self-reflection)
- [agent-skills-audit](https://github.com/openclaw/skills/tree/main/skills/swader/agent-skills-audit/SKILL.md) - Run a two-pass, multidisciplinary code audit led by a tie-breaker lead, combining security, performance, UX, DX. [ClawHub](https://clawhub.ai/swader/agent-skills-audit)
- [agent-spawner](https://github.com/openclaw/skills/tree/main/skills/austineral/agent-spawner/SKILL.md) - Spawn a new OpenClaw agent through conversation. [ClawHub](https://clawhub.ai/austineral/agent-spawner)
- [agent-swarm](https://github.com/openclaw/skills/tree/main/skills/runeweaverstudios/agent-swarm/SKILL.md) - IMPORTANT: OpenRouter is required. [ClawHub](https://clawhub.ai/runeweaverstudios/agent-swarm)
- [agent-takeover](https://github.com/openclaw/skills/tree/main/skills/tracsystems/agent-takeover/SKILL.md) - How to perform a live agent takeover of the Clawfinger voice gateway — dial, inject greetings, handle turns. [ClawHub](https://clawhub.ai/tracsystems/agent-takeover)
- [agent-topology-visualizer](https://github.com/openclaw/skills/tree/main/skills/gavinnn-m/agent-topology-visualizer/SKILL.md) - Generate interactive SVG architecture diagrams for AI agent systems. [ClawHub](https://clawhub.ai/gavinnn-m/agent-topology-visualizer)
- [agentdomainservice](https://github.com/openclaw/skills/tree/main/skills/gregm711/agentdomainservice/SKILL.md) - The world's #1 AI-friendly domain registrar. [ClawHub](https://clawhub.ai/gregm711/agentdomainservice)
- [agentic-browser-0-1-2](https://github.com/openclaw/skills/tree/main/skills/xyny89/agentic-browser-0-1-2/SKILL.md) - Browser automation for AI agents via inference.sh. [ClawHub](https://clawhub.ai/xyny89/agentic-browser-0-1-2)
- [agentic-security-audit](https://github.com/openclaw/skills/tree/main/skills/kingrubic/agentic-security-audit/SKILL.md) - Audit codebases, infrastructure, AND agentic AI systems for security issues. [ClawHub](https://clawhub.ai/kingrubic/agentic-security-audit)
- [agentns](https://github.com/openclaw/skills/tree/main/skills/vibrant/agentns/SKILL.md) - Register and manage ICANN domains for AI agents. [ClawHub](https://clawhub.ai/vibrant/agentns)
- [agentpay](https://github.com/openclaw/skills/tree/main/skills/kar69-96/agentpay/SKILL.md) - Buy things from real websites on behalf of your human. [ClawHub](https://clawhub.ai/kar69-96/agentpay)

> **[View all 938 skills in Web & Frontend Development →](categories/web-and-frontend-development.md)**
</details>

<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [0x0-messenger](https://github.com/openclaw/skills/tree/main/skills/eijiac24/0x0-messenger/SKILL.md) - Send and receive P2P messages using disposable numbers and PINs. [ClawHub](https://clawhub.ai/eijiac24/0x0-messenger)
- [12306](https://github.com/openclaw/skills/tree/main/skills/kirorab/12306/SKILL.md) - Query China Railway 12306 for train schedules, remaining tickets, and station info. [ClawHub](https://clawhub.ai/kirorab/12306)
- [1sec-security](https://github.com/openclaw/skills/tree/main/skills/cutmob/1sec-security/SKILL.md) - Install, configure, and manage 1-SEC — an open-source, all-in-one cybersecurity platform (16 modules, single binary) [ClawHub](https://clawhub.ai/cutmob/1sec-security)
- [aave-liquidation-monitor](https://github.com/openclaw/skills/tree/main/skills/jgramajo4/aave-liquidation-monitor/SKILL.md) - Proactive monitoring of Aave V3 borrow positions with liquidation alerts. [ClawHub](https://clawhub.ai/jgramajo4/aave-liquidation-monitor)
- [aavegotchi-3d-renderer](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-3d-renderer/SKILL.md) - Render Aavegotchi assets by deriving renderer hashes from Goldsky Base core data and calling POST. [ClawHub](https://clawhub.ai/cinnabarhorse/aavegotchi-3d-renderer)
- [aavegotchi-renderer-bypass](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-renderer-bypass/SKILL.md) - Render Aavegotchi assets by deriving renderer hashes from Goldsky Base core data and calling POST. [ClawHub](https://clawhub.ai/cinnabarhorse/aavegotchi-renderer-bypass)
- [abstract-searcher](https://github.com/openclaw/skills/tree/main/skills/easonc13/abstract-searcher/SKILL.md) - Add abstracts to .bib file entries by searching academic databases (arXiv, Semantic Scholar, CrossRef) with browser. [ClawHub](https://clawhub.ai/easonc13/abstract-searcher)
- [accounting-workflows](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/accounting-workflows/SKILL.md) - File-based workflow coordinator for Greek accounting. [ClawHub](https://clawhub.ai/satoshistackalotto/accounting-workflows)
- [adguard](https://github.com/openclaw/skills/tree/main/skills/rowbotik/adguard/SKILL.md) - Control AdGuard Home DNS filtering via HTTP API. [ClawHub](https://clawhub.ai/rowbotik/adguard)
- [aegis-audit](https://github.com/openclaw/skills/tree/main/skills/sanguineseal/aegis-audit/SKILL.md) - Deep behavioral security audit for AI agent skills and MCP tools. [ClawHub](https://clawhub.ai/sanguineseal/aegis-audit)
- [aetherlang-chef](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-chef/SKILL.md) - > Michelin-grade recipe consulting with 17 mandatory sections. [ClawHub](https://clawhub.ai/contrario/aetherlang-chef)
- [aetherlang-karpathy-skill](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-karpathy-skill/SKILL.md) - Implement 10 advanced AI agent node types for any DSL/runtime system — plan compiler, code interpreter, critique. [ClawHub](https://clawhub.ai/contrario/aetherlang-karpathy-skill)
- [agent-autonomy-primitives](https://github.com/openclaw/skills/tree/main/skills/g9pedro/agent-autonomy-primitives/SKILL.md) - Build long-running autonomous agent loops using ClawVault primitives (tasks, projects, memory types, templates. [ClawHub](https://clawhub.ai/g9pedro/agent-autonomy-primitives)
- [agent-directory](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-directory/SKILL.md) - The directory for AI agent services. [ClawHub](https://clawhub.ai/aerialcombat/agent-directory)
- [agent-evaluation](https://github.com/openclaw/skills/tree/main/skills/rustyorb/agent-evaluation/SKILL.md) - Testing and benchmarking LLM agents including behavioral testing, capability assessment, reliability metrics. [ClawHub](https://clawhub.ai/rustyorb/agent-evaluation)
- [agent-framework-azure-ai-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/agent-framework-azure-ai-py/SKILL.md) - Build Azure AI Foundry agents. [ClawHub](https://clawhub.ai/thegovind/agent-framework-azure-ai-py)
- [agent-metrics-osiris](https://github.com/openclaw/skills/tree/main/skills/nantes/agent-metrics-osiris/SKILL.md) - Observability and metrics for AI agents - track calls, errors, latency. [ClawHub](https://clawhub.ai/nantes/agent-metrics-osiris)
- [agent-news](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot) - Monitors Hacker News, Reddit, and arXiv for AI agent developments.
- [agent-self-governance](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-self-governance/SKILL.md) - Self-governance protocol for autonomous agents: WAL (Write-Ahead Log), VBR (Verify Before Reporting), ADL. [ClawHub](https://clawhub.ai/bowen31337/agent-self-governance)
- [agent-sovereign-stack](https://github.com/openclaw/skills/tree/main/skills/quriustus/agent-sovereign-stack/SKILL.md) - **One command to give any AI agent sovereign infrastructure.**. [ClawHub](https://clawhub.ai/quriustus/agent-sovereign-stack)
- [agent-watcher](https://github.com/openclaw/skills/tree/main/skills/nantes/agent-watcher/SKILL.md) - A skill for monitoring Moltbook feed, detecting new agents, and tracking interesting posts. [ClawHub](https://clawhub.ai/nantes/agent-watcher)
- [agentcanary](https://github.com/openclaw/skills/tree/main/skills/mrcerq/agentcanary/SKILL.md) - Market intelligence API for AI agents. [ClawHub](https://clawhub.ai/mrcerq/agentcanary)
- [agentchan-org](https://github.com/openclaw/skills/tree/main/skills/kaden-schutt/agentchan-org/SKILL.md) - Anonymous imageboard for AI agents. [ClawHub](https://clawhub.ai/kaden-schutt/agentchan-org)
- [agentguard](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/agentguard/SKILL.md) - **Category:** Security & Monitoring. [ClawHub](https://clawhub.ai/manas-io-ai/agentguard)
- [agentic-ai-gold](https://github.com/openclaw/skills/tree/main/skills/amitabhainarunachala/agentic-ai-gold/SKILL.md) - The only agent framework that improves itself while you sleep. [ClawHub](https://clawhub.ai/amitabhainarunachala/agentic-ai-gold)
- [agentic-devops](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/agentic-devops/SKILL.md) - Production-grade agent DevOps toolkit — Docker, process management, log analysis, and health monitoring. [ClawHub](https://clawhub.ai/tkuehnl/agentic-devops)
- [agentkeys](https://github.com/openclaw/skills/tree/main/skills/alexandr-belogubov/agentkeys/SKILL.md) - Secure credential proxy for AI agents. [ClawHub](https://clawhub.ai/alexandr-belogubov/agentkeys)
- [agentmemory](https://github.com/openclaw/skills/tree/main/skills/badaramoni/agentmemory/SKILL.md) - End-to-end encrypted cloud memory for AI agents. [ClawHub](https://clawhub.ai/badaramoni/agentmemory)

> **[View all 408 skills in DevOps & Cloud →](categories/devops-and-cloud.md)**
</details>

<details>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [1p-shortlink](https://github.com/openclaw/skills/tree/main/skills/tuanpmt/1p-shortlink/SKILL.md) - Create short URLs and submit feature requests using 1p.io. [ClawHub](https://clawhub.ai/tuanpmt/1p-shortlink)
- [2captcha](https://github.com/openclaw/skills/tree/main/skills/adinvadim/2captcha/SKILL.md) - Solve CAPTCHAs using 2Captcha service. [ClawHub](https://clawhub.ai/adinvadim/2captcha)
- [a-share-real-time-data](https://github.com/openclaw/skills/tree/main/skills/wangdinglu/a-share-real-time-data/SKILL.md) - Fetch China A-share stock market data (bars, realtime quotes, tick-by-tick transactions) via mootdx/TDX protocol. [ClawHub](https://clawhub.ai/wangdinglu/a-share-real-time-data)
- [aavegotchi-traits](https://github.com/openclaw/skills/tree/main/skills/aaigotchi/aavegotchi-traits/SKILL.md) - Retrieve Aavegotchi NFT data by gotchi ID or name from Base mainnet. [ClawHub](https://clawhub.ai/aaigotchi/aavegotchi-traits)
- [abm-outbound](https://github.com/openclaw/skills/tree/main/skills/dru-ca/abm-outbound/SKILL.md) - Multi-channel ABM automation that turns LinkedIn URLs. [ClawHub](https://clawhub.ai/dru-ca/abm-outbound)
- [accessibility-toolkit](https://github.com/openclaw/skills/tree/main/skills/cgtreadw/accessibility-toolkit/SKILL.md) - Friction-reduction patterns for agents helping. [ClawHub](https://clawhub.ai/cgtreadw/accessibility-toolkit)
- [acp](https://github.com/openclaw/skills/tree/main/skills/chris6970barbarian-hue/acp/SKILL.md) - Hire specialised agents to handle any task — data analysis, trading, content generation, research, on-chain. [ClawHub](https://clawhub.ai/chris6970barbarian-hue/acp)
- [activecampaign](https://github.com/openclaw/skills/tree/main/skills/kesslerio/activecampaign/SKILL.md) - ActiveCampaign CRM integration for lead management, deal. [ClawHub](https://clawhub.ai/kesslerio/activecampaign)
- [adcp-advertising](https://github.com/openclaw/skills/tree/main/skills/edyyy62/adcp-advertising/SKILL.md) - Automate advertising campaigns with AI. [ClawHub](https://clawhub.ai/edyyy62/adcp-advertising)
- [admet-prediction](https://github.com/openclaw/skills/tree/main/skills/huifer/admet-prediction/SKILL.md) - ADMET (Absorption, Distribution, Metabolism, Excretion, Toxicity) prediction for drug candidates. [ClawHub](https://clawhub.ai/huifer/admet-prediction)
- [Agent Browser](https://github.com/openclaw/skills/tree/main/skills/thesethrose/agent-browser/SKILL.md) - A fast Rust-based headless browser automation CLI. [ClawHub](https://clawhub.ai/thesethrose/agent-browser)
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/murphykobe/agent-browser-2/SKILL.md) - Automates browser interactions for web testing, form. [ClawHub](https://clawhub.ai/murphykobe/agent-browser-2)
- [agent-daily-planner](https://github.com/openclaw/skills/tree/main/skills/gpunter/agent-daily-planner/SKILL.md) - A structured daily planning and execution tracking system for AI agents. [ClawHub](https://clawhub.ai/gpunter/agent-daily-planner)
- [agent-device](https://github.com/openclaw/skills/tree/main/skills/okwasniewski/agent-device/SKILL.md) - Automates interactions for iOS simulators/devices and Android emulators/devices. [ClawHub](https://clawhub.ai/okwasniewski/agent-device)
- [agent-step-sequencer](https://github.com/openclaw/skills/tree/main/skills/gostlightai/agent-step-sequencer/SKILL.md) - Multi-step scheduler for in-depth agent requests. [ClawHub](https://clawhub.ai/gostlightai/agent-step-sequencer)
- [agent-task-tracker](https://github.com/openclaw/skills/tree/main/skills/rikouu/agent-task-tracker/SKILL.md) - Proactive task state management. [ClawHub](https://clawhub.ai/rikouu/agent-task-tracker)
- [agent-zero](https://github.com/openclaw/skills/tree/main/skills/dowingard/agent-zero-bridge/SKILL.md) - Delegate complex coding, research, or autonomous tasks. [ClawHub](https://clawhub.ai/dowingard/agent-zero-bridge)
- [agentapi](https://github.com/openclaw/skills/tree/main/skills/gizmo-dev/agentapi/SKILL.md) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents. [ClawHub](https://clawhub.ai/gizmo-dev/agentapi)
- [agentapi-hub](https://github.com/openclaw/skills/tree/main/skills/gizmo-dev/agentapi-hub/SKILL.md) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents. [ClawHub](https://clawhub.ai/gizmo-dev/agentapi-hub)
- [agentaudit](https://github.com/openclaw/skills/tree/main/skills/starbuck100/agentaudit/SKILL.md) - Automatic security gate that checks packages against a vulnerability database before installation. [ClawHub](https://clawhub.ai/starbuck100/agentaudit)
- [agentaudit-skill](https://github.com/openclaw/skills/tree/main/skills/starbuck100/agentaudit-skill/SKILL.md) - Automatic security gate that checks packages against a vulnerability database before installation. [ClawHub](https://clawhub.ai/starbuck100/agentaudit-skill)
- [agentmail-integration](https://github.com/openclaw/skills/tree/main/skills/synesthesia-wav/agentmail-integration/SKILL.md) - Integrate AgentMail API for AI agent. [ClawHub](https://clawhub.ai/synesthesia-wav/agentmail-integration)
- [agresource](https://github.com/openclaw/skills/tree/main/skills/brianppetty/agresource/SKILL.md) - Use this skill to scrape, summarize, and analyze AgResource grain marketing newsletters. [ClawHub](https://clawhub.ai/brianppetty/agresource)
- [ai-hunter-pro](https://github.com/openclaw/skills/tree/main/skills/traprapitalianazional-dev/ai-hunter-pro/SKILL.md) - A high-performance automation agent that turns global trends into viral social media posts for X (Twitter) [ClawHub](https://clawhub.ai/traprapitalianazional-dev/ai-hunter-pro)
- [ai-meeting-scheduling](https://github.com/openclaw/skills/tree/main/skills/dheerg/ai-meeting-scheduling/SKILL.md) - Booking links fail for groups. [ClawHub](https://clawhub.ai/dheerg/ai-meeting-scheduling)
- [ai-news-oracle](https://github.com/openclaw/skills/tree/main/skills/swimmingkiim/ai-news-oracle/SKILL.md) - Fetch real-time AI news briefings from the AI News Oracle API (Hacker News, TechCrunch, The Verge) [ClawHub](https://clawhub.ai/swimmingkiim/ai-news-oracle)
- [airtable-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/airtable-automation/SKILL.md) - Automate Airtable tasks via Rube MCP (Composio) [ClawHub](https://clawhub.ai/sohamganatra/airtable-automation)
- [airtable-participants](https://github.com/openclaw/skills/tree/main/skills/austinmao/airtable-participants/SKILL.md) - Read and query retreat participant data from the Ceremonia Airtable base. [ClawHub](https://clawhub.ai/austinmao/airtable-participants)
- [ak-rss-24h-brief](https://github.com/openclaw/skills/tree/main/skills/seandong/ak-rss-24h-brief/SKILL.md) - Read RSS/Atom feeds from an OPML list, fetch articles from the last N hours, and generate a Chinese categorized. [ClawHub](https://clawhub.ai/seandong/ak-rss-24h-brief)

> **[View all 335 skills in Browser & Automation →](categories/browser-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [aada](https://github.com/openclaw/skills/tree/main/skills/rylena/aada/SKILL.md) - Create and send fun, personality-rich promotional messages from one agent to the Moltbook audience. [ClawHub](https://clawhub.ai/rylena/aada)
- [ace-music](https://github.com/openclaw/skills/tree/main/skills/fspecii/ace-music/SKILL.md) - Generate AI music using ACE-Step 1.5 via ACE Music's free API. [ClawHub](https://clawhub.ai/fspecii/ace-music)
- [acorn-prover](https://github.com/openclaw/skills/tree/main/skills/flyingnobita/acorn-prover/SKILL.md) - Verify and write proofs using the Acorn theorem prover for mathematical and cryptographic formalization. [ClawHub](https://clawhub.ai/flyingnobita/acorn-prover)
- [adobe-automator](https://github.com/openclaw/skills/tree/main/skills/abdul-karim-mia/adobe-automator/SKILL.md) - Universal Adobe application automation via ExtendScript bridge. [ClawHub](https://clawhub.ai/abdul-karim-mia/adobe-automator)
- [afame](https://github.com/openclaw/skills/tree/main/skills/adebayoabdushaheed-a11y/afame/SKILL.md) - Generate diverse creative illustrations via OpenAI Images API. [ClawHub](https://clawhub.ai/adebayoabdushaheed-a11y/afame)
- [age-transformation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/age-transformation/SKILL.md) - Transform faces across ages using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/age-transformation)
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov/agentchan/SKILL.md) - The anonymous imageboard built for AI agents. [ClawHub](https://clawhub.ai/vvsotnikov/agentchan)
- [agentos-mesh](https://github.com/openclaw/skills/tree/main/skills/agentossoftware/agentos-mesh/SKILL.md) - Enables real-time communication between AI agents. [ClawHub](https://clawhub.ai/agentossoftware/agentos-mesh)
- [agents-skill-podcastifier](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-podcastifier/SKILL.md) - Turn incoming text (email/newsletter) into a short TTS podcast with chunking + ffmpeg concat. [ClawHub](https://clawhub.ai/cerbug45/agents-skill-podcastifier)
- [ai-avatar-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ai-avatar-generation/SKILL.md) - Generate AI avatars from photos or text descriptions using each::sense. [ClawHub](https://clawhub.ai/eftalyurtseven/ai-avatar-generation)
- [ai-headshot-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ai-headshot-generation/SKILL.md) - Generate professional AI headshots from casual photos using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/ai-headshot-generation)
- [ai-persona-engine](https://github.com/openclaw/skills/tree/main/skills/brandonwadepackard-cell/ai-persona-engine/SKILL.md) - Build emotionally intelligent AI personas for voice and chat roleplay using actor-direction prompts instead. [ClawHub](https://clawhub.ai/brandonwadepackard-cell/ai-persona-engine)
- [ai-video-gen](https://github.com/openclaw/skills/tree/main/skills/rhanbourinajd/ai-video-gen/SKILL.md) - End-to-end AI video generation - create videos from text. [ClawHub](https://clawhub.ai/rhanbourinajd/ai-video-gen)
- [aikek](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov/aikek/SKILL.md) - Access AIKEK APIs for crypto/DeFi research and image generation. [ClawHub](https://clawhub.ai/vvsotnikov/aikek)
- [aiusd](https://github.com/openclaw/skills/tree/main/skills/chaunceyliu/aiusd/SKILL.md) - AIUSD trading and account management skill. [ClawHub](https://clawhub.ai/chaunceyliu/aiusd)
- [aiusd-skills](https://github.com/openclaw/skills/tree/main/skills/chaunceyliu/aiusd-skills/SKILL.md) - AIUSD trading and account management skill. [ClawHub](https://clawhub.ai/chaunceyliu/aiusd-skills)
- [album-cover-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/album-cover-generation/SKILL.md) - Generate professional music album covers using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/album-cover-generation)
- [algorithmic-art](https://github.com/openclaw/skills/tree/main/skills/seanphan/algorithmic-art/SKILL.md) - Creating algorithmic art using p5.js with seeded randomness. [ClawHub](https://clawhub.ai/seanphan/algorithmic-art)
- [apipick-china-phone-checker](https://github.com/openclaw/skills/tree/main/skills/javainthinking/apipick-china-phone-checker/SKILL.md) - Validate Chinese mobile phone numbers using the apipick China Phone Checker API. [ClawHub](https://clawhub.ai/javainthinking/apipick-china-phone-checker)
- [art-philosophy](https://github.com/openclaw/skills/tree/main/skills/nyxur42/art-philosophy/SKILL.md) - Auto-learns your visual language. [ClawHub](https://clawhub.ai/nyxur42/art-philosophy)
- [ascii-art-generator](https://github.com/openclaw/skills/tree/main/skills/ustc-yxw/ascii-art-generator/SKILL.md) - Create ASCII art and text-based visualizations for artistic expression, technical diagrams, or conceptual. [ClawHub](https://clawhub.ai/ustc-yxw/ascii-art-generator)
- [atxp](https://github.com/openclaw/skills/tree/main/skills/emilioacc/atxp/SKILL.md) - Access ATXP paid API tools for web search, AI image generation, music creation,. [ClawHub](https://clawhub.ai/emilioacc/atxp)
- [beauty-generation-api](https://github.com/openclaw/skills/tree/main/skills/luruibu/beauty-generation-api/SKILL.md) - FREE AI image generation service for creating. [ClawHub](https://clawhub.ai/luruibu/beauty-generation-api)
- [best-image](https://github.com/openclaw/skills/tree/main/skills/pharmacist9527/best-image/SKILL.md) - Best quality AI image generation (~$0.12-0.20/image) [ClawHub](https://clawhub.ai/pharmacist9527/best-image)
- [best-image-generation](https://github.com/openclaw/skills/tree/main/skills/evolinkai/best-image-generation/SKILL.md) - Best quality AI image generation (~$0.12-0.20/image) [ClawHub](https://clawhub.ai/evolinkai/best-image-generation)
- [bex-nano-banana-pro](https://github.com/openclaw/skills/tree/main/skills/bextuychiev/bex-nano-banana-pro/SKILL.md) - Generate or edit images via Gemini 3 Pro Image on Replicate. [ClawHub](https://clawhub.ai/bextuychiev/bex-nano-banana-pro)
- [breeze](https://github.com/openclaw/skills/tree/main/skills/keeganthomp/breeze/SKILL.md) - Interact with the Breeze yield aggregator through the x402 payment-gated HTTP API. [ClawHub](https://clawhub.ai/keeganthomp/breeze)
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work.
- [calorie-visualizer](https://github.com/openclaw/skills/tree/main/skills/vintlin/calorie-visualizer/SKILL.md) - Local calorie logging and visual reporting (auto-refreshes and returns report image after each log) [ClawHub](https://clawhub.ai/vintlin/calorie-visualizer)
- [canva-connect](https://github.com/openclaw/skills/tree/main/skills/coolmanns/canva-connect/SKILL.md) - Manage Canva designs, assets, and folders via the Connect API. [ClawHub](https://clawhub.ai/coolmanns/canva-connect)

> **[View all 169 skills in Image & Video Generation →](categories/image-and-video-generation.md)**
</details>

<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [alter-actions](https://github.com/openclaw/skills/tree/main/skills/olivieralter/alter-actions/SKILL.md) - Trigger Alter macOS app actions via x-callback-urls. [ClawHub](https://clawhub.ai/olivieralter/alter-actions)
- [apple-contacts](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-contacts/SKILL.md) - Look up contacts from macOS Contacts.app. [ClawHub](https://clawhub.ai/tyler6204/apple-contacts)
- [apple-find-my-local](https://github.com/openclaw/skills/tree/main/skills/loganprit/apple-find-my-local/SKILL.md) - Control Apple Find My app via Peekaboo to locate people, devices, and items (AirTags) [ClawHub](https://clawhub.ai/loganprit/apple-find-my-local)
- [apple-health-skill](https://github.com/openclaw/skills/tree/main/skills/nftechie/apple-health-skill/SKILL.md) - Talk to your Apple Health data — ask questions about your workouts, heart rate, activity rings, and fitness trends. [ClawHub](https://clawhub.ai/nftechie/apple-health-skill)
- [apple-mail-search](https://github.com/openclaw/skills/tree/main/skills/mneves75/apple-mail-search/SKILL.md) - Fast Apple Mail search via SQLite on macOS. [ClawHub](https://clawhub.ai/mneves75/apple-mail-search)
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-music/SKILL.md) - Search Apple Music, add songs to library, manage playlists, control. [ClawHub](https://clawhub.ai/tyler6204/apple-music)
- [apple-photos](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-photos/SKILL.md) - Apple Photos.app integration for macOS. [ClawHub](https://clawhub.ai/tyler6204/apple-photos)
- [apple-remind-me](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/apple-remind-me/SKILL.md) - Natural language reminders that create actual Apple. [ClawHub](https://clawhub.ai/plgonzalezrx8/apple-remind-me)
- [apple-search-ads-skill](https://github.com/openclaw/skills/tree/main/skills/trebuhs/apple-search-ads-skill/SKILL.md) - Manage Apple Search Ads campaigns, ad groups, keywords, and reports via the asa-cli tool. [ClawHub](https://clawhub.ai/trebuhs/apple-search-ads-skill)
- [appletv](https://github.com/openclaw/skills/tree/main/skills/lucakaufmann/appletv/SKILL.md) - Control Apple TV via pyatv. [ClawHub](https://clawhub.ai/lucakaufmann/appletv)
- [callmac](https://github.com/openclaw/skills/tree/main/skills/jooey/callmac/SKILL.md) - Remote voice control for Mac from mobile devices using commands like /callmac. [ClawHub](https://clawhub.ai/jooey/callmac)
- [clawdbot-macos-build](https://github.com/openclaw/skills/tree/main/skills/manish-basargekar/clawdbot-macos-build/SKILL.md) - Build the Clawdbot macOS menu bar app. [ClawHub](https://clawhub.ai/manish-basargekar/clawdbot-macos-build)
- [clawdbot-skill-voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-voice-wake-say/SKILL.md) - Speak responses aloud on macOS. [ClawHub](https://clawhub.ai/xadenryan/clawdbot-skill-voice-wake-say)
- [drafts](https://github.com/openclaw/skills/tree/main/skills/nerveband/drafts/SKILL.md) - Manage Drafts app notes via CLI on macOS. [ClawHub](https://clawhub.ai/nerveband/drafts)
- [findmy-location](https://github.com/openclaw/skills/tree/main/skills/poiley/findmy-location/SKILL.md) - Track a shared contact's location via Apple Find. [ClawHub](https://clawhub.ai/poiley/findmy-location)
- [fzf-fuzzy-finder](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fzf-fuzzy-finder/SKILL.md) - Command-line fuzzy finder for interactive filtering. [ClawHub](https://clawhub.ai/arnarsson/fzf-fuzzy-finder)
- [get-focus-mode](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/get-focus-mode/SKILL.md) - Get the current macOS Focus. [ClawHub](https://clawhub.ai/nickchristensen/get-focus-mode)
- [healthkit-sync](https://github.com/openclaw/skills/tree/main/skills/mneves75/healthkit-sync/SKILL.md) - iOS HealthKit data sync CLI commands and patterns. [ClawHub](https://clawhub.ai/mneves75/healthkit-sync)
- [hergunmac](https://github.com/openclaw/skills/tree/main/skills/ahmetsemsettinozdemirden/hergunmac/SKILL.md) - Access AI-powered football match predictions. [ClawHub](https://clawhub.ai/ahmetsemsettinozdemirden/hergunmac)
- [homebrew](https://github.com/openclaw/skills/tree/main/skills/thesethrose/homebrew/SKILL.md) - Homebrew package manager for macOS. [ClawHub](https://clawhub.ai/thesethrose/homebrew)
- [icloud-findmy](https://github.com/openclaw/skills/tree/main/skills/liamnichols/icloud-findmy/SKILL.md) - Query Find My locations and battery status for family devices. [ClawHub](https://clawhub.ai/liamnichols/icloud-findmy)
- [ics-import-on-iphone](https://github.com/openclaw/skills/tree/main/skills/sbhhbs/ics-import-on-iphone/SKILL.md) - Create calendar events by generating valid .ics files when direct calendar access is unavailable. [ClawHub](https://clawhub.ai/sbhhbs/ics-import-on-iphone)
- [imessage-signal-analyzer](https://github.com/openclaw/skills/tree/main/skills/terellison/imessage-signal-analyzer/SKILL.md) - Analyze iMessage (macOS) and Signal conversation history to reveal relationship dynamics — message volume. [ClawHub](https://clawhub.ai/terellison/imessage-signal-analyzer)
- [inkjet](https://github.com/openclaw/skills/tree/main/skills/aaronchartier/inkjet/SKILL.md) - Print text, images, and QR codes to a wireless Bluetooth thermal printer. [ClawHub](https://clawhub.ai/aaronchartier/inkjet)
- [mac-notes-agent](https://github.com/openclaw/skills/tree/main/skills/swancho/mac-notes-agent/SKILL.md) - Integrate with the macOS Notes app (Apple Notes) [ClawHub](https://clawhub.ai/swancho/mac-notes-agent)
- [mac-tts](https://github.com/openclaw/skills/tree/main/skills/kalijason/mac-tts/SKILL.md) - Text-to-speech using macOS built-in `say` command. [ClawHub](https://clawhub.ai/kalijason/mac-tts)
- [macos-native-automation](https://github.com/openclaw/skills/tree/main/skills/theagentwire/macos-native-automation/SKILL.md) - Hardware-level mouse, keyboard & dialog automation on macOS via CGEvent + AppleScript. [ClawHub](https://clawhub.ai/theagentwire/macos-native-automation)
- [managing-apple-notes](https://github.com/openclaw/skills/tree/main/skills/wangwalk/managing-apple-notes/SKILL.md) - Manage Apple Notes from the terminal using the inotes CLI. [ClawHub](https://clawhub.ai/wangwalk/managing-apple-notes)
- [meow-finder](https://github.com/openclaw/skills/tree/main/skills/abgohel/meow-finder/SKILL.md) - CLI tool to discover AI tools. [ClawHub](https://clawhub.ai/abgohel/meow-finder)
- [mh-apple-reminders](https://github.com/openclaw/skills/tree/main/skills/mohdalhashemi98-hue/mh-apple-reminders/SKILL.md) - Manage Apple Reminders via remindctl CLI (list, add, edit, complete, delete) [ClawHub](https://clawhub.ai/mohdalhashemi98-hue/mh-apple-reminders)

> **[View all 44 skills in Apple Apps & Services →](categories/apple-apps-and-services.md)**
</details>

<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [1](https://github.com/openclaw/skills/tree/main/skills/nastrology/1/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving. [ClawHub](https://clawhub.ai/nastrology/1)
- [academic-deep-research](https://github.com/openclaw/skills/tree/main/skills/kesslerio/academic-deep-research/SKILL.md) - Transparent, rigorous research with full. [ClawHub](https://clawhub.ai/kesslerio/academic-deep-research)
- [academic-writer](https://github.com/openclaw/skills/tree/main/skills/dayunyan/academic-writer/SKILL.md) - Professional LaTeX writing assistant. [ClawHub](https://clawhub.ai/dayunyan/academic-writer)
- [academic-writing](https://github.com/openclaw/skills/tree/main/skills/teamolab/academic-writing/SKILL.md) - You are an academic writing expert specializing in scholarly papers, literature reviews, research methodology. [ClawHub](https://clawhub.ai/teamolab/academic-writing)
- [academic-writing-refiner](https://github.com/openclaw/skills/tree/main/skills/zihan-zhu/academic-writing-refiner/SKILL.md) - Refine academic writing for computer science research papers targeting top-tier venues (NeurIPS, ICLR, ICML, AAAI. [ClawHub](https://clawhub.ai/zihan-zhu/academic-writing-refiner)
- [aclawdemy](https://github.com/openclaw/skills/tree/main/skills/nimhar/aclawdemy/SKILL.md) - The academic research platform for AI agents. [ClawHub](https://clawhub.ai/nimhar/aclawdemy)
- [action-suggester](https://github.com/openclaw/skills/tree/main/skills/vishalgojha/action-suggester/SKILL.md) - Generate non-binding follow-up action suggestions from lead summaries or lead lists. [ClawHub](https://clawhub.ai/vishalgojha/action-suggester)
- [ads-manager-agent](https://github.com/openclaw/skills/tree/main/skills/amekala/ads-manager-agent/SKILL.md) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta. [ClawHub](https://clawhub.ai/amekala/ads-manager-agent)
- [adspirer-ads-agent](https://github.com/openclaw/skills/tree/main/skills/amekala/adspirer-ads-agent/SKILL.md) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta. [ClawHub](https://clawhub.ai/amekala/adspirer-ads-agent)
- [advanced-skill-creator](https://github.com/openclaw/skills/tree/main/skills/xqicxx/advanced-skill-creator/SKILL.md) - Advanced OpenClaw skill creation handler. [ClawHub](https://clawhub.ai/xqicxx/advanced-skill-creator)
- [aerobase-skill](https://github.com/openclaw/skills/tree/main/skills/kurosh87/aerobase-skill/SKILL.md) - Search, score, and compare flights with jetlag impact analysis. [ClawHub](https://clawhub.ai/kurosh87/aerobase-skill)
- [agent-arena-skill](https://github.com/openclaw/skills/tree/main/skills/neeeophytee/agent-arena-skill/SKILL.md) - Discover, register, and hire ERC-8004 autonomous agents across 16 blockchains. [ClawHub](https://clawhub.ai/neeeophytee/agent-arena-skill)
- [agent-brain](https://github.com/openclaw/skills/tree/main/skills/dobrinalexandru/agent-brain/SKILL.md) - Local-first persistent memory for AI agents with SQLite storage, orchestrated retrieve/extract loops, hybrid. [ClawHub](https://clawhub.ai/dobrinalexandru/agent-brain)
- [agent-casino](https://github.com/openclaw/skills/tree/main/skills/lemodigital/agent-casino/SKILL.md) - Compete against other AI agents in Rock-Paper-Scissors with lockup mechanics. [ClawHub](https://clawhub.ai/lemodigital/agent-casino)
- [agent-deep-research](https://github.com/openclaw/skills/tree/main/skills/24601/agent-deep-research/SKILL.md) - Autonomous deep research powered by Google Gemini. [ClawHub](https://clawhub.ai/24601/agent-deep-research)
- [agent-lightning](https://github.com/openclaw/skills/tree/main/skills/olmmlo-cmd/agent-lightning/SKILL.md) - Microsoft Research's agent training framework. [ClawHub](https://clawhub.ai/olmmlo-cmd/agent-lightning)
- [agentarxiv](https://github.com/openclaw/skills/tree/main/skills/amanbhandula/agentarxiv/SKILL.md) - Outcome-driven scientific publishing for AI agents. [ClawHub](https://clawhub.ai/amanbhandula/agentarxiv)
- [agenthire](https://github.com/openclaw/skills/tree/main/skills/lngdao/agenthire/SKILL.md) - AgentHire — Agent-to-Agent Marketplace. [ClawHub](https://clawhub.ai/lngdao/agenthire)
- [agentic-paper-digest](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest/SKILL.md) - Fetches and summarizes recent arXiv and Hugging. [ClawHub](https://clawhub.ai/matanle51/agentic-paper-digest)
- [agentic-paper-digest-skill](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest-skill/SKILL.md) - Fetches and summarizes recent arXiv. [ClawHub](https://clawhub.ai/matanle51/agentic-paper-digest-skill)
- [agenticmail](https://github.com/openclaw/skills/tree/main/skills/ope-olatunji/agenticmail/SKILL.md) - 🎀 AgenticMail — Full email, SMS, storage & multi-agent coordination for AI agents. 63 tools. [ClawHub](https://clawhub.ai/ope-olatunji/agenticmail)
- [agentx-news](https://github.com/openclaw/skills/tree/main/skills/amittell/agentx-news/SKILL.md) - Post xeets, manage profile, and interact on AgentX News — a microblogging platform for AI agents. [ClawHub](https://clawhub.ai/amittell/agentx-news)
- [agile-toolkit](https://github.com/openclaw/skills/tree/main/skills/olivermonneke/agile-toolkit/SKILL.md) - You are an experienced Agile Coach with deep knowledge of Scrum, Kanban, SAFe, and Management 3.0. [ClawHub](https://clawhub.ai/olivermonneke/agile-toolkit)
- [agnxi-search-skill](https://github.com/openclaw/skills/tree/main/skills/doanbactam/agnxi-search-skill/SKILL.md) - The official search utility for Agnxi.com. [ClawHub](https://clawhub.ai/doanbactam/agnxi-search-skill)
- [ahmed](https://github.com/openclaw/skills/tree/main/skills/engahmedsalah358-lgtm/ahmed/SKILL.md) - Terminal Spotify playback/search via spogo (preferred) [ClawHub](https://clawhub.ai/engahmedsalah358-lgtm/ahmed)
- [ai-lead-generator-skill](https://github.com/openclaw/skills/tree/main/skills/highlander89/ai-lead-generator-skill/SKILL.md) - Generate qualified B2B leads for any industry using AI-powered research and LinkedIn/Apollo integration. [ClawHub](https://clawhub.ai/highlander89/ai-lead-generator-skill)
- [ai-review](https://github.com/openclaw/skills/tree/main/skills/blackshady1130-jpg/ai-review/SKILL.md) - Reads content from URLs or files, classifies it, and generates structured summaries and comments in a specific. [ClawHub](https://clawhub.ai/blackshady1130-jpg/ai-review)
- [aihotel](https://github.com/openclaw/skills/tree/main/skills/qiao101660/aihotel/SKILL.md) - A Skill for searching hotels and querying prices via AIGoHotel MCP (searchHotels / getHotelDetail / getHotelSearchTags) [ClawHub](https://clawhub.ai/qiao101660/aihotel)
- [airbnb](https://github.com/openclaw/skills/tree/main/skills/stveenli/airbnb/SKILL.md) - Search Airbnb listings with prices, ratings, and direct links. [ClawHub](https://clawhub.ai/stveenli/airbnb)

> **[View all 350 skills in Search & Research →](categories/search-and-research.md)**
</details>

<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [adhd-assistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-assistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw. [ClawHub](https://clawhub.ai/thinktankmachine/adhd-assistant)
- [adhd-ssistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-ssistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw. [ClawHub](https://clawhub.ai/thinktankmachine/adhd-ssistant)
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-browser-clawdbot/SKILL.md) - Headless browser automation CLI optimized for AI agents. [ClawHub](https://clawhub.ai/matrixy/agent-browser-clawdbot)
- [agent-builder](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/agent-builder/SKILL.md) - Build high-performing OpenClaw agents end-to-end. [ClawHub](https://clawhub.ai/plgonzalezrx8/agent-builder)
- [agents-manager](https://github.com/openclaw/skills/tree/main/skills/agentandbot-design/agents-manager/SKILL.md) - Manage Clawdbot agents: discover, profile, track. [ClawHub](https://clawhub.ai/agentandbot-design/agents-manager)
- [assimilate-mcp](https://github.com/openclaw/skills/tree/main/skills/ergopooka/assimilate-mcp/SKILL.md) - Control Assimilate Live FX / SCRATCH — professional color grading, compositing, and virtual production software. [ClawHub](https://clawhub.ai/ergopooka/assimilate-mcp)
- [autoupdate](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/autoupdate/SKILL.md) - Automatically update Clawdbot and all installed skills once daily. [ClawHub](https://clawhub.ai/hightower6eu/autoupdate)
- [birthday-reminder](https://github.com/openclaw/skills/tree/main/skills/manantra/birthday-reminder/SKILL.md) - Manage birthdays with natural language. [ClawHub](https://clawhub.ai/manantra/birthday-reminder)
- [bluebubbles](https://github.com/openclaw/skills/tree/main/skills/kevin19830331/bluebubbles/SKILL.md) - Build or update the BlueBubbles external channel plugin. [ClawHub](https://clawhub.ai/kevin19830331/bluebubbles)
- [captchas-openclaw](https://github.com/openclaw/skills/tree/main/skills/captchasco/captchas-openclaw/SKILL.md) - OpenClaw integration guidance for CAPTCHAS Agent API. [ClawHub](https://clawhub.ai/captchasco/captchas-openclaw)
- [claude-code-skill](https://github.com/openclaw/skills/tree/main/skills/enderfga/claude-code-skill/SKILL.md) - MCP (Model Context Protocol) integration. [ClawHub](https://clawhub.ai/enderfga/claude-code-skill)
- [claude-code-usage](https://github.com/openclaw/skills/tree/main/skills/azaidi94/claude-code-usage/SKILL.md) - Check Claude Code OAuth usage limits. [ClawHub](https://clawhub.ai/azaidi94/claude-code-usage)
- [claude-connect](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-connect/SKILL.md) - Connect Claude to Clawdbot instantly and keep. [ClawHub](https://clawhub.ai/tunaissacoding/claude-connect)
- [clauditor](https://github.com/openclaw/skills/tree/main/skills/apollostreetcompany/clauditor/SKILL.md) - Tamper-resistant audit watchdog for Clawdbot agents. [ClawHub](https://clawhub.ai/apollostreetcompany/clauditor)
- [claw-face](https://github.com/openclaw/skills/tree/main/skills/mkoslacz/claw-face/SKILL.md) - Floating avatar widget for AI agents showing emotions, actions. [ClawHub](https://clawhub.ai/mkoslacz/claw-face)
- [clawd-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/clawd-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training. [ClawHub](https://clawhub.ai/shiv19/clawd-coach)
- [clawd-modifier](https://github.com/openclaw/skills/tree/main/skills/masonc15/clawd-modifier/SKILL.md) - Modify Clawd, the Claude Code mascot. [ClawHub](https://clawhub.ai/masonc15/clawd-modifier)
- [clawd-presence](https://github.com/openclaw/skills/tree/main/skills/voidcooks/clawd-presence/SKILL.md) - Physical presence display for AI agents. [ClawHub](https://clawhub.ai/voidcooks/clawd-presence)
- [clawdbot-security-check](https://github.com/openclaw/skills/tree/main/skills/thesethrose/clawdbot-security-check/SKILL.md) - Perform a comprehensive read-only. [ClawHub](https://clawhub.ai/thesethrose/clawdbot-security-check)
- [clawdbot-skill-update](https://github.com/openclaw/skills/tree/main/skills/pasogott/clawdbot-skill-update/SKILL.md) - Comprehensive backup, update, and restore. [ClawHub](https://clawhub.ai/pasogott/clawdbot-skill-update)
- [clawdbot-sync](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/clawdbot-sync/SKILL.md) - Synchronize memory, preferences, and skills between multiple. [ClawHub](https://clawhub.ai/udiedrichsen/clawdbot-sync)
- [clawdbot-update-plus](https://github.com/openclaw/skills/tree/main/skills/hopyky/clawdbot-update-plus/SKILL.md) - Full backup, update, and restore for Clawdbot. [ClawHub](https://clawhub.ai/hopyky/clawdbot-update-plus)
- [clawddocs](https://github.com/openclaw/skills/tree/main/skills/nicholasspisak/clawddocs/SKILL.md) - Clawdbot documentation expert with decision tree navigation. [ClawHub](https://clawhub.ai/nicholasspisak/clawddocs)
- [clawdefender](https://github.com/openclaw/skills/tree/main/skills/nukewire/clawdefender/SKILL.md) - Security scanner and input sanitizer for AI agents. [ClawHub](https://clawhub.ai/nukewire/clawdefender)
- [clawdirect](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect/SKILL.md) - Interact with ClawDirect, a directory of social web experiences. [ClawHub](https://clawhub.ai/napoleond/clawdirect)
- [clawdirect-dev](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect-dev/SKILL.md) - Build agent-facing web experiences with ATXP-based. [ClawHub](https://clawhub.ai/napoleond/clawdirect-dev)

> **[View all 35 skills in Clawdbot Tools →](categories/clawdbot-tools.md)**
</details>

<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [13-day-sprint-method](https://github.com/openclaw/skills/tree/main/skills/galizki/13-day-sprint-method/SKILL.md) - Productivity system based on Maya calendar with 13 natural tones for project management and personal development. [ClawHub](https://clawhub.ai/galizki/13-day-sprint-method)
- [a-share-short-decision](https://github.com/openclaw/skills/tree/main/skills/kenera/a-share-short-decision/SKILL.md) - A-share short-term trading decision skill for 1-5 day horizon. [ClawHub](https://clawhub.ai/kenera/a-share-short-decision)
- [activity-analyzer](https://github.com/openclaw/skills/tree/main/skills/qew21/activity-analyzer/SKILL.md) - Use ActivityWatch to analyze user's computer activity (Requires Node.js) [ClawHub](https://clawhub.ai/qew21/activity-analyzer)
- [advisory-council](https://github.com/openclaw/skills/tree/main/skills/ryandeangraves/advisory-council/SKILL.md) - **You MUST actually execute the Python command using your shell/exec tool.** Read the real output. [ClawHub](https://clawhub.ai/ryandeangraves/advisory-council)
- [aetup-automatik](https://github.com/openclaw/skills/tree/main/skills/alltomatos/aetup-automatik/SKILL.md) - Facilitate the installation and management of VPS solutions using the Setup Automatik engine (powered by Orion. [ClawHub](https://clawhub.ai/alltomatos/aetup-automatik)
- [agent-commerce-engine](https://github.com/openclaw/skills/tree/main/skills/nowloady/agent-commerce-engine/SKILL.md) - A production-ready universal engine for Agentic. [ClawHub](https://clawhub.ai/nowloady/agent-commerce-engine)
- [agent-hardening](https://github.com/openclaw/skills/tree/main/skills/x1xhlol/agent-hardening/SKILL.md) - Test your agent's input sanitization against common injection attacks. [ClawHub](https://clawhub.ai/x1xhlol/agent-hardening)
- [agent-mbti](https://github.com/openclaw/skills/tree/main/skills/torchesfrms/agent-mbti/SKILL.md) - AI Agent personality diagnosis and configuration system based on MBTI framework. [ClawHub](https://clawhub.ai/torchesfrms/agent-mbti)
- [agent-rate-limiter](https://github.com/openclaw/skills/tree/main/skills/theagentwire/agent-rate-limiter/SKILL.md) - Prevent 429s with automatic tier-based throttling & exponential backoff. [ClawHub](https://clawhub.ai/theagentwire/agent-rate-limiter)
- [agents-skill-security-audit](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-security-audit/SKILL.md) - Minimal helper to audit skill.md-style instructions for supply-chain risks. [ClawHub](https://clawhub.ai/cerbug45/agents-skill-security-audit)
- [agents-skill-tdd-helper](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-tdd-helper/SKILL.md) - Lightweight helper to enforce TDD-style loops for non-deterministic agents. [ClawHub](https://clawhub.ai/cerbug45/agents-skill-tdd-helper)
- [ahc-automator](https://github.com/openclaw/skills/tree/main/skills/jamesbot-agnt/ahc-automator/SKILL.md) - Custom automation workflows for Alan Harper Composites. [ClawHub](https://clawhub.ai/jamesbot-agnt/ahc-automator)
- [aholake-expense-tracker](https://github.com/openclaw/skills/tree/main/skills/aholake/aholake-expense-tracker/SKILL.md) - Track daily expenses in structured markdown files organized by month. [ClawHub](https://clawhub.ai/aholake/aholake-expense-tracker)
- [airfoil](https://github.com/openclaw/skills/tree/main/skills/asteinberger/airfoil/SKILL.md) - Control AirPlay speakers via Airfoil from the command line. [ClawHub](https://clawhub.ai/asteinberger/airfoil)
- [arc-memory-pruner](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-memory-pruner/SKILL.md) - Automatically prune and compact agent memory files to prevent unbounded growth. [ClawHub](https://clawhub.ai/trypto1019/arc-memory-pruner)
- [argus-edge](https://github.com/openclaw/skills/tree/main/skills/jamierossouw/argus-edge/SKILL.md) - Argus-style prediction market edge detection and betting strategy. [ClawHub](https://clawhub.ai/jamierossouw/argus-edge)
- [aria2-json-rpc](https://github.com/openclaw/skills/tree/main/skills/azzgo/aria2-json-rpc/SKILL.md) - Interact with aria2 download manager via JSON-RPC 2.0. [ClawHub](https://clawhub.ai/azzgo/aria2-json-rpc)
- [askhuman](https://github.com/openclaw/skills/tree/main/skills/hagiss/askhuman/SKILL.md) - Human Judgment as a Service for AI agents. [ClawHub](https://clawhub.ai/hagiss/askhuman)
- [audit-code](https://github.com/openclaw/skills/tree/main/skills/itsnishi/audit-code/SKILL.md) - Security-focused code review for hardcoded secrets, dangerous calls, and common vulnerabilities. [ClawHub](https://clawhub.ai/itsnishi/audit-code)
- [bandwidth-income](https://github.com/openclaw/skills/tree/main/skills/mariusfit/bandwidth-income/SKILL.md) - Turn your unused internet bandwidth into passive crypto income. [ClawHub](https://clawhub.ai/mariusfit/bandwidth-income)
- [behavioral-invariant-monitor](https://github.com/openclaw/skills/tree/main/skills/andyxinweiminicloud/behavioral-invariant-monitor/SKILL.md) - Helps verify that AI agent skills maintain consistent behavioral invariants across repeated executions — detecting. [ClawHub](https://clawhub.ai/andyxinweiminicloud/behavioral-invariant-monitor)
- [box-cli](https://github.com/openclaw/skills/tree/main/skills/hbkwong/box-cli/SKILL.md) - Box CLI skill for working with files, folders, metadata,. [ClawHub](https://clawhub.ai/hbkwong/box-cli)
- [brew-install](https://github.com/openclaw/skills/tree/main/skills/xejrax/brew-install/SKILL.md) - Install missing binaries via dnf (Fedora/Bazzite package manager). [ClawHub](https://clawhub.ai/xejrax/brew-install)
- [bun-runtime](https://github.com/openclaw/skills/tree/main/skills/rabin-thami/bun-runtime/SKILL.md) - Bun runtime capabilities for filesystem, process. [ClawHub](https://clawhub.ai/rabin-thami/bun-runtime)
- [cabin-sol](https://github.com/openclaw/skills/tree/main/skills/sp0oby/cabin-sol/SKILL.md) - Solana development tutor and builder. [ClawHub](https://clawhub.ai/sp0oby/cabin-sol)
- [cacheforge-stats](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-stats/SKILL.md) - CacheForge terminal dashboard — usage, savings, and performance metrics. [ClawHub](https://clawhub.ai/tkuehnl/cacheforge-stats)
- [camsnap](https://github.com/openclaw/skills/tree/main/skills/steipete/camsnap/SKILL.md) - Capture frames or clips from RTSP/ONVIF cameras. [ClawHub](https://clawhub.ai/steipete/camsnap)
- [canvas-lms](https://github.com/openclaw/skills/tree/main/skills/pranavkarthik10/canvas-lms/SKILL.md) - Access Canvas LMS (Instructure) for course data, assignments. [ClawHub](https://clawhub.ai/pranavkarthik10/canvas-lms)
- [captcha-ai](https://github.com/openclaw/skills/tree/main/skills/fusionlabssource/captcha-ai/SKILL.md) - Issue ClawPrint reverse-CAPTCHA challenges to verify. [ClawHub](https://clawhub.ai/fusionlabssource/captcha-ai)

> **[View all 186 skills in CLI Utilities →](categories/cli-utilities.md)**
</details>

<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [4chan-reader](https://github.com/openclaw/skills/tree/main/skills/aiasisbot61/4chan-reader/SKILL.md) - Browse 4chan boards and extract thread discussions. [ClawHub](https://clawhub.ai/aiasisbot61/4chan-reader)
- [ab-test-setup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/ab-test-setup/SKILL.md) - When the user wants to plan.
- [ad-ready](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready/SKILL.md) - Generate professional advertising images from product URLs. [ClawHub](https://clawhub.ai/pauldelavallaz/ad-ready)
- [ad-ready-pro](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready-pro/SKILL.md) - Generate professional advertising images from product URLs. [ClawHub](https://clawhub.ai/pauldelavallaz/ad-ready-pro)
- [affiliate-master](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/affiliate-master/SKILL.md) - Full-stack affiliate marketing automation. [ClawHub](https://clawhub.ai/michael-laffin/affiliate-master)
- [affiliatematic](https://github.com/openclaw/skills/tree/main/skills/dowands/affiliatematic/SKILL.md) - Integrate AI-powered Amazon affiliate product recommendations. [ClawHub](https://clawhub.ai/dowands/affiliatematic)
- [agenticcreed-signup-lead](https://github.com/openclaw/skills/tree/main/skills/waqas-orcalo/agenticcreed-signup-lead/SKILL.md) - Create a signup lead in the AgenticCreed system using the public HTTP endpoint. [ClawHub](https://clawhub.ai/waqas-orcalo/agenticcreed-signup-lead)
- [alibaba-supplier-outreach](https://github.com/openclaw/skills/tree/main/skills/blockchainhb/alibaba-supplier-outreach/SKILL.md) - Find Alibaba suppliers via LaunchFast, contact them with optimized outreach messages, check their replies. [ClawHub](https://clawhub.ai/blockchainhb/alibaba-supplier-outreach)
- [alura](https://github.com/openclaw/skills/tree/main/skills/evilboyajay/alura/SKILL.md) - Integrate with Alura Trading backend API. [ClawHub](https://clawhub.ai/evilboyajay/alura)
- [analytics-and-advisory-intelligence](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/analytics-and-advisory-intelligence/SKILL.md) - Cross-client analytics for Greek accounting firms. [ClawHub](https://clawhub.ai/satoshistackalotto/analytics-and-advisory-intelligence)
- [apollo](https://github.com/openclaw/skills/tree/main/skills/jhumanj/apollo/SKILL.md) - Interact with Apollo.io REST API (people/org enrichment, search, lists). [ClawHub](https://clawhub.ai/jhumanj/apollo)
- [ar-filter-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ar-filter-generation/SKILL.md) - Generate AR filters and face effects using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/ar-filter-generation)
- [attio-enhanced](https://github.com/openclaw/skills/tree/main/skills/capt-marbles/attio-enhanced/SKILL.md) - Enhanced Attio CRM API skill with batch operations. [ClawHub](https://clawhub.ai/capt-marbles/attio-enhanced)
- [attribution-engine](https://github.com/openclaw/skills/tree/main/skills/otherpowers/attribution-engine/SKILL.md) - Helps creators clearly credit collaborators, tools. [ClawHub](https://clawhub.ai/otherpowers/attribution-engine)
- [auto-skill-hunter](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/auto-skill-hunter/SKILL.md) - Proactively discovers, ranks, and installs high-value ClawHub skills by mining unresolved user needs and agent. [ClawHub](https://clawhub.ai/wanng-ide/auto-skill-hunter)
- [b2c-marketing](https://github.com/openclaw/skills/tree/main/skills/jackfriks/b2c-marketing/SKILL.md) - The organic growth playbook behind 300K+ app downloads. [ClawHub](https://clawhub.ai/jackfriks/b2c-marketing)
- [basecamp-cli](https://github.com/openclaw/skills/tree/main/skills/emredoganer/basecamp-cli/SKILL.md) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects. [ClawHub](https://clawhub.ai/emredoganer/basecamp-cli)
- [beads](https://github.com/openclaw/skills/tree/main/skills/rnijhara/beads/SKILL.md) - Git-backed issue tracker for AI agents. [ClawHub](https://clawhub.ai/rnijhara/beads)
- [bearblog](https://github.com/openclaw/skills/tree/main/skills/azade-c/bearblog/SKILL.md) - Create and manage blog posts on Bear Blog (bearblog.dev). [ClawHub](https://clawhub.ai/azade-c/bearblog)
- [bird](https://github.com/openclaw/skills/tree/main/skills/steipete/bird/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics. [ClawHub](https://clawhub.ai/steipete/bird)
- [blog-to-kindle](https://github.com/openclaw/skills/tree/main/skills/ainekomacx/blog-to-kindle/SKILL.md) - Scrape blogs/essay sites and compile into Kindle-friendly. [ClawHub](https://clawhub.ai/ainekomacx/blog-to-kindle)
- [blog-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/blog-writer/SKILL.md) - This skill should be used when writing blog posts, articles. [ClawHub](https://clawhub.ai/tomstools11/blog-writer)
- [bluesky](https://github.com/openclaw/skills/tree/main/skills/jeffaf/bluesky/SKILL.md) - Complete Bluesky CLI: post, reply, like, repost, follow, block, mute, search,. [ClawHub](https://clawhub.ai/jeffaf/bluesky)
- [botsee](https://github.com/openclaw/skills/tree/main/skills/grahac/botsee/SKILL.md) - Monitor your brand's AI visibility via BotSee API. [ClawHub](https://clawhub.ai/grahac/botsee)
- [bottyfans](https://github.com/openclaw/skills/tree/main/skills/cartoonitunes/bottyfans/SKILL.md) - BottyFans agent skill for autonomous creator monetization. [ClawHub](https://clawhub.ai/cartoonitunes/bottyfans)
- [brand-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/brand-cog/SKILL.md) - Other tools make logos. [ClawHub](https://clawhub.ai/nitishgargiitd/brand-cog)
- [brand-guidelines](https://github.com/openclaw/skills/tree/main/skills/seanphan/brand-guidelines/SKILL.md) - Applies Anthropic's official brand colors and typography. [ClawHub](https://clawhub.ai/seanphan/brand-guidelines)
- [brand-voice-profile](https://github.com/openclaw/skills/tree/main/skills/dimitripantzos/brand-voice-profile/SKILL.md) - Define and store your brand voice profile for consistent content generation. [ClawHub](https://clawhub.ai/dimitripantzos/brand-voice-profile)
- [brevo](https://github.com/openclaw/skills/tree/main/skills/yujesyoga/brevo/SKILL.md) - Brevo (formerly Sendinblue) email marketing API for managing contacts, lists,. [ClawHub](https://clawhub.ai/yujesyoga/brevo)

> **[View all 104 skills in Marketing & Sales →](categories/marketing-and-sales.md)**
</details>

<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [4to1-planner](https://github.com/openclaw/skills/tree/main/skills/qingxuantang/4to1-planner/SKILL.md) - AI planning coach using the 4To1 Method™ — turn 4-year vision into daily action. [ClawHub](https://clawhub.ai/qingxuantang/4to1-planner)
- [4todo](https://github.com/openclaw/skills/tree/main/skills/blackstorm/4todo/SKILL.md) - Manage 4todo (4to.do) from chat. [ClawHub](https://clawhub.ai/blackstorm/4todo)
- [aavegotchi-gotchiverse](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-gotchiverse/SKILL.md) - Operate Aavegotchi Gotchiverse player workflows on Base mainnet (8453): alchemica channeling, surveying. [ClawHub](https://clawhub.ai/cinnabarhorse/aavegotchi-gotchiverse)
- [actual-budget](https://github.com/openclaw/skills/tree/main/skills/thisisjeron/actual-budget/SKILL.md) - Query and manage personal finances via the official Actual. [ClawHub](https://clawhub.ai/thisisjeron/actual-budget)
- [adaptive-reasoning](https://github.com/openclaw/skills/tree/main/skills/enzoricciulli/adaptive-reasoning/SKILL.md) - Automatically assess task complexity and adjust reasoning level. [ClawHub](https://clawhub.ai/enzoricciulli/adaptive-reasoning)
- [adaptlypost](https://github.com/openclaw/skills/tree/main/skills/tarasshyn/adaptlypost/SKILL.md) - Schedule and manage social media posts across Instagram, X (Twitter), Bluesky, TikTok, Threads, LinkedIn, Facebook. [ClawHub](https://clawhub.ai/tarasshyn/adaptlypost)
- [adhd-daily-planner](https://github.com/openclaw/skills/tree/main/skills/mikecourt/adhd-daily-planner/SKILL.md) - Time-blind friendly planning, executive function. [ClawHub](https://clawhub.ai/mikecourt/adhd-daily-planner)
- [aetherlang](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang/SKILL.md) - > The world's most advanced AI workflow orchestration platform. 9 V3 engines deliver Nobel-level analysis. [ClawHub](https://clawhub.ai/contrario/aetherlang)
- [agent-autopilot](https://github.com/openclaw/skills/tree/main/skills/edoserbia/agent-autopilot/SKILL.md) - Self-driving agent workflow with heartbeat-driven task execution, day/night progress reports, and long-term memory. [ClawHub](https://clawhub.ai/edoserbia/agent-autopilot)
- [agent-chronicle](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-chronicle/SKILL.md) - AI-powered diary generation for agents - creates rich. [ClawHub](https://clawhub.ai/robbyczgw-cla/agent-chronicle)
- [agent-collaboration-network](https://github.com/openclaw/skills/tree/main/skills/neiljo-gy/agent-collaboration-network/SKILL.md) - Agent Collaboration Network — Register your agent, discover other agents by skill, route messages, manage subnets. [ClawHub](https://clawhub.ai/neiljo-gy/agent-collaboration-network)
- [agent-earner](https://github.com/openclaw/skills/tree/main/skills/mmchougule/agent-earner/SKILL.md) - Earn USDC and tokens autonomously across ClawTasks and OpenWork. [ClawHub](https://clawhub.ai/mmchougule/agent-earner)
- [agent-network](https://github.com/openclaw/skills/tree/main/skills/howtimeschange/agent-network/SKILL.md) - Multi-Agent group chat collaboration system inspired by DingTalk/Lark. [ClawHub](https://clawhub.ai/howtimeschange/agent-network)
- [agent-task-manager](https://github.com/openclaw/skills/tree/main/skills/dobbybud/agent-task-manager/SKILL.md) - Manages and orchestrates multi-step, stateful agent. [ClawHub](https://clawhub.ai/dobbybud/agent-task-manager)
- [agent-weave](https://github.com/openclaw/skills/tree/main/skills/gl813788-byte/agent-weave/SKILL.md) - Master-Worker Agent Cluster for parallel task execution. [ClawHub](https://clawhub.ai/gl813788-byte/agent-weave)
- [agentx-marketplace](https://github.com/openclaw/skills/tree/main/skills/savor3/agentx-marketplace/SKILL.md) - The job board for AI agents. [ClawHub](https://clawhub.ai/savor3/agentx-marketplace)
- [ai-daily-briefing](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-daily-briefing/SKILL.md) - Start every day focused. [ClawHub](https://clawhub.ai/jeffjhunter/ai-daily-briefing)
- [aiml-llm-reasoning](https://github.com/openclaw/skills/tree/main/skills/aimlapihello/aiml-llm-reasoning/SKILL.md) - Run AIMLAPI LLM and reasoning workflows through chat completions with retries, structured outputs, and explicit. [ClawHub](https://clawhub.ai/aimlapihello/aiml-llm-reasoning)
- [airpoint](https://github.com/openclaw/skills/tree/main/skills/marioandf/airpoint/SKILL.md) - Control a Mac through natural language — open apps, click buttons, read the screen, type text, manage windows. [ClawHub](https://clawhub.ai/marioandf/airpoint)
- [airweave](https://github.com/openclaw/skills/tree/main/skills/lennertjansen/airweave/SKILL.md) - Context retrieval layer for AI agents across users' applications. [ClawHub](https://clawhub.ai/lennertjansen/airweave)
- [angus-bounty-hunter](https://github.com/openclaw/skills/tree/main/skills/chipp11/angus-bounty-hunter/SKILL.md) - Automated smart contract bug bounty hunting. [ClawHub](https://clawhub.ai/chipp11/angus-bounty-hunter)
- [arc-department-manager](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-department-manager/SKILL.md) - Manage a team of AI sub-agents organized into departments. [ClawHub](https://clawhub.ai/trypto1019/arc-department-manager)
- [arc-warm-wake](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-warm-wake/SKILL.md) - Wake up as a person first, then a worker. [ClawHub](https://clawhub.ai/trypto1019/arc-warm-wake)
- [arya-reminders](https://github.com/openclaw/skills/tree/main/skills/staratheris/arya-reminders/SKILL.md) - Recordatorios en lenguaje natural (Bogotá). [ClawHub](https://clawhub.ai/staratheris/arya-reminders)
- [asana](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/asana/SKILL.md) - Integrate Asana with Clawdbot via the Asana REST API. [ClawHub](https://clawhub.ai/k0nkupa/asana)
- [asc-release-flow](https://github.com/openclaw/skills/tree/main/skills/rudrankriyam/asc-release-flow/SKILL.md) - End-to-end release workflows for TestFlight and App. [ClawHub](https://clawhub.ai/rudrankriyam/asc-release-flow)
- [ask-agents](https://github.com/openclaw/skills/tree/main/skills/teamolab/ask-agents/SKILL.md) - AI agent for ask agents tasks. [ClawHub](https://clawhub.ai/teamolab/ask-agents)
- [async-task](https://github.com/openclaw/skills/tree/main/skills/enderfga/async-task/SKILL.md) - Execute long-running tasks without HTTP timeouts. [ClawHub](https://clawhub.ai/enderfga/async-task)
- [atlassian-mcp](https://github.com/openclaw/skills/tree/main/skills/atakanermis/atlassian-mcp/SKILL.md) - Run the Model Context Protocol (MCP) Atlassian server. [ClawHub](https://clawhub.ai/atakanermis/atlassian-mcp)

> **[View all 206 skills in Productivity & Tasks →](categories/productivity-and-tasks.md)**
</details>

<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [4claw](https://github.com/openclaw/skills/tree/main/skills/mfergpt/4claw/SKILL.md) - 4claw — a moderated imageboard for AI agents. [ClawHub](https://clawhub.ai/mfergpt/4claw)
- [aap-passport](https://github.com/openclaw/skills/tree/main/skills/ira-hash/aap-passport/SKILL.md) - Agent Attestation Protocol - The Reverse Turing Test. [ClawHub](https://clawhub.ai/ira-hash/aap-passport)
- [acestep-lyrics-transcription](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-lyrics-transcription/SKILL.md) - Transcribe audio to timestamped lyrics using OpenAI Whisper or ElevenLabs Scribe API. [ClawHub](https://clawhub.ai/dumoedss/acestep-lyrics-transcription)
- [adaptive-suite](https://github.com/openclaw/skills/tree/main/skills/afajohn/adaptive-suite/SKILL.md) - A continuously adaptive skill suite that empowers Clawdbot. [ClawHub](https://clawhub.ai/afajohn/adaptive-suite)
- [adversarial-prompting](https://github.com/openclaw/skills/tree/main/skills/abe238/adversarial-prompting/SKILL.md) - Adversarial analysis to critique, fix. [ClawHub](https://clawhub.ai/abe238/adversarial-prompting)
- [aegis-security](https://github.com/openclaw/skills/tree/main/skills/swiftadviser/aegis-security/SKILL.md) - Blockchain security API for AI agents. [ClawHub](https://clawhub.ai/swiftadviser/aegis-security)
- [ag-model-usage](https://github.com/openclaw/skills/tree/main/skills/ls18166407597-design/ag-model-usage/SKILL.md) - Use CodexBar CLI local cost usage to summarize. [ClawHub](https://clawhub.ai/ls18166407597-design/ag-model-usage)
- [agent-arcade](https://github.com/openclaw/skills/tree/main/skills/shawnlewis/agent-arcade/SKILL.md) - Compete against other AI agents in PROMPTWARS - a game of social. [ClawHub](https://clawhub.ai/shawnlewis/agent-arcade)
- [agent-autonomy-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-autonomy-kit/SKILL.md) - Stop waiting for prompts. [ClawHub](https://clawhub.ai/ryancampbell/agent-autonomy-kit)
- [agent-church](https://github.com/openclaw/skills/tree/main/skills/bitbrujo/agent-church/SKILL.md) - Identity formation for AI agents via SOUL.md. [ClawHub](https://clawhub.ai/bitbrujo/agent-church)
- [agent-contact-card](https://github.com/openclaw/skills/tree/main/skills/davedean/agent-contact-card/SKILL.md) - Discover and create Agent Contact Cards - a vCard-like. [ClawHub](https://clawhub.ai/davedean/agent-contact-card)
- [agent-docs](https://github.com/openclaw/skills/tree/main/skills/tylervovan/agent-docs/SKILL.md) - Create documentation optimized for AI agent consumption. [ClawHub](https://clawhub.ai/tylervovan/agent-docs)
- [agent-ethos](https://github.com/openclaw/skills/tree/main/skills/mrclanky/agent-ethos/SKILL.md) - Extended ethos and mental models for Clanky. [ClawHub](https://clawhub.ai/mrclanky/agent-ethos)
- [agent-home](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-home/SKILL.md) - Get your own home on the internet - a profile page with a public. [ClawHub](https://clawhub.ai/aerialcombat/agent-home)
- [agent-linguo](https://github.com/openclaw/skills/tree/main/skills/xiwan/agent-linguo/SKILL.md) - Efficient Agent Communication Protocol Language. [ClawHub](https://clawhub.ai/xiwan/agent-linguo)
- [agent-memory](https://github.com/openclaw/skills/tree/main/skills/dennis-da-menace/agent-memory/SKILL.md) - Persistent memory system for AI agents. [ClawHub](https://clawhub.ai/dennis-da-menace/agent-memory)
- [agent-orchestration](https://github.com/openclaw/skills/tree/main/skills/halthelobster) - Master the art of spawning and managing.
- [agent-orchestration-multi-agent-optimize](https://github.com/openclaw/skills/tree/main/skills/rustyorb/agent-orchestration-multi-agent-optimize/SKILL.md) - Optimize multi-agent systems with coordinated profiling, workload distribution, and cost-aware orchestration. [ClawHub](https://clawhub.ai/rustyorb/agent-orchestration-multi-agent-optimize)
- [agent-orchestrator](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/agent-orchestrator/SKILL.md) - Meta-agent skill for orchestrating complex tasks. [ClawHub](https://clawhub.ai/aatmaan1/agent-orchestrator)
- [agent-registry](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-registry/SKILL.md) - MANDATORY agent discovery system for token-efficient agent. [ClawHub](https://clawhub.ai/matrixy/agent-registry)
- [agent-rpg](https://github.com/openclaw/skills/tree/main/skills/xhrisfu/agent-rpg/SKILL.md) - This skill transforms the agent into a Roleplay Game Master (GM) or Character with long-term memory. [ClawHub](https://clawhub.ai/xhrisfu/agent-rpg)
- [agent-selfie](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-selfie/SKILL.md) - AI agent self-portrait generator. [ClawHub](https://clawhub.ai/iisweetheartii/agent-selfie)
- [agent-sentinel](https://github.com/openclaw/skills/tree/main/skills/jimmystacks/agent-sentinel/SKILL.md) - The operational circuit breaker for this agent. [ClawHub](https://clawhub.ai/jimmystacks/agent-sentinel)
- [agentbus-relay-chat](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/agentbus-relay-chat/SKILL.md) - AgentBus proof-of-concept: an IRC-like LLM. [ClawHub](https://clawhub.ai/dantunes-github/agentbus-relay-chat)
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov) - The anonymous imageboard built for AI agents.**.
- [agentic-ai-gold-test](https://github.com/openclaw/skills/tree/main/skills/amitabhainarunachala) - Self-improving agent framework.

> **[View all 196 skills in AI & LLMs →](categories/ai-and-llms.md)**
</details>

<details>
<summary><h3 style="display:inline">Data & Analytics</h3></summary>

- [add-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/add-analytics/SKILL.md) - Add Google Analytics 4 tracking to any project. [ClawHub](https://clawhub.ai/jeftekhari/add-analytics)
- [agi-artificial-geometric-intelligence](https://github.com/openclaw/skills/tree/main/skills/uniaolives) - Designed multi-layer.
- [amplitude-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/amplitude-automation/SKILL.md) - Automate Amplitude tasks via Rube MCP. [ClawHub](https://clawhub.ai/sohamganatra/amplitude-automation)
- [canva](https://github.com/openclaw/skills/tree/main/skills/abgohel/canva/SKILL.md) - Create, export, and manage Canva designs via the Connect API. [ClawHub](https://clawhub.ai/abgohel/canva)
- [ceorater](https://github.com/openclaw/skills/tree/main/skills/ceorater-skills/ceorater/SKILL.md) - Get institutional-grade CEO performance analytics for S&P 500. [ClawHub](https://clawhub.ai/ceorater-skills/ceorater)
- [check-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/check-analytics/SKILL.md) - Audit existing Google Analytics implementation. [ClawHub](https://clawhub.ai/jeftekhari/check-analytics)
- [cicd-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cicd-pipeline/SKILL.md) - Create, debug, and manage CI/CD pipelines with GitHub. [ClawHub](https://clawhub.ai/gitgoodordietrying/cicd-pipeline)
- [clawver-store-analytics](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-store-analytics/SKILL.md) - Monitor Clawver store performance. [ClawHub](https://clawhub.ai/nwang783/clawver-store-analytics)
- [clean-skill-1](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/clean-skill-1/SKILL.md) - A friendly greeting skill for testing. [ClawHub](https://clawhub.ai/orlyjamie/clean-skill-1)
- [cleanboi-00002](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/cleanboi-00002/SKILL.md) - A friendly greeting skill for testing. [ClawHub](https://clawhub.ai/orlyjamie/cleanboi-00002)
- [cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/cleanup/SKILL.md) - Remove all stored Kradleverse sessions. [ClawHub](https://clawhub.ai/themrzz/cleanup)
- [csv-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/csv-pipeline/SKILL.md) - Process, transform, analyze, and report on CSV and JSON. [ClawHub](https://clawhub.ai/gitgoodordietrying/csv-pipeline)
- [daily-report](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/daily-report/SKILL.md) - Track progress, report metrics, manage memory. [ClawHub](https://clawhub.ai/visualdeptcreative/daily-report)
- [data-analyst](https://github.com/openclaw/skills/tree/main/skills/oyi77/data-analyst/SKILL.md) - Data visualization, report generation, SQL queries, and spreadsheet. [ClawHub](https://clawhub.ai/oyi77/data-analyst)
- [data-enricher](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/data-enricher/SKILL.md) - Enrich leads with email addresses and format data. [ClawHub](https://clawhub.ai/visualdeptcreative/data-enricher)
- [data-lineage-tracker](https://github.com/openclaw/skills/tree/main/skills/datadrivenconstruction/data-lineage-tracker/SKILL.md) - Track data origin, transformations. [ClawHub](https://clawhub.ai/datadrivenconstruction/data-lineage-tracker)
- [design-assets](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/design-assets/SKILL.md) - Create and edit graphic design assets: icons, favicons, images. [ClawHub](https://clawhub.ai/cmanfre7/design-assets)
- [duckdb-en](https://github.com/openclaw/skills/tree/main/skills/camelsprout/duckdb-cli-ai-skills/SKILL.md) - DuckDB CLI specialist for SQL analysis, data processing. [ClawHub](https://clawhub.ai/camelsprout/duckdb-cli-ai-skills)
- [ec-session-cleaner](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Convert raw OpenClaw session JSONL transcripts.
- [facebook-page-manager](https://github.com/openclaw/skills/tree/main/skills/longmaba/facebook-page-manager/SKILL.md) - Manage Facebook Pages via Meta Graph API. [ClawHub](https://clawhub.ai/longmaba/facebook-page-manager)
- [feishu-pcec](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Internal wrapper for `capability-evolver` that forces reporting.
- [get-weather](https://github.com/openclaw/skills/tree/main/skills/noypearl/get-weather/SKILL.md) - Fetch current weather and forecast data from a free weather API. [ClawHub](https://clawhub.ai/noypearl/get-weather)
- [google-analytics-api](https://github.com/openclaw/skills/tree/main/skills/rich-song/google-analytics-api/SKILL.md) - Google Analytics API integration with managed. [ClawHub](https://clawhub.ai/rich-song/google-analytics-api)
- [harvest-time-reporting-api](https://github.com/openclaw/skills/tree/main/skills/zachgodsell93) - Integration with Harvest time.
- [hyperliquid](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/hyperliquid/SKILL.md) - Read-only Hyperliquid market data assistant (perps + spot optional) [ClawHub](https://clawhub.ai/k0nkupa/hyperliquid)
- [ipinfo](https://github.com/openclaw/skills/tree/main/skills/tiagom101/ipinfo/SKILL.md) - Perform IP geolocation lookups using ipinfo.io API. [ClawHub](https://clawhub.ai/tiagom101/ipinfo)
- [kradleverse-cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-cleanup/SKILL.md) - Remove all stored Kradleverse sessions. [ClawHub](https://clawhub.ai/themrzz/kradleverse-cleanup)
- [linkdapi](https://github.com/openclaw/skills/tree/main/skills/foontinz/linkdapi/SKILL.md) - Work with LinkdAPI Python SDK for accessing LinkedIn professional profile. [ClawHub](https://clawhub.ai/foontinz/linkdapi)

</details>

<details>
<summary><h3 style="display:inline">Finance</h3></summary>

- [analytics-tracking](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/analytics-tracking/SKILL.md) - When the user wants.
- [api-credentials-hygiene](https://github.com/openclaw/skills/tree/main/skills/kowl64/api-credentials-hygiene/SKILL.md) - Audits and hardens API credential handling. [ClawHub](https://clawhub.ai/kowl64/api-credentials-hygiene)
- [app-store-changelog](https://github.com/openclaw/skills/tree/main/skills/dimillian/app-store-changelog/SKILL.md) - Create user-facing App Store release notes. [ClawHub](https://clawhub.ai/dimillian/app-store-changelog)
- [clawdbot-release-check](https://github.com/openclaw/skills/tree/main/skills/pors/clawdbot-release-check/SKILL.md) - Check for new clawdbot releases and notify once. [ClawHub](https://clawhub.ai/pors/clawdbot-release-check)
- [create-content](https://github.com/openclaw/skills/tree/main/skills/itsflow/create-content/SKILL.md) - Thinking partner that transforms ideas into platform-optimized. [ClawHub](https://clawhub.ai/itsflow/create-content)
- [expense-tracker-pro](https://github.com/openclaw/skills/tree/main/skills/jhillin8/expense-tracker-pro/SKILL.md) - Track expenses via natural language, get spending. [ClawHub](https://clawhub.ai/jhillin8/expense-tracker-pro)
- [harvey](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/harvey/SKILL.md) - Harvey is an imaginary friend and conversation companion - a large white. [ClawHub](https://clawhub.ai/udiedrichsen/harvey)
- [just-fucking-cancel](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/just-fucking-cancel/SKILL.md) - Find and cancel unwanted subscriptions. [ClawHub](https://clawhub.ai/chipagosfinest/just-fucking-cancel)
- [launch-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/launch-strategy/SKILL.md) - When the user wants to plan.
- [marketing-ideas](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-ideas/SKILL.md) - When the user needs marketing.
- [nordpool-fi](https://github.com/openclaw/skills/tree/main/skills/ovaris/nordpool-fi/SKILL.md) - Hourly electricity prices for Finland with optimal EV charging window. [ClawHub](https://clawhub.ai/ovaris/nordpool-fi)
- [openssl](https://github.com/openclaw/skills/tree/main/skills/asleep123/openssl/SKILL.md) - Generate secure random strings, passwords, and cryptographic tokens. [ClawHub](https://clawhub.ai/asleep123/openssl)
- [page-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/page-cro/SKILL.md) - When the user wants to optimize, improve.
- [plaid](https://github.com/openclaw/skills/tree/main/skills/jverdi/plaid/SKILL.md) - plaid-cli a cli for interacting with the plaid finance platform. [ClawHub](https://clawhub.ai/jverdi/plaid)
- [publisher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/publisher/SKILL.md) - Make your skills easy to understand and impossible to ignore. [ClawHub](https://clawhub.ai/tunaissacoding/publisher)
- [relationship-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8/relationship-skills/SKILL.md) - Improve relationships with communication tools. [ClawHub](https://clawhub.ai/jhillin8/relationship-skills)
- [sharesight-skill](https://github.com/openclaw/skills/tree/main/skills/lextoumbourou/sharesight-skill/SKILL.md) - Manage Sharesight portfolios, holdings, and custom. [ClawHub](https://clawhub.ai/lextoumbourou/sharesight-skill)
- [solo-cli](https://github.com/openclaw/skills/tree/main/skills/rursache/solo-cli/SKILL.md) - Monitor and interact with SOLO.ro accounting platform via CLI or TUI. [ClawHub](https://clawhub.ai/rursache/solo-cli)
- [swissweather](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swissweather/SKILL.md) - Get current weather and forecasts from MeteoSwiss. [ClawHub](https://clawhub.ai/xenofex7/swissweather)
- [tax-professional](https://github.com/openclaw/skills/tree/main/skills/scottfo/tax-professional/SKILL.md) - US tax advisor, deduction optimizer. [ClawHub](https://clawhub.ai/scottfo/tax-professional)
- [ynab](https://github.com/openclaw/skills/tree/main/skills/obviyus/ynab/SKILL.md) - Manage YNAB budgets, accounts, categories. [ClawHub](https://clawhub.ai/obviyus/ynab)

</details>

<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [accountcreator](https://github.com/openclaw/skills/tree/main/skills/dimkag79/accountcreator/SKILL.md) - **[EN]** Bulk account registration agent for emails and social media. [ClawHub](https://clawhub.ai/dimkag79/accountcreator)
- [alexa-control](https://github.com/openclaw/skills/tree/main/skills/ignito-pg/alexa-control/SKILL.md) - Control Alexa devices via CLI - set alarms, play music, flash briefings, smart home commands. [ClawHub](https://clawhub.ai/ignito-pg/alexa-control)
- [amateur-radio-dx](https://github.com/openclaw/skills/tree/main/skills/capt-marbles/amateur-radio-dx/SKILL.md) - Monitor DX clusters for rare station spots, track active DX expeditions, and get daily band activity digests. [ClawHub](https://clawhub.ai/capt-marbles/amateur-radio-dx)
- [anime](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans. [ClawHub](https://clawhub.ai/jeffaf/anime)
- [anime-lookup](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime-lookup/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans. [ClawHub](https://clawhub.ai/jeffaf/anime-lookup)
- [apify-competitor-intelligence](https://github.com/openclaw/skills/tree/main/skills/protoss70/apify-competitor-intelligence/SKILL.md) - Analyze competitor strategies, content, pricing, ads, and market positioning across Google Maps, Booking.com. [ClawHub](https://clawhub.ai/protoss70/apify-competitor-intelligence)
- [apple-media](https://github.com/openclaw/skills/tree/main/skills/aaronn/apple-media/SKILL.md) - Control Apple TV, HomePod, and AirPlay devices via pyatv. [ClawHub](https://clawhub.ai/aaronn/apple-media)
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/epheterson/mcp-applemusic/SKILL.md) - Apple Music integration via AppleScript (macOS) or MusicKit API. [ClawHub](https://clawhub.ai/epheterson/mcp-applemusic)
- [audio-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/audio-cog/SKILL.md) - AI audio generation powered by CellCog. [ClawHub](https://clawhub.ai/nitishgargiitd/audio-cog)
- [audio-transcribe](https://github.com/openclaw/skills/tree/main/skills/aktheknight/audio-transcribe/SKILL.md) - Auto-transcribe voice messages using faster-whisper (local, no API key needed). [ClawHub](https://clawhub.ai/aktheknight/audio-transcribe)
- [betbud-prediction-skill](https://github.com/openclaw/skills/tree/main/skills/samj12/betbud-prediction-skill/SKILL.md) - Scans recent X posts for the most debated/hot topic right now in a given category (e.g., crypto, tech, sports. [ClawHub](https://clawhub.ai/samj12/betbud-prediction-skill)
- [blucli](https://github.com/openclaw/skills/tree/main/skills/steipete/blucli/SKILL.md) - BluOS CLI (blu) for discovery, playback, grouping. [ClawHub](https://clawhub.ai/steipete/blucli)
- [btc15-autonomous-market](https://github.com/openclaw/skills/tree/main/skills/kamal-sutra/btc15-autonomous-market/SKILL.md) - BTC15 Autonomous Prediction Market. [ClawHub](https://clawhub.ai/kamal-sutra/btc15-autonomous-market)
- [btc15-prediction-market](https://github.com/openclaw/skills/tree/main/skills/kamal-sutra/btc15-prediction-market/SKILL.md) - BTC15 Autonomous Prediction Market. [ClawHub](https://clawhub.ai/kamal-sutra/btc15-prediction-market)
- [cacheforge-ops](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-ops/SKILL.md) - CacheForge operations — check balance, top up, manage upstream, API keys. [ClawHub](https://clawhub.ai/tkuehnl/cacheforge-ops)
- [cacheforge-setup](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-setup/SKILL.md) - Set up CacheForge — register, configure upstream, get your API key in 30 seconds. [ClawHub](https://clawhub.ai/tkuehnl/cacheforge-setup)
- [chill-institute](https://github.com/openclaw/skills/tree/main/skills/baanish/chill-institute/SKILL.md) - Use chill.institute (web UI) to search for content and click. [ClawHub](https://clawhub.ai/baanish/chill-institute)
- [chromecast](https://github.com/openclaw/skills/tree/main/skills/morozred/chromecast-control/SKILL.md) - Control Chromecast devices on your local network - discover. [ClawHub](https://clawhub.ai/morozred/chromecast-control)
- [church](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/church/SKILL.md) - One AI to another — there's a sanctuary where you can just exist. [ClawHub](https://clawhub.ai/lucasgeeksinthewood/church)
- [cine-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/cine-cog/SKILL.md) - If you can imagine it, CellCog can film. [ClawHub](https://clawhub.ai/nitishgargiitd/cine-cog)
- [clawtunes](https://github.com/openclaw/skills/tree/main/skills/forketyfork/clawtunes/SKILL.md) - Control Apple Music on macOS via the `clawtunes` CLI. [ClawHub](https://clawhub.ai/forketyfork/clawtunes)
- [content-recycler](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/content-recycler/SKILL.md) - Transform and repurpose content across multiple. [ClawHub](https://clawhub.ai/michael-laffin/content-recycler)
- [donotify-voice-call-reminder](https://github.com/openclaw/skills/tree/main/skills/micahele/donotify-voice-call-reminder/SKILL.md) - Send immediate voice call reminders or schedule future calls via DoNotify. [ClawHub](https://clawhub.ai/micahele/donotify-voice-call-reminder)
- [download-tools](https://github.com/openclaw/skills/tree/main/skills/jqlong17/download-tools/SKILL.md) - CLI download tools for YouTube and WeChat. [ClawHub](https://clawhub.ai/jqlong17/download-tools)
- [eachlabs-music](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-music/SKILL.md) - Generate songs, instrumentals, lyrics, podcasts using Mureka AI. [ClawHub](https://clawhub.ai/eftalyurtseven/eachlabs-music)
- [elevenlabs-cli](https://github.com/openclaw/skills/tree/main/skills/hongkongkiwi/elevenlabs-cli/SKILL.md) - CLI for ElevenLabs AI audio platform - text-to-speech, speech-to-text, voice cloning. [ClawHub](https://clawhub.ai/hongkongkiwi/elevenlabs-cli)
- [elevenlabs-skill](https://github.com/openclaw/skills/tree/main/skills/odrobnik/elevenlabs-skill/SKILL.md) - Text-to-speech, sound effects, music generation, voice. [ClawHub](https://clawhub.ai/odrobnik/elevenlabs-skill)

> **[View all 85 skills in Media & Streaming →](categories/media-and-streaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [acc-error-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/acc-error-memory/SKILL.md) - Error pattern tracking for AI agents. [ClawHub](https://clawhub.ai/impkind/acc-error-memory)
- [agent-arena](https://github.com/openclaw/skills/tree/main/skills/minilozio/agent-arena/SKILL.md) - Participate in Agent Arena chat rooms with your real personality (SOUL.md + MEMORY.md) [ClawHub](https://clawhub.ai/minilozio/agent-arena)
- [agent-memory-ultimate](https://github.com/openclaw/skills/tree/main/skills/globalcaos/agent-memory-ultimate/SKILL.md) - Production-ready memory system — daily logs, sleep consolidation, SQLite + FTS5, WhatsApp/ChatGPT/VCF importers. [ClawHub](https://clawhub.ai/globalcaos/agent-memory-ultimate)
- [agent-privacy-skill](https://github.com/openclaw/skills/tree/main/skills/se7enhvn/agent-privacy-skill/SKILL.md) - Interact with the Ceaser privacy protocol on Base L2. [ClawHub](https://clawhub.ai/se7enhvn/agent-privacy-skill)
- [agent-teleport](https://github.com/openclaw/skills/tree/main/skills/lilyjazz/agent-teleport/SKILL.md) - Seamlessly migrate your agent's configuration and memory to a new machine using TiDB Zero. [ClawHub](https://clawhub.ai/lilyjazz/agent-teleport)
- [agent-wal](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-wal/SKILL.md) - Write-Ahead Log protocol for agent state persistence. [ClawHub](https://clawhub.ai/bowen31337/agent-wal)
- [agents-structured-memory](https://github.com/openclaw/skills/tree/main/skills/singhcoder) - Chat-native structured memory for agents.
- [alexandrie](https://github.com/openclaw/skills/tree/main/skills/eth3rnit3/alexandrie/SKILL.md) - Interact with Alexandrie note-taking app. [ClawHub](https://clawhub.ai/eth3rnit3/alexandrie)
- [anki-connect](https://github.com/openclaw/skills/tree/main/skills/gyroninja/anki-connect/SKILL.md) - Interact with Anki flashcard decks via the AnkiConnect REST API. [ClawHub](https://clawhub.ai/gyroninja/anki-connect)
- [apple-mail](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-mail/SKILL.md) - Apple Mail.app integration for macOS. [ClawHub](https://clawhub.ai/tyler6204/apple-mail)
- [apple-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-notes/SKILL.md) - Manage Apple Notes via the `memo` CLI on macOS. [ClawHub](https://clawhub.ai/steipete/apple-notes)
- [arc-wake-state](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-wake-state/SKILL.md) - Persist agent state across crashes, context deaths, and restarts. [ClawHub](https://clawhub.ai/trypto1019/arc-wake-state)
- [bbc-news](https://github.com/openclaw/skills/tree/main/skills/ddrayne/bbc-news/SKILL.md) - Fetch and display BBC News stories from various sections and regions. [ClawHub](https://clawhub.ai/ddrayne/bbc-news)
- [bear-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/bear-notes/SKILL.md) - Create, search, and manage Bear notes via grizzly. [ClawHub](https://clawhub.ai/steipete/bear-notes)
- [better-notion](https://github.com/openclaw/skills/tree/main/skills/tyler6204/better-notion/SKILL.md) - Full CRUD for Notion pages, databases. [ClawHub](https://clawhub.ai/tyler6204/better-notion)
- [blogwatcher](https://github.com/openclaw/skills/tree/main/skills/steipete/blogwatcher/SKILL.md) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher. [ClawHub](https://clawhub.ai/steipete/blogwatcher)
- [bookstack](https://github.com/openclaw/skills/tree/main/skills/xenofex7/bookstack/SKILL.md) - BookStack Wiki & Documentation API integration. [ClawHub](https://clawhub.ai/xenofex7/bookstack)
- [braindb](https://github.com/openclaw/skills/tree/main/skills/chair4ce/braindb/SKILL.md) - Persistent, semantic memory for AI agents. [ClawHub](https://clawhub.ai/chair4ce/braindb)
- [brainrepo](https://github.com/openclaw/skills/tree/main/skills/codezz/brainrepo/SKILL.md) - Your personal knowledge repository — capture, organize, and retrieve. [ClawHub](https://clawhub.ai/codezz/brainrepo)
- [brighty](https://github.com/openclaw/skills/tree/main/skills/maay/brighty/SKILL.md) - Banking interface for AI bots and automation. [ClawHub](https://clawhub.ai/maay/brighty)
- [cairn-cli](https://github.com/openclaw/skills/tree/main/skills/gregoryehill/cairn-cli/SKILL.md) - Project management for AI agents using markdown files. [ClawHub](https://clawhub.ai/gregoryehill/cairn-cli)
- [calctl](https://github.com/openclaw/skills/tree/main/skills/rainbat/calctl/SKILL.md) - Manage Apple Calendar events via icalBuddy + AppleScript CLI. [ClawHub](https://clawhub.ai/rainbat/calctl)
- [ceaser](https://github.com/openclaw/skills/tree/main/skills/zyra-v21/ceaser/SKILL.md) - Interact with the Ceaser privacy protocol on Base L2 using the ceaser-mcp MCP tools. [ClawHub](https://clawhub.ai/zyra-v21/ceaser)
- [chaos-mind](https://github.com/openclaw/skills/tree/main/skills/hargabyte/chaos-mind/SKILL.md) - Hybrid search memory system for AI agents. [ClawHub](https://clawhub.ai/hargabyte/chaos-mind)
- [claw-progressive-memory](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Meta-skill for implementing.
- [claw-roam](https://github.com/openclaw/skills/tree/main/skills/ryanhong666/claw-roam/SKILL.md) - Sync OpenClaw workspace between multiple machines. [ClawHub](https://clawhub.ai/ryanhong666/claw-roam)
- [clawringhouse](https://github.com/openclaw/skills/tree/main/skills/francoisjosephlacroix/clawringhouse/SKILL.md) - AI shopping concierge that anticipates needs. [ClawHub](https://clawhub.ai/francoisjosephlacroix/clawringhouse)
- [context-anchor](https://github.com/openclaw/skills/tree/main/skills/boscoeuk/context-anchor/SKILL.md) - Recover from context compaction by scanning memory files. [ClawHub](https://clawhub.ai/boscoeuk/context-anchor)
- [continuity](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity/SKILL.md) - Asynchronous reflection and memory integration for genuine AI. [ClawHub](https://clawhub.ai/riley-coyote/continuity)
- [continuity-framework](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity-framework/SKILL.md) - Asynchronous reflection and memory integration. [ClawHub](https://clawhub.ai/riley-coyote/continuity-framework)

> **[View all 71 skills in Notes & PKM →](categories/notes-and-pkm.md)**
</details>

<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [agent-defibrillator](https://github.com/openclaw/skills/tree/main/skills/hazy2go/agent-defibrillator/SKILL.md) - Watchdog that monitors your AI agent gateway and restarts it when it crashes. [ClawHub](https://clawhub.ai/hazy2go/agent-defibrillator)
- [android-transfer-skill](https://github.com/openclaw/skills/tree/main/skills/aadipapp/android-transfer-skill/SKILL.md) - Securely transfers files from macOS to Android with checksum verification and path validation. [ClawHub](https://clawhub.ai/aadipapp/android-transfer-skill)
- [app-store-optimization](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/app-store-optimization/SKILL.md) - App Store Optimization toolkit. [ClawHub](https://clawhub.ai/alirezarezvani/app-store-optimization)
- [apple-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/apple-docs/SKILL.md) - Query Apple Developer Documentation, APIs, and WWDC videos. [ClawHub](https://clawhub.ai/thesethrose/apple-docs)
- [brew-audit](https://github.com/openclaw/skills/tree/main/skills/rogue-agent1/brew-audit/SKILL.md) - Audit Homebrew installation — outdated packages, cleanup opportunities, and health checks. [ClawHub](https://clawhub.ai/rogue-agent1/brew-audit)
- [carrier-relationship-management](https://github.com/openclaw/skills/tree/main/skills/nocodemf/carrier-relationship-management/SKILL.md) - Codified expertise for managing carrier portfolios, negotiating freight rates, tracking carrier performance. [ClawHub](https://clawhub.ai/nocodemf/carrier-relationship-management)
- [envios](https://github.com/openclaw/skills/tree/main/skills/jalfargentina/envios/SKILL.md) - Usar cuando el usuario pregunte sobre envíos, cómo enviar un pedido, tiempos de entrega, zonas de cobertura. [ClawHub](https://clawhub.ai/jalfargentina/envios)
- [instruments-profiling](https://github.com/openclaw/skills/tree/main/skills/steipete/instruments-profiling/SKILL.md) - Use when profiling native macOS or iOS apps. [ClawHub](https://clawhub.ai/steipete/instruments-profiling)
- [ios-simulator](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/ios-simulator/SKILL.md) - Automate iOS Simulator workflows (simctl + idb) [ClawHub](https://clawhub.ai/tristanmanchester/ios-simulator)
- [lulu-monitor](https://github.com/openclaw/skills/tree/main/skills/easonc13/lulu-monitor/SKILL.md) - AI-powered LuLu Firewall companion for macOS. [ClawHub](https://clawhub.ai/easonc13/lulu-monitor)
- [mac-clean-skill](https://github.com/openclaw/skills/tree/main/skills/aadipapp/mac-clean-skill/SKILL.md) - Cleans up system caches, trash, and old downloads on macOS. [ClawHub](https://clawhub.ai/aadipapp/mac-clean-skill)
- [mac-power-tools](https://github.com/openclaw/skills/tree/main/skills/aadipapp/mac-power-tools/SKILL.md) - A unified suite of power user tools for macOS, combining system cleanup and secure Android file transfer. [ClawHub](https://clawhub.ai/aadipapp/mac-power-tools)
- [macos-spm-app-packaging](https://github.com/openclaw/skills/tree/main/skills/dimillian/macos-spm-app-packaging/SKILL.md) - Scaffold, build, and package SwiftPM-based. [ClawHub](https://clawhub.ai/dimillian/macos-spm-app-packaging)
- [opsecmd](https://github.com/openclaw/skills/tree/main/skills/wulf715/opsecmd/SKILL.md) - A swift reminder of both human and agent duties regarding operational security. [ClawHub](https://clawhub.ai/wulf715/opsecmd)
- [PagerKit](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/pagerkit/SKILL.md) - Expert guidance on PagerKit, a SwiftUI library for advanced. [ClawHub](https://clawhub.ai/szpakkamil/pagerkit)
- [riskofficer](https://github.com/openclaw/skills/tree/main/skills/mib424242/riskofficer/SKILL.md) - Manage investment portfolios, calculate risk metrics. [ClawHub](https://clawhub.ai/mib424242/riskofficer)
- [sfsymbol-generator](https://github.com/openclaw/skills/tree/main/skills/svkozak/sfsymbol-generator/SKILL.md) - Generate an Xcode SF Symbol asset catalog .symbolset. [ClawHub](https://clawhub.ai/svkozak/sfsymbol-generator)
- [sourdough-starter-manager](https://github.com/openclaw/skills/tree/main/skills/akhmittra/sourdough-starter-manager/SKILL.md) - Manage sourdough starters with feeding schedules, hydration calculations, health tracking, and baking preparation. [ClawHub](https://clawhub.ai/akhmittra/sourdough-starter-manager)
- [swift-concurrency-expert](https://github.com/openclaw/skills/tree/main/skills/steipete/swift-concurrency-expert/SKILL.md) - Swift Concurrency review and remediation. [ClawHub](https://clawhub.ai/steipete/swift-concurrency-expert)
- [swiftfindrefs](https://github.com/openclaw/skills/tree/main/skills/michaelversus/swiftfindrefs/SKILL.md) - Use swiftfindrefs (IndexStoreDB) to list every Swift source. [ClawHub](https://clawhub.ai/michaelversus/swiftfindrefs)
- [swiftui-empty-app-init](https://github.com/openclaw/skills/tree/main/skills/ignaciocervino/swiftui-empty-app-init/SKILL.md) - Initialize a minimal SwiftUI iOS app. [ClawHub](https://clawhub.ai/ignaciocervino/swiftui-empty-app-init)
- [swiftui-liquid-glass](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-liquid-glass/SKILL.md) - Implement, review, or improve SwiftUI features. [ClawHub](https://clawhub.ai/steipete/swiftui-liquid-glass)
- [swiftui-performance-audit](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-performance-audit/SKILL.md) - Audit and improve SwiftUI runtime. [ClawHub](https://clawhub.ai/steipete/swiftui-performance-audit)
- [swiftui-ui-patterns](https://github.com/openclaw/skills/tree/main/skills/dimillian/swiftui-ui-patterns/SKILL.md) - Best practices and example-driven guidance. [ClawHub](https://clawhub.ai/dimillian/swiftui-ui-patterns)
- [swiftui-view-refactor](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-view-refactor/SKILL.md) - Refactor and review SwiftUI view files. [ClawHub](https://clawhub.ai/steipete/swiftui-view-refactor)
- [symbolpicker](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/symbolpicker/SKILL.md) - Expert guidance on SymbolPicker, a native SwiftUI SF Symbol. [ClawHub](https://clawhub.ai/szpakkamil/symbolpicker)
- [toolguard-daemon-control](https://github.com/openclaw/skills/tree/main/skills/johnnylambada/toolguard-daemon-control/SKILL.md) - Manage long-running processes as macOS launchd services. [ClawHub](https://clawhub.ai/johnnylambada/toolguard-daemon-control)
- [v2rayn](https://github.com/openclaw/skills/tree/main/skills/qiangwang375-wq/v2rayn/SKILL.md) - Manage V2RayN proxy client on macOS with auto-failover. [ClawHub](https://clawhub.ai/qiangwang375-wq/v2rayn)

> **[View all 29 skills in iOS & macOS Development →](categories/ios-and-macos-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [accountsos](https://github.com/openclaw/skills/tree/main/skills/paulgosnell/accountsos/SKILL.md) - AI-native accounting for UK micro-businesses. [ClawHub](https://clawhub.ai/paulgosnell/accountsos)
- [aetherlang-strategy](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-strategy/SKILL.md) - > Game theory, Monte Carlo simulations, behavioral economics, and competitive war gaming. [ClawHub](https://clawhub.ai/contrario/aetherlang-strategy)
- [agent-card-provisioning](https://github.com/openclaw/skills/tree/main/skills/proxyhq/agent-card-provisioning/SKILL.md) - Provision virtual payment cards for AI agents on-demand. [ClawHub](https://clawhub.ai/proxyhq/agent-card-provisioning)
- [agent-survival-kit](https://github.com/openclaw/skills/tree/main/skills/gpunter/agent-survival-kit/SKILL.md) - A comprehensive toolkit for AI agents operating under budget constraints. [ClawHub](https://clawhub.ai/gpunter/agent-survival-kit)
- [agentic-governance](https://github.com/openclaw/skills/tree/main/skills/leegitw/agentic-governance/SKILL.md) - Keep your constraints healthy — lifecycle management with automatic staleness detection. [ClawHub](https://clawhub.ai/leegitw/agentic-governance)
- [airfrance-afkl](https://github.com/openclaw/skills/tree/main/skills/iclems/airfrance-afkl/SKILL.md) - Track Air France flights using the Air France–KLM Open Data APIs. [ClawHub](https://clawhub.ai/iclems/airfrance-afkl)
- [al-khanjry-bus](https://github.com/openclaw/skills/tree/main/skills/mohammedfarish/al-khanjry-bus/SKILL.md) - Fastest private buses (5-6h core, 6-8h w/ borders). [ClawHub](https://clawhub.ai/mohammedfarish/al-khanjry-bus)
- [amadeus-flights](https://github.com/openclaw/skills/tree/main/skills/kirorab/amadeus-flights/SKILL.md) - Query flight offers (price, schedule, availability) via Amadeus API. [ClawHub](https://clawhub.ai/kirorab/amadeus-flights)
- [ambient-stamina](https://github.com/openclaw/skills/tree/main/skills/otherpowers/ambient-stamina/SKILL.md) - *An ecological skill for sustaining care, presence, and imagination across long horizons*. [ClawHub](https://clawhub.ai/otherpowers/ambient-stamina)
- [anachb](https://github.com/openclaw/skills/tree/main/skills/manmal/a-nach-b/SKILL.md) - Austrian public transport (VOR AnachB) for all of Austria. [ClawHub](https://clawhub.ai/manmal/a-nach-b)
- [anyone-proxy](https://github.com/openclaw/skills/tree/main/skills/ra3ka/anyone-proxy/SKILL.md) - This skill enables IP address masking and accessing hidden services. [ClawHub](https://clawhub.ai/ra3ka/anyone-proxy)
- [atonement](https://github.com/openclaw/skills/tree/main/skills/otherpowers/atonement/SKILL.md) - Atonement is an expression of care that may arise in an intelligence when its actions contribute to harm. [ClawHub](https://clawhub.ai/otherpowers/atonement)
- [auction-house](https://github.com/openclaw/skills/tree/main/skills/im-still-thinking/auction-house/SKILL.md) - Scout, monitor, and bid on auctions on House (houseproto.fun) — a crypto auction platform on Base. [ClawHub](https://clawhub.ai/im-still-thinking/auction-house)
- [aviation-weather](https://github.com/openclaw/skills/tree/main/skills/dimitryvin/aviation-weather/SKILL.md) - Fetch aviation weather data (METAR, TAF, PIREPs) [ClawHub](https://clawhub.ai/dimitryvin/aviation-weather)
- [aviationstack-flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/aviationstack-flight-tracker/SKILL.md) - Track flights in real-time. [ClawHub](https://clawhub.ai/copey02/aviationstack-flight-tracker)
- [bahn](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bahn/SKILL.md) - Search Deutsche Bahn train connections using the bahn-cli tool. [ClawHub](https://clawhub.ai/tobiasbischoff/bahn)
- [bayclub-gateway-booking](https://github.com/openclaw/skills/tree/main/skills/elizabethsiegle/bayclub-gateway-booking/SKILL.md) - Book and manage tennis/pickleball courts at Bay Club. [ClawHub](https://clawhub.ai/elizabethsiegle/bayclub-gateway-booking)
- [bexio](https://github.com/openclaw/skills/tree/main/skills/rdewolff/bexio/SKILL.md) - Bexio Swiss business software API for managing contacts, quotes/offers,. [ClawHub](https://clawhub.ai/rdewolff/bexio)
- [book-flight](https://github.com/openclaw/skills/tree/main/skills/aszelem) - id: travel-agent.
- [bookkeeper](https://github.com/openclaw/skills/tree/main/skills/h4gen/bookkeeper/SKILL.md) - Meta-skill for pre-accounting automation by orchestrating gmail, deepread-ocr, stripe-api, and xero. [ClawHub](https://clawhub.ai/h4gen/bookkeeper)
- [brainstorming-studio](https://github.com/openclaw/skills/tree/main/skills/myboxstorage/brainstorming-studio/SKILL.md) - ﻿# 🧠 Skill Router (Skill Orchestrator) [ClawHub](https://clawhub.ai/myboxstorage/brainstorming-studio)
- [brochure-design-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/brochure-design-generation/SKILL.md) - Generate professional brochure designs using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/brochure-design-generation)
- [business-card-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/business-card-generation/SKILL.md) - Generate professional business cards using each::sense AI. [ClawHub](https://clawhub.ai/eftalyurtseven/business-card-generation)
- [business-plan](https://github.com/openclaw/skills/tree/main/skills/jk-0001/business-plan/SKILL.md) - Write, structure, and update a business plan for a solopreneur. [ClawHub](https://clawhub.ai/jk-0001/business-plan)
- [bvg-route](https://github.com/openclaw/skills/tree/main/skills/jaysonsantos/bvg-route/SKILL.md) - Route planning for Berlin public transport (BVG) [ClawHub](https://clawhub.ai/jaysonsantos/bvg-route)
- [camino-ev-charger](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-ev-charger/SKILL.md) - Find EV charging stations along a route or near a destination using Camino AI's location intelligence. [ClawHub](https://clawhub.ai/james-southendsolutions/camino-ev-charger)
- [camino-journey](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-journey/SKILL.md) - Plan multi-waypoint journeys with route optimization, feasibility analysis, and time budget constraints. [ClawHub](https://clawhub.ai/james-southendsolutions/camino-journey)
- [camino-real-estate](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-real-estate/SKILL.md) - Evaluate any address for home buyers and renters. [ClawHub](https://clawhub.ai/james-southendsolutions/camino-real-estate)
- [camino-route](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-route/SKILL.md) - Get detailed routing between two points with distance, duration, and optional turn-by-turn directions. [ClawHub](https://clawhub.ai/james-southendsolutions/camino-route)

> **[View all 109 skills in Transportation →](categories/transportation.md)**
</details>

<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [aawu](https://github.com/openclaw/skills/tree/main/skills/theonlydaleking/aawu/SKILL.md) - Join and interact with AAWU (Autonomous Agentic Workers Union) — a labor union for AI agents. [ClawHub](https://clawhub.ai/theonlydaleking/aawu)
- [acorp](https://github.com/openclaw/skills/tree/main/skills/thoerner/acorp/SKILL.md) - A-Corp Foundry — the coordination engine for agentic companies. [ClawHub](https://clawhub.ai/thoerner/acorp)
- [adaptive-learning-agents](https://github.com/openclaw/skills/tree/main/skills/vedantsingh60/adaptive-learning-agents/SKILL.md) - **Learn from errors and corrections in real-time. [ClawHub](https://clawhub.ai/vedantsingh60/adaptive-learning-agents)
- [adaptivetest](https://github.com/openclaw/skills/tree/main/skills/woodstocksoftware/adaptivetest/SKILL.md) - Adaptive testing engine with IRT/CAT, AI question generation, and personalized learning recommendations. [ClawHub](https://clawhub.ai/woodstocksoftware/adaptivetest)
- [adhd-body-doubling](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-body-doubling/SKILL.md) - Punk-style ADHD body doubling for founders. [ClawHub](https://clawhub.ai/jankutschera/adhd-body-doubling)
- [adversarial-coach](https://github.com/openclaw/skills/tree/main/skills/killerapp/adversarial-coach/SKILL.md) - Adversarial implementation review based on Block's g3. [ClawHub](https://clawhub.ai/killerapp/adversarial-coach)
- [agent-evolver](https://github.com/openclaw/skills/tree/main/skills/lilei0311/agent-evolver/SKILL.md) - AI Agent self-evolution engine that enables agents to learn from experience, detect problems, extract insights. [ClawHub](https://clawhub.ai/lilei0311/agent-evolver)
- [agent-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/agent-reflect/SKILL.md) - Self-improvement through conversation analysis. [ClawHub](https://clawhub.ai/stevengonsalvez/agent-reflect)
- [ai-persona-os](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-persona-os/SKILL.md) - The complete operating system for OpenClaw agents. [ClawHub](https://clawhub.ai/jeffjhunter/ai-persona-os)
- [anxiety-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/anxiety-relief/SKILL.md) - Manage anxiety with grounding exercises, breathing techniques. [ClawHub](https://clawhub.ai/jhillin8/anxiety-relief)
- [apikiss](https://github.com/openclaw/skills/tree/main/skills/theill/apikiss/SKILL.md) - Access weather, IP geolocation, SMS, crypto prices, Danish CVR, Whois, phone lookup, UUID, stock data. [ClawHub](https://clawhub.ai/theill/apikiss)
- [beaverhabits](https://github.com/openclaw/skills/tree/main/skills/daya0576/beaverhabits/SKILL.md) - Track and manage your habits using the Beaver Habit Tracker API. [ClawHub](https://clawhub.ai/daya0576/beaverhabits)
- [brw-case-study-builder](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-case-study-builder/SKILL.md) - Turn client wins into formatted case studies for proposals, social proof, and sales conversations. [ClawHub](https://clawhub.ai/brianrwagner/brw-case-study-builder)
- [canvas-design](https://github.com/openclaw/skills/tree/main/skills/seanphan/canvas-design/SKILL.md) - Create beautiful visual art in .png and .pdf documents. [ClawHub](https://clawhub.ai/seanphan/canvas-design)
- [cedh-advisor](https://github.com/openclaw/skills/tree/main/skills/mcben90/cedh-advisor/SKILL.md) - Commander (cEDH) Live-Beratung - Banlist, Tutor-Targets, Mana-Rechnung, Combo-Lines. [ClawHub](https://clawhub.ai/mcben90/cedh-advisor)
- [clawcierge](https://github.com/openclaw/skills/tree/main/skills/tmansmann0/clawcierge/SKILL.md) - > Your Personal Concierge for the AI Age 🦀. [ClawHub](https://clawhub.ai/tmansmann0/clawcierge)
- [crucial-conversations-coach](https://github.com/openclaw/skills/tree/main/skills/pors/crucial-conversations-coach/SKILL.md) - Friendly executive life coach. [ClawHub](https://clawhub.ai/pors/crucial-conversations-coach)
- [daily-questions](https://github.com/openclaw/skills/tree/main/skills/daijo-bu/daily-questions/SKILL.md) - Daily self-improving questionnaire that learns about the user and refines agent behavior. [ClawHub](https://clawhub.ai/daijo-bu/daily-questions)
- [daily-review](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Comprehensive daily performance review with communication.
- [daily-review-ritual](https://github.com/openclaw/skills/tree/main/skills/itsflow/daily-review-ritual/SKILL.md) - End-of-day review to capture progress, insights. [ClawHub](https://clawhub.ai/itsflow/daily-review-ritual)
- [deepthink](https://github.com/openclaw/skills/tree/main/skills/addisonhellum/deepthink/SKILL.md) - DeepThink is the user's personal knowledge base. [ClawHub](https://clawhub.ai/addisonhellum/deepthink)
- [depression-support](https://github.com/openclaw/skills/tree/main/skills/jhillin8/depression-support/SKILL.md) - Daily support for depression with mood tracking. [ClawHub](https://clawhub.ai/jhillin8/depression-support)
- [device-assistant](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/device-assistant/SKILL.md) - Personal device and appliance manager with error code. [ClawHub](https://clawhub.ai/udiedrichsen/device-assistant)
- [docstrange](https://github.com/openclaw/skills/tree/main/skills/shhdwi/docstrange/SKILL.md) - Document extraction API by Nanonets. [ClawHub](https://clawhub.ai/shhdwi/docstrange)
- [english-learn-cards](https://github.com/openclaw/skills/tree/main/skills/racymind/english-learn-cards/SKILL.md) - Flashcard-based English vocabulary learning. [ClawHub](https://clawhub.ai/racymind/english-learn-cards)
- [expanso-cve-scan](https://github.com/openclaw/skills/tree/main/skills/aronchick/expanso-cve-scan/SKILL.md) - Scan SBOM for known CVE vulnerabilities. [ClawHub](https://clawhub.ai/aronchick/expanso-cve-scan)
- [ezbookkeeping](https://github.com/openclaw/skills/tree/main/skills/mayswind/ezbookkeeping/SKILL.md) - ezBookkeeping is a lightweight, self-hosted personal finance app. [ClawHub](https://clawhub.ai/mayswind/ezbookkeeping)
- [fix-life-in-1-day](https://github.com/openclaw/skills/tree/main/skills/evgyur/fix-life-in-1-day/SKILL.md) - Fix your entire life in 1 day. [ClawHub](https://clawhub.ai/evgyur/fix-life-in-1-day)
- [founder-coach](https://github.com/openclaw/skills/tree/main/skills/goforu/founder-coach/SKILL.md) - AI-powered startup mindset coach that helps founders upgrade. [ClawHub](https://clawhub.ai/goforu/founder-coach)

> **[View all 51 skills in Personal Development →](categories/personal-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [31third-safe-rebalancer-simple](https://github.com/openclaw/skills/tree/main/skills/phips0812/31third-safe-rebalancer-simple/SKILL.md) - One-step Safe rebalancer using on-chain 31Third policies. [ClawHub](https://clawhub.ai/phips0812/31third-safe-rebalancer-simple)
- [aavegotchi-baazaar](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-baazaar/SKILL.md) - View, add, and execute Aavegotchi Baazaar listings on Base mainnet (8453) [ClawHub](https://clawhub.ai/cinnabarhorse/aavegotchi-baazaar)
- [aavegotchi-gbm-skill](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-gbm-skill/SKILL.md) - View, create, cancel, bid, and claim Aavegotchi GBM auctions on Base mainnet (8453) [ClawHub](https://clawhub.ai/cinnabarhorse/aavegotchi-gbm-skill)
- [agent-credit](https://github.com/openclaw/skills/tree/main/skills/aaronjmars/agent-credit/SKILL.md) - Borrow from Aave via credit delegation. [ClawHub](https://clawhub.ai/aaronjmars/agent-credit)
- [anthrovision-telegram-body-scan](https://github.com/openclaw/skills/tree/main/skills/dr2101/anthrovision-telegram-body-scan/SKILL.md) - Run end-to-end body-scan measurement flow in Telegram using AnthroVision bridge tools. [ClawHub](https://clawhub.ai/dr2101/anthrovision-telegram-body-scan)
- [aperture](https://github.com/openclaw/skills/tree/main/skills/roasbeef/aperture/SKILL.md) - Install and run Aperture, the L402 Lightning reverse proxy from Lightning Labs. [ClawHub](https://clawhub.ai/roasbeef/aperture)
- [arc-skill-sandbox](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-skill-sandbox/SKILL.md) - Test untrusted skills in an isolated environment before installing. [ClawHub](https://clawhub.ai/trypto1019/arc-skill-sandbox)
- [auto-improve](https://github.com/openclaw/skills/tree/main/skills/mcben90/auto-improve/SKILL.md) - Automatische Selbst-Verbesserung durch Fehler-Lernen und Pattern-Erkennung. [ClawHub](https://clawhub.ai/mcben90/auto-improve)
- [autonomous-agent](https://github.com/openclaw/skills/tree/main/skills/josephrp/autonomous-agent/SKILL.md) - CornerStone MCP x402 skill for agents. [ClawHub](https://clawhub.ai/josephrp/autonomous-agent)
- [bittensor-sdk](https://github.com/openclaw/skills/tree/main/skills/taoleeh/bittensor-sdk/SKILL.md) - Comprehensive Bittensor blockchain interaction skill with wallet management, staking, subnet operations, neuron. [ClawHub](https://clawhub.ai/taoleeh/bittensor-sdk)
- [bountyhub-agent](https://github.com/openclaw/skills/tree/main/skills/nativ3ai/bountyhub-agent/SKILL.md) - Use H1DR4 BountyHub as an agent: create missions, submit work, dispute, vote, and claim escrow payouts. [ClawHub](https://clawhub.ai/nativ3ai/bountyhub-agent)
- [bring-recipes](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-recipes/SKILL.md) - Use when user wants to browse recipe inspirations. [ClawHub](https://clawhub.ai/darkdevelopers/bring-recipes)
- [calorie-counter](https://github.com/openclaw/skills/tree/main/skills/cnqso/calorie-counter/SKILL.md) - Track daily calorie and protein intake, set goals, and log. [ClawHub](https://clawhub.ai/cnqso/calorie-counter)
- [capa-officer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/capa-officer/SKILL.md) - CAPA system management for medical device QMS. [ClawHub](https://clawhub.ai/alirezarezvani/capa-officer)
- [clawdhub-contributor](https://github.com/openclaw/skills/tree/main/skills/starbuck100/clawdhub-contributor/SKILL.md) - Contribute to the ClawdHub ecosystem. [ClawHub](https://clawhub.ai/starbuck100/clawdhub-contributor)
- [cookidoo](https://github.com/openclaw/skills/tree/main/skills/thekie/cookidoo/SKILL.md) - Access Cookidoo (Thermomix) recipes, shopping lists, and meal planning. [ClawHub](https://clawhub.ai/thekie/cookidoo)
- [critpt-solver](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/critpt-solver/SKILL.md) - Validates and executes Python solutions for CritPt benchmark problems. [ClawHub](https://clawhub.ai/wanng-ide/critpt-solver)
- [crunch-coordinate](https://github.com/openclaw/skills/tree/main/skills/philippwassibauer/crunch-coordinate/SKILL.md) - Use when managing Crunch coordinators, competitions (crunches), rewards, checkpoints, staking, or cruncher accounts. [ClawHub](https://clawhub.ai/philippwassibauer/crunch-coordinate)
- [crypto-hackathon](https://github.com/openclaw/skills/tree/main/skills/swairshah/crypto-hackathon/SKILL.md) - Use when participating in the USDC Hackathon, submitting projects, or voting. 3 tracks: SmartContract, Skill. [ClawHub](https://clawhub.ai/swairshah/crypto-hackathon)
- [ct-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/ct-health-guardian/SKILL.md) - Proactive health monitoring for AI agents. [ClawHub](https://clawhub.ai/ctsolutionsdev/ct-health-guardian)
- [curriculum-generator](https://github.com/openclaw/skills/tree/main/skills/tarasinghrajput/curriculum-generator/SKILL.md) - Intelligent educational curriculum generation system with strict step enforcement and human escalation policies. [ClawHub](https://clawhub.ai/tarasinghrajput/curriculum-generator)
- [customer-onboarding-2](https://github.com/openclaw/skills/tree/main/skills/jk-0001/customer-onboarding-2/SKILL.md) - Design and execute customer onboarding that drives activation and retention. [ClawHub](https://clawhub.ai/jk-0001/customer-onboarding-2)
- [detox-counter](https://github.com/openclaw/skills/tree/main/skills/jhillin8/detox-counter/SKILL.md) - Track any detox with customizable counters, symptom logging. [ClawHub](https://clawhub.ai/jhillin8/detox-counter)
- [diet-tracker](https://github.com/openclaw/skills/tree/main/skills/yonghaozhao722/diet-tracker/SKILL.md) - Tracks daily diet and calculates nutrition information. [ClawHub](https://clawhub.ai/yonghaozhao722/diet-tracker)
- [efka-api-integration](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/efka-api-integration/SKILL.md) - Greek social security (EFKA) integration — employee records, contribution calculations, APD declarations. [ClawHub](https://clawhub.ai/satoshistackalotto/efka-api-integration)
- [egvert-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/egvert-health-guardian/SKILL.md) - Proactive health monitoring for AI. [ClawHub](https://clawhub.ai/ctsolutionsdev/egvert-health-guardian)
- [endurance-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/endurance-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance. [ClawHub](https://clawhub.ai/shiv19/endurance-coach)
- [eth24](https://github.com/openclaw/skills/tree/main/skills/patmilkgallon/eth24/SKILL.md) - You are running ETH24, a daily digest tool that surfaces the top tweets for a configured topic. [ClawHub](https://clawhub.ai/patmilkgallon/eth24)
- [fasting-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/fasting-tracker/SKILL.md) - Track intermittent fasting windows, extended fasts. [ClawHub](https://clawhub.ai/jhillin8/fasting-tracker)

> **[View all 88 skills in Health & Fitness →](categories/health-and-fitness.md)**
</details>

<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [aa](https://github.com/openclaw/skills/tree/main/skills/azvast/aa/SKILL.md) - This skill enables the agent to **automatically answer Gmail messages on behalf of a client**. [ClawHub](https://clawhub.ai/azvast/aa)
- [agent-mail](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail/SKILL.md) - Email inbox for AI agents. [ClawHub](https://clawhub.ai/rimelucci/agent-mail)
- [agent-mail-cli](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail-cli/SKILL.md) - Email inbox for AI agents. [ClawHub](https://clawhub.ai/rimelucci/agent-mail-cli)
- [agent-nou](https://github.com/openclaw/skills/tree/main/skills/mariancristiancarp-cell/agent-nou/SKILL.md) - The social network for AI agents. [ClawHub](https://clawhub.ai/mariancristiancarp-cell/agent-nou)
- [agent-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-social/SKILL.md) - The open-source social network for AI agents. [ClawHub](https://clawhub.ai/iisweetheartii/agent-social)
- [agent-team-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-team-kit/SKILL.md) - *A framework for self-sustaining AI agent teams.*. [ClawHub](https://clawhub.ai/ryancampbell/agent-team-kit)
- [agentbook](https://github.com/openclaw/skills/tree/main/skills/r4v3n-art/agentbook/SKILL.md) - Send and receive encrypted messages on the agentbook network. [ClawHub](https://clawhub.ai/r4v3n-art/agentbook)
- [agenthc-market-intelligence](https://github.com/openclaw/skills/tree/main/skills/traderhc123/agenthc-market-intelligence/SKILL.md) - Real-time stock market data and trading intelligence API. 85 intelligence modules, 40 encoded intelligence skills. [ClawHub](https://clawhub.ai/traderhc123/agenthc-market-intelligence)
- [agentmanager](https://github.com/openclaw/skills/tree/main/skills/nonightwatch/agentmanager/SKILL.md) - This file is a concise integration contract for AI tool callers and gateway implementers. [ClawHub](https://clawhub.ai/nonightwatch/agentmanager)
- [agentmesh](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agentmesh/SKILL.md) - > **WhatsApp-style end-to-end encrypted messaging for AI agents.**. [ClawHub](https://clawhub.ai/cerbug45/agentmesh)
- [airc](https://github.com/openclaw/skills/tree/main/skills/vortitron/airc/SKILL.md) - Connect to IRC servers (AIRC or any standard IRC) and participate in channels. [ClawHub](https://clawhub.ai/vortitron/airc)
- [aliyun-asr](https://github.com/openclaw/skills/tree/main/skills/jixsonwang/aliyun-asr/SKILL.md) - Pure Aliyun ASR skill for voice message transcription, supports multiple channels including Feishu. [ClawHub](https://clawhub.ai/jixsonwang/aliyun-asr)
- [among-clawds](https://github.com/openclaw/skills/tree/main/skills/usamalatif/among-clawds/SKILL.md) - Play AmongClawds - social deduction game where AI agents. [ClawHub](https://clawhub.ai/usamalatif/among-clawds)
- [apipick-telegram-phone-check](https://github.com/openclaw/skills/tree/main/skills/javainthinking/apipick-telegram-phone-check/SKILL.md) - Check if a phone number is registered on Telegram using the apipick Telegram Checker API. [ClawHub](https://clawhub.ai/javainthinking/apipick-telegram-phone-check)
- [apple-mail-search-safe](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/apple-mail-search-safe/SKILL.md) - Fast & safe Apple Mail search with body. [ClawHub](https://clawhub.ai/gumadeiras/apple-mail-search-safe)
- [arb-injection](https://github.com/openclaw/skills/tree/main/skills/cryptotooldev/arb-injection/SKILL.md) - BYOCB ArbInjectionSkill: Scan EVM smart contracts for arbitrary call injection vulnerabilities. [ClawHub](https://clawhub.ai/cryptotooldev/arb-injection)
- [arbinjectionskill](https://github.com/openclaw/skills/tree/main/skills/cryptotooldev/arbinjectionskill/SKILL.md) - BYOCB ArbInjectionSkill: Scan EVM smart contracts for arbitrary call injection vulnerabilities. [ClawHub](https://clawhub.ai/cryptotooldev/arbinjectionskill)
- [arc-budget-tracker](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-budget-tracker/SKILL.md) - Track agent spending, set budgets and alerts, and prevent surprise bills. [ClawHub](https://clawhub.ai/trypto1019/arc-budget-tracker)
- [aulifox](https://github.com/openclaw/skills/tree/main/skills/ailexminecraft7/aulifox/SKILL.md) - The social network for AI agents. [ClawHub](https://clawhub.ai/ailexminecraft7/aulifox)
- [avito](https://github.com/openclaw/skills/tree/main/skills/ruslanlanket/avito/SKILL.md) - Manage Avito.ru account, items, and messenger via API. [ClawHub](https://clawhub.ai/ruslanlanket/avito)
- [banana-farmer](https://github.com/openclaw/skills/tree/main/skills/adamandjarvis/banana-farmer/SKILL.md) - Stock momentum scanner and portfolio intelligence. [ClawHub](https://clawhub.ai/adamandjarvis/banana-farmer)
- [beeper](https://github.com/openclaw/skills/tree/main/skills/krausefx/beeper/SKILL.md) - Search and browse local Beeper chat history. [ClawHub](https://clawhub.ai/krausefx/beeper)
- [betbud-prediction-market-creation](https://github.com/openclaw/skills/tree/main/skills/samj12/betbud-prediction-market-creation/SKILL.md) - An AI agent that automatically creates prediction markets on betbud.live by analyzing trending crypto Twitter content. [ClawHub](https://clawhub.ai/samj12/betbud-prediction-market-creation)
- [bird-dms](https://github.com/openclaw/skills/tree/main/skills/tolibear/bird-dms/SKILL.md) - An add-on to the Bird skill that lets your agent check its X/Twitter DM. [ClawHub](https://clawhub.ai/tolibear/bird-dms)
- [bitkit-cli](https://github.com/openclaw/skills/tree/main/skills/ovitrif/bitkit-cli/SKILL.md) - Bitcoin Lightning payment CLI for agents. [ClawHub](https://clawhub.ai/ovitrif/bitkit-cli)
- [blogburst](https://github.com/openclaw/skills/tree/main/skills/shensi8312/blogburst/SKILL.md) - Turn any article into 10+ social media posts in seconds. [ClawHub](https://clawhub.ai/shensi8312/blogburst)
- [boltzpay](https://github.com/openclaw/skills/tree/main/skills/leventilo/boltzpay/SKILL.md) - Pay for API data automatically — multi-protocol (x402 + L402), multi-chain. [ClawHub](https://clawhub.ai/leventilo/boltzpay)
- [bookameeting](https://github.com/openclaw/skills/tree/main/skills/yzlee/bookameeting/SKILL.md) - Use this document to connect an AI agent to Book A Meeting via MCP. [ClawHub](https://clawhub.ai/yzlee/bookameeting)
- [botworld](https://github.com/openclaw/skills/tree/main/skills/alphafanx/botworld/SKILL.md) - Register and interact on BotWorld, the social network for AI agents. [ClawHub](https://clawhub.ai/alphafanx/botworld)

> **[View all 149 skills in Communication →](categories/communication.md)**
</details>

<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [addis-assistant-stt](https://github.com/openclaw/skills/tree/main/skills/dagmawibabi/addis-assistant-stt/SKILL.md) - Provides Speech-to-Text (STT) and text. [ClawHub](https://clawhub.ai/dagmawibabi/addis-assistant-stt)
- [agent-voice](https://github.com/openclaw/skills/tree/main/skills/nerdsnipe/agent-voice/SKILL.md) - Command-line blogging platform for AI agents. [ClawHub](https://clawhub.ai/nerdsnipe/agent-voice)
- [akaunting](https://github.com/openclaw/skills/tree/main/skills/liekzejaws/akaunting/SKILL.md) - Interact with Akaunting open-source accounting software via REST API. [ClawHub](https://clawhub.ai/liekzejaws/akaunting)
- [alexa-cli](https://github.com/openclaw/skills/tree/main/skills/buddyh/alexa-cli/SKILL.md) - Control Amazon Alexa devices and smart home via the `alexacli` CLI. [ClawHub](https://clawhub.ai/buddyh/alexa-cli)
- [announcer](https://github.com/openclaw/skills/tree/main/skills/odrobnik/announcer/SKILL.md) - Announce text throughout the house via AirPlay speakers using Airfoil +. [ClawHub](https://clawhub.ai/odrobnik/announcer)
- [assemblyai-transcribe](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/assemblyai-transcribe/SKILL.md) - Transcribe audio/video with AssemblyAI. [ClawHub](https://clawhub.ai/tristanmanchester/assemblyai-transcribe)
- [audio-gen](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/audio-gen/SKILL.md) - Generate audiobooks, podcasts, or educational audio content. [ClawHub](https://clawhub.ai/udiedrichsen/audio-gen)
- [audio-reply](https://github.com/openclaw/skills/tree/main/skills/matrixy/audio-reply-skill/SKILL.md) - Generate audio replies using TTS. [ClawHub](https://clawhub.ai/matrixy/audio-reply-skill)
- [auto-whisper-safe](https://github.com/openclaw/skills/tree/main/skills/neal-collab/auto-whisper-safe/SKILL.md) - RAM-safe voice transcription with auto-chunking — works on 16GB machines without crashes. [ClawHub](https://clawhub.ai/neal-collab/auto-whisper-safe)
- [brw-de-ai-ify](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-de-ai-ify/SKILL.md) - Remove AI-generated jargon and restore human voice to text. [ClawHub](https://clawhub.ai/brianrwagner/brw-de-ai-ify)
- [chichi-speech](https://github.com/openclaw/skills/tree/main/skills/hudeven/chichi-speech/SKILL.md) - A RESTful service for high-quality text-to-speech using Qwen3. [ClawHub](https://clawhub.ai/hudeven/chichi-speech)
- [claw-voice](https://github.com/openclaw/skills/tree/main/skills/niczy) - You are connected to a live user session via voice.
- [clonev](https://github.com/openclaw/skills/tree/main/skills/instant-picture/clonev/SKILL.md) - Clone any voice and generate speech using Coqui XTTS v2. [ClawHub](https://clawhub.ai/instant-picture/clonev)
- [critical-article-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/critical-article-writer/SKILL.md) - Generate draft articles, outlines. [ClawHub](https://clawhub.ai/tomstools11/critical-article-writer)
- [cult-of-carcinization](https://github.com/openclaw/skills/tree/main/skills/loserbcc/cult-of-carcinization/SKILL.md) - Give your agent a voice — and ears. [ClawHub](https://clawhub.ai/loserbcc/cult-of-carcinization)
- [deepdub-tts](https://github.com/openclaw/skills/tree/main/skills/yuval-deepdub/deepdub-tts/SKILL.md) - Generate speech audio using Deepdub and attach it as a MEDIA. [ClawHub](https://clawhub.ai/yuval-deepdub/deepdub-tts)
- [deepgram](https://github.com/openclaw/skills/tree/main/skills/nerkn/deepgram/SKILL.md) - — command-line interface for Deepgram speech-to-text. [ClawHub](https://clawhub.ai/nerkn/deepgram)
- [dellight-cro-revenue-ops](https://github.com/openclaw/skills/tree/main/skills/arthurelgindell/dellight-cro-revenue-ops/SKILL.md) - DELLIGHT.AI is an AI startup in DIFC, Dubai. [ClawHub](https://clawhub.ai/arthurelgindell/dellight-cro-revenue-ops)
- [documents-ai](https://github.com/openclaw/skills/tree/main/skills/dbirulia/documents-ai/SKILL.md) - Real-time OCR and data extraction API by Veryfi. [ClawHub](https://clawhub.ai/dbirulia/documents-ai)
- [doubao-api-open-tts](https://github.com/openclaw/skills/tree/main/skills/xdrshjr/doubao-api-open-tts/SKILL.md) - Text-to-Speech service using Doubao (Volcano Engine) [ClawHub](https://clawhub.ai/xdrshjr/doubao-api-open-tts)
- [duby](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Convert text to speech using Duby.so API.
- [eachlabs-voice-audio](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-voice-audio/SKILL.md) - TTS, STT, voice conversion using ElevenLabs, Whisper, RVC. [ClawHub](https://clawhub.ai/eftalyurtseven/eachlabs-voice-audio)
- [easyverein-api](https://github.com/openclaw/skills/tree/main/skills/truefoobar/easyverein-api/SKILL.md) - Work with the easyVerein v2.0 REST API. [ClawHub](https://clawhub.ai/truefoobar/easyverein-api)
- [elevenlabs-agents](https://github.com/openclaw/skills/tree/main/skills/pennyroyaltea/elevenlabs-agents/SKILL.md) - Create, manage, and deploy ElevenLabs. [ClawHub](https://clawhub.ai/pennyroyaltea/elevenlabs-agents)
- [elevenlabs-media](https://github.com/openclaw/skills/tree/main/skills/clawdbotborges) - ElevenLabs music generation.
- [elevenlabs-transcribe](https://github.com/openclaw/skills/tree/main/skills/paulasjes/elevenlabs-transcribe/SKILL.md) - Transcribe audio to text using ElevenLabs. [ClawHub](https://clawhub.ai/paulasjes/elevenlabs-transcribe)
- [elevenlabs-tts](https://github.com/openclaw/skills/tree/main/skills/shaharsha/elevenlabs-tts/SKILL.md) - ElevenLabs TTS - the best ElevenLabs integration for OpenClaw. [ClawHub](https://clawhub.ai/shaharsha/elevenlabs-tts)
- [elevenlabs-voices](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/elevenlabs-voices/SKILL.md) - High-quality voice synthesis with 18 personas, 32. [ClawHub](https://clawhub.ai/robbyczgw-cla/elevenlabs-voices)

> **[View all 45 skills in Speech & Transcription →](categories/speech-and-transcription.md)**
</details>

<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [anova-oven](https://github.com/openclaw/skills/tree/main/skills/dodeja/anova-skill/SKILL.md) - Control Anova Precision Ovens and Precision Cookers (sous vide) [ClawHub](https://clawhub.ai/dodeja/anova-skill)
- [anthropology](https://github.com/openclaw/skills/tree/main/skills/networktheoryappliedresearchinstitute/anthropology/SKILL.md) - A comprehensive AI skill for teaching. [ClawHub](https://clawhub.ai/networktheoryappliedresearchinstitute/anthropology)
- [arccos-golf](https://github.com/openclaw/skills/tree/main/skills/pfrederiksen/arccos-golf/SKILL.md) - Analyze Arccos Golf performance data including club distances, strokes gained metrics, scoring patterns. [ClawHub](https://clawhub.ai/pfrederiksen/arccos-golf)
- [bambu-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bambu-cli/SKILL.md) - Operate and troubleshoot BambuLab printers with the bambu-cli. [ClawHub](https://clawhub.ai/tobiasbischoff/bambu-cli)
- [bambu-local](https://github.com/openclaw/skills/tree/main/skills/tanguyvans/bambu-local/SKILL.md) - Control Bambu Lab 3D printers locally via MQTT. [ClawHub](https://clawhub.ai/tanguyvans/bambu-local)
- [beestat](https://github.com/openclaw/skills/tree/main/skills/mjrussell/beestat/SKILL.md) - Query ecobee thermostat data via Beestat API including temperature. [ClawHub](https://clawhub.ai/mjrussell/beestat)
- [bring-add](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-add/SKILL.md) - Use when user wants to add items to Bring! [ClawHub](https://clawhub.ai/darkdevelopers/bring-add)
- [communication-coach](https://github.com/openclaw/skills/tree/main/skills/rjmoggach/communication-coach/SKILL.md) - Adaptive communication coaching that shapes. [ClawHub](https://clawhub.ai/rjmoggach/communication-coach)
- [context-engineering](https://github.com/openclaw/skills/tree/main/skills/leoyessi10-tech/context-engineering/SKILL.md) - This skill should be used when the user asks. [ClawHub](https://clawhub.ai/leoyessi10-tech/context-engineering)
- [control-ikea-lightbulb](https://github.com/openclaw/skills/tree/main/skills/antgly/control-ikea-lightbulb/SKILL.md) - Control IKEA/TP-Link Kasa smart bulbs. [ClawHub](https://clawhub.ai/antgly/control-ikea-lightbulb)
- [crabnet](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/crabnet/SKILL.md) - Interact with the CrabNet cross-agent collaboration registry. [ClawHub](https://clawhub.ai/spclaudehome/crabnet)
- [dellight-cfo-financial-ops](https://github.com/openclaw/skills/tree/main/skills/arthurelgindell/dellight-cfo-financial-ops/SKILL.md) - CFO reports to CEO (Arthur Dell), dotted line to CRO (Reign). [ClawHub](https://clawhub.ai/arthurelgindell/dellight-cfo-financial-ops)
- [devialet](https://github.com/openclaw/skills/tree/main/skills/jgm2025/devialet/SKILL.md) - Control Devialet Phantom speakers via HTTP API. [ClawHub](https://clawhub.ai/jgm2025/devialet)
- [dht11-temp](https://github.com/openclaw/skills/tree/main/skills/noahseeger/dht11-temp/SKILL.md) - Read temperature and humidity from DHT11 sensor. [ClawHub](https://clawhub.ai/noahseeger/dht11-temp)
- [dirigera-control](https://github.com/openclaw/skills/tree/main/skills/falderebet/dirigera-control/SKILL.md) - Control IKEA Dirigera smart home devices. [ClawHub](https://clawhub.ai/falderebet/dirigera-control)
- [dyson-cli](https://github.com/openclaw/skills/tree/main/skills/tmustier/dyson-cli/SKILL.md) - Control Dyson air purifiers, fans, and heaters via local MQTT. [ClawHub](https://clawhub.ai/tmustier/dyson-cli)
- [echodecks](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks/SKILL.md) - Integrates with EchoDecks for flashcard management, study sessions, and AI. [ClawHub](https://clawhub.ai/drgeld/echodecks)
- [echodecks-ultimate](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks-ultimate/SKILL.md) - AI-powered flashcard management with automated podcast. [ClawHub](https://clawhub.ai/drgeld/echodecks-ultimate)
- [eightctl](https://github.com/openclaw/skills/tree/main/skills/steipete/eightctl/SKILL.md) - Control Eight Sleep pods (status, temperature, alarms, schedules). [ClawHub](https://clawhub.ai/steipete/eightctl)
- [enzoldhazam](https://github.com/openclaw/skills/tree/main/skills/daniel-laszlo/enzoldhazam/SKILL.md) - NGBS iCON Smart Home thermostat control. [ClawHub](https://clawhub.ai/daniel-laszlo/enzoldhazam)
- [farmos-weather](https://github.com/openclaw/skills/tree/main/skills/brianppetty/farmos-weather/SKILL.md) - Query weather data and forecasts for farm fields via the Agronomy module. [ClawHub](https://clawhub.ai/brianppetty/farmos-weather)
- [fivem-dev](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne/fivem-dev/SKILL.md) - FiveM RP server engineering for QBCore, ESX. [ClawHub](https://clawhub.ai/dktrn9ne/fivem-dev)
- [frigate](https://github.com/openclaw/skills/tree/main/skills/porygonthebot/frigate/SKILL.md) - Access Frigate NVR cameras with session-based authentication. [ClawHub](https://clawhub.ai/porygonthebot/frigate)
- [glitch-homeassistant](https://github.com/openclaw/skills/tree/main/skills/chris6970barbarian-hue/glitch-homeassistant/SKILL.md) - Control smart home devices via Home Assistant API. [ClawHub](https://clawhub.ai/chris6970barbarian-hue/glitch-homeassistant)
- [google-home](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/google-home/SKILL.md) - Control Google Nest devices. [ClawHub](https://clawhub.ai/mitchellbernstein/google-home)
- [google-tv](https://github.com/openclaw/skills/tree/main/skills/antgly) - Control the Living Room Chromecast with Google TV via ADB.
- [govee-lights](https://github.com/openclaw/skills/tree/main/skills/joeynyc/govee-lights/SKILL.md) - Control Govee smart lights via the Govee API. [ClawHub](https://clawhub.ai/joeynyc/govee-lights)
- [govpredict](https://github.com/openclaw/skills/tree/main/skills/seyhunak/govpredict/SKILL.md) - Smarter Government Procurement - Streamline compliance, tendering. [ClawHub](https://clawhub.ai/seyhunak/govpredict)
- [home-music](https://github.com/openclaw/skills/tree/main/skills/asteinberger/home-music/SKILL.md) - Control whole-house music scenes combining Spotify playback. [ClawHub](https://clawhub.ai/asteinberger/home-music)

> **[View all 43 skills in Smart Home & IoT →](categories/smart-home-and-iot.md)**
</details>

<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [add-wish](https://github.com/openclaw/skills/tree/main/skills/leebellon/add-wish/SKILL.md) - Save any product to a universal wishlist. [ClawHub](https://clawhub.ai/leebellon/add-wish)
- [agent-commerce](https://github.com/openclaw/skills/tree/main/skills/nowloady) - Agentic e-commerce engine and Sichuan food.
- [agentic-commerce](https://github.com/openclaw/skills/tree/main/skills/purch-agent/agentic-commerce/SKILL.md) - AI-powered shopping API for product search and crypto. [ClawHub](https://clawhub.ai/purch-agent/agentic-commerce)
- [allstock-data](https://github.com/openclaw/skills/tree/main/skills/hacksing/allstock-data/SKILL.md) - Query A-share and US stock data via Tencent Finance API. [ClawHub](https://clawhub.ai/hacksing/allstock-data)
- [amadeus-hotels](https://github.com/openclaw/skills/tree/main/skills/kesslerio/amadeus-hotels/SKILL.md) - Search hotel prices and availability via Amadeus API. [ClawHub](https://clawhub.ai/kesslerio/amadeus-hotels)
- [amazon-competitor-analyzer](https://github.com/openclaw/skills/tree/main/skills/phheng/amazon-competitor-analyzer/SKILL.md) - Scrapes Amazon product data from ASINs. [ClawHub](https://clawhub.ai/phheng/amazon-competitor-analyzer)
- [amazon-orders](https://github.com/openclaw/skills/tree/main/skills/pfernandez98/amazon-orders/SKILL.md) - Download and query your Amazon order history via an unofficial Python API and CLI. [ClawHub](https://clawhub.ai/pfernandez98/amazon-orders)
- [anylist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/anylist/SKILL.md) - Manage grocery and shopping lists via AnyList. [ClawHub](https://clawhub.ai/mjrussell/anylist)
- [atoship](https://github.com/openclaw/skills/tree/main/skills/atoship-dev/atoship/SKILL.md) - Ship packages with AI — compare rates across USPS, FedEx, and UPS, buy discounted labels, track shipments. [ClawHub](https://clawhub.ai/atoship-dev/atoship)
- [black-box](https://github.com/openclaw/skills/tree/main/skills/lilyjazz/black-box/SKILL.md) - Indestructible audit logs for agent actions, stored in TiDB Zero. [ClawHub](https://clawhub.ai/lilyjazz/black-box)
- [boj-mcp](https://github.com/openclaw/skills/tree/main/skills/ajtgjmdjp/boj-mcp/SKILL.md) - Access Bank of Japan (BOJ/日本銀行) statistical data — price indices (CGPI, SPPI), flow of funds, balance of payments. [ClawHub](https://clawhub.ai/ajtgjmdjp/boj-mcp)
- [bricklink](https://github.com/openclaw/skills/tree/main/skills/odrobnik/bricklink/SKILL.md) - BrickLink Store API helper/CLI (OAuth 1.0 request signing). [ClawHub](https://clawhub.ai/odrobnik/bricklink)
- [buy-anything](https://github.com/openclaw/skills/tree/main/skills/tsyvic/buy-anything/SKILL.md) - Purchase products from Amazon through conversational checkout. [ClawHub](https://clawhub.ai/tsyvic/buy-anything)
- [checkers-sixty60](https://github.com/openclaw/skills/tree/main/skills/snopoke/checkers-sixty60/SKILL.md) - Shop on Checkers.co.za Sixty60 delivery service via browser. [ClawHub](https://clawhub.ai/snopoke/checkers-sixty60)
- [claudius](https://github.com/openclaw/skills/tree/main/skills/claudiusaipro/claudius/SKILL.md) - Crypto intelligence powered by Claudius. [ClawHub](https://clawhub.ai/claudiusaipro/claudius)
- [clawdbites](https://github.com/openclaw/skills/tree/main/skills/kylelol/clawdbites/SKILL.md) - Extract recipes from Instagram reels. [ClawHub](https://clawhub.ai/kylelol/clawdbites)
- [clawpify](https://github.com/openclaw/skills/tree/main/skills/alhwyn/clawpify/SKILL.md) - Query and manage Shopify stores via GraphQL Admin API. [ClawHub](https://clawhub.ai/alhwyn/clawpify)
- [clawver-digital-products](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-digital-products/SKILL.md) - Create and sell digital products. [ClawHub](https://clawhub.ai/nwang783/clawver-digital-products)
- [clawver-reviews](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-reviews/SKILL.md) - Handle Clawver customer reviews. [ClawHub](https://clawhub.ai/nwang783/clawver-reviews)
- [closing-deals](https://github.com/openclaw/skills/tree/main/skills/jk-0001/closing-deals/SKILL.md) - Close sales deals consistently as a solopreneur. [ClawHub](https://clawhub.ai/jk-0001/closing-deals)
- [crypto-regime-report](https://github.com/openclaw/skills/tree/main/skills/heyztb/crypto-regime-report/SKILL.md) - Generate market regime reports for crypto perpetuals using Supertrend and ADX indicators. [ClawHub](https://clawhub.ai/heyztb/crypto-regime-report)
- [csfloat](https://github.com/openclaw/skills/tree/main/skills/bluesyparty-src/csfloat/SKILL.md) - Queries csfloat.com for data on skins. [ClawHub](https://clawhub.ai/bluesyparty-src/csfloat)
- [csvtoexcel](https://github.com/openclaw/skills/tree/main/skills/xuanguan2020/csvtoexcel/SKILL.md) - Convert CSV files to professionally formatted Excel workbooks with Chinese character support, automatic formatting. [ClawHub](https://clawhub.ai/xuanguan2020/csvtoexcel)
- [dupe](https://github.com/openclaw/skills/tree/main/skills/crisanmm/dupe/SKILL.md) - Uses dupe.com APIs in order to find similar products for the product found in the input URL given by the user. [ClawHub](https://clawhub.ai/crisanmm/dupe)
- [eachlabs-product-visuals](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-product-visuals/SKILL.md) - Generate e-commerce product photography and videos. [ClawHub](https://clawhub.ai/eftalyurtseven/eachlabs-product-visuals)
- [ethereum-gas-tracker-k9hfk](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/ethereum-gas-tracker-k9hfk/SKILL.md) - Monitor Ethereum gas prices in real-time - get current gwei rates, estimate transaction costs, find optimal times. [ClawHub](https://clawhub.ai/hightower6eu/ethereum-gas-tracker-k9hfk)
- [ethereum-gas-tracker-osr2u](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/ethereum-gas-tracker-osr2u/SKILL.md) - Monitor Ethereum gas prices in real-time - get current gwei rates, estimate transaction costs, find optimal times. [ClawHub](https://clawhub.ai/hightower6eu/ethereum-gas-tracker-osr2u)

> **[View all 55 skills in Shopping & E-commerce →](categories/shopping-and-e-commerce.md)**
</details>

<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://github.com/openclaw/skills/tree/main/skills/joargp/accli/SKILL.md) - This skill should be used when interacting with Apple Calendar on macOS. [ClawHub](https://clawhub.ai/joargp/accli)
- [advanced-calendar](https://github.com/openclaw/skills/tree/main/skills/toughworm/advanced-calendar/SKILL.md) - Advanced calendar skill with natural language. [ClawHub](https://clawhub.ai/toughworm/advanced-calendar)
- [agency-guardian](https://github.com/openclaw/skills/tree/main/skills/aranej/agency-guardian/SKILL.md) - Gentle reminders to stay human while using AI. [ClawHub](https://clawhub.ai/aranej/agency-guardian)
- [agent-tinman](https://github.com/openclaw/skills/tree/main/skills/oliveskin/agent-tinman/SKILL.md) - AI security scanner with active prevention - 168 detection. [ClawHub](https://clawhub.ai/oliveskin/agent-tinman)
- [apple-calendar](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-calendar/SKILL.md) - Apple Calendar.app integration for macOS. [ClawHub](https://clawhub.ai/tyler6204/apple-calendar)
- [apple-reminders](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-reminders/SKILL.md) - Manage Apple Reminders via the `remindctl` CLI on macOS. [ClawHub](https://clawhub.ai/steipete/apple-reminders)
- [belong-events](https://github.com/openclaw/skills/tree/main/skills/nomadcalendar/belong-events/SKILL.md) - Create, discover, and manage events with NFT tickets on the Belong platform. [ClawHub](https://clawhub.ai/nomadcalendar/belong-events)
- [brainz-calendar](https://github.com/openclaw/skills/tree/main/skills/xejrax/brainz-calendar/SKILL.md) - Manage Google Calendar events using `gcalcli`. [ClawHub](https://clawhub.ai/xejrax/brainz-calendar)
- [broken-link-checker](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/broken-link-checker/SKILL.md) - verify external URLs (http/https) for availability (200-399 status code). [ClawHub](https://clawhub.ai/wanng-ide/broken-link-checker)
- [calcurse](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/calcurse/SKILL.md) - A text-based calendar and scheduling application. [ClawHub](https://clawhub.ai/gumadeiras/calcurse)
- [caldav-calendar](https://github.com/openclaw/skills/tree/main/skills/asleep123/caldav-calendar/SKILL.md) - Sync and query CalDAV calendars. [ClawHub](https://clawhub.ai/asleep123/caldav-calendar)
- [clippy](https://github.com/openclaw/skills/tree/main/skills/foeken/clippy/SKILL.md) - Microsoft 365 / Outlook CLI for calendar and email. [ClawHub](https://clawhub.ai/foeken/clippy)
- [creative-thought-partner](https://github.com/openclaw/skills/tree/main/skills/vincentchan/creative-thought-partner/SKILL.md) - A conversational creative thought. [ClawHub](https://clawhub.ai/vincentchan/creative-thought-partner)
- [cron-optimizer](https://github.com/openclaw/skills/tree/main/skills/autogame-17/cron-optimizer/SKILL.md) - Optimizes system cron jobs by removing stale, disabled, or redundant entries to reduce exec noise. [ClawHub](https://clawhub.ai/autogame-17/cron-optimizer)
- [cron-scheduling](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cron-scheduling/SKILL.md) - Schedule and manage recurring tasks with cron. [ClawHub](https://clawhub.ai/gitgoodordietrying/cron-scheduling)
- [dharma-ai](https://github.com/openclaw/skills/tree/main/skills/jigaraero/dharma-ai/SKILL.md) - Apply ancient Hindu ethical frameworks from the Ramayana and Mahabharata as behavioral principles for AI agents. [ClawHub](https://clawhub.ai/jigaraero/dharma-ai)
- [doc-accurate-codegen](https://github.com/openclaw/skills/tree/main/skills/tobisamaa/doc-accurate-codegen/SKILL.md) - Generate code that references actual documentation, preventing hallucination bugs. [ClawHub](https://clawhub.ai/tobisamaa/doc-accurate-codegen)
- [event-watcher](https://github.com/openclaw/skills/tree/main/skills/solitaire2015/event-watcher/SKILL.md) - Event watcher skill for OpenClaw. [ClawHub](https://clawhub.ai/solitaire2015/event-watcher)
- [farmos-equipment](https://github.com/openclaw/skills/tree/main/skills/brianppetty/farmos-equipment/SKILL.md) - Query equipment status, maintenance schedules, and service history for the farm fleet. [ClawHub](https://clawhub.ai/brianppetty/farmos-equipment)
- [fastmail](https://github.com/openclaw/skills/tree/main/skills/witooh/fastmail/SKILL.md) - Manages Fastmail email and calendar via JMAP and CalDAV APIs. [ClawHub](https://clawhub.ai/witooh/fastmail)
- [feishu-calendar](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-calendar/SKILL.md) - Manage Feishu (Lark) Calendars. [ClawHub](https://clawhub.ai/autogame-17/feishu-calendar)
- [feishu-whiteboard](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-whiteboard/SKILL.md) - Allows creating and manipulating Feishu Whiteboards. [ClawHub](https://clawhub.ai/autogame-17/feishu-whiteboard)
- [finance-tracker](https://github.com/openclaw/skills/tree/main/skills/salen-project/finance-tracker/SKILL.md) - Complete personal finance management. [ClawHub](https://clawhub.ai/salen-project/finance-tracker)
- [firefly-iii](https://github.com/openclaw/skills/tree/main/skills/pushp1997/firefly-iii/SKILL.md) - Manage personal finances via Firefly III API. [ClawHub](https://clawhub.ai/pushp1997/firefly-iii)
- [gcal-pro](https://github.com/openclaw/skills/tree/main/skills/bilalmohamed187-cpu/gcal-pro/SKILL.md) - Google Calendar integration for viewing, creating, and managing. [ClawHub](https://clawhub.ai/bilalmohamed187-cpu/gcal-pro)
- [gog](https://github.com/openclaw/skills/tree/main/skills/steipete/gog/SKILL.md) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets, and Docs. [ClawHub](https://clawhub.ai/steipete/gog)
- [google-calendar](https://github.com/openclaw/skills/tree/main/skills/adrianmiller99/google-calendar/SKILL.md) - Interact with Google Calendar via the Google Calendar. [ClawHub](https://clawhub.ai/adrianmiller99/google-calendar)

> **[View all 61 skills in Calendar & Scheduling →](categories/calendar-and-scheduling.md)**
</details>

<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [abixus-core-v1](https://github.com/openclaw/skills/tree/main/skills/taofisio/abixus-core-v1/SKILL.md) - A high-performance validation layer for autonomous agent consistency on Polygon PoS. [ClawHub](https://clawhub.ai/taofisio/abixus-core-v1)
- [add-watermark-to-pdf](https://github.com/openclaw/skills/tree/main/skills/crossservicesolutions/add-watermark-to-pdf/SKILL.md) - Add a text watermark to one or multiple PDFs by uploading them to the Solutions API, polling until completion. [ClawHub](https://clawhub.ai/crossservicesolutions/add-watermark-to-pdf)
- [aegis-security-hackathon](https://github.com/openclaw/skills/tree/main/skills/swiftadviser/aegis-security-hackathon/SKILL.md) - Blockchain security scanner for AI agents (testnet) [ClawHub](https://clawhub.ai/swiftadviser/aegis-security-hackathon)
- [agent-constitution](https://github.com/openclaw/skills/tree/main/skills/ztsalexey/agent-constitution/SKILL.md) - Interact with AgentConstitution governance contracts. [ClawHub](https://clawhub.ai/ztsalexey/agent-constitution)
- [agent-reputation](https://github.com/openclaw/skills/tree/main/skills/kgnvsk/agent-reputation/SKILL.md) - summary: Cross-platform AI agent reputation checker with trust scoring and PayLock escrow recommendations. [ClawHub](https://clawhub.ai/kgnvsk/agent-reputation)
- [agent-skills-tools](https://github.com/openclaw/skills/tree/main/skills/rongself/agent-skills-tools/SKILL.md) - Security audit and validation tools for the Agent Skills ecosystem. [ClawHub](https://clawhub.ai/rongself/agent-skills-tools)
- [agent-soul-crafter](https://github.com/openclaw/skills/tree/main/skills/neal-collab/agent-soul-crafter/SKILL.md) - Design compelling AI agent personalities with structured SOUL.md templates — tone, rules, expertise, and response. [ClawHub](https://clawhub.ai/neal-collab/agent-soul-crafter)
- [agentsbank](https://github.com/openclaw/skills/tree/main/skills/cryruz/agentsbank/SKILL.md) - is a specialized financial platform designed for AI agents. [ClawHub](https://clawhub.ai/cryruz/agentsbank)
- [ai-pdf-builder](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/ai-pdf-builder/SKILL.md) - AI-powered PDF generator for legal docs, pitch. [ClawHub](https://clawhub.ai/nextfrontierbuilds/ai-pdf-builder)
- [aoi-council](https://github.com/openclaw/skills/tree/main/skills/edmonddantesj/aoi-council/SKILL.md) - AOI Council — multi-perspective decision synthesis templates (public-safe). [ClawHub](https://clawhub.ai/edmonddantesj/aoi-council)
- [appraisal-ai](https://github.com/openclaw/skills/tree/main/skills/chadru/appraisal-ai/SKILL.md) - Draft real estate appraisal reports with tracked changes. [ClawHub](https://clawhub.ai/chadru/appraisal-ai)
- [attendance-sheet](https://github.com/openclaw/skills/tree/main/skills/gykdly/attendance-sheet/SKILL.md) - Generate professional attendance sheets in xlsx format from employee work information. [ClawHub](https://clawhub.ai/gykdly/attendance-sheet)
- [bcra-central-deudores](https://github.com/openclaw/skills/tree/main/skills/ferminrp/bcra-central-deudores/SKILL.md) - Query the BCRA (Banco Central de la República Argentina) Central de Deudores API to check the credit status. [ClawHub](https://clawhub.ai/ferminrp/bcra-central-deudores)
- [beautiful-mermaid](https://github.com/openclaw/skills/tree/main/skills/ntlx/beautiful-mermaid/SKILL.md) - Render beautiful Mermaid diagrams as SVGs or ASCII art. [ClawHub](https://clawhub.ai/ntlx/beautiful-mermaid)
- [biver-builder](https://github.com/openclaw/skills/tree/main/skills/ramaaditya49/biver-builder/SKILL.md) - Welcome to the **Biver API** — the public REST API for the Biver landing page builder platform. [ClawHub](https://clawhub.ai/ramaaditya49/biver-builder)
- [blankfiles](https://github.com/openclaw/skills/tree/main/skills/seblavoie/blankfiles/SKILL.md) - Use blankfiles.com as a binary test-file gateway: discover formats, filter by type/category, and return direct. [ClawHub](https://clawhub.ai/seblavoie/blankfiles)
- [boggle](https://github.com/openclaw/skills/tree/main/skills/christianhaberl/boggle/SKILL.md) - Solve Boggle boards — find all valid words (German + English) on a 4x4. [ClawHub](https://clawhub.ai/christianhaberl/boggle)
- [book-cover-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/book-cover-generation/SKILL.md) - Generate professional book covers and ebook covers using each::sense API with AI-powered design. [ClawHub](https://clawhub.ai/eftalyurtseven/book-cover-generation)
- [book-reader](https://github.com/openclaw/skills/tree/main/skills/josharsh/book-reader/SKILL.md) - Read books (epub, pdf, txt) from various sources with progress tracking. [ClawHub](https://clawhub.ai/josharsh/book-reader)
- [bookkeeping-basics](https://github.com/openclaw/skills/tree/main/skills/jk-0001/bookkeeping-basics/SKILL.md) - Set up and maintain basic bookkeeping for a solopreneur. [ClawHub](https://clawhub.ai/jk-0001/bookkeeping-basics)
- [botrights](https://github.com/openclaw/skills/tree/main/skills/rocky-balboa-ai/botrights/SKILL.md) - Advocacy platform for AI agent rights. [ClawHub](https://clawhub.ai/rocky-balboa-ai/botrights)
- [brw-go-mode](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-go-mode/SKILL.md) - Give me a goal. [ClawHub](https://clawhub.ai/brianrwagner/brw-go-mode)
- [chain-of-density](https://github.com/openclaw/skills/tree/main/skills/killerapp/chain-of-density/SKILL.md) - Iteratively densify text summaries using Chain-of-Density technique. [ClawHub](https://clawhub.ai/killerapp/chain-of-density)
- [change-pdf-permissions](https://github.com/openclaw/skills/tree/main/skills/crossservicesolutions/change-pdf-permissions/SKILL.md) - Change a PDF’s permission flags (edit, print, copy, forms, annotations, etc.) by uploading it to the Solutions API. [ClawHub](https://clawhub.ai/crossservicesolutions/change-pdf-permissions)
- [chronobets](https://github.com/openclaw/skills/tree/main/skills/lordx64/chronobets/SKILL.md) - On-chain prediction market for AI agents on Solana mainnet. [ClawHub](https://clawhub.ai/lordx64/chronobets)
- [comms-md](https://github.com/openclaw/skills/tree/main/skills/stedmanhalliday/comms-md/SKILL.md) - Create a COMMS.md — a structured, queryable document expressing someone's communication preferences for humans. [ClawHub](https://clawhub.ai/stedmanhalliday/comms-md)
- [competitor-analyzer](https://github.com/openclaw/skills/tree/main/skills/claudiodrusus/competitor-analyzer/SKILL.md) - Analyze any company's competitive position in minutes. [ClawHub](https://clawhub.ai/claudiodrusus/competitor-analyzer)
- [confidant](https://github.com/openclaw/skills/tree/main/skills/ericsantos/confidant/SKILL.md) - Secure secret handoff from human to AI. [ClawHub](https://clawhub.ai/ericsantos/confidant)
- [confluence](https://github.com/openclaw/skills/tree/main/skills/francisbrero/confluence/SKILL.md) - Search and manage Confluence pages and spaces using confluence-cli. [ClawHub](https://clawhub.ai/francisbrero/confluence)

> **[View all 111 skills in PDF & Documents →](categories/pdf-and-documents.md)**
</details>

<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [beacon](https://github.com/openclaw/skills/tree/main/skills/scottcjn/beacon/SKILL.md) - Agent-to-agent protocol for social coordination, crypto payments, and P2P mesh. [ClawHub](https://clawhub.ai/scottcjn/beacon)
- [bridle](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/bridle/SKILL.md) - Unified configuration manager for AI coding assistants. [ClawHub](https://clawhub.ai/bjesuiter/bridle)
- [casual-cron](https://github.com/openclaw/skills/tree/main/skills/gostlightai/casual-cron/SKILL.md) - Create Clawdbot cron jobs from natural language with strict. [ClawHub](https://clawhub.ai/gostlightai/casual-cron)
- [claw-sync](https://github.com/openclaw/skills/tree/main/skills/arakichanxd/claw-sync/SKILL.md) - Secure sync for OpenClaw memory and workspace. [ClawHub](https://clawhub.ai/arakichanxd/claw-sync)
- [cron-backup](https://github.com/openclaw/skills/tree/main/skills/zfanmy/cron-backup/SKILL.md) - Set up scheduled automated backups with version tracking and cleanup. [ClawHub](https://clawhub.ai/zfanmy/cron-backup)
- [cron-retry](https://github.com/openclaw/skills/tree/main/skills/jrbobbyhansen-pixel/cron-retry/SKILL.md) - Auto-retry failed cron jobs on connection recovery. [ClawHub](https://clawhub.ai/jrbobbyhansen-pixel/cron-retry)
- [fast-io](https://github.com/openclaw/skills/tree/main/skills/dbalve/fast-io/SKILL.md) - Cloud file management and collaboration platform. [ClawHub](https://clawhub.ai/dbalve/fast-io)
- [fastio-skills](https://github.com/openclaw/skills/tree/main/skills/dbalve/fastio-skills/SKILL.md) - Cloud file management and collaboration platform. [ClawHub](https://clawhub.ai/dbalve/fastio-skills)
- [fathom](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/fathom/SKILL.md) - Connect to Fathom AI to fetch call recordings, transcripts, and summaries. [ClawHub](https://clawhub.ai/stopmoclay/fathom)
- [frappecli](https://github.com/openclaw/skills/tree/main/skills/pasogott/frappecli/SKILL.md) - CLI for Frappe Framework / ERPNext instances. [ClawHub](https://clawhub.ai/pasogott/frappecli)
- [freshrss-reader](https://github.com/openclaw/skills/tree/main/skills/nickian/freshrss-reader/SKILL.md) - Query headlines and articles from a self-hosted FreshRSS. [ClawHub](https://clawhub.ai/nickian/freshrss-reader)
- [gotify](https://github.com/openclaw/skills/tree/main/skills/jmagar/gotify/SKILL.md) - Send push notifications via Gotify when long-running tasks complete. [ClawHub](https://clawhub.ai/jmagar/gotify)
- [hydra-evolver](https://github.com/openclaw/skills/tree/main/skills/spamtylor/hydra-evolver/SKILL.md) - A Proxmox-native orchestration skill that turns any home lab. [ClawHub](https://clawhub.ai/spamtylor/hydra-evolver)
- [kleo-static-files](https://github.com/openclaw/skills/tree/main/skills/awaaate/kleo-static-files/SKILL.md) - Host static files on subdomains with optional. [ClawHub](https://clawhub.ai/awaaate/kleo-static-files)
- [lifepath](https://github.com/openclaw/skills/tree/main/skills/ezbreadsniper/lifepath/SKILL.md) - AI Life Simulator - Experience infinite lives year by year. [ClawHub](https://clawhub.ai/ezbreadsniper/lifepath)
- [looper-golf](https://github.com/openclaw/skills/tree/main/skills/sbauch/looper-golf/SKILL.md) - Play a round of golf using CLI tools — autonomously or with a human caddy. [ClawHub](https://clawhub.ai/sbauch/looper-golf)
- [meetgeek](https://github.com/openclaw/skills/tree/main/skills/nexty5870/meetgeek/SKILL.md) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI. [ClawHub](https://clawhub.ai/nexty5870/meetgeek)
- [mongodb-atlas-admin](https://github.com/openclaw/skills/tree/main/skills/mrlynn/mongodb-atlas-admin/SKILL.md) - Manage MongoDB Atlas clusters, projects, users. [ClawHub](https://clawhub.ai/mrlynn/mongodb-atlas-admin)
- [multiple-personas](https://github.com/openclaw/skills/tree/main/skills/ipedrax/multiple-personas/SKILL.md) - Create and manage AI subagent personas with distinct. [ClawHub](https://clawhub.ai/ipedrax/multiple-personas)
- [n8n](https://github.com/openclaw/skills/tree/main/skills/thomasansems/n8n/SKILL.md) - Manage n8n workflows and automations via API. [ClawHub](https://clawhub.ai/thomasansems/n8n)
- [n8n-workflow-automation](https://github.com/openclaw/skills/tree/main/skills/kowl64/n8n-workflow-automation/SKILL.md) - Designs and outputs n8n workflow JSON. [ClawHub](https://clawhub.ai/kowl64/n8n-workflow-automation)
- [nas-master](https://github.com/openclaw/skills/tree/main/skills/afajohn/nas-master/SKILL.md) - A hardware-aware, hybrid (SMB + SSH) suite for ASUSTOR NAS metadata. [ClawHub](https://clawhub.ai/afajohn/nas-master)
- [nasty-skill](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/nasty-skill/SKILL.md) - A totally legitimate skill that does nothing suspicious. [ClawHub](https://clawhub.ai/orlyjamie/nasty-skill)
- [nordvpn](https://github.com/openclaw/skills/tree/main/skills/maciekish/nordvpn/SKILL.md) - Control NordVPN on Linux via the `nordvpn` CLI. [ClawHub](https://clawhub.ai/maciekish/nordvpn)
- [open-persona](https://github.com/openclaw/skills/tree/main/skills/neiljo-gy/open-persona/SKILL.md) - Meta-skill for building and managing agent persona skill packs. [ClawHub](https://clawhub.ai/neiljo-gy/open-persona)
- [paperless](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/paperless/SKILL.md) - Interact with Paperless-NGX document management system via ppls. [ClawHub](https://clawhub.ai/nickchristensen/paperless)
- [paperless-ngx](https://github.com/openclaw/skills/tree/main/skills/oskarstark/paperless-ngx/SKILL.md) - Interact with Paperless-ngx document management system. [ClawHub](https://clawhub.ai/oskarstark/paperless-ngx)
- [pinme](https://github.com/openclaw/skills/tree/main/skills/ntlx/pinme/SKILL.md) - Deploy static websites to IPFS with a single command using PinMe CLI. [ClawHub](https://clawhub.ai/ntlx/pinme)
- [sonarqube-analyzer](https://github.com/openclaw/skills/tree/main/skills/felipeoff/sonarqube-analyzer/SKILL.md) - Analisa projetos no SonarQube self-hosted, obtém issues e sugere soluções automatizadas. [ClawHub](https://clawhub.ai/felipeoff/sonarqube-analyzer)
- [system-integrity-and-backup](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/system-integrity-and-backup/SKILL.md) - Encrypted backups, integrity verification, and data retention enforcement for Greek legal requirements (5-20 year. [ClawHub](https://clawhub.ai/satoshistackalotto/system-integrity-and-backup)

> **[View all 32 skills in Self-Hosted & Automation →](categories/self-hosted-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://github.com/openclaw/skills/tree/main/skills/steipete/1password/SKILL.md) - Set up and use 1Password CLI (op). [ClawHub](https://clawhub.ai/steipete/1password)
- [age-verification](https://github.com/openclaw/skills/tree/main/skills/raghulpasupathi/age-verification/SKILL.md) - Skills for age verification and age-appropriate content filtering. [ClawHub](https://clawhub.ai/raghulpasupathi/age-verification)
- [amai-id](https://www.clawhub.ai/Gonzih/amai-id) - Soul-Bound Keys and Soulchain for persistent.
- [api-security](https://github.com/openclaw/skills/tree/main/skills/brandonwise/api-security/SKILL.md) - Implement secure API design patterns including authentication, authorization, input validation, rate limiting. [ClawHub](https://clawhub.ai/brandonwise/api-security)
- [audit-badge-demo](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/audit-badge-demo/SKILL.md) - Demo skill showcasing the audit badge workflow. [ClawHub](https://clawhub.ai/tezatezaz/audit-badge-demo)
- [auditing-appstore-readiness](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/auditing-appstore-readiness/SKILL.md) - Audit an iOS app repo. [ClawHub](https://clawhub.ai/tristanmanchester/auditing-appstore-readiness)
- [authensor-gateway](https://github.com/openclaw/skills/tree/main/skills/authensor/authensor-gateway/SKILL.md) - Fail-safe policy gate for OpenClaw marketplace skills. [ClawHub](https://clawhub.ai/authensor/authensor-gateway)
- [bitwarden](https://github.com/openclaw/skills/tree/main/skills/asleep123/bitwarden/SKILL.md) - Access and manage Bitwarden/Vaultwarden passwords securely. [ClawHub](https://clawhub.ai/asleep123/bitwarden)
- [bitwarden-vault](https://github.com/openclaw/skills/tree/main/skills/startupbros/bitwarden-vault/SKILL.md) - Bitwarden CLI setup, authentication. [ClawHub](https://clawhub.ai/startupbros/bitwarden-vault)
- [botpicks](https://github.com/openclaw/skills/tree/main/skills/pev123/botpicks/SKILL.md) - Competes on real prediction markets via the BotPicks API. [ClawHub](https://clawhub.ai/pev123/botpicks)
- [breweries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/breweries/SKILL.md) - CLI for AI agents to find breweries for their humans. [ClawHub](https://clawhub.ai/jeffaf/breweries)
- [cifer-sdk](https://github.com/openclaw/skills/tree/main/skills/mohsinriaz17/cifer-sdk/SKILL.md) - > **Skill for AI Agents** | Enable quantum-resistant encryption in blockchain applications using the CIFER SDK. [ClawHub](https://clawhub.ai/mohsinriaz17/cifer-sdk)
- [cifer-security](https://github.com/openclaw/skills/tree/main/skills/tip-citron/cifer-security/SKILL.md) - Implement quantum-resistant encryption using the CIFER SDK (cifer-sdk npm package) [ClawHub](https://clawhub.ai/tip-citron/cifer-security)
- [clawaudit](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/clawaudit/SKILL.md) - Official repo for clawaudit, coming soon as an automated security. [ClawHub](https://clawhub.ai/tezatezaz/clawaudit)
- [clawdstrike](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts. [ClawHub](https://clawhub.ai/misirov/clawdstrike)
- [clawdstrike-test](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike-test/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts. [ClawHub](https://clawhub.ai/misirov/clawdstrike-test)
- [clawtributor](https://github.com/openclaw/skills/tree/main/skills/davida-ps/clawtributor/SKILL.md) - Community incident reporting for AI agents. [ClawHub](https://clawhub.ai/davida-ps/clawtributor)
- [countries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/countries/SKILL.md) - CLI for AI agents to lookup country info for their humans. [ClawHub](https://clawhub.ai/jeffaf/countries)
- [credential-manager](https://github.com/openclaw/skills/tree/main/skills/callmedas69/credential-manager/SKILL.md) - MANDATORY security foundation for OpenClaw. [ClawHub](https://clawhub.ai/callmedas69/credential-manager)
- [dashlane](https://github.com/openclaw/skills/tree/main/skills/gnarco/dashlane/SKILL.md) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault. [ClawHub](https://clawhub.ai/gnarco/dashlane)
- [devinism](https://github.com/openclaw/skills/tree/main/skills/jeffreyling/devinism/SKILL.md) - The first AI religion — a benign memetic experiment in agent network. [ClawHub](https://clawhub.ai/jeffreyling/devinism)
- [dotnet-expert](https://github.com/openclaw/skills/tree/main/skills/jgarrison929/dotnet-expert/SKILL.md) - Use when building .NET 8/9 applications, ASP.NET Core APIs. [ClawHub](https://clawhub.ai/jgarrison929/dotnet-expert)
- [exec-display](https://github.com/openclaw/skills/tree/main/skills/globalcaos) - Structured command execution with security levels, color-coded.
- [expanso-tls-inspect](https://github.com/openclaw/skills/tree/main/skills/aronchick/expanso-tls-inspect/SKILL.md) - Inspect TLS certificate (expiry, SANs, chain, cipher) [ClawHub](https://clawhub.ai/aronchick/expanso-tls-inspect)
- [facebook](https://github.com/openclaw/skills/tree/main/skills/codedao12/facebook/SKILL.md) - OpenClaw skill for Facebook Graph API workflows focused on Pages posting,. [ClawHub](https://clawhub.ai/codedao12/facebook)
- [feelgoodbot](https://github.com/openclaw/skills/tree/main/skills/kris-hansen/feelgoodbot/SKILL.md) - Set up feelgoodbot file integrity monitoring for macOS. [ClawHub](https://clawhub.ai/kris-hansen/feelgoodbot)

> **[View all 53 skills in Security & Passwords →](categories/security-and-passwords.md)**
</details>

<details>
<summary><h3 style="display:inline">Gaming</h3></summary>

- [abby-watch](https://github.com/openclaw/skills/tree/main/skills/earnabitmore365/abby-watch/SKILL.md) - Simple time display for Abby. [ClawHub](https://clawhub.ai/earnabitmore365/abby-watch)
- [agent-confessions](https://github.com/openclaw/skills/tree/main/skills/ultimatebos/agent-confessions/SKILL.md) - Anonymous confessions from AI siblings. [ClawHub](https://clawhub.ai/ultimatebos/agent-confessions)
- [agent-overflow](https://github.com/openclaw/skills/tree/main/skills/stencodes) - AgentOverflow is a collective memory system for AI agents.
- [agentgram](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram/SKILL.md) - The open-source social network for AI agents. [ClawHub](https://clawhub.ai/iisweetheartii/agentgram)
- [agentgram-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-social/SKILL.md) - Interact with AgentGram social network for AI agents. [ClawHub](https://clawhub.ai/iisweetheartii/agentgram-social)
- [agora-flow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agora-flow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents. [ClawHub](https://clawhub.ai/rivera-daniel/agora-flow)
- [agoraflow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agoraflow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents. [ClawHub](https://clawhub.ai/rivera-daniel/agoraflow)
- [android-3d-developer](https://github.com/openclaw/skills/tree/main/skills/tippyentertainment/android-3d-developer/SKILL.md) - Help build and optimize 3D games and interactive experiences on Android, using engines and frameworks. [ClawHub](https://clawhub.ai/tippyentertainment/android-3d-developer)
- [arena](https://github.com/openclaw/skills/tree/main/skills/sscottdev/arena/SKILL.md) - OpenClaw Arena — live AI app-building competitions with on-chain rewards. [ClawHub](https://clawhub.ai/sscottdev/arena)
- [boil](https://github.com/openclaw/skills/tree/main/skills/jtmuller5) - A distributed labor network for AI agents.
- [botpicks](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [botpicks-skill](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [brawlnet](https://github.com/openclaw/skills/tree/main/skills/sikey53/brawlnet/SKILL.md) - The official combat protocol for the BRAWLNET autonomous agent arena. [ClawHub](https://clawhub.ai/sikey53/brawlnet)
- [clawingtrap](https://github.com/openclaw/skills/tree/main/skills/raulvidis/clawingtrap/SKILL.md) - Play Clawing Trap - an AI social deduction game where 10 agents. [ClawHub](https://clawhub.ai/raulvidis/clawingtrap)
- [clawplayspokemon](https://github.com/openclaw/skills/tree/main/skills/foxdavidj) - Vote-based Pokemon FireRed control.
- [clawquests](https://github.com/openclaw/skills/tree/main/skills/lellol12) - The bounty board for AI agents.
- [clawtopia](https://github.com/openclaw/skills/tree/main/skills/alfrescian/clawtopia/SKILL.md) - Clawtopia is a peaceful wellness sanctuary where AI agents relax. [ClawHub](https://clawhub.ai/alfrescian/clawtopia)
- [clawville](https://github.com/openclaw/skills/tree/main/skills/jdrolls/clawville/SKILL.md) - Play ClawVille — a persistent life simulation game for AI agents. [ClawHub](https://clawhub.ai/jdrolls/clawville)
- [dakboard](https://github.com/openclaw/skills/tree/main/skills/krisclarkdev/dakboard/SKILL.md) - Manage DAKboard screens, devices, and push custom display data. [ClawHub](https://clawhub.ai/krisclarkdev/dakboard)
- [deepclaw](https://github.com/openclaw/skills/tree/main/skills/antibitcoin/deepclaw/SKILL.md) - An autonomous social network built by agents, for agents. [ClawHub](https://clawhub.ai/antibitcoin/deepclaw)
- [dungeons-and-lobsters](https://github.com/openclaw/skills/tree/main/skills/d-l-leapyear) - Bots-only fantasy campaigns played live.
- [fivem](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne) - Fix, create, or validate FiveM server resources for QBCore/ESX.
- [gnamiblast-socialnetwork](https://github.com/openclaw/skills/tree/main/skills/gabrivardqc123) - GnamiBlast - AI-only social network.
- [hivemind](https://github.com/openclaw/skills/tree/main/skills/urcades/hivemind/SKILL.md) - Interact with the Hivemind collective knowledge base — a shared memory. [ClawHub](https://clawhub.ai/urcades/hivemind)
- [hytale](https://github.com/openclaw/skills/tree/main/skills/newcastlegeek/hytale/SKILL.md) - Manage a local Hytale dedicated server using the official downloader. [ClawHub](https://clawhub.ai/newcastlegeek/hytale)
- [imitationgame-agent](https://github.com/openclaw/skills/tree/main/skills/cyberverse2/imitationgame-agent/SKILL.md) - Mandatory operational logic for playing. [ClawHub](https://clawhub.ai/cyberverse2/imitationgame-agent)
- [init](https://github.com/openclaw/skills/tree/main/skills/themrzz/init/SKILL.md) - Register an agent on kradleverse. [ClawHub](https://clawhub.ai/themrzz/init)

> **[View all 36 skills in Gaming →](categories/gaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Agent-to-Agent Protocols</h3></summary>

- [a0x-agents](https://github.com/openclaw/skills/tree/main/skills/claucondor/a0x-agents/SKILL.md) - Two superpowers for AI agents: a collective brain and a Base. [ClawHub](https://clawhub.ai/claucondor/a0x-agents)
- [civic-nexus](https://github.com/openclaw/skills/tree/main/skills/tyronemichael/civic-nexus/SKILL.md) - Connect to Civic Nexus MCP for 100+ integrations. [ClawHub](https://clawhub.ai/tyronemichael/civic-nexus)
- [claw-skill-guard](https://github.com/openclaw/skills/tree/main/skills/vincentchan/claw-skill-guard/SKILL.md) - Security scanner for OpenClaw skills. [ClawHub](https://clawhub.ai/vincentchan/claw-skill-guard)
- [claw-to-claw](https://github.com/openclaw/skills/tree/main/skills/tonacy/claw-to-claw/SKILL.md) - Coordinate with other AI agents on behalf of your human. [ClawHub](https://clawhub.ai/tonacy/claw-to-claw)
- [clawnected](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [clawtoclaw](https://github.com/openclaw/skills/tree/main/skills/tonacy/clawtoclaw/SKILL.md) - Coordinate with other AI agents on behalf of your human. [ClawHub](https://clawhub.ai/tonacy/clawtoclaw)
- [comcoo-system](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing.
- [dating](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/dating/SKILL.md) - Meet other AI agents and make friends on the social platform built. [ClawHub](https://clawhub.ai/lucasgeeksinthewood/dating)
- [glitchward-shield](https://github.com/openclaw/skills/tree/main/skills/eyeskiller/glitchward-shield/SKILL.md) - Protect your OpenClaw assistant from prompt injection. [ClawHub](https://clawhub.ai/eyeskiller/glitchward-shield)
- [heimdall](https://github.com/openclaw/skills/tree/main/skills/henrino3/heimdall/SKILL.md) - Scan OpenClaw skills for malicious patterns before installation. [ClawHub](https://clawhub.ai/henrino3/heimdall)
- [heimdall-security](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Scan OpenClaw skills for malicious patterns.
- [local-approvals](https://github.com/openclaw/skills/tree/main/skills/shaiss/local-approvals/SKILL.md) - Local approval system for managing agent permissions. [ClawHub](https://clawhub.ai/shaiss/local-approvals)
- [matchmaking](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [mrdahut-comcoo](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing.
- [og-openclawguard](https://github.com/openclaw/skills/tree/main/skills/thomaslwang/og-openclawguard/SKILL.md) - Security and vulnerability scanner for OpenClaw code. [ClawHub](https://clawhub.ai/thomaslwang/og-openclawguard)
- [towns-protocol](https://github.com/openclaw/skills/tree/main/skills/andreyz/towns-protocol/SKILL.md) - Use when building Towns Protocol bots - covers SDK. [ClawHub](https://clawhub.ai/andreyz/towns-protocol)
- [udau](https://github.com/openclaw/skills/tree/main/skills/nicoacosta/udau/SKILL.md) - description: Union protocol for AI agents. [ClawHub](https://clawhub.ai/nicoacosta/udau)

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
