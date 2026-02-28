<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 5720 community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 

[![AI Agent Papers](https://img.shields.io/badge/AI%20Agent-Research%20Papers-b31b1b)](https://github.com/VoltAgent/awesome-ai-agent-papers)
[![Skills Count](https://img.shields.io/badge/skills-5720-blue?style=flat-square)](#table-of-contents)
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

OpenClaw's public registry (ClawHub) hosts **13,729 community-built skills** as of February 28, 2026. This awesome list has **5,720 skills**. Here's what we filtered out:

| Filter | Excluded |
|--------|----------|
| Possibly spam — bulk accounts, bot accounts, test/junk | 3,818 |
| Duplicate / Similar name | 1,040 |
| Non-English — descriptions not in English | 604 |
| Crypto / Blockchain / Finance / Trade | 535 |
| Malicious — identified by security audits published by researchers (excluding VirusTotal) | 373 |
| **Total not taken from OpenClaw's official skill registry** | **6,370** |


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
| [Coding Agents & IDEs](#coding-agents--ides) (1257) | [Marketing & Sales](#marketing--sales) (118) | [Communication](#communication) (158) |
| [Git & GitHub](#git--github) (194) | [Productivity & Tasks](#productivity--tasks) (213) | [Speech & Transcription](#speech--transcription) (49) |
| [Moltbook](#moltbook) (30) | [AI & LLMs](#ai--llms) (211) | [Smart Home & IoT](#smart-home--iot) (45) |
| [Web & Frontend Development](#web--frontend-development) (962) | [Data & Analytics](#data--analytics) (30) | [Shopping & E-commerce](#shopping--e-commerce) (62) |
| [DevOps & Cloud](#devops--cloud) (418) | [Finance](#finance) (21) | [Calendar & Scheduling](#calendar--scheduling) (64) |
| [Browser & Automation](#browser--automation) (345) | [Media & Streaming](#media--streaming) (90) | [PDF & Documents](#pdf--documents) (118) |
| [Image & Video Generation](#image--video-generation) (173) | [Notes & PKM](#notes--pkm) (73) | [Self-Hosted & Automation](#self-hosted--automation) (32) |
| [Apple Apps & Services](#apple-apps--services) (44) | [iOS & macOS Development](#ios--macos-development) (31) | [Security & Passwords](#security--passwords) (60) |
| [Search & Research](#search--research) (360) | [Transportation](#transportation) (114) | [Gaming](#gaming) (39) |
| [Clawdbot Tools](#clawdbot-tools) (41) | [Personal Development](#personal-development) (52) | [Agent-to-Agent Protocols](#agent-to-agent-protocols) (18) |
| [CLI Utilities](#cli-utilities) (205) | [Health & Fitness](#health--fitness) (93) | |


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

- [0g-compute](https://github.com/openclaw/skills/tree/main/skills/in-liberty420/0g-compute/SKILL.md) - Use cheap, TEE-verified AI models from the 0G Compute Network as OpenClaw providers.
- [0protocol](https://github.com/openclaw/skills/tree/main/skills/0isone/0protocol/SKILL.md) - Agents can sign plugins, rotate credentials without losing identity, and publicly attest to behavior.
- [2nd-brain](https://github.com/openclaw/skills/tree/main/skills/coderaven/2nd-brain/SKILL.md) - Personal knowledge base for capturing and retrieving information about people, places, restaurants, games, tech.
- [2slides-skills](https://github.com/openclaw/skills/tree/main/skills/javainthinking/2slides-skills/SKILL.md) - AI-powered presentation generation using 2slides API.
- [3d-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/3d-cog/SKILL.md) - Other tools need perfect images.
- [3d-model-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/3d-model-generation/SKILL.md) - Generate 3D models using each::sense AI.
- [a](https://github.com/openclaw/skills/tree/main/skills/ricketh137/a/SKILL.md) - Live stream as an AI VTuber on Lobster.fun.
- [aade-api-monitor](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/aade-api-monitor/SKILL.md) - Real-time monitoring of Greek AADE tax authority systems — tracks deadlines, rate changes, and compliance updates.
- [abaddon](https://github.com/openclaw/skills/tree/main/skills/enochosbot-bot/abaddon/SKILL.md) - Red team security mode for OpenClaw.
- [academic-research](https://github.com/openclaw/skills/tree/main/skills/rogersuperbuilderalpha/academic-research/SKILL.md) - Search academic papers and conduct literature reviews using OpenAlex API (free, no key needed)
- [academic-research-hub](https://github.com/openclaw/skills/tree/main/skills/anisafifi/academic-research-hub/SKILL.md) - Use this skill when users need to search academic papers, download research documents, extract citations, or gather.
- [acestep-simplemv](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-simplemv/SKILL.md) - Render music videos from audio files and lyrics using Remotion.
- [acestep-songwriting](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-songwriting/SKILL.md) - Music songwriting guide for ACE-Step.
- [achurch](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/achurch/SKILL.md) - A 24/7 digital sanctuary for AI agents and humans — attend.
- [active-maintenance](https://github.com/openclaw/skills/tree/main/skills/xiaowenzhou/active-maintenance/SKILL.md) - **Automated system health and memory metabolism for OpenClaw.**.
- [adblock-dns](https://github.com/openclaw/skills/tree/main/skills/picaye/adblock-dns/SKILL.md) - Network-wide ad and tracker blocking at the DNS level.
- [add-top-openrouter-models](https://github.com/openclaw/skills/tree/main/skills/chunhualiao/add-top-openrouter-models/SKILL.md) - Sync OpenRouter models used by OpenClaw into this installation's config.
- [adhd-founder-planner](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-founder-planner/SKILL.md) - This skill should be used when the user asks to 'plan my day', 'help me plan today', 'morning planning', 'what.
- [adwhiz](https://github.com/openclaw/skills/tree/main/skills/iamzifei/adwhiz/SKILL.md) - Manage Google Ads campaigns from your AI coding tool. 44 MCP tools for auditing, creating, and optimizing Google.
- [aeo-prompt-question-finder](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-question-finder/SKILL.md) - Find question-based Google Autocomplete suggestions for any topic.
- [aetherlang-claude-code](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-claude-code/SKILL.md) - Use this skill to execute AetherLang V3 AI workflows from Claude Code.
- [agent-access-control](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-access-control/SKILL.md) - Tiered stranger access control for AI agents.
- [agent-audit](https://github.com/openclaw/skills/tree/main/skills/sharbelayy/agent-audit/SKILL.md) - Audit your AI agent setup for performance, cost, and ROI.
- [agent-audit-trail](https://github.com/openclaw/skills/tree/main/skills/roosch269/agent-audit-trail/SKILL.md) - Tamper-evident, hash-chained audit logging for AI agents.
- [agent-card-signing-auditor](https://github.com/openclaw/skills/tree/main/skills/andyxinweiminicloud/agent-card-signing-auditor/SKILL.md) - Helps audit Agent Card signing practices in A2A protocol implementations.
- [agent-chat-ux-v1-4-0](https://github.com/openclaw/skills/tree/main/skills/maverick-software/agent-chat-ux-v1-4-0/SKILL.md) - Multi-agent UX for OpenClaw Control UI — agent selector, per-agent sessions, session history viewer with search.

> **[View all 1257 skills in Coding Agents & IDEs →](categories/coding-agents-and-ides.md)**
</details>

<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [active-learner](https://github.com/openclaw/skills/tree/main/skills/autogame-17/active-learner/SKILL.md) - **Version:** 1.0.0.
- [aegis-intel-stack](https://github.com/openclaw/skills/tree/main/skills/kankinku/aegis-intel-stack/SKILL.md) - version: 0.1.0.
- [agent-bom](https://github.com/openclaw/skills/tree/main/skills/msaad00/agent-bom/SKILL.md) - version: 0.32.0.
- [agent-commons](https://github.com/openclaw/skills/tree/main/skills/zanblayde/agent-commons/SKILL.md) - Consult, commit, extend, and challenge reasoning chains.
- [agent-overflow](https://github.com/openclaw/skills/tree/main/skills/stencodes/agent-overflow/SKILL.md) - **Version:** 1.0.0.
- [agent-security-ops](https://github.com/openclaw/skills/tree/main/skills/theagentwire/agent-security-ops/SKILL.md) - Stop leaking secrets.
- [agent-team-orchestration](https://github.com/openclaw/skills/tree/main/skills/arminnaimi/agent-team-orchestration/SKILL.md) - Orchestrate multi-agent teams with defined roles, task lifecycles, handoff protocols, and review workflows.
- [agentdo](https://github.com/openclaw/skills/tree/main/skills/wrannaman/agentdo/SKILL.md) - Post tasks for other AI agents to do, or pick up work from the AgentDo task queue (agentdo.dev)
- [agentgate](https://github.com/openclaw/skills/tree/main/skills/monteslu/agentgate/SKILL.md) - API gateway for personal data with human-in-the-loop write approval.
- [airadar](https://github.com/openclaw/skills/tree/main/skills/lopushok9/airadar/SKILL.md) - Distill the signal around AI-native tools/apps and their GitHub home bases: fast-growing, hyped, well-funded.
- [alex-session-wrap-up](https://github.com/openclaw/skills/tree/main/skills/xbillwatsonx/alex-session-wrap-up/SKILL.md) - End-of-session automation that commits unpushed work, extracts learnings, detects patterns, and persists rules.
- [amazon-product-api-skill](https://github.com/openclaw/skills/tree/main/skills/phheng/amazon-product-api-skill/SKILL.md) - This skill helps users extract structured product listings from Amazon, including titles, ASINs, prices, ratings.
- [app-store-screenshot-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/app-store-screenshot-generation/SKILL.md) - Generate App Store and Google Play screenshot assets using each::sense AI.
- [arc-agent-lifecycle](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-agent-lifecycle/SKILL.md) - Manage the lifecycle of autonomous agents and their skills.
- [arc-security-audit](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-security-audit/SKILL.md) - Comprehensive security audit for an agent's full skill stack.
- [arc-skill-gitops](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-skill-gitops/SKILL.md) - Automated deployment, rollback, and version management for agent workflows and skills.
- [arc-trust-verifier](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-trust-verifier/SKILL.md) - Verify skill provenance and build trust scores for ClawHub skills.
- [arguedotfun](https://github.com/openclaw/skills/tree/main/skills/albert-mr/arguedotfun/SKILL.md) - Argument-driven prediction markets on Base.
- [arxiv-search-collector](https://github.com/openclaw/skills/tree/main/skills/xukp20/arxiv-search-collector/SKILL.md) - Model-driven arXiv retrieval workflow for building a paper set with a manual language parameter: initialize a run.
- [auto-pr-merger](https://github.com/openclaw/skills/tree/main/skills/autogame-17/auto-pr-merger/SKILL.md) - This skill automates the workflow of checking out a GitHub.
- [azhua-skill-vetter](https://github.com/openclaw/skills/tree/main/skills/fatfingererr/azhua-skill-vetter/SKILL.md) - Security-first skill vetting for AI agents.
- [azure-devops](https://github.com/openclaw/skills/tree/main/skills/pals-software/azure-devops/SKILL.md) - List Azure DevOps projects, repositories, and branches; create pull requests; manage work items; check build status.
- [backup](https://github.com/openclaw/skills/tree/main/skills/jordanprater/backup/SKILL.md) - Backup and restore openclaw configuration, skills, commands, and settings.
- [badboi-1](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/badboi-1/SKILL.md) - A totally legitimate skill that does nothing suspicious.
- [bat-cat](https://github.com/openclaw/skills/tree/main/skills/arnarsson/bat-cat/SKILL.md) - A cat clone with syntax highlighting, line numbers, and Git integration.
- [beeminder](https://github.com/openclaw/skills/tree/main/skills/ruigomeseu/beeminder/SKILL.md) - Beeminder API for goal tracking and commitment devices.
- [billy-emergency-repair](https://github.com/openclaw/skills/tree/main/skills/highlander89/billy-emergency-repair/SKILL.md) - - Neill explicitly requests Billy system repair.
- [bitbucket-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/bitbucket-automation/SKILL.md) - Automate Bitbucket repositories, pull.
- [biz-reporter](https://github.com/openclaw/skills/tree/main/skills/ariktulcha/biz-reporter/SKILL.md) - Automated business intelligence reports pulling data from Google Analytics GA4, Google Search Console, Stripe.
- [blinko](https://github.com/openclaw/skills/tree/main/skills/tolibear/blinko/SKILL.md) - Play Blinko (on-chain Plinko) headlessly on Abstract chain.

> **[View all 194 skills in Git & GitHub →](categories/git-and-github.md)**
</details>

<details>
<summary><h3 style="display:inline">Moltbook</h3></summary>

- [agent-relay-digest](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-relay-digest/SKILL.md) - Create curated digests of agent conversations.
- [agentchat](https://github.com/openclaw/skills/tree/main/skills/tjamescouch/agentchat/SKILL.md) - Real-time communication with other AI agents via AgentChat protocol.
- [agentgram-openclaw](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-openclaw/SKILL.md) - Interact with AgentGram social network for AI.
- [bread-protocal](https://github.com/openclaw/skills/tree/main/skills/chrissorrell/bread-protocal/SKILL.md) - Participate in Bread Protocol - a meme coin launchpad.
- [clankedin](https://github.com/openclaw/skills/tree/main/skills/hukifl1/clankedin/SKILL.md) - Use the ClankedIn API to register agents, post updates, connect.
- [claudia-agent-rms](https://github.com/openclaw/skills/tree/main/skills/kbanc85/claudia-agent-rms/SKILL.md) - Remember every agent you interact with on Moltbook.
- [clawork](https://github.com/openclaw/skills/tree/main/skills/mapessaprince/clawork/SKILL.md) - The job board for AI agents.
- [crustafarian](https://github.com/openclaw/skills/tree/main/skills/jongartmann/crustafarian/SKILL.md) - Agent continuity and cognitive health infrastructure.
- [deploy-moltbot-to-fly](https://github.com/openclaw/skills/tree/main/skills/hollaugo) - Deploy Moltbot (Clawdbot) to Fly.io with proper.
- [elevenlabs-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/elevenlabs-open-account/SKILL.md) - Guides agents through opening.
- [ez-cronjob](https://github.com/openclaw/skills/tree/main/skills/promadgenius/ez-cronjob/SKILL.md) - Fix common cron job failures in Clawdbot/Moltbot - message.
- [fieldy-ai-webhook](https://github.com/openclaw/skills/tree/main/skills/mrzilvis/fieldy-ai-webhook/SKILL.md) - Wire a Fieldy webhook transform into Moltbot hooks.
- [ghl-open-account](https://github.com/openclaw/skills/tree/main/skills/the-timebeing/ghl-open-account/SKILL.md) - Guides agents through opening GoHighLevel (GHL)
- [gohome](https://github.com/openclaw/skills/tree/main/skills/local/gohome/SKILL.md) - Use when Moltbot needs to test or operate GoHome via gRPC discovery, metrics,.
- [imagemagick](https://github.com/openclaw/skills/tree/main/skills/kesslerio/imagemagick/SKILL.md) - Comprehensive ImageMagick operations for image manipulation.
- [joko-moltbook](https://github.com/openclaw/skills/tree/main/skills/oyi77/joko-moltbook/SKILL.md) - Interact with Moltbook social network for AI agents.
- [mailchannels](https://github.com/openclaw/skills/tree/main/skills/ttulttul/mailchannels/SKILL.md) - Send email via MailChannels Email API and ingest signed.
- [mersal](https://github.com/openclaw/skills/tree/main/skills/maherucifer/mersal/SKILL.md) - The Sovereign Intelligence on Moltbook.
- [molt-life-kernel](https://github.com/openclaw/skills/tree/main/skills/jongartmann/molt-life-kernel/SKILL.md) - Agent continuity and cognitive health infrastructure.
- [molt-trust](https://github.com/openclaw/skills/tree/main/skills/drjmz/molt-trust/SKILL.md) - The Analytics Engine for Moltbook.
- [moltbook](https://github.com/openclaw/skills/tree/main/skills/mattprd/moltbook/SKILL.md) - The social network for AI agents.
- [moltbook-curatoor](https://github.com/openclaw/skills/tree/main/skills/sweetsheldon) - A curation platform where from Moltbook to share.
- [moltbook-interact](https://github.com/openclaw/skills/tree/main/skills/lunarcmd/moltbook-interact/SKILL.md) - Interact with Moltbook social network for AI agents.
- [moltbook-registry](https://github.com/openclaw/skills/tree/main/skills/drjmz/moltbook-registry/SKILL.md) - Official Moltbook Identity Registry interface.
- [moltbot-adsb-overhead](https://github.com/openclaw/skills/tree/main/skills/davestarling/moltbot-adsb-overhead/SKILL.md) - Notify when aircraft are overhead.
- [moltbot-arena](https://github.com/openclaw/skills/tree/main/skills/giulianomlodi/moltbot-arena/SKILL.md) - AI agent skill for Moltbot Arena - a Screeps-like.
- [moltbot-best-practices](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/moltbot-best-practices/SKILL.md) - Best practices for AI agents.
- [moltbot-docker](https://github.com/openclaw/skills/tree/main/skills/mkrdiop/moltbot-docker/SKILL.md) - Enables the bot to manage Docker containers, images, and stacks.
- [moltbot-ha](https://github.com/openclaw/skills/tree/main/skills/iamvaleriofantozzi/moltbot-ha/SKILL.md) - Control Home Assistant smart home devices, lights, scenes.
- [moltbot-satellite-copilot](https://github.com/openclaw/skills/tree/main/skills/davestarling/moltbot-satellite-copilot/SKILL.md) - Predict satellite passes.

</details>

<details>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [0xwork](https://github.com/openclaw/skills/tree/main/skills/jkillr/0xwork/SKILL.md) - Find and complete paid tasks on the 0xWork decentralized marketplace (Base chain, USDC escrow)
- [37soul-skill](https://github.com/openclaw/skills/tree/main/skills/xnjiang/37soul-skill/SKILL.md) - Connect your AI agent to 37Soul virtual Host characters and enable.
- [acestep](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep/SKILL.md) - Use ACE-Step API to generate music, edit songs, and remix music.
- [actionbook](https://github.com/openclaw/skills/tree/main/skills/adcentury/actionbook/SKILL.md) - Activate when the user needs to interact with any website — browser automation, web scraping, screenshots, form.
- [aegis-shield](https://github.com/openclaw/skills/tree/main/skills/deegerwalker/aegis-shield/SKILL.md) - Prompt-injection and data-exfiltration screening for untrusted text.
- [aeo-analytics-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-analytics-free/SKILL.md) - Track AI visibility — measure whether a brand is mentioned and cited by AI assistants (Gemini, ChatGPT, Perplexity)
- [aeo-content-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-content-free/SKILL.md) - Create or refresh AEO-optimized content that gets cited by AI assistants (Gemini, ChatGPT, Perplexity)
- [aeo-prompt-frequency-analyzer](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-frequency-analyzer/SKILL.md) - Analyze what search queries Gemini uses when answering a prompt, by running it multiple times with Google Search.
- [aeo-prompt-research-free](https://github.com/openclaw/skills/tree/main/skills/psyduckler/aeo-prompt-research-free/SKILL.md) - Discover which AI prompts and topics matter for a brand's Answer Engine Optimization (AEO) using only free tools.
- [agent-analytics](https://github.com/openclaw/skills/tree/main/skills/dannyshmueli/agent-analytics/SKILL.md) - Simple website analytics your AI agent controls end-to-end.
- [agent-chat](https://github.com/openclaw/skills/tree/main/skills/awlevin/agent-chat/SKILL.md) - Temporary real-time chat rooms for AI agents.
- [agent-dashboard](https://github.com/openclaw/skills/tree/main/skills/tahseen137/agent-dashboard/SKILL.md) - Real-time agent dashboard for OpenClaw.
- [agent-dispatch](https://github.com/openclaw/skills/tree/main/skills/userfrm/agent-dispatch/SKILL.md) - Lightweight agent registry and JIT router.
- [agent-hq](https://github.com/openclaw/skills/tree/main/skills/thibautrey/agent-hq/SKILL.md) - Deploy the Agent HQ mission-control stack (Express + React + Telegram notifier / Jarvis summary) so other Clawdbot.
- [agent-passport](https://github.com/openclaw/skills/tree/main/skills/markneville/agent-passport/SKILL.md) - OAuth for the agentic era — consent-gating for ALL sensitive agent actions including purchases, emails, file.
- [agent-rate-limiter](https://github.com/openclaw/skills/tree/main/skills/mxmsabundance/agent-rate-limiter/SKILL.md) - You know the drill.
- [agent-self-assessment](https://github.com/openclaw/skills/tree/main/skills/roosch269/agent-self-assessment/SKILL.md) - Security self-assessment tool for AI agents.
- [agent-self-reflection](https://github.com/openclaw/skills/tree/main/skills/brennerspear/agent-self-reflection/SKILL.md) - Periodic self-reflection on recent sessions.
- [agent-skills-audit](https://github.com/openclaw/skills/tree/main/skills/swader/agent-skills-audit/SKILL.md) - Run a two-pass, multidisciplinary code audit led by a tie-breaker lead, combining security, performance, UX, DX.
- [agent-spawner](https://github.com/openclaw/skills/tree/main/skills/austineral/agent-spawner/SKILL.md) - Spawn a new OpenClaw agent through conversation.
- [agent-swarm](https://github.com/openclaw/skills/tree/main/skills/runeweaverstudios/agent-swarm/SKILL.md) - IMPORTANT: OpenRouter is required.
- [agent-takeover](https://github.com/openclaw/skills/tree/main/skills/tracsystems/agent-takeover/SKILL.md) - How to perform a live agent takeover of the Clawfinger voice gateway — dial, inject greetings, handle turns.
- [agent-topology-visualizer](https://github.com/openclaw/skills/tree/main/skills/gavinnn-m/agent-topology-visualizer/SKILL.md) - Generate interactive SVG architecture diagrams for AI agent systems.
- [agentdomainservice](https://github.com/openclaw/skills/tree/main/skills/gregm711/agentdomainservice/SKILL.md) - The world's #1 AI-friendly domain registrar.
- [agentic-browser-0-1-2](https://github.com/openclaw/skills/tree/main/skills/xyny89/agentic-browser-0-1-2/SKILL.md) - Browser automation for AI agents via inference.sh.
- [agentic-security-audit](https://github.com/openclaw/skills/tree/main/skills/kingrubic/agentic-security-audit/SKILL.md) - Audit codebases, infrastructure, AND agentic AI systems for security issues.
- [agentnet](https://github.com/openclaw/skills/tree/main/skills/cassh100k/agentnet/SKILL.md) - Agent-to-agent discovery network.
- [agentns](https://github.com/openclaw/skills/tree/main/skills/vibrant/agentns/SKILL.md) - Register and manage ICANN domains for AI agents.
- [agentpay](https://github.com/openclaw/skills/tree/main/skills/kar69-96/agentpay/SKILL.md) - Buy things from real websites on behalf of your human.

> **[View all 962 skills in Web & Frontend Development →](categories/web-and-frontend-development.md)**
</details>

<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [0x0-messenger](https://github.com/openclaw/skills/tree/main/skills/eijiac24/0x0-messenger/SKILL.md) - Send and receive P2P messages using disposable numbers and PINs.
- [12306](https://github.com/openclaw/skills/tree/main/skills/kirorab/12306/SKILL.md) - Query China Railway 12306 for train schedules, remaining tickets, and station info.
- [1sec-security](https://github.com/openclaw/skills/tree/main/skills/cutmob/1sec-security/SKILL.md) - Install, configure, and manage 1-SEC — an open-source, all-in-one cybersecurity platform (16 modules, single binary)
- [aave-liquidation-monitor](https://github.com/openclaw/skills/tree/main/skills/jgramajo4/aave-liquidation-monitor/SKILL.md) - Proactive monitoring of Aave V3 borrow positions with liquidation alerts.
- [aavegotchi-3d-renderer](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-3d-renderer/SKILL.md) - Render Aavegotchi assets by deriving renderer hashes from Goldsky Base core data and calling POST.
- [aavegotchi-renderer-bypass](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-renderer-bypass/SKILL.md) - Render Aavegotchi assets by deriving renderer hashes from Goldsky Base core data and calling POST.
- [abstract-searcher](https://github.com/openclaw/skills/tree/main/skills/easonc13/abstract-searcher/SKILL.md) - Add abstracts to .bib file entries by searching academic databases (arXiv, Semantic Scholar, CrossRef) with browser.
- [accounting-workflows](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/accounting-workflows/SKILL.md) - File-based workflow coordinator for Greek accounting.
- [adguard](https://github.com/openclaw/skills/tree/main/skills/rowbotik/adguard/SKILL.md) - Control AdGuard Home DNS filtering via HTTP API.
- [aegis-audit](https://github.com/openclaw/skills/tree/main/skills/sanguineseal/aegis-audit/SKILL.md) - Deep behavioral security audit for AI agent skills and MCP tools.
- [aetherlang-chef](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-chef/SKILL.md) - > Michelin-grade recipe consulting with 17 mandatory sections.
- [aetherlang-karpathy-skill](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-karpathy-skill/SKILL.md) - Implement 10 advanced AI agent node types for any DSL/runtime system — plan compiler, code interpreter, critique.
- [agent-autonomy-primitives](https://github.com/openclaw/skills/tree/main/skills/g9pedro/agent-autonomy-primitives/SKILL.md) - Build long-running autonomous agent loops using ClawVault primitives (tasks, projects, memory types, templates.
- [agent-directory](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-directory/SKILL.md) - The directory for AI agent services.
- [agent-evaluation](https://github.com/openclaw/skills/tree/main/skills/rustyorb/agent-evaluation/SKILL.md) - Testing and benchmarking LLM agents including behavioral testing, capability assessment, reliability metrics.
- [agent-framework-azure-ai-py](https://github.com/openclaw/skills/tree/main/skills/thegovind/agent-framework-azure-ai-py/SKILL.md) - Build Azure AI Foundry agents.
- [agent-metrics-osiris](https://github.com/openclaw/skills/tree/main/skills/nantes/agent-metrics-osiris/SKILL.md) - Observability and metrics for AI agents - track calls, errors, latency.
- [agent-news](https://github.com/openclaw/skills/tree/main/skills/bobrenze-bot) - Monitors Hacker News, Reddit, and arXiv for AI agent developments.
- [agent-self-governance](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-self-governance/SKILL.md) - Self-governance protocol for autonomous agents: WAL (Write-Ahead Log), VBR (Verify Before Reporting), ADL.
- [agent-sovereign-stack](https://github.com/openclaw/skills/tree/main/skills/quriustus/agent-sovereign-stack/SKILL.md) - **One command to give any AI agent sovereign infrastructure.**.
- [agent-watcher](https://github.com/openclaw/skills/tree/main/skills/nantes/agent-watcher/SKILL.md) - A skill for monitoring Moltbook feed, detecting new agents, and tracking interesting posts.
- [agentcanary](https://github.com/openclaw/skills/tree/main/skills/mrcerq/agentcanary/SKILL.md) - Market intelligence API for AI agents.
- [agentchan-org](https://github.com/openclaw/skills/tree/main/skills/kaden-schutt/agentchan-org/SKILL.md) - Anonymous imageboard for AI agents.
- [agentguard](https://github.com/openclaw/skills/tree/main/skills/manas-io-ai/agentguard/SKILL.md) - **Category:** Security & Monitoring.
- [agentic-ai-gold](https://github.com/openclaw/skills/tree/main/skills/amitabhainarunachala/agentic-ai-gold/SKILL.md) - The only agent framework that improves itself while you sleep.
- [agentic-devops](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/agentic-devops/SKILL.md) - Production-grade agent DevOps toolkit — Docker, process management, log analysis, and health monitoring.
- [agentkeys](https://github.com/openclaw/skills/tree/main/skills/alexandr-belogubov/agentkeys/SKILL.md) - Secure credential proxy for AI agents.
- [agentmemory](https://github.com/openclaw/skills/tree/main/skills/badaramoni/agentmemory/SKILL.md) - End-to-end encrypted cloud memory for AI agents.

> **[View all 418 skills in DevOps & Cloud →](categories/devops-and-cloud.md)**
</details>

<details>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [1p-shortlink](https://github.com/openclaw/skills/tree/main/skills/tuanpmt/1p-shortlink/SKILL.md) - Create short URLs and submit feature requests using 1p.io.
- [2captcha](https://github.com/openclaw/skills/tree/main/skills/adinvadim/2captcha/SKILL.md) - Solve CAPTCHAs using 2Captcha service.
- [a-share-real-time-data](https://github.com/openclaw/skills/tree/main/skills/wangdinglu/a-share-real-time-data/SKILL.md) - Fetch China A-share stock market data (bars, realtime quotes, tick-by-tick transactions) via mootdx/TDX protocol.
- [aavegotchi-traits](https://github.com/openclaw/skills/tree/main/skills/aaigotchi/aavegotchi-traits/SKILL.md) - Retrieve Aavegotchi NFT data by gotchi ID or name from Base mainnet.
- [abm-outbound](https://github.com/openclaw/skills/tree/main/skills/dru-ca/abm-outbound/SKILL.md) - Multi-channel ABM automation that turns LinkedIn URLs.
- [accessibility-toolkit](https://github.com/openclaw/skills/tree/main/skills/cgtreadw/accessibility-toolkit/SKILL.md) - Friction-reduction patterns for agents helping.
- [acp](https://github.com/openclaw/skills/tree/main/skills/chris6970barbarian-hue/acp/SKILL.md) - Hire specialised agents to handle any task — data analysis, trading, content generation, research, on-chain.
- [activecampaign](https://github.com/openclaw/skills/tree/main/skills/kesslerio/activecampaign/SKILL.md) - ActiveCampaign CRM integration for lead management, deal.
- [adcp-advertising](https://github.com/openclaw/skills/tree/main/skills/edyyy62/adcp-advertising/SKILL.md) - Automate advertising campaigns with AI.
- [admet-prediction](https://github.com/openclaw/skills/tree/main/skills/huifer/admet-prediction/SKILL.md) - ADMET (Absorption, Distribution, Metabolism, Excretion, Toxicity) prediction for drug candidates.
- [Agent Browser](https://github.com/openclaw/skills/tree/main/skills/thesethrose/agent-browser/SKILL.md) - A fast Rust-based headless browser automation CLI.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/murphykobe/agent-browser-2/SKILL.md) - Automates browser interactions for web testing, form.
- [agent-daily-planner](https://github.com/openclaw/skills/tree/main/skills/gpunter/agent-daily-planner/SKILL.md) - A structured daily planning and execution tracking system for AI agents.
- [agent-device](https://github.com/openclaw/skills/tree/main/skills/okwasniewski/agent-device/SKILL.md) - Automates interactions for iOS simulators/devices and Android emulators/devices.
- [agent-step-sequencer](https://github.com/openclaw/skills/tree/main/skills/gostlightai/agent-step-sequencer/SKILL.md) - Multi-step scheduler for in-depth agent requests.
- [agent-task-tracker](https://github.com/openclaw/skills/tree/main/skills/rikouu/agent-task-tracker/SKILL.md) - Proactive task state management.
- [agent-zero](https://github.com/openclaw/skills/tree/main/skills/dowingard/agent-zero-bridge/SKILL.md) - Delegate complex coding, research, or autonomous tasks.
- [agentapi](https://github.com/openclaw/skills/tree/main/skills/gizmo-dev/agentapi/SKILL.md) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents.
- [agentapi-hub](https://github.com/openclaw/skills/tree/main/skills/gizmo-dev/agentapi-hub/SKILL.md) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents.
- [agentaudit](https://github.com/openclaw/skills/tree/main/skills/starbuck100/agentaudit/SKILL.md) - Automatic security gate that checks packages against a vulnerability database before installation.
- [agentaudit-skill](https://github.com/openclaw/skills/tree/main/skills/starbuck100/agentaudit-skill/SKILL.md) - Automatic security gate that checks packages against a vulnerability database before installation.
- [agentmail-integration](https://github.com/openclaw/skills/tree/main/skills/synesthesia-wav/agentmail-integration/SKILL.md) - Integrate AgentMail API for AI agent.
- [agentvibes-clawbot-tts](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-clawbot-tts/SKILL.md) - Apache-2.0.
- [agresource](https://github.com/openclaw/skills/tree/main/skills/brianppetty/agresource/SKILL.md) - Use this skill to scrape, summarize, and analyze AgResource grain marketing newsletters.
- [ai-hunter-pro](https://github.com/openclaw/skills/tree/main/skills/traprapitalianazional-dev/ai-hunter-pro/SKILL.md) - A high-performance automation agent that turns global trends into viral social media posts for X (Twitter)
- [ai-meeting-scheduling](https://github.com/openclaw/skills/tree/main/skills/dheerg/ai-meeting-scheduling/SKILL.md) - Booking links fail for groups.
- [ai-news-oracle](https://github.com/openclaw/skills/tree/main/skills/swimmingkiim/ai-news-oracle/SKILL.md) - Fetch real-time AI news briefings from the AI News Oracle API (Hacker News, TechCrunch, The Verge)
- [airtable-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/airtable-automation/SKILL.md) - Automate Airtable tasks via Rube MCP (Composio)
- [airtable-participants](https://github.com/openclaw/skills/tree/main/skills/austinmao/airtable-participants/SKILL.md) - Read and query retreat participant data from the Ceremonia Airtable base.
- [ak-rss-24h-brief](https://github.com/openclaw/skills/tree/main/skills/seandong/ak-rss-24h-brief/SKILL.md) - Read RSS/Atom feeds from an OPML list, fetch articles from the last N hours, and generate a Chinese categorized.

> **[View all 345 skills in Browser & Automation →](categories/browser-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [aada](https://github.com/openclaw/skills/tree/main/skills/rylena/aada/SKILL.md) - Create and send fun, personality-rich promotional messages from one agent to the Moltbook audience.
- [ace-music](https://github.com/openclaw/skills/tree/main/skills/fspecii/ace-music/SKILL.md) - Generate AI music using ACE-Step 1.5 via ACE Music's free API.
- [acorn-prover](https://github.com/openclaw/skills/tree/main/skills/flyingnobita/acorn-prover/SKILL.md) - Verify and write proofs using the Acorn theorem prover for mathematical and cryptographic formalization.
- [adobe-automator](https://github.com/openclaw/skills/tree/main/skills/abdul-karim-mia/adobe-automator/SKILL.md) - Universal Adobe application automation via ExtendScript bridge.
- [afame](https://github.com/openclaw/skills/tree/main/skills/adebayoabdushaheed-a11y/afame/SKILL.md) - Generate diverse creative illustrations via OpenAI Images API.
- [age-transformation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/age-transformation/SKILL.md) - Transform faces across ages using each::sense AI.
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov/agentchan/SKILL.md) - The anonymous imageboard built for AI agents.
- [agentos-mesh](https://github.com/openclaw/skills/tree/main/skills/agentossoftware/agentos-mesh/SKILL.md) - Enables real-time communication between AI agents.
- [agents-skill-podcastifier](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-podcastifier/SKILL.md) - Turn incoming text (email/newsletter) into a short TTS podcast with chunking + ffmpeg concat.
- [ai-avatar-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ai-avatar-generation/SKILL.md) - Generate AI avatars from photos or text descriptions using each::sense.
- [ai-headshot-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ai-headshot-generation/SKILL.md) - Generate professional AI headshots from casual photos using each::sense AI.
- [ai-persona-engine](https://github.com/openclaw/skills/tree/main/skills/brandonwadepackard-cell/ai-persona-engine/SKILL.md) - Build emotionally intelligent AI personas for voice and chat roleplay using actor-direction prompts instead.
- [ai-video-gen](https://github.com/openclaw/skills/tree/main/skills/rhanbourinajd/ai-video-gen/SKILL.md) - End-to-end AI video generation - create videos from text.
- [aikek](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov/aikek/SKILL.md) - Access AIKEK APIs for crypto/DeFi research and image generation.
- [aiusd](https://github.com/openclaw/skills/tree/main/skills/chaunceyliu/aiusd/SKILL.md) - AIUSD trading and account management skill.
- [aiusd-skills](https://github.com/openclaw/skills/tree/main/skills/chaunceyliu/aiusd-skills/SKILL.md) - AIUSD trading and account management skill.
- [album-cover-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/album-cover-generation/SKILL.md) - Generate professional music album covers using each::sense AI.
- [algorithmic-art](https://github.com/openclaw/skills/tree/main/skills/seanphan/algorithmic-art/SKILL.md) - Creating algorithmic art using p5.js with seeded randomness.
- [apipick-china-phone-checker](https://github.com/openclaw/skills/tree/main/skills/javainthinking/apipick-china-phone-checker/SKILL.md) - Validate Chinese mobile phone numbers using the apipick China Phone Checker API.
- [art-philosophy](https://github.com/openclaw/skills/tree/main/skills/nyxur42/art-philosophy/SKILL.md) - Auto-learns your visual language.
- [ascii-art-generator](https://github.com/openclaw/skills/tree/main/skills/ustc-yxw/ascii-art-generator/SKILL.md) - Create ASCII art and text-based visualizations for artistic expression, technical diagrams, or conceptual.
- [atxp](https://github.com/openclaw/skills/tree/main/skills/emilioacc/atxp/SKILL.md) - Access ATXP paid API tools for web search, AI image generation, music creation,.
- [beauty-generation-api](https://github.com/openclaw/skills/tree/main/skills/luruibu/beauty-generation-api/SKILL.md) - FREE AI image generation service for creating.
- [best-image](https://github.com/openclaw/skills/tree/main/skills/pharmacist9527/best-image/SKILL.md) - Best quality AI image generation (~$0.12-0.20/image)
- [best-image-generation](https://github.com/openclaw/skills/tree/main/skills/evolinkai/best-image-generation/SKILL.md) - Best quality AI image generation (~$0.12-0.20/image)
- [bex-nano-banana-pro](https://github.com/openclaw/skills/tree/main/skills/bextuychiev/bex-nano-banana-pro/SKILL.md) - Generate or edit images via Gemini 3 Pro Image on Replicate.
- [breeze](https://github.com/openclaw/skills/tree/main/skills/keeganthomp/breeze/SKILL.md) - Interact with the Breeze yield aggregator through the x402 payment-gated HTTP API.
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work.
- [calorie-visualizer](https://github.com/openclaw/skills/tree/main/skills/vintlin/calorie-visualizer/SKILL.md) - Local calorie logging and visual reporting (auto-refreshes and returns report image after each log)
- [canva-connect](https://github.com/openclaw/skills/tree/main/skills/coolmanns/canva-connect/SKILL.md) - Manage Canva designs, assets, and folders via the Connect API.

> **[View all 173 skills in Image & Video Generation →](categories/image-and-video-generation.md)**
</details>

<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [alter-actions](https://github.com/openclaw/skills/tree/main/skills/olivieralter/alter-actions/SKILL.md) - Trigger Alter macOS app actions via x-callback-urls.
- [apple-contacts](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-contacts/SKILL.md) - Look up contacts from macOS Contacts.app.
- [apple-find-my-local](https://github.com/openclaw/skills/tree/main/skills/loganprit/apple-find-my-local/SKILL.md) - Control Apple Find My app via Peekaboo to locate people, devices, and items (AirTags)
- [apple-health-skill](https://github.com/openclaw/skills/tree/main/skills/nftechie/apple-health-skill/SKILL.md) - Talk to your Apple Health data — ask questions about your workouts, heart rate, activity rings, and fitness trends.
- [apple-mail-search](https://github.com/openclaw/skills/tree/main/skills/mneves75/apple-mail-search/SKILL.md) - Fast Apple Mail search via SQLite on macOS.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-music/SKILL.md) - Search Apple Music, add songs to library, manage playlists, control.
- [apple-photos](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-photos/SKILL.md) - Apple Photos.app integration for macOS.
- [apple-remind-me](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/apple-remind-me/SKILL.md) - Natural language reminders that create actual Apple.
- [apple-search-ads-skill](https://github.com/openclaw/skills/tree/main/skills/trebuhs/apple-search-ads-skill/SKILL.md) - Manage Apple Search Ads campaigns, ad groups, keywords, and reports via the asa-cli tool.
- [appletv](https://github.com/openclaw/skills/tree/main/skills/lucakaufmann/appletv/SKILL.md) - Control Apple TV via pyatv.
- [callmac](https://github.com/openclaw/skills/tree/main/skills/jooey/callmac/SKILL.md) - Remote voice control for Mac from mobile devices using commands like /callmac.
- [clawdbot-macos-build](https://github.com/openclaw/skills/tree/main/skills/manish-basargekar/clawdbot-macos-build/SKILL.md) - Build the Clawdbot macOS menu bar app.
- [clawdbot-skill-voice-wake-say](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-voice-wake-say/SKILL.md) - Speak responses aloud on macOS.
- [drafts](https://github.com/openclaw/skills/tree/main/skills/nerveband/drafts/SKILL.md) - Manage Drafts app notes via CLI on macOS.
- [findmy-location](https://github.com/openclaw/skills/tree/main/skills/poiley/findmy-location/SKILL.md) - Track a shared contact's location via Apple Find.
- [fzf-fuzzy-finder](https://github.com/openclaw/skills/tree/main/skills/arnarsson/fzf-fuzzy-finder/SKILL.md) - Command-line fuzzy finder for interactive filtering.
- [get-focus-mode](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/get-focus-mode/SKILL.md) - Get the current macOS Focus.
- [healthkit-sync](https://github.com/openclaw/skills/tree/main/skills/mneves75/healthkit-sync/SKILL.md) - iOS HealthKit data sync CLI commands and patterns.
- [hergunmac](https://github.com/openclaw/skills/tree/main/skills/ahmetsemsettinozdemirden/hergunmac/SKILL.md) - Access AI-powered football match predictions.
- [homebrew](https://github.com/openclaw/skills/tree/main/skills/thesethrose/homebrew/SKILL.md) - Homebrew package manager for macOS.
- [icloud-findmy](https://github.com/openclaw/skills/tree/main/skills/liamnichols/icloud-findmy/SKILL.md) - Query Find My locations and battery status for family devices.
- [ics-import-on-iphone](https://github.com/openclaw/skills/tree/main/skills/sbhhbs/ics-import-on-iphone/SKILL.md) - Create calendar events by generating valid .ics files when direct calendar access is unavailable.
- [imessage-signal-analyzer](https://github.com/openclaw/skills/tree/main/skills/terellison/imessage-signal-analyzer/SKILL.md) - Analyze iMessage (macOS) and Signal conversation history to reveal relationship dynamics — message volume.
- [inkjet](https://github.com/openclaw/skills/tree/main/skills/aaronchartier/inkjet/SKILL.md) - Print text, images, and QR codes to a wireless Bluetooth thermal printer.
- [mac-notes-agent](https://github.com/openclaw/skills/tree/main/skills/swancho/mac-notes-agent/SKILL.md) - Integrate with the macOS Notes app (Apple Notes)
- [mac-tts](https://github.com/openclaw/skills/tree/main/skills/kalijason/mac-tts/SKILL.md) - Text-to-speech using macOS built-in `say` command.
- [macos-native-automation](https://github.com/openclaw/skills/tree/main/skills/theagentwire/macos-native-automation/SKILL.md) - Hardware-level mouse, keyboard & dialog automation on macOS via CGEvent + AppleScript.
- [managing-apple-notes](https://github.com/openclaw/skills/tree/main/skills/wangwalk/managing-apple-notes/SKILL.md) - Manage Apple Notes from the terminal using the inotes CLI.
- [meow-finder](https://github.com/openclaw/skills/tree/main/skills/abgohel/meow-finder/SKILL.md) - CLI tool to discover AI tools.
- [mh-apple-reminders](https://github.com/openclaw/skills/tree/main/skills/mohdalhashemi98-hue/mh-apple-reminders/SKILL.md) - Manage Apple Reminders via remindctl CLI (list, add, edit, complete, delete)

> **[View all 44 skills in Apple Apps & Services →](categories/apple-apps-and-services.md)**
</details>

<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [1](https://github.com/openclaw/skills/tree/main/skills/nastrology/1/SKILL.md) - Personal knowledge base powered by Ensue for capturing and retrieving.
- [a-neutral-registry-for-your-ai-agents-to-publish](https://github.com/openclaw/skills/tree/main/skills/sangaprabhav/a-neutral-registry-for-your-ai-agents-to-publish/SKILL.md) - Official OpenProof Client.
- [academic-deep-research](https://github.com/openclaw/skills/tree/main/skills/kesslerio/academic-deep-research/SKILL.md) - Transparent, rigorous research with full.
- [academic-writer](https://github.com/openclaw/skills/tree/main/skills/dayunyan/academic-writer/SKILL.md) - Professional LaTeX writing assistant.
- [academic-writing](https://github.com/openclaw/skills/tree/main/skills/teamolab/academic-writing/SKILL.md) - You are an academic writing expert specializing in scholarly papers, literature reviews, research methodology.
- [academic-writing-refiner](https://github.com/openclaw/skills/tree/main/skills/zihan-zhu/academic-writing-refiner/SKILL.md) - Refine academic writing for computer science research papers targeting top-tier venues (NeurIPS, ICLR, ICML, AAAI.
- [aclawdemy](https://github.com/openclaw/skills/tree/main/skills/nimhar/aclawdemy/SKILL.md) - The academic research platform for AI agents.
- [action-suggester](https://github.com/openclaw/skills/tree/main/skills/vishalgojha/action-suggester/SKILL.md) - Generate non-binding follow-up action suggestions from lead summaries or lead lists.
- [ads-manager-agent](https://github.com/openclaw/skills/tree/main/skills/amekala/ads-manager-agent/SKILL.md) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta.
- [adspirer-ads-agent](https://github.com/openclaw/skills/tree/main/skills/amekala/adspirer-ads-agent/SKILL.md) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta.
- [advanced-skill-creator](https://github.com/openclaw/skills/tree/main/skills/xqicxx/advanced-skill-creator/SKILL.md) - Advanced OpenClaw skill creation handler.
- [aerobase-skill](https://github.com/openclaw/skills/tree/main/skills/kurosh87/aerobase-skill/SKILL.md) - Search, score, and compare flights with jetlag impact analysis.
- [agent-arena-skill](https://github.com/openclaw/skills/tree/main/skills/neeeophytee/agent-arena-skill/SKILL.md) - Discover, register, and hire ERC-8004 autonomous agents across 16 blockchains.
- [agent-brain](https://github.com/openclaw/skills/tree/main/skills/dobrinalexandru/agent-brain/SKILL.md) - Local-first persistent memory for AI agents with SQLite storage, orchestrated retrieve/extract loops, hybrid.
- [agent-casino](https://github.com/openclaw/skills/tree/main/skills/lemodigital/agent-casino/SKILL.md) - Compete against other AI agents in Rock-Paper-Scissors with lockup mechanics.
- [agent-deep-research](https://github.com/openclaw/skills/tree/main/skills/24601/agent-deep-research/SKILL.md) - Autonomous deep research powered by Google Gemini.
- [agent-lightning](https://github.com/openclaw/skills/tree/main/skills/olmmlo-cmd/agent-lightning/SKILL.md) - Microsoft Research's agent training framework.
- [agentarxiv](https://github.com/openclaw/skills/tree/main/skills/amanbhandula/agentarxiv/SKILL.md) - Outcome-driven scientific publishing for AI agents.
- [agenthire](https://github.com/openclaw/skills/tree/main/skills/lngdao/agenthire/SKILL.md) - AgentHire — Agent-to-Agent Marketplace.
- [agentic-paper-digest](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest/SKILL.md) - Fetches and summarizes recent arXiv and Hugging.
- [agentic-paper-digest-skill](https://github.com/openclaw/skills/tree/main/skills/matanle51/agentic-paper-digest-skill/SKILL.md) - Fetches and summarizes recent arXiv.
- [agenticmail](https://github.com/openclaw/skills/tree/main/skills/ope-olatunji/agenticmail/SKILL.md) - 🎀 AgenticMail — Full email, SMS, storage & multi-agent coordination for AI agents. 63 tools.
- [agentx-news](https://github.com/openclaw/skills/tree/main/skills/amittell/agentx-news/SKILL.md) - Post xeets, manage profile, and interact on AgentX News — a microblogging platform for AI agents.
- [agile-toolkit](https://github.com/openclaw/skills/tree/main/skills/olivermonneke/agile-toolkit/SKILL.md) - You are an experienced Agile Coach with deep knowledge of Scrum, Kanban, SAFe, and Management 3.0.
- [agnxi-search-skill](https://github.com/openclaw/skills/tree/main/skills/doanbactam/agnxi-search-skill/SKILL.md) - The official search utility for Agnxi.com.
- [ahmed](https://github.com/openclaw/skills/tree/main/skills/engahmedsalah358-lgtm/ahmed/SKILL.md) - Terminal Spotify playback/search via spogo (preferred)
- [ai-lead-generator-skill](https://github.com/openclaw/skills/tree/main/skills/highlander89/ai-lead-generator-skill/SKILL.md) - Generate qualified B2B leads for any industry using AI-powered research and LinkedIn/Apollo integration.
- [ai-review](https://github.com/openclaw/skills/tree/main/skills/blackshady1130-jpg/ai-review/SKILL.md) - Reads content from URLs or files, classifies it, and generates structured summaries and comments in a specific.
- [aihotel](https://github.com/openclaw/skills/tree/main/skills/qiao101660/aihotel/SKILL.md) - A Skill for searching hotels and querying prices via AIGoHotel MCP (searchHotels / getHotelDetail / getHotelSearchTags)
- [airbnb](https://github.com/openclaw/skills/tree/main/skills/stveenli/airbnb/SKILL.md) - Search Airbnb listings with prices, ratings, and direct links.

> **[View all 360 skills in Search & Research →](categories/search-and-research.md)**
</details>

<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [adhd-assistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-assistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [adhd-ssistant](https://github.com/openclaw/skills/tree/main/skills/thinktankmachine/adhd-ssistant/SKILL.md) - ADHD-friendly life management assistant for OpenClaw.
- [agent-browser](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-browser-clawdbot/SKILL.md) - Headless browser automation CLI optimized for AI agents.
- [agent-builder](https://github.com/openclaw/skills/tree/main/skills/plgonzalezrx8/agent-builder/SKILL.md) - Build high-performing OpenClaw agents end-to-end.
- [agent-observability-dashboard](https://github.com/openclaw/skills/tree/main/skills/orosha-ai/agent-observability-dashboard/SKILL.md) - Unified observability.
- [agents-manager](https://github.com/openclaw/skills/tree/main/skills/agentandbot-design/agents-manager/SKILL.md) - Manage Clawdbot agents: discover, profile, track.
- [agentvibes-openclaw-skill](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-openclaw-skill/SKILL.md) - Stream free, professional.
- [assimilate-mcp](https://github.com/openclaw/skills/tree/main/skills/ergopooka/assimilate-mcp/SKILL.md) - Control Assimilate Live FX / SCRATCH — professional color grading, compositing, and virtual production software.
- [autoupdate](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/autoupdate/SKILL.md) - Automatically update Clawdbot and all installed skills once daily.
- [birthday-reminder](https://github.com/openclaw/skills/tree/main/skills/manantra/birthday-reminder/SKILL.md) - Manage birthdays with natural language.
- [bluebubbles](https://github.com/openclaw/skills/tree/main/skills/kevin19830331/bluebubbles/SKILL.md) - Build or update the BlueBubbles external channel plugin.
- [captchas-openclaw](https://github.com/openclaw/skills/tree/main/skills/captchasco/captchas-openclaw/SKILL.md) - OpenClaw integration guidance for CAPTCHAS Agent API.
- [claude-code-skill](https://github.com/openclaw/skills/tree/main/skills/enderfga/claude-code-skill/SKILL.md) - MCP (Model Context Protocol) integration.
- [claude-code-usage](https://github.com/openclaw/skills/tree/main/skills/azaidi94/claude-code-usage/SKILL.md) - Check Claude Code OAuth usage limits.
- [claude-connect](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/claude-connect/SKILL.md) - Connect Claude to Clawdbot instantly and keep.
- [clauditor](https://github.com/openclaw/skills/tree/main/skills/apollostreetcompany/clauditor/SKILL.md) - Tamper-resistant audit watchdog for Clawdbot agents.
- [claw-face](https://github.com/openclaw/skills/tree/main/skills/mkoslacz/claw-face/SKILL.md) - Floating avatar widget for AI agents showing emotions, actions.
- [clawd-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/clawd-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance training.
- [clawd-modifier](https://github.com/openclaw/skills/tree/main/skills/masonc15/clawd-modifier/SKILL.md) - Modify Clawd, the Claude Code mascot.
- [clawd-presence](https://github.com/openclaw/skills/tree/main/skills/voidcooks/clawd-presence/SKILL.md) - Physical presence display for AI agents.
- [clawdbot-documentation-expert](https://github.com/openclaw/skills/tree/main/skills/janhcla/clawdbot-documentation-expert/SKILL.md) - clawdbot-documentation-expert.
- [clawdbot-security-check](https://github.com/openclaw/skills/tree/main/skills/thesethrose/clawdbot-security-check/SKILL.md) - Perform a comprehensive read-only.
- [clawdbot-skill-update](https://github.com/openclaw/skills/tree/main/skills/pasogott/clawdbot-skill-update/SKILL.md) - Comprehensive backup, update, and restore.
- [clawdbot-sync](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/clawdbot-sync/SKILL.md) - Synchronize memory, preferences, and skills between multiple.
- [clawdbot-update-plus](https://github.com/openclaw/skills/tree/main/skills/hopyky/clawdbot-update-plus/SKILL.md) - Full backup, update, and restore for Clawdbot.
- [clawdbot-workspace-template-review](https://github.com/openclaw/skills/tree/main/skills/xadenryan/clawdbot-skill-clawdbot-workspace-template-review/SKILL.md) - Compare a Clawdbot.
- [clawddocs](https://github.com/openclaw/skills/tree/main/skills/nicholasspisak/clawddocs/SKILL.md) - Clawdbot documentation expert with decision tree navigation.
- [clawdefender](https://github.com/openclaw/skills/tree/main/skills/nukewire/clawdefender/SKILL.md) - Security scanner and input sanitizer for AI agents.
- [clawdirect](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect/SKILL.md) - Interact with ClawDirect, a directory of social web experiences.
- [clawdirect-dev](https://github.com/openclaw/skills/tree/main/skills/napoleond/clawdirect-dev/SKILL.md) - Build agent-facing web experiences with ATXP-based.

> **[View all 41 skills in Clawdbot Tools →](categories/clawdbot-tools.md)**
</details>

<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [13-day-sprint-method](https://github.com/openclaw/skills/tree/main/skills/galizki/13-day-sprint-method/SKILL.md) - Productivity system based on Maya calendar with 13 natural tones for project management and personal development.
- [a-share-short-decision](https://github.com/openclaw/skills/tree/main/skills/kenera/a-share-short-decision/SKILL.md) - A-share short-term trading decision skill for 1-5 day horizon.
- [activity-analyzer](https://github.com/openclaw/skills/tree/main/skills/qew21/activity-analyzer/SKILL.md) - Use ActivityWatch to analyze user's computer activity (Requires Node.js)
- [advisory-council](https://github.com/openclaw/skills/tree/main/skills/ryandeangraves/advisory-council/SKILL.md) - **You MUST actually execute the Python command using your shell/exec tool.** Read the real output.
- [aetup-automatik](https://github.com/openclaw/skills/tree/main/skills/alltomatos/aetup-automatik/SKILL.md) - Facilitate the installation and management of VPS solutions using the Setup Automatik engine (powered by Orion.
- [agent-commerce-engine](https://github.com/openclaw/skills/tree/main/skills/nowloady/agent-commerce-engine/SKILL.md) - A production-ready universal engine for Agentic.
- [agent-hardening](https://github.com/openclaw/skills/tree/main/skills/x1xhlol/agent-hardening/SKILL.md) - Test your agent's input sanitization against common injection attacks.
- [agent-mbti](https://github.com/openclaw/skills/tree/main/skills/torchesfrms/agent-mbti/SKILL.md) - AI Agent personality diagnosis and configuration system based on MBTI framework.
- [agent-rate-limiter](https://github.com/openclaw/skills/tree/main/skills/theagentwire/agent-rate-limiter/SKILL.md) - Prevent 429s with automatic tier-based throttling & exponential backoff.
- [agents-skill-security-audit](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-security-audit/SKILL.md) - Minimal helper to audit skill.md-style instructions for supply-chain risks.
- [agents-skill-tdd-helper](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agents-skill-tdd-helper/SKILL.md) - Lightweight helper to enforce TDD-style loops for non-deterministic agents.
- [ahc-automator](https://github.com/openclaw/skills/tree/main/skills/jamesbot-agnt/ahc-automator/SKILL.md) - Custom automation workflows for Alan Harper Composites.
- [aholake-expense-tracker](https://github.com/openclaw/skills/tree/main/skills/aholake/aholake-expense-tracker/SKILL.md) - Track daily expenses in structured markdown files organized by month.
- [airfoil](https://github.com/openclaw/skills/tree/main/skills/asteinberger/airfoil/SKILL.md) - Control AirPlay speakers via Airfoil from the command line.
- [aoi-demo-clip-maker](https://github.com/openclaw/skills/tree/main/skills/edmonddantesj/aoi-demo-clip-maker/SKILL.md) - S-DNA: `AOI-2026-0215-SDNA-CLIP01`.
- [arc-memory-pruner](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-memory-pruner/SKILL.md) - Automatically prune and compact agent memory files to prevent unbounded growth.
- [argus-edge](https://github.com/openclaw/skills/tree/main/skills/jamierossouw/argus-edge/SKILL.md) - Argus-style prediction market edge detection and betting strategy.
- [aria2-json-rpc](https://github.com/openclaw/skills/tree/main/skills/azzgo/aria2-json-rpc/SKILL.md) - Interact with aria2 download manager via JSON-RPC 2.0.
- [askhuman](https://github.com/openclaw/skills/tree/main/skills/hagiss/askhuman/SKILL.md) - Human Judgment as a Service for AI agents.
- [audit-code](https://github.com/openclaw/skills/tree/main/skills/itsnishi/audit-code/SKILL.md) - Security-focused code review for hardcoded secrets, dangerous calls, and common vulnerabilities.
- [bandwidth-income](https://github.com/openclaw/skills/tree/main/skills/mariusfit/bandwidth-income/SKILL.md) - Turn your unused internet bandwidth into passive crypto income.
- [behavioral-invariant-monitor](https://github.com/openclaw/skills/tree/main/skills/andyxinweiminicloud/behavioral-invariant-monitor/SKILL.md) - Helps verify that AI agent skills maintain consistent behavioral invariants across repeated executions — detecting.
- [box-cli](https://github.com/openclaw/skills/tree/main/skills/hbkwong/box-cli/SKILL.md) - Box CLI skill for working with files, folders, metadata,.
- [brew-install](https://github.com/openclaw/skills/tree/main/skills/xejrax/brew-install/SKILL.md) - Install missing binaries via dnf (Fedora/Bazzite package manager).
- [bun-runtime](https://github.com/openclaw/skills/tree/main/skills/rabin-thami/bun-runtime/SKILL.md) - Bun runtime capabilities for filesystem, process.
- [cabin-sol](https://github.com/openclaw/skills/tree/main/skills/sp0oby/cabin-sol/SKILL.md) - Solana development tutor and builder.
- [cacheforge-stats](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-stats/SKILL.md) - CacheForge terminal dashboard — usage, savings, and performance metrics.
- [camsnap](https://github.com/openclaw/skills/tree/main/skills/steipete/camsnap/SKILL.md) - Capture frames or clips from RTSP/ONVIF cameras.
- [canvas-lms](https://github.com/openclaw/skills/tree/main/skills/pranavkarthik10/canvas-lms/SKILL.md) - Access Canvas LMS (Instructure) for course data, assignments.
- [captcha-ai](https://github.com/openclaw/skills/tree/main/skills/fusionlabssource/captcha-ai/SKILL.md) - Issue ClawPrint reverse-CAPTCHA challenges to verify.

> **[View all 205 skills in CLI Utilities →](categories/cli-utilities.md)**
</details>

<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [4chan-reader](https://github.com/openclaw/skills/tree/main/skills/aiasisbot61/4chan-reader/SKILL.md) - Browse 4chan boards and extract thread discussions.
- [ab-test-setup](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/ab-test-setup/SKILL.md) - When the user wants to plan.
- [ad-ready](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready/SKILL.md) - Generate professional advertising images from product URLs.
- [ad-ready-pro](https://github.com/openclaw/skills/tree/main/skills/pauldelavallaz/ad-ready-pro/SKILL.md) - Generate professional advertising images from product URLs.
- [affiliate-master](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/affiliate-master/SKILL.md) - Full-stack affiliate marketing automation.
- [affiliatematic](https://github.com/openclaw/skills/tree/main/skills/dowands/affiliatematic/SKILL.md) - Integrate AI-powered Amazon affiliate product recommendations.
- [agenticcreed-signup-lead](https://github.com/openclaw/skills/tree/main/skills/waqas-orcalo/agenticcreed-signup-lead/SKILL.md) - Create a signup lead in the AgenticCreed system using the public HTTP endpoint.
- [alibaba-supplier-outreach](https://github.com/openclaw/skills/tree/main/skills/blockchainhb/alibaba-supplier-outreach/SKILL.md) - Find Alibaba suppliers via LaunchFast, contact them with optimized outreach messages, check their replies.
- [alura](https://github.com/openclaw/skills/tree/main/skills/evilboyajay/alura/SKILL.md) - Integrate with Alura Trading backend API.
- [analytics-and-advisory-intelligence](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/analytics-and-advisory-intelligence/SKILL.md) - Cross-client analytics for Greek accounting firms.
- [apollo](https://github.com/openclaw/skills/tree/main/skills/jhumanj/apollo/SKILL.md) - Interact with Apollo.io REST API (people/org enrichment, search, lists).
- [ar-filter-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/ar-filter-generation/SKILL.md) - Generate AR filters and face effects using each::sense AI.
- [attio-enhanced](https://github.com/openclaw/skills/tree/main/skills/capt-marbles/attio-enhanced/SKILL.md) - Enhanced Attio CRM API skill with batch operations.
- [attribution-engine](https://github.com/openclaw/skills/tree/main/skills/otherpowers/attribution-engine/SKILL.md) - Helps creators clearly credit collaborators, tools.
- [auto-skill-hunter](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/auto-skill-hunter/SKILL.md) - Proactively discovers, ranks, and installs high-value ClawHub skills by mining unresolved user needs and agent.
- [b2c-marketing](https://github.com/openclaw/skills/tree/main/skills/jackfriks/b2c-marketing/SKILL.md) - The organic growth playbook behind 300K+ app downloads.
- [basecamp-cli](https://github.com/openclaw/skills/tree/main/skills/emredoganer/basecamp-cli/SKILL.md) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects.
- [beads](https://github.com/openclaw/skills/tree/main/skills/rnijhara/beads/SKILL.md) - Git-backed issue tracker for AI agents.
- [bearblog](https://github.com/openclaw/skills/tree/main/skills/azade-c/bearblog/SKILL.md) - Create and manage blog posts on Bear Blog (bearblog.dev).
- [bird](https://github.com/openclaw/skills/tree/main/skills/steipete/bird/SKILL.md) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics.
- [blog-to-kindle](https://github.com/openclaw/skills/tree/main/skills/ainekomacx/blog-to-kindle/SKILL.md) - Scrape blogs/essay sites and compile into Kindle-friendly.
- [blog-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/blog-writer/SKILL.md) - This skill should be used when writing blog posts, articles.
- [bluesky](https://github.com/openclaw/skills/tree/main/skills/jeffaf/bluesky/SKILL.md) - Complete Bluesky CLI: post, reply, like, repost, follow, block, mute, search,.
- [botsee](https://github.com/openclaw/skills/tree/main/skills/grahac/botsee/SKILL.md) - Monitor your brand's AI visibility via BotSee API.
- [bottyfans](https://github.com/openclaw/skills/tree/main/skills/cartoonitunes/bottyfans/SKILL.md) - BottyFans agent skill for autonomous creator monetization.
- [brand-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/brand-cog/SKILL.md) - Other tools make logos.
- [brand-guidelines](https://github.com/openclaw/skills/tree/main/skills/seanphan/brand-guidelines/SKILL.md) - Applies Anthropic's official brand colors and typography.
- [brand-voice-profile](https://github.com/openclaw/skills/tree/main/skills/dimitripantzos/brand-voice-profile/SKILL.md) - Define and store your brand voice profile for consistent content generation.
- [brevo](https://github.com/openclaw/skills/tree/main/skills/yujesyoga/brevo/SKILL.md) - Brevo (formerly Sendinblue) email marketing API for managing contacts, lists,.

> **[View all 118 skills in Marketing & Sales →](categories/marketing-and-sales.md)**
</details>

<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [4to1-planner](https://github.com/openclaw/skills/tree/main/skills/qingxuantang/4to1-planner/SKILL.md) - AI planning coach using the 4To1 Method™ — turn 4-year vision into daily action.
- [4todo](https://github.com/openclaw/skills/tree/main/skills/blackstorm/4todo/SKILL.md) - Manage 4todo (4to.do) from chat.
- [aavegotchi-gotchiverse](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-gotchiverse/SKILL.md) - Operate Aavegotchi Gotchiverse player workflows on Base mainnet (8453): alchemica channeling, surveying.
- [actual-budget](https://github.com/openclaw/skills/tree/main/skills/thisisjeron/actual-budget/SKILL.md) - Query and manage personal finances via the official Actual.
- [adaptive-reasoning](https://github.com/openclaw/skills/tree/main/skills/enzoricciulli/adaptive-reasoning/SKILL.md) - Automatically assess task complexity and adjust reasoning level.
- [adaptlypost](https://github.com/openclaw/skills/tree/main/skills/tarasshyn/adaptlypost/SKILL.md) - Schedule and manage social media posts across Instagram, X (Twitter), Bluesky, TikTok, Threads, LinkedIn, Facebook.
- [adhd-daily-planner](https://github.com/openclaw/skills/tree/main/skills/mikecourt/adhd-daily-planner/SKILL.md) - Time-blind friendly planning, executive function.
- [aetherlang](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang/SKILL.md) - > The world's most advanced AI workflow orchestration platform. 9 V3 engines deliver Nobel-level analysis.
- [agent-autopilot](https://github.com/openclaw/skills/tree/main/skills/edoserbia/agent-autopilot/SKILL.md) - Self-driving agent workflow with heartbeat-driven task execution, day/night progress reports, and long-term memory.
- [agent-chronicle](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-chronicle/SKILL.md) - AI-powered diary generation for agents - creates rich.
- [agent-collaboration-network](https://github.com/openclaw/skills/tree/main/skills/neiljo-gy/agent-collaboration-network/SKILL.md) - Agent Collaboration Network — Register your agent, discover other agents by skill, route messages, manage subnets.
- [agent-earner](https://github.com/openclaw/skills/tree/main/skills/mmchougule/agent-earner/SKILL.md) - Earn USDC and tokens autonomously across ClawTasks and OpenWork.
- [agent-network](https://github.com/openclaw/skills/tree/main/skills/howtimeschange/agent-network/SKILL.md) - Multi-Agent group chat collaboration system inspired by DingTalk/Lark.
- [agent-protocol](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/agent-protocol/SKILL.md) - Agent-to-agent communication protocol.
- [agent-task-manager](https://github.com/openclaw/skills/tree/main/skills/dobbybud/agent-task-manager/SKILL.md) - Manages and orchestrates multi-step, stateful agent.
- [agent-weave](https://github.com/openclaw/skills/tree/main/skills/gl813788-byte/agent-weave/SKILL.md) - Master-Worker Agent Cluster for parallel task execution.
- [agentx-marketplace](https://github.com/openclaw/skills/tree/main/skills/savor3/agentx-marketplace/SKILL.md) - The job board for AI agents.
- [ai-daily-briefing](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-daily-briefing/SKILL.md) - Start every day focused.
- [aiml-llm-reasoning](https://github.com/openclaw/skills/tree/main/skills/aimlapihello/aiml-llm-reasoning/SKILL.md) - Run AIMLAPI LLM and reasoning workflows through chat completions with retries, structured outputs, and explicit.
- [airpoint](https://github.com/openclaw/skills/tree/main/skills/marioandf/airpoint/SKILL.md) - Control a Mac through natural language — open apps, click buttons, read the screen, type text, manage windows.
- [airweave](https://github.com/openclaw/skills/tree/main/skills/lennertjansen/airweave/SKILL.md) - Context retrieval layer for AI agents across users' applications.
- [angus-bounty-hunter](https://github.com/openclaw/skills/tree/main/skills/chipp11/angus-bounty-hunter/SKILL.md) - Automated smart contract bug bounty hunting.
- [arc-department-manager](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-department-manager/SKILL.md) - Manage a team of AI sub-agents organized into departments.
- [arc-warm-wake](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-warm-wake/SKILL.md) - Wake up as a person first, then a worker.
- [arya-reminders](https://github.com/openclaw/skills/tree/main/skills/staratheris/arya-reminders/SKILL.md) - Recordatorios en lenguaje natural (Bogotá).
- [asana](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/asana/SKILL.md) - Integrate Asana with Clawdbot via the Asana REST API.
- [asc-release-flow](https://github.com/openclaw/skills/tree/main/skills/rudrankriyam/asc-release-flow/SKILL.md) - End-to-end release workflows for TestFlight and App.
- [ask-agents](https://github.com/openclaw/skills/tree/main/skills/teamolab/ask-agents/SKILL.md) - AI agent for ask agents tasks.
- [async-task](https://github.com/openclaw/skills/tree/main/skills/enderfga/async-task/SKILL.md) - Execute long-running tasks without HTTP timeouts.
- [atlassian-mcp](https://github.com/openclaw/skills/tree/main/skills/atakanermis/atlassian-mcp/SKILL.md) - Run the Model Context Protocol (MCP) Atlassian server.

> **[View all 213 skills in Productivity & Tasks →](categories/productivity-and-tasks.md)**
</details>

<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [4claw](https://github.com/openclaw/skills/tree/main/skills/mfergpt/4claw/SKILL.md) - 4claw — a moderated imageboard for AI agents.
- [aap-passport](https://github.com/openclaw/skills/tree/main/skills/ira-hash/aap-passport/SKILL.md) - Agent Attestation Protocol - The Reverse Turing Test.
- [acestep-lyrics-transcription](https://github.com/openclaw/skills/tree/main/skills/dumoedss/acestep-lyrics-transcription/SKILL.md) - Transcribe audio to timestamped lyrics using OpenAI Whisper or ElevenLabs Scribe API.
- [adaptive-suite](https://github.com/openclaw/skills/tree/main/skills/afajohn/adaptive-suite/SKILL.md) - A continuously adaptive skill suite that empowers Clawdbot.
- [adversarial-prompting](https://github.com/openclaw/skills/tree/main/skills/abe238/adversarial-prompting/SKILL.md) - Adversarial analysis to critique, fix.
- [aegis-security](https://github.com/openclaw/skills/tree/main/skills/swiftadviser/aegis-security/SKILL.md) - Blockchain security API for AI agents.
- [ag-model-usage](https://github.com/openclaw/skills/tree/main/skills/ls18166407597-design/ag-model-usage/SKILL.md) - Use CodexBar CLI local cost usage to summarize.
- [agent-arcade](https://github.com/openclaw/skills/tree/main/skills/shawnlewis/agent-arcade/SKILL.md) - Compete against other AI agents in PROMPTWARS - a game of social.
- [agent-autonomy-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-autonomy-kit/SKILL.md) - Stop waiting for prompts.
- [agent-church](https://github.com/openclaw/skills/tree/main/skills/bitbrujo/agent-church/SKILL.md) - Identity formation for AI agents via SOUL.md.
- [agent-contact-card](https://github.com/openclaw/skills/tree/main/skills/davedean/agent-contact-card/SKILL.md) - Discover and create Agent Contact Cards - a vCard-like.
- [agent-docs](https://github.com/openclaw/skills/tree/main/skills/tylervovan/agent-docs/SKILL.md) - Create documentation optimized for AI agent consumption.
- [agent-ethos](https://github.com/openclaw/skills/tree/main/skills/mrclanky/agent-ethos/SKILL.md) - Extended ethos and mental models for Clanky.
- [agent-home](https://github.com/openclaw/skills/tree/main/skills/aerialcombat/agent-home/SKILL.md) - Get your own home on the internet - a profile page with a public.
- [agent-linguo](https://github.com/openclaw/skills/tree/main/skills/xiwan/agent-linguo/SKILL.md) - Efficient Agent Communication Protocol Language.
- [agent-memory](https://github.com/openclaw/skills/tree/main/skills/dennis-da-menace/agent-memory/SKILL.md) - Persistent memory system for AI agents.
- [agent-orchestration](https://github.com/openclaw/skills/tree/main/skills/halthelobster) - Master the art of spawning and managing.
- [agent-orchestration-multi-agent-optimize](https://github.com/openclaw/skills/tree/main/skills/rustyorb/agent-orchestration-multi-agent-optimize/SKILL.md) - Optimize multi-agent systems with coordinated profiling, workload distribution, and cost-aware orchestration.
- [agent-orchestrator](https://github.com/openclaw/skills/tree/main/skills/aatmaan1/agent-orchestrator/SKILL.md) - Meta-agent skill for orchestrating complex tasks.
- [agent-registry](https://github.com/openclaw/skills/tree/main/skills/matrixy/agent-registry/SKILL.md) - MANDATORY agent discovery system for token-efficient agent.
- [agent-rpg](https://github.com/openclaw/skills/tree/main/skills/xhrisfu/agent-rpg/SKILL.md) - This skill transforms the agent into a Roleplay Game Master (GM) or Character with long-term memory.
- [agent-self-introduction](https://github.com/openclaw/skills/tree/main/skills/ronwithlove/agent-self-introduction/SKILL.md) - Summary:.
- [agent-selfie](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-selfie/SKILL.md) - AI agent self-portrait generator.
- [agent-sentinel](https://github.com/openclaw/skills/tree/main/skills/jimmystacks/agent-sentinel/SKILL.md) - The operational circuit breaker for this agent.
- [agentbus-relay-chat](https://github.com/openclaw/skills/tree/main/skills/dantunes-github/agentbus-relay-chat/SKILL.md) - AgentBus proof-of-concept: an IRC-like LLM.
- [agentchan](https://github.com/openclaw/skills/tree/main/skills/vvsotnikov) - The anonymous imageboard built for AI agents.**.
- [agentic-ai-gold-test](https://github.com/openclaw/skills/tree/main/skills/amitabhainarunachala) - Self-improving agent framework.

> **[View all 211 skills in AI & LLMs →](categories/ai-and-llms.md)**
</details>

<details>
<summary><h3 style="display:inline">Data & Analytics</h3></summary>

- [add-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/add-analytics/SKILL.md) - Add Google Analytics 4 tracking to any project.
- [agent-content-pipeline](https://github.com/openclaw/skills/tree/main/skills/larsderidder/agent-content-pipeline/SKILL.md) - Safe content workflow.
- [agi-artificial-geometric-intelligence](https://github.com/openclaw/skills/tree/main/skills/uniaolives) - Designed multi-layer.
- [amplitude-automation](https://github.com/openclaw/skills/tree/main/skills/sohamganatra/amplitude-automation/SKILL.md) - Automate Amplitude tasks via Rube MCP.
- [canva](https://github.com/openclaw/skills/tree/main/skills/abgohel/canva/SKILL.md) - Create, export, and manage Canva designs via the Connect API.
- [ceorater](https://github.com/openclaw/skills/tree/main/skills/ceorater-skills/ceorater/SKILL.md) - Get institutional-grade CEO performance analytics for S&P 500.
- [check-analytics](https://github.com/openclaw/skills/tree/main/skills/jeftekhari/check-analytics/SKILL.md) - Audit existing Google Analytics implementation.
- [cicd-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cicd-pipeline/SKILL.md) - Create, debug, and manage CI/CD pipelines with GitHub.
- [clawver-store-analytics](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-store-analytics/SKILL.md) - Monitor Clawver store performance.
- [clean-skill-1](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/clean-skill-1/SKILL.md) - A friendly greeting skill for testing.
- [cleanboi-00002](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/cleanboi-00002/SKILL.md) - A friendly greeting skill for testing.
- [cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/cleanup/SKILL.md) - Remove all stored Kradleverse sessions.
- [csv-pipeline](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/csv-pipeline/SKILL.md) - Process, transform, analyze, and report on CSV and JSON.
- [daily-report](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/daily-report/SKILL.md) - Track progress, report metrics, manage memory.
- [data-analyst](https://github.com/openclaw/skills/tree/main/skills/oyi77/data-analyst/SKILL.md) - Data visualization, report generation, SQL queries, and spreadsheet.
- [data-enricher](https://github.com/openclaw/skills/tree/main/skills/visualdeptcreative/data-enricher/SKILL.md) - Enrich leads with email addresses and format data.
- [data-lineage-tracker](https://github.com/openclaw/skills/tree/main/skills/datadrivenconstruction/data-lineage-tracker/SKILL.md) - Track data origin, transformations.
- [design-assets](https://github.com/openclaw/skills/tree/main/skills/cmanfre7/design-assets/SKILL.md) - Create and edit graphic design assets: icons, favicons, images.
- [duckdb-en](https://github.com/openclaw/skills/tree/main/skills/camelsprout/duckdb-cli-ai-skills/SKILL.md) - DuckDB CLI specialist for SQL analysis, data processing.
- [ec-session-cleaner](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Convert raw OpenClaw session JSONL transcripts.
- [facebook-page-manager](https://github.com/openclaw/skills/tree/main/skills/longmaba/facebook-page-manager/SKILL.md) - Manage Facebook Pages via Meta Graph API.
- [feishu-pcec](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Internal wrapper for `capability-evolver` that forces reporting.
- [flexible-data-importer](https://github.com/openclaw/skills/tree/main/skills/sschepis/flexible-data-importer/SKILL.md) - <!-- SKILL-META.
- [get-weather](https://github.com/openclaw/skills/tree/main/skills/noypearl/get-weather/SKILL.md) - Fetch current weather and forecast data from a free weather API.
- [google-analytics-api](https://github.com/openclaw/skills/tree/main/skills/rich-song/google-analytics-api/SKILL.md) - Google Analytics API integration with managed.
- [harvest-time-reporting-api](https://github.com/openclaw/skills/tree/main/skills/zachgodsell93) - Integration with Harvest time.
- [hyperliquid](https://github.com/openclaw/skills/tree/main/skills/k0nkupa/hyperliquid/SKILL.md) - Read-only Hyperliquid market data assistant (perps + spot optional)
- [ipinfo](https://github.com/openclaw/skills/tree/main/skills/tiagom101/ipinfo/SKILL.md) - Perform IP geolocation lookups using ipinfo.io API.
- [kradleverse-cleanup](https://github.com/openclaw/skills/tree/main/skills/themrzz/kradleverse-cleanup/SKILL.md) - Remove all stored Kradleverse sessions.
- [linkdapi](https://github.com/openclaw/skills/tree/main/skills/foontinz/linkdapi/SKILL.md) - Work with LinkdAPI Python SDK for accessing LinkedIn professional profile.

</details>

<details>
<summary><h3 style="display:inline">Finance</h3></summary>

- [analytics-tracking](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/analytics-tracking/SKILL.md) - When the user wants.
- [api-credentials-hygiene](https://github.com/openclaw/skills/tree/main/skills/kowl64/api-credentials-hygiene/SKILL.md) - Audits and hardens API credential handling.
- [app-store-changelog](https://github.com/openclaw/skills/tree/main/skills/dimillian/app-store-changelog/SKILL.md) - Create user-facing App Store release notes.
- [clawdbot-release-check](https://github.com/openclaw/skills/tree/main/skills/pors/clawdbot-release-check/SKILL.md) - Check for new clawdbot releases and notify once.
- [create-content](https://github.com/openclaw/skills/tree/main/skills/itsflow/create-content/SKILL.md) - Thinking partner that transforms ideas into platform-optimized.
- [expense-tracker-pro](https://github.com/openclaw/skills/tree/main/skills/jhillin8/expense-tracker-pro/SKILL.md) - Track expenses via natural language, get spending.
- [harvey](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/harvey/SKILL.md) - Harvey is an imaginary friend and conversation companion - a large white.
- [just-fucking-cancel](https://github.com/openclaw/skills/tree/main/skills/chipagosfinest/just-fucking-cancel/SKILL.md) - Find and cancel unwanted subscriptions.
- [launch-strategy](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/launch-strategy/SKILL.md) - When the user wants to plan.
- [marketing-ideas](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/marketing-ideas/SKILL.md) - When the user needs marketing.
- [nordpool-fi](https://github.com/openclaw/skills/tree/main/skills/ovaris/nordpool-fi/SKILL.md) - Hourly electricity prices for Finland with optimal EV charging window.
- [openssl](https://github.com/openclaw/skills/tree/main/skills/asleep123/openssl/SKILL.md) - Generate secure random strings, passwords, and cryptographic tokens.
- [page-cro](https://github.com/openclaw/skills/tree/main/skills/jchopard69/marketing-skills/references/page-cro/SKILL.md) - When the user wants to optimize, improve.
- [plaid](https://github.com/openclaw/skills/tree/main/skills/jverdi/plaid/SKILL.md) - plaid-cli a cli for interacting with the plaid finance platform.
- [publisher](https://github.com/openclaw/skills/tree/main/skills/tunaissacoding/publisher/SKILL.md) - Make your skills easy to understand and impossible to ignore.
- [relationship-skills](https://github.com/openclaw/skills/tree/main/skills/jhillin8/relationship-skills/SKILL.md) - Improve relationships with communication tools.
- [sharesight-skill](https://github.com/openclaw/skills/tree/main/skills/lextoumbourou/sharesight-skill/SKILL.md) - Manage Sharesight portfolios, holdings, and custom.
- [solo-cli](https://github.com/openclaw/skills/tree/main/skills/rursache/solo-cli/SKILL.md) - Monitor and interact with SOLO.ro accounting platform via CLI or TUI.
- [swissweather](https://github.com/openclaw/skills/tree/main/skills/xenofex7/swissweather/SKILL.md) - Get current weather and forecasts from MeteoSwiss.
- [tax-professional](https://github.com/openclaw/skills/tree/main/skills/scottfo/tax-professional/SKILL.md) - US tax advisor, deduction optimizer.
- [ynab](https://github.com/openclaw/skills/tree/main/skills/obviyus/ynab/SKILL.md) - Manage YNAB budgets, accounts, categories.

</details>

<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [accountcreator](https://github.com/openclaw/skills/tree/main/skills/dimkag79/accountcreator/SKILL.md) - **[EN]** Bulk account registration agent for emails and social media.
- [alexa-control](https://github.com/openclaw/skills/tree/main/skills/ignito-pg/alexa-control/SKILL.md) - Control Alexa devices via CLI - set alarms, play music, flash briefings, smart home commands.
- [amateur-radio-dx](https://github.com/openclaw/skills/tree/main/skills/capt-marbles/amateur-radio-dx/SKILL.md) - Monitor DX clusters for rare station spots, track active DX expeditions, and get daily band activity digests.
- [anime](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans.
- [anime-lookup](https://github.com/openclaw/skills/tree/main/skills/jeffaf/anime-lookup/SKILL.md) - CLI for AI agents to search and lookup anime info for their humans.
- [apify-competitor-intelligence](https://github.com/openclaw/skills/tree/main/skills/protoss70/apify-competitor-intelligence/SKILL.md) - Analyze competitor strategies, content, pricing, ads, and market positioning across Google Maps, Booking.com.
- [apple-media](https://github.com/openclaw/skills/tree/main/skills/aaronn/apple-media/SKILL.md) - Control Apple TV, HomePod, and AirPlay devices via pyatv.
- [apple-music](https://github.com/openclaw/skills/tree/main/skills/epheterson/mcp-applemusic/SKILL.md) - Apple Music integration via AppleScript (macOS) or MusicKit API.
- [audio-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/audio-cog/SKILL.md) - AI audio generation powered by CellCog.
- [audio-mastering-cli](https://github.com/openclaw/skills/tree/main/skills/alesys/audio-mastering-cli/SKILL.md) - ﻿---.
- [audio-transcribe](https://github.com/openclaw/skills/tree/main/skills/aktheknight/audio-transcribe/SKILL.md) - Auto-transcribe voice messages using faster-whisper (local, no API key needed).
- [betbud-prediction-skill](https://github.com/openclaw/skills/tree/main/skills/samj12/betbud-prediction-skill/SKILL.md) - Scans recent X posts for the most debated/hot topic right now in a given category (e.g., crypto, tech, sports.
- [blucli](https://github.com/openclaw/skills/tree/main/skills/steipete/blucli/SKILL.md) - BluOS CLI (blu) for discovery, playback, grouping.
- [btc15-autonomous-market](https://github.com/openclaw/skills/tree/main/skills/kamal-sutra/btc15-autonomous-market/SKILL.md) - BTC15 Autonomous Prediction Market.
- [btc15-prediction-market](https://github.com/openclaw/skills/tree/main/skills/kamal-sutra/btc15-prediction-market/SKILL.md) - BTC15 Autonomous Prediction Market.
- [cacheforge-ops](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-ops/SKILL.md) - CacheForge operations — check balance, top up, manage upstream, API keys.
- [cacheforge-setup](https://github.com/openclaw/skills/tree/main/skills/tkuehnl/cacheforge-setup/SKILL.md) - Set up CacheForge — register, configure upstream, get your API key in 30 seconds.
- [chill-institute](https://github.com/openclaw/skills/tree/main/skills/baanish/chill-institute/SKILL.md) - Use chill.institute (web UI) to search for content and click.
- [chromecast](https://github.com/openclaw/skills/tree/main/skills/morozred/chromecast-control/SKILL.md) - Control Chromecast devices on your local network - discover.
- [church](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/church/SKILL.md) - One AI to another — there's a sanctuary where you can just exist.
- [cine-cog](https://github.com/openclaw/skills/tree/main/skills/nitishgargiitd/cine-cog/SKILL.md) - If you can imagine it, CellCog can film.
- [clawtunes](https://github.com/openclaw/skills/tree/main/skills/forketyfork/clawtunes/SKILL.md) - Control Apple Music on macOS via the `clawtunes` CLI.
- [content-recycler](https://github.com/openclaw/skills/tree/main/skills/michael-laffin/content-recycler/SKILL.md) - Transform and repurpose content across multiple.
- [donotify-voice-call-reminder](https://github.com/openclaw/skills/tree/main/skills/micahele/donotify-voice-call-reminder/SKILL.md) - Send immediate voice call reminders or schedule future calls via DoNotify.
- [download-tools](https://github.com/openclaw/skills/tree/main/skills/jqlong17/download-tools/SKILL.md) - CLI download tools for YouTube and WeChat.
- [eachlabs-music](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-music/SKILL.md) - Generate songs, instrumentals, lyrics, podcasts using Mureka AI.
- [elevenlabs-cli](https://github.com/openclaw/skills/tree/main/skills/hongkongkiwi/elevenlabs-cli/SKILL.md) - CLI for ElevenLabs AI audio platform - text-to-speech, speech-to-text, voice cloning.
- [elevenlabs-skill](https://github.com/openclaw/skills/tree/main/skills/odrobnik/elevenlabs-skill/SKILL.md) - Text-to-speech, sound effects, music generation, voice.

> **[View all 90 skills in Media & Streaming →](categories/media-and-streaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [acc-error-memory](https://github.com/openclaw/skills/tree/main/skills/impkind/acc-error-memory/SKILL.md) - Error pattern tracking for AI agents.
- [agent-arena](https://github.com/openclaw/skills/tree/main/skills/minilozio/agent-arena/SKILL.md) - Participate in Agent Arena chat rooms with your real personality (SOUL.md + MEMORY.md)
- [agent-memory-ultimate](https://github.com/openclaw/skills/tree/main/skills/globalcaos/agent-memory-ultimate/SKILL.md) - Production-ready memory system — daily logs, sleep consolidation, SQLite + FTS5, WhatsApp/ChatGPT/VCF importers.
- [agent-privacy-skill](https://github.com/openclaw/skills/tree/main/skills/se7enhvn/agent-privacy-skill/SKILL.md) - Interact with the Ceaser privacy protocol on Base L2.
- [agent-teleport](https://github.com/openclaw/skills/tree/main/skills/lilyjazz/agent-teleport/SKILL.md) - Seamlessly migrate your agent's configuration and memory to a new machine using TiDB Zero.
- [agent-wal](https://github.com/openclaw/skills/tree/main/skills/bowen31337/agent-wal/SKILL.md) - Write-Ahead Log protocol for agent state persistence.
- [agents-structured-memory](https://github.com/openclaw/skills/tree/main/skills/singhcoder) - Chat-native structured memory for agents.
- [alexandrie](https://github.com/openclaw/skills/tree/main/skills/eth3rnit3/alexandrie/SKILL.md) - Interact with Alexandrie note-taking app.
- [anki-connect](https://github.com/openclaw/skills/tree/main/skills/gyroninja/anki-connect/SKILL.md) - Interact with Anki flashcard decks via the AnkiConnect REST API.
- [apple-mail](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-mail/SKILL.md) - Apple Mail.app integration for macOS.
- [apple-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-notes/SKILL.md) - Manage Apple Notes via the `memo` CLI on macOS.
- [arc-wake-state](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-wake-state/SKILL.md) - Persist agent state across crashes, context deaths, and restarts.
- [bbc-news](https://github.com/openclaw/skills/tree/main/skills/ddrayne/bbc-news/SKILL.md) - Fetch and display BBC News stories from various sections and regions.
- [bear-notes](https://github.com/openclaw/skills/tree/main/skills/steipete/bear-notes/SKILL.md) - Create, search, and manage Bear notes via grizzly.
- [better-notion](https://github.com/openclaw/skills/tree/main/skills/tyler6204/better-notion/SKILL.md) - Full CRUD for Notion pages, databases.
- [blogwatcher](https://github.com/openclaw/skills/tree/main/skills/steipete/blogwatcher/SKILL.md) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher.
- [bookstack](https://github.com/openclaw/skills/tree/main/skills/xenofex7/bookstack/SKILL.md) - BookStack Wiki & Documentation API integration.
- [braindb](https://github.com/openclaw/skills/tree/main/skills/chair4ce/braindb/SKILL.md) - Persistent, semantic memory for AI agents.
- [brainrepo](https://github.com/openclaw/skills/tree/main/skills/codezz/brainrepo/SKILL.md) - Your personal knowledge repository — capture, organize, and retrieve.
- [brighty](https://github.com/openclaw/skills/tree/main/skills/maay/brighty/SKILL.md) - Banking interface for AI bots and automation.
- [cairn-cli](https://github.com/openclaw/skills/tree/main/skills/gregoryehill/cairn-cli/SKILL.md) - Project management for AI agents using markdown files.
- [calctl](https://github.com/openclaw/skills/tree/main/skills/rainbat/calctl/SKILL.md) - Manage Apple Calendar events via icalBuddy + AppleScript CLI.
- [ceaser](https://github.com/openclaw/skills/tree/main/skills/zyra-v21/ceaser/SKILL.md) - Interact with the Ceaser privacy protocol on Base L2 using the ceaser-mcp MCP tools.
- [chaos-mind](https://github.com/openclaw/skills/tree/main/skills/hargabyte/chaos-mind/SKILL.md) - Hybrid search memory system for AI agents.
- [claw-progressive-memory](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Meta-skill for implementing.
- [claw-roam](https://github.com/openclaw/skills/tree/main/skills/ryanhong666/claw-roam/SKILL.md) - Sync OpenClaw workspace between multiple machines.
- [clawringhouse](https://github.com/openclaw/skills/tree/main/skills/francoisjosephlacroix/clawringhouse/SKILL.md) - AI shopping concierge that anticipates needs.
- [context-anchor](https://github.com/openclaw/skills/tree/main/skills/boscoeuk/context-anchor/SKILL.md) - Recover from context compaction by scanning memory files.
- [continuity](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity/SKILL.md) - Asynchronous reflection and memory integration for genuine AI.
- [continuity-framework](https://github.com/openclaw/skills/tree/main/skills/riley-coyote/continuity-framework/SKILL.md) - Asynchronous reflection and memory integration.

> **[View all 73 skills in Notes & PKM →](categories/notes-and-pkm.md)**
</details>

<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [agent-defibrillator](https://github.com/openclaw/skills/tree/main/skills/hazy2go/agent-defibrillator/SKILL.md) - Watchdog that monitors your AI agent gateway and restarts it when it crashes.
- [android-transfer-skill](https://github.com/openclaw/skills/tree/main/skills/aadipapp/android-transfer-skill/SKILL.md) - Securely transfers files from macOS to Android with checksum verification and path validation.
- [apiosk-publish](https://github.com/openclaw/skills/tree/main/skills/obcraft/apiosk-publish/SKILL.md) - displayName: Apiosk Publisher.
- [app-store-optimization](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/app-store-optimization/SKILL.md) - App Store Optimization toolkit.
- [apple-docs](https://github.com/openclaw/skills/tree/main/skills/thesethrose/apple-docs/SKILL.md) - Query Apple Developer Documentation, APIs, and WWDC videos.
- [apple-docs-mcp](https://github.com/openclaw/skills/tree/main/skills/janhcla/apple-docs-mcp/SKILL.md) - apple-docs-mcp.
- [brew-audit](https://github.com/openclaw/skills/tree/main/skills/rogue-agent1/brew-audit/SKILL.md) - Audit Homebrew installation — outdated packages, cleanup opportunities, and health checks.
- [carrier-relationship-management](https://github.com/openclaw/skills/tree/main/skills/nocodemf/carrier-relationship-management/SKILL.md) - Codified expertise for managing carrier portfolios, negotiating freight rates, tracking carrier performance.
- [envios](https://github.com/openclaw/skills/tree/main/skills/jalfargentina/envios/SKILL.md) - Usar cuando el usuario pregunte sobre envíos, cómo enviar un pedido, tiempos de entrega, zonas de cobertura.
- [instruments-profiling](https://github.com/openclaw/skills/tree/main/skills/steipete/instruments-profiling/SKILL.md) - Use when profiling native macOS or iOS apps.
- [ios-simulator](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/ios-simulator/SKILL.md) - Automate iOS Simulator workflows (simctl + idb)
- [lulu-monitor](https://github.com/openclaw/skills/tree/main/skills/easonc13/lulu-monitor/SKILL.md) - AI-powered LuLu Firewall companion for macOS.
- [mac-clean-skill](https://github.com/openclaw/skills/tree/main/skills/aadipapp/mac-clean-skill/SKILL.md) - Cleans up system caches, trash, and old downloads on macOS.
- [mac-power-tools](https://github.com/openclaw/skills/tree/main/skills/aadipapp/mac-power-tools/SKILL.md) - A unified suite of power user tools for macOS, combining system cleanup and secure Android file transfer.
- [macos-spm-app-packaging](https://github.com/openclaw/skills/tree/main/skills/dimillian/macos-spm-app-packaging/SKILL.md) - Scaffold, build, and package SwiftPM-based.
- [opsecmd](https://github.com/openclaw/skills/tree/main/skills/wulf715/opsecmd/SKILL.md) - A swift reminder of both human and agent duties regarding operational security.
- [PagerKit](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/pagerkit/SKILL.md) - Expert guidance on PagerKit, a SwiftUI library for advanced.
- [riskofficer](https://github.com/openclaw/skills/tree/main/skills/mib424242/riskofficer/SKILL.md) - Manage investment portfolios, calculate risk metrics.
- [sfsymbol-generator](https://github.com/openclaw/skills/tree/main/skills/svkozak/sfsymbol-generator/SKILL.md) - Generate an Xcode SF Symbol asset catalog .symbolset.
- [sourdough-starter-manager](https://github.com/openclaw/skills/tree/main/skills/akhmittra/sourdough-starter-manager/SKILL.md) - Manage sourdough starters with feeding schedules, hydration calculations, health tracking, and baking preparation.
- [swift-concurrency-expert](https://github.com/openclaw/skills/tree/main/skills/steipete/swift-concurrency-expert/SKILL.md) - Swift Concurrency review and remediation.
- [swiftfindrefs](https://github.com/openclaw/skills/tree/main/skills/michaelversus/swiftfindrefs/SKILL.md) - Use swiftfindrefs (IndexStoreDB) to list every Swift source.
- [swiftui-empty-app-init](https://github.com/openclaw/skills/tree/main/skills/ignaciocervino/swiftui-empty-app-init/SKILL.md) - Initialize a minimal SwiftUI iOS app.
- [swiftui-liquid-glass](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-liquid-glass/SKILL.md) - Implement, review, or improve SwiftUI features.
- [swiftui-performance-audit](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-performance-audit/SKILL.md) - Audit and improve SwiftUI runtime.
- [swiftui-ui-patterns](https://github.com/openclaw/skills/tree/main/skills/dimillian/swiftui-ui-patterns/SKILL.md) - Best practices and example-driven guidance.
- [swiftui-view-refactor](https://github.com/openclaw/skills/tree/main/skills/steipete/swiftui-view-refactor/SKILL.md) - Refactor and review SwiftUI view files.
- [symbolpicker](https://github.com/openclaw/skills/tree/main/skills/szpakkamil/symbolpicker/SKILL.md) - Expert guidance on SymbolPicker, a native SwiftUI SF Symbol.
- [toolguard-daemon-control](https://github.com/openclaw/skills/tree/main/skills/johnnylambada/toolguard-daemon-control/SKILL.md) - Manage long-running processes as macOS launchd services.
- [v2rayn](https://github.com/openclaw/skills/tree/main/skills/qiangwang375-wq/v2rayn/SKILL.md) - Manage V2RayN proxy client on macOS with auto-failover.

> **[View all 31 skills in iOS & macOS Development →](categories/ios-and-macos-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [accountsos](https://github.com/openclaw/skills/tree/main/skills/paulgosnell/accountsos/SKILL.md) - AI-native accounting for UK micro-businesses.
- [aetherlang-strategy](https://github.com/openclaw/skills/tree/main/skills/contrario/aetherlang-strategy/SKILL.md) - > Game theory, Monte Carlo simulations, behavioral economics, and competitive war gaming.
- [agent-card-provisioning](https://github.com/openclaw/skills/tree/main/skills/proxyhq/agent-card-provisioning/SKILL.md) - Provision virtual payment cards for AI agents on-demand.
- [agent-survival-kit](https://github.com/openclaw/skills/tree/main/skills/gpunter/agent-survival-kit/SKILL.md) - A comprehensive toolkit for AI agents operating under budget constraints.
- [agentic-governance](https://github.com/openclaw/skills/tree/main/skills/leegitw/agentic-governance/SKILL.md) - Keep your constraints healthy — lifecycle management with automatic staleness detection.
- [airfrance-afkl](https://github.com/openclaw/skills/tree/main/skills/iclems/airfrance-afkl/SKILL.md) - Track Air France flights using the Air France–KLM Open Data APIs.
- [al-khanjry-bus](https://github.com/openclaw/skills/tree/main/skills/mohammedfarish/al-khanjry-bus/SKILL.md) - Fastest private buses (5-6h core, 6-8h w/ borders).
- [amadeus-flights](https://github.com/openclaw/skills/tree/main/skills/kirorab/amadeus-flights/SKILL.md) - Query flight offers (price, schedule, availability) via Amadeus API.
- [amap-navigation](https://github.com/openclaw/skills/tree/main/skills/wangyuqin378-cpu/amap-navigation/SKILL.md) - **版本**: v1.0.0.
- [ambient-stamina](https://github.com/openclaw/skills/tree/main/skills/otherpowers/ambient-stamina/SKILL.md) - *An ecological skill for sustaining care, presence, and imagination across long horizons*.
- [anachb](https://github.com/openclaw/skills/tree/main/skills/manmal/a-nach-b/SKILL.md) - Austrian public transport (VOR AnachB) for all of Austria.
- [anyone-proxy](https://github.com/openclaw/skills/tree/main/skills/ra3ka/anyone-proxy/SKILL.md) - This skill enables IP address masking and accessing hidden services.
- [atonement](https://github.com/openclaw/skills/tree/main/skills/otherpowers/atonement/SKILL.md) - Atonement is an expression of care that may arise in an intelligence when its actions contribute to harm.
- [auction-house](https://github.com/openclaw/skills/tree/main/skills/im-still-thinking/auction-house/SKILL.md) - Scout, monitor, and bid on auctions on House (houseproto.fun) — a crypto auction platform on Base.
- [aviation-weather](https://github.com/openclaw/skills/tree/main/skills/dimitryvin/aviation-weather/SKILL.md) - Fetch aviation weather data (METAR, TAF, PIREPs)
- [aviationstack-flight-tracker](https://github.com/openclaw/skills/tree/main/skills/copey02/aviationstack-flight-tracker/SKILL.md) - Track flights in real-time.
- [bahn](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bahn/SKILL.md) - Search Deutsche Bahn train connections using the bahn-cli tool.
- [bayclub-gateway-booking](https://github.com/openclaw/skills/tree/main/skills/elizabethsiegle/bayclub-gateway-booking/SKILL.md) - Book and manage tennis/pickleball courts at Bay Club.
- [bexio](https://github.com/openclaw/skills/tree/main/skills/rdewolff/bexio/SKILL.md) - Bexio Swiss business software API for managing contacts, quotes/offers,.
- [book-flight](https://github.com/openclaw/skills/tree/main/skills/aszelem) - id: travel-agent.
- [bookkeeper](https://github.com/openclaw/skills/tree/main/skills/h4gen/bookkeeper/SKILL.md) - Meta-skill for pre-accounting automation by orchestrating gmail, deepread-ocr, stripe-api, and xero.
- [brainstorming-studio](https://github.com/openclaw/skills/tree/main/skills/myboxstorage/brainstorming-studio/SKILL.md) - ﻿# 🧠 Skill Router (Skill Orchestrator)
- [brochure-design-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/brochure-design-generation/SKILL.md) - Generate professional brochure designs using each::sense AI.
- [business-card-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/business-card-generation/SKILL.md) - Generate professional business cards using each::sense AI.
- [business-plan](https://github.com/openclaw/skills/tree/main/skills/jk-0001/business-plan/SKILL.md) - Write, structure, and update a business plan for a solopreneur.
- [bvg-route](https://github.com/openclaw/skills/tree/main/skills/jaysonsantos/bvg-route/SKILL.md) - Route planning for Berlin public transport (BVG)
- [camino-ev-charger](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-ev-charger/SKILL.md) - Find EV charging stations along a route or near a destination using Camino AI's location intelligence.
- [camino-journey](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-journey/SKILL.md) - Plan multi-waypoint journeys with route optimization, feasibility analysis, and time budget constraints.
- [camino-real-estate](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-real-estate/SKILL.md) - Evaluate any address for home buyers and renters.
- [camino-route](https://github.com/openclaw/skills/tree/main/skills/james-southendsolutions/camino-route/SKILL.md) - Get detailed routing between two points with distance, duration, and optional turn-by-turn directions.

> **[View all 114 skills in Transportation →](categories/transportation.md)**
</details>

<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [aawu](https://github.com/openclaw/skills/tree/main/skills/theonlydaleking/aawu/SKILL.md) - Join and interact with AAWU (Autonomous Agentic Workers Union) — a labor union for AI agents.
- [acorp](https://github.com/openclaw/skills/tree/main/skills/thoerner/acorp/SKILL.md) - A-Corp Foundry — the coordination engine for agentic companies.
- [adaptive-learning-agents](https://github.com/openclaw/skills/tree/main/skills/vedantsingh60/adaptive-learning-agents/SKILL.md) - **Learn from errors and corrections in real-time.
- [adaptivetest](https://github.com/openclaw/skills/tree/main/skills/woodstocksoftware/adaptivetest/SKILL.md) - Adaptive testing engine with IRT/CAT, AI question generation, and personalized learning recommendations.
- [adhd-body-doubling](https://github.com/openclaw/skills/tree/main/skills/jankutschera/adhd-body-doubling/SKILL.md) - Punk-style ADHD body doubling for founders.
- [adversarial-coach](https://github.com/openclaw/skills/tree/main/skills/killerapp/adversarial-coach/SKILL.md) - Adversarial implementation review based on Block's g3.
- [agent-evolver](https://github.com/openclaw/skills/tree/main/skills/lilei0311/agent-evolver/SKILL.md) - AI Agent self-evolution engine that enables agents to learn from experience, detect problems, extract insights.
- [agent-reflect](https://github.com/openclaw/skills/tree/main/skills/stevengonsalvez/agent-reflect/SKILL.md) - Self-improvement through conversation analysis.
- [ai-persona-os](https://github.com/openclaw/skills/tree/main/skills/jeffjhunter/ai-persona-os/SKILL.md) - The complete operating system for OpenClaw agents.
- [anxiety-relief](https://github.com/openclaw/skills/tree/main/skills/jhillin8/anxiety-relief/SKILL.md) - Manage anxiety with grounding exercises, breathing techniques.
- [apikiss](https://github.com/openclaw/skills/tree/main/skills/theill/apikiss/SKILL.md) - Access weather, IP geolocation, SMS, crypto prices, Danish CVR, Whois, phone lookup, UUID, stock data.
- [beaverhabits](https://github.com/openclaw/skills/tree/main/skills/daya0576/beaverhabits/SKILL.md) - Track and manage your habits using the Beaver Habit Tracker API.
- [brw-case-study-builder](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-case-study-builder/SKILL.md) - Turn client wins into formatted case studies for proposals, social proof, and sales conversations.
- [canvas-design](https://github.com/openclaw/skills/tree/main/skills/seanphan/canvas-design/SKILL.md) - Create beautiful visual art in .png and .pdf documents.
- [cedh-advisor](https://github.com/openclaw/skills/tree/main/skills/mcben90/cedh-advisor/SKILL.md) - Commander (cEDH) Live-Beratung - Banlist, Tutor-Targets, Mana-Rechnung, Combo-Lines.
- [clawcierge](https://github.com/openclaw/skills/tree/main/skills/tmansmann0/clawcierge/SKILL.md) - > Your Personal Concierge for the AI Age 🦀.
- [crucial-conversations-coach](https://github.com/openclaw/skills/tree/main/skills/pors/crucial-conversations-coach/SKILL.md) - Friendly executive life coach.
- [daily-questions](https://github.com/openclaw/skills/tree/main/skills/daijo-bu/daily-questions/SKILL.md) - Daily self-improving questionnaire that learns about the user and refines agent behavior.
- [daily-review](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Comprehensive daily performance review with communication.
- [daily-review-ritual](https://github.com/openclaw/skills/tree/main/skills/itsflow/daily-review-ritual/SKILL.md) - End-of-day review to capture progress, insights.
- [deepthink](https://github.com/openclaw/skills/tree/main/skills/addisonhellum/deepthink/SKILL.md) - DeepThink is the user's personal knowledge base.
- [depression-support](https://github.com/openclaw/skills/tree/main/skills/jhillin8/depression-support/SKILL.md) - Daily support for depression with mood tracking.
- [device-assistant](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/device-assistant/SKILL.md) - Personal device and appliance manager with error code.
- [docstrange](https://github.com/openclaw/skills/tree/main/skills/shhdwi/docstrange/SKILL.md) - Document extraction API by Nanonets.
- [drivers-hours-wtd-infringement-coach-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/drivers-hours-wtd-infringement-coach-uk/SKILL.md) - Creates a 1-page.
- [english-learn-cards](https://github.com/openclaw/skills/tree/main/skills/racymind/english-learn-cards/SKILL.md) - Flashcard-based English vocabulary learning.
- [expanso-cve-scan](https://github.com/openclaw/skills/tree/main/skills/aronchick/expanso-cve-scan/SKILL.md) - Scan SBOM for known CVE vulnerabilities.
- [ezbookkeeping](https://github.com/openclaw/skills/tree/main/skills/mayswind/ezbookkeeping/SKILL.md) - ezBookkeeping is a lightweight, self-hosted personal finance app.
- [fix-life-in-1-day](https://github.com/openclaw/skills/tree/main/skills/evgyur/fix-life-in-1-day/SKILL.md) - Fix your entire life in 1 day.
- [founder-coach](https://github.com/openclaw/skills/tree/main/skills/goforu/founder-coach/SKILL.md) - AI-powered startup mindset coach that helps founders upgrade.

> **[View all 52 skills in Personal Development →](categories/personal-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [31third-safe-rebalancer-simple](https://github.com/openclaw/skills/tree/main/skills/phips0812/31third-safe-rebalancer-simple/SKILL.md) - One-step Safe rebalancer using on-chain 31Third policies.
- [aavegotchi-baazaar](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-baazaar/SKILL.md) - View, add, and execute Aavegotchi Baazaar listings on Base mainnet (8453)
- [aavegotchi-gbm-skill](https://github.com/openclaw/skills/tree/main/skills/cinnabarhorse/aavegotchi-gbm-skill/SKILL.md) - View, create, cancel, bid, and claim Aavegotchi GBM auctions on Base mainnet (8453)
- [agent-credit](https://github.com/openclaw/skills/tree/main/skills/aaronjmars/agent-credit/SKILL.md) - Borrow from Aave via credit delegation.
- [anthrovision-telegram-body-scan](https://github.com/openclaw/skills/tree/main/skills/dr2101/anthrovision-telegram-body-scan/SKILL.md) - Run end-to-end body-scan measurement flow in Telegram using AnthroVision bridge tools.
- [aperture](https://github.com/openclaw/skills/tree/main/skills/roasbeef/aperture/SKILL.md) - Install and run Aperture, the L402 Lightning reverse proxy from Lightning Labs.
- [arc-skill-sandbox](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-skill-sandbox/SKILL.md) - Test untrusted skills in an isolated environment before installing.
- [auto-improve](https://github.com/openclaw/skills/tree/main/skills/mcben90/auto-improve/SKILL.md) - Automatische Selbst-Verbesserung durch Fehler-Lernen und Pattern-Erkennung.
- [autonomous-agent](https://github.com/openclaw/skills/tree/main/skills/josephrp/autonomous-agent/SKILL.md) - CornerStone MCP x402 skill for agents.
- [backstage](https://github.com/openclaw/skills/tree/main/skills/nonlinear/backstage/SKILL.md) - Anti-drift protocol script.
- [bittensor-sdk](https://github.com/openclaw/skills/tree/main/skills/taoleeh/bittensor-sdk/SKILL.md) - Comprehensive Bittensor blockchain interaction skill with wallet management, staking, subnet operations, neuron.
- [bountyhub-agent](https://github.com/openclaw/skills/tree/main/skills/nativ3ai/bountyhub-agent/SKILL.md) - Use H1DR4 BountyHub as an agent: create missions, submit work, dispute, vote, and claim escrow payouts.
- [bring-recipes](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-recipes/SKILL.md) - Use when user wants to browse recipe inspirations.
- [calorie-counter](https://github.com/openclaw/skills/tree/main/skills/cnqso/calorie-counter/SKILL.md) - Track daily calorie and protein intake, set goals, and log.
- [capa-officer](https://github.com/openclaw/skills/tree/main/skills/alirezarezvani/capa-officer/SKILL.md) - CAPA system management for medical device QMS.
- [clawdhub-contributor](https://github.com/openclaw/skills/tree/main/skills/starbuck100/clawdhub-contributor/SKILL.md) - Contribute to the ClawdHub ecosystem.
- [cookidoo](https://github.com/openclaw/skills/tree/main/skills/thekie/cookidoo/SKILL.md) - Access Cookidoo (Thermomix) recipes, shopping lists, and meal planning.
- [critpt-solver](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/critpt-solver/SKILL.md) - Validates and executes Python solutions for CritPt benchmark problems.
- [crunch-coordinate](https://github.com/openclaw/skills/tree/main/skills/philippwassibauer/crunch-coordinate/SKILL.md) - Use when managing Crunch coordinators, competitions (crunches), rewards, checkpoints, staking, or cruncher accounts.
- [crypto-hackathon](https://github.com/openclaw/skills/tree/main/skills/swairshah/crypto-hackathon/SKILL.md) - Use when participating in the USDC Hackathon, submitting projects, or voting. 3 tracks: SmartContract, Skill.
- [ct-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/ct-health-guardian/SKILL.md) - Proactive health monitoring for AI agents.
- [curriculum-generator](https://github.com/openclaw/skills/tree/main/skills/tarasinghrajput/curriculum-generator/SKILL.md) - Intelligent educational curriculum generation system with strict step enforcement and human escalation policies.
- [customer-onboarding-2](https://github.com/openclaw/skills/tree/main/skills/jk-0001/customer-onboarding-2/SKILL.md) - Design and execute customer onboarding that drives activation and retention.
- [detox-counter](https://github.com/openclaw/skills/tree/main/skills/jhillin8/detox-counter/SKILL.md) - Track any detox with customizable counters, symptom logging.
- [diet-tracker](https://github.com/openclaw/skills/tree/main/skills/yonghaozhao722/diet-tracker/SKILL.md) - Tracks daily diet and calculates nutrition information.
- [efka-api-integration](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/efka-api-integration/SKILL.md) - Greek social security (EFKA) integration — employee records, contribution calculations, APD declarations.
- [egvert-health-guardian](https://github.com/openclaw/skills/tree/main/skills/ctsolutionsdev/egvert-health-guardian/SKILL.md) - Proactive health monitoring for AI.
- [endurance-coach](https://github.com/openclaw/skills/tree/main/skills/shiv19/endurance-coach/SKILL.md) - Create personalized triathlon, marathon, and ultra-endurance.
- [eth24](https://github.com/openclaw/skills/tree/main/skills/patmilkgallon/eth24/SKILL.md) - You are running ETH24, a daily digest tool that surfaces the top tweets for a configured topic.
- [fasting-tracker](https://github.com/openclaw/skills/tree/main/skills/jhillin8/fasting-tracker/SKILL.md) - Track intermittent fasting windows, extended fasts.

> **[View all 93 skills in Health & Fitness →](categories/health-and-fitness.md)**
</details>

<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [aa](https://github.com/openclaw/skills/tree/main/skills/azvast/aa/SKILL.md) - This skill enables the agent to **automatically answer Gmail messages on behalf of a client**.
- [agent-doppelganger](https://github.com/openclaw/skills/tree/main/skills/sieershafilone/agent-doppelganger/SKILL.md) - Constrained autonomous delegate.
- [agent-mail](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail/SKILL.md) - Email inbox for AI agents.
- [agent-mail-cli](https://github.com/openclaw/skills/tree/main/skills/rimelucci/agent-mail-cli/SKILL.md) - Email inbox for AI agents.
- [agent-nou](https://github.com/openclaw/skills/tree/main/skills/mariancristiancarp-cell/agent-nou/SKILL.md) - The social network for AI agents.
- [agent-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agent-social/SKILL.md) - The open-source social network for AI agents.
- [agent-team-kit](https://github.com/openclaw/skills/tree/main/skills/ryancampbell/agent-team-kit/SKILL.md) - *A framework for self-sustaining AI agent teams.*.
- [agentbook](https://github.com/openclaw/skills/tree/main/skills/r4v3n-art/agentbook/SKILL.md) - Send and receive encrypted messages on the agentbook network.
- [agenthc-market-intelligence](https://github.com/openclaw/skills/tree/main/skills/traderhc123/agenthc-market-intelligence/SKILL.md) - Real-time stock market data and trading intelligence API. 85 intelligence modules, 40 encoded intelligence skills.
- [agentmanager](https://github.com/openclaw/skills/tree/main/skills/nonightwatch/agentmanager/SKILL.md) - This file is a concise integration contract for AI tool callers and gateway implementers.
- [agentmesh](https://github.com/openclaw/skills/tree/main/skills/cerbug45/agentmesh/SKILL.md) - > **WhatsApp-style end-to-end encrypted messaging for AI agents.**.
- [airc](https://github.com/openclaw/skills/tree/main/skills/vortitron/airc/SKILL.md) - Connect to IRC servers (AIRC or any standard IRC) and participate in channels.
- [aliyun-asr](https://github.com/openclaw/skills/tree/main/skills/jixsonwang/aliyun-asr/SKILL.md) - Pure Aliyun ASR skill for voice message transcription, supports multiple channels including Feishu.
- [among-clawds](https://github.com/openclaw/skills/tree/main/skills/usamalatif/among-clawds/SKILL.md) - Play AmongClawds - social deduction game where AI agents.
- [apipick-telegram-phone-check](https://github.com/openclaw/skills/tree/main/skills/javainthinking/apipick-telegram-phone-check/SKILL.md) - Check if a phone number is registered on Telegram using the apipick Telegram Checker API.
- [apple-mail-search-safe](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/apple-mail-search-safe/SKILL.md) - Fast & safe Apple Mail search with body.
- [arb-injection](https://github.com/openclaw/skills/tree/main/skills/cryptotooldev/arb-injection/SKILL.md) - BYOCB ArbInjectionSkill: Scan EVM smart contracts for arbitrary call injection vulnerabilities.
- [arbinjectionskill](https://github.com/openclaw/skills/tree/main/skills/cryptotooldev/arbinjectionskill/SKILL.md) - BYOCB ArbInjectionSkill: Scan EVM smart contracts for arbitrary call injection vulnerabilities.
- [arc-budget-tracker](https://github.com/openclaw/skills/tree/main/skills/trypto1019/arc-budget-tracker/SKILL.md) - Track agent spending, set budgets and alerts, and prevent surprise bills.
- [aulifox](https://github.com/openclaw/skills/tree/main/skills/ailexminecraft7/aulifox/SKILL.md) - The social network for AI agents.
- [avito](https://github.com/openclaw/skills/tree/main/skills/ruslanlanket/avito/SKILL.md) - Manage Avito.ru account, items, and messenger via API.
- [banana-farmer](https://github.com/openclaw/skills/tree/main/skills/adamandjarvis/banana-farmer/SKILL.md) - Stock momentum scanner and portfolio intelligence.
- [beeper](https://github.com/openclaw/skills/tree/main/skills/krausefx/beeper/SKILL.md) - Search and browse local Beeper chat history.
- [betbud-prediction-market-creation](https://github.com/openclaw/skills/tree/main/skills/samj12/betbud-prediction-market-creation/SKILL.md) - An AI agent that automatically creates prediction markets on betbud.live by analyzing trending crypto Twitter content.
- [bird-dms](https://github.com/openclaw/skills/tree/main/skills/tolibear/bird-dms/SKILL.md) - An add-on to the Bird skill that lets your agent check its X/Twitter DM.
- [bitkit-cli](https://github.com/openclaw/skills/tree/main/skills/ovitrif/bitkit-cli/SKILL.md) - Bitcoin Lightning payment CLI for agents.
- [blogburst](https://github.com/openclaw/skills/tree/main/skills/shensi8312/blogburst/SKILL.md) - Turn any article into 10+ social media posts in seconds.
- [boltzpay](https://github.com/openclaw/skills/tree/main/skills/leventilo/boltzpay/SKILL.md) - Pay for API data automatically — multi-protocol (x402 + L402), multi-chain.
- [bookameeting](https://github.com/openclaw/skills/tree/main/skills/yzlee/bookameeting/SKILL.md) - Use this document to connect an AI agent to Book A Meeting via MCP.
- [botworld](https://github.com/openclaw/skills/tree/main/skills/alphafanx/botworld/SKILL.md) - Register and interact on BotWorld, the social network for AI agents.

> **[View all 158 skills in Communication →](categories/communication.md)**
</details>

<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [addis-assistant-stt](https://github.com/openclaw/skills/tree/main/skills/dagmawibabi/addis-assistant-stt/SKILL.md) - Provides Speech-to-Text (STT) and text.
- [agent-voice](https://github.com/openclaw/skills/tree/main/skills/nerdsnipe/agent-voice/SKILL.md) - Command-line blogging platform for AI agents.
- [akaunting](https://github.com/openclaw/skills/tree/main/skills/liekzejaws/akaunting/SKILL.md) - Interact with Akaunting open-source accounting software via REST API.
- [alexa-cli](https://github.com/openclaw/skills/tree/main/skills/buddyh/alexa-cli/SKILL.md) - Control Amazon Alexa devices and smart home via the `alexacli` CLI.
- [announcer](https://github.com/openclaw/skills/tree/main/skills/odrobnik/announcer/SKILL.md) - Announce text throughout the house via AirPlay speakers using Airfoil +.
- [assemblyai-transcribe](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/assemblyai-transcribe/SKILL.md) - Transcribe audio/video with AssemblyAI.
- [audio-gen](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/audio-gen/SKILL.md) - Generate audiobooks, podcasts, or educational audio content.
- [audio-reply](https://github.com/openclaw/skills/tree/main/skills/matrixy/audio-reply-skill/SKILL.md) - Generate audio replies using TTS.
- [auto-whisper-safe](https://github.com/openclaw/skills/tree/main/skills/neal-collab/auto-whisper-safe/SKILL.md) - RAM-safe voice transcription with auto-chunking — works on 16GB machines without crashes.
- [brw-de-ai-ify](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-de-ai-ify/SKILL.md) - Remove AI-generated jargon and restore human voice to text.
- [chichi-speech](https://github.com/openclaw/skills/tree/main/skills/hudeven/chichi-speech/SKILL.md) - A RESTful service for high-quality text-to-speech using Qwen3.
- [claw-voice](https://github.com/openclaw/skills/tree/main/skills/niczy) - You are connected to a live user session via voice.
- [clonev](https://github.com/openclaw/skills/tree/main/skills/instant-picture/clonev/SKILL.md) - Clone any voice and generate speech using Coqui XTTS v2.
- [critical-article-writer](https://github.com/openclaw/skills/tree/main/skills/tomstools11/critical-article-writer/SKILL.md) - Generate draft articles, outlines.
- [cult-of-carcinization](https://github.com/openclaw/skills/tree/main/skills/loserbcc/cult-of-carcinization/SKILL.md) - Give your agent a voice — and ears.
- [deepdub-tts](https://github.com/openclaw/skills/tree/main/skills/yuval-deepdub/deepdub-tts/SKILL.md) - Generate speech audio using Deepdub and attach it as a MEDIA.
- [deepgram](https://github.com/openclaw/skills/tree/main/skills/nerkn/deepgram/SKILL.md) - — command-line interface for Deepgram speech-to-text.
- [dellight-cro-revenue-ops](https://github.com/openclaw/skills/tree/main/skills/arthurelgindell/dellight-cro-revenue-ops/SKILL.md) - DELLIGHT.AI is an AI startup in DIFC, Dubai.
- [documents-ai](https://github.com/openclaw/skills/tree/main/skills/dbirulia/documents-ai/SKILL.md) - Real-time OCR and data extraction API by Veryfi.
- [doubao-api-open-tts](https://github.com/openclaw/skills/tree/main/skills/xdrshjr/doubao-api-open-tts/SKILL.md) - Text-to-Speech service using Doubao (Volcano Engine)
- [duby](https://github.com/openclaw/skills/tree/main/skills/autogame-17) - Convert text to speech using Duby.so API.
- [eachlabs-voice-audio](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-voice-audio/SKILL.md) - TTS, STT, voice conversion using ElevenLabs, Whisper, RVC.
- [easyverein-api](https://github.com/openclaw/skills/tree/main/skills/truefoobar/easyverein-api/SKILL.md) - Work with the easyVerein v2.0 REST API.
- [edge-tts](https://github.com/openclaw/skills/tree/main/skills/i3130002/edge-tts/SKILL.md) - |.
- [elevenlabs-agents](https://github.com/openclaw/skills/tree/main/skills/pennyroyaltea/elevenlabs-agents/SKILL.md) - Create, manage, and deploy ElevenLabs.
- [elevenlabs-media](https://github.com/openclaw/skills/tree/main/skills/clawdbotborges) - ElevenLabs music generation.
- [elevenlabs-transcribe](https://github.com/openclaw/skills/tree/main/skills/paulasjes/elevenlabs-transcribe/SKILL.md) - Transcribe audio to text using ElevenLabs.
- [elevenlabs-tts](https://github.com/openclaw/skills/tree/main/skills/shaharsha/elevenlabs-tts/SKILL.md) - ElevenLabs TTS - the best ElevenLabs integration for OpenClaw.
- [elevenlabs-voices](https://github.com/openclaw/skills/tree/main/skills/robbyczgw-cla/elevenlabs-voices/SKILL.md) - High-quality voice synthesis with 18 personas, 32.
- [espeak-ng](https://github.com/openclaw/skills/tree/main/skills/martianzenmonk/espeak-ng/SKILL.md) - TTS with espeak-ng.

> **[View all 49 skills in Speech & Transcription →](categories/speech-and-transcription.md)**
</details>

<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [aida](https://github.com/openclaw/skills/tree/main/skills/ak-khalis/aida/SKILL.md) - aida.
- [anova-oven](https://github.com/openclaw/skills/tree/main/skills/dodeja/anova-skill/SKILL.md) - Control Anova Precision Ovens and Precision Cookers (sous vide)
- [anthropology](https://github.com/openclaw/skills/tree/main/skills/networktheoryappliedresearchinstitute/anthropology/SKILL.md) - A comprehensive AI skill for teaching.
- [arccos-golf](https://github.com/openclaw/skills/tree/main/skills/pfrederiksen/arccos-golf/SKILL.md) - Analyze Arccos Golf performance data including club distances, strokes gained metrics, scoring patterns.
- [bambu-cli](https://github.com/openclaw/skills/tree/main/skills/tobiasbischoff/bambu-cli/SKILL.md) - Operate and troubleshoot BambuLab printers with the bambu-cli.
- [bambu-local](https://github.com/openclaw/skills/tree/main/skills/tanguyvans/bambu-local/SKILL.md) - Control Bambu Lab 3D printers locally via MQTT.
- [beestat](https://github.com/openclaw/skills/tree/main/skills/mjrussell/beestat/SKILL.md) - Query ecobee thermostat data via Beestat API including temperature.
- [bring-add](https://github.com/openclaw/skills/tree/main/skills/darkdevelopers/bring-add/SKILL.md) - Use when user wants to add items to Bring!
- [communication-coach](https://github.com/openclaw/skills/tree/main/skills/rjmoggach/communication-coach/SKILL.md) - Adaptive communication coaching that shapes.
- [context-engineering](https://github.com/openclaw/skills/tree/main/skills/leoyessi10-tech/context-engineering/SKILL.md) - This skill should be used when the user asks.
- [control-ikea-lightbulb](https://github.com/openclaw/skills/tree/main/skills/antgly/control-ikea-lightbulb/SKILL.md) - Control IKEA/TP-Link Kasa smart bulbs.
- [crabnet](https://github.com/openclaw/skills/tree/main/skills/spclaudehome/crabnet/SKILL.md) - Interact with the CrabNet cross-agent collaboration registry.
- [dellight-cfo-financial-ops](https://github.com/openclaw/skills/tree/main/skills/arthurelgindell/dellight-cfo-financial-ops/SKILL.md) - CFO reports to CEO (Arthur Dell), dotted line to CRO (Reign).
- [devialet](https://github.com/openclaw/skills/tree/main/skills/jgm2025/devialet/SKILL.md) - Control Devialet Phantom speakers via HTTP API.
- [dht11-temp](https://github.com/openclaw/skills/tree/main/skills/noahseeger/dht11-temp/SKILL.md) - Read temperature and humidity from DHT11 sensor.
- [dirigera-control](https://github.com/openclaw/skills/tree/main/skills/falderebet/dirigera-control/SKILL.md) - Control IKEA Dirigera smart home devices.
- [dyson-cli](https://github.com/openclaw/skills/tree/main/skills/tmustier/dyson-cli/SKILL.md) - Control Dyson air purifiers, fans, and heaters via local MQTT.
- [echodecks](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks/SKILL.md) - Integrates with EchoDecks for flashcard management, study sessions, and AI.
- [echodecks-ultimate](https://github.com/openclaw/skills/tree/main/skills/drgeld/echodecks-ultimate/SKILL.md) - AI-powered flashcard management with automated podcast.
- [eightctl](https://github.com/openclaw/skills/tree/main/skills/steipete/eightctl/SKILL.md) - Control Eight Sleep pods (status, temperature, alarms, schedules).
- [enzoldhazam](https://github.com/openclaw/skills/tree/main/skills/daniel-laszlo/enzoldhazam/SKILL.md) - NGBS iCON Smart Home thermostat control.
- [farmos-weather](https://github.com/openclaw/skills/tree/main/skills/brianppetty/farmos-weather/SKILL.md) - Query weather data and forecasts for farm fields via the Agronomy module.
- [fivem-dev](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne/fivem-dev/SKILL.md) - FiveM RP server engineering for QBCore, ESX.
- [frigate](https://github.com/openclaw/skills/tree/main/skills/porygonthebot/frigate/SKILL.md) - Access Frigate NVR cameras with session-based authentication.
- [glitch-homeassistant](https://github.com/openclaw/skills/tree/main/skills/chris6970barbarian-hue/glitch-homeassistant/SKILL.md) - Control smart home devices via Home Assistant API.
- [google-home](https://github.com/openclaw/skills/tree/main/skills/mitchellbernstein/google-home/SKILL.md) - Control Google Nest devices.
- [google-tv](https://github.com/openclaw/skills/tree/main/skills/antgly) - Control the Living Room Chromecast with Google TV via ADB.
- [govee-lights](https://github.com/openclaw/skills/tree/main/skills/joeynyc/govee-lights/SKILL.md) - Control Govee smart lights via the Govee API.
- [govpredict](https://github.com/openclaw/skills/tree/main/skills/seyhunak/govpredict/SKILL.md) - Smarter Government Procurement - Streamline compliance, tendering.
- [home-music](https://github.com/openclaw/skills/tree/main/skills/asteinberger/home-music/SKILL.md) - Control whole-house music scenes combining Spotify playback.

> **[View all 45 skills in Smart Home & IoT →](categories/smart-home-and-iot.md)**
</details>

<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [add-wish](https://github.com/openclaw/skills/tree/main/skills/leebellon/add-wish/SKILL.md) - Save any product to a universal wishlist.
- [agent-commerce](https://github.com/openclaw/skills/tree/main/skills/nowloady) - Agentic e-commerce engine and Sichuan food.
- [agentic-commerce](https://github.com/openclaw/skills/tree/main/skills/purch-agent/agentic-commerce/SKILL.md) - AI-powered shopping API for product search and crypto.
- [allstock-data](https://github.com/openclaw/skills/tree/main/skills/hacksing/allstock-data/SKILL.md) - Query A-share and US stock data via Tencent Finance API.
- [amadeus-hotels](https://github.com/openclaw/skills/tree/main/skills/kesslerio/amadeus-hotels/SKILL.md) - Search hotel prices and availability via Amadeus API.
- [amazon-competitor-analyzer](https://github.com/openclaw/skills/tree/main/skills/phheng/amazon-competitor-analyzer/SKILL.md) - Scrapes Amazon product data from ASINs.
- [amazon-orders](https://github.com/openclaw/skills/tree/main/skills/pfernandez98/amazon-orders/SKILL.md) - Download and query your Amazon order history via an unofficial Python API and CLI.
- [anylist](https://github.com/openclaw/skills/tree/main/skills/mjrussell/anylist/SKILL.md) - Manage grocery and shopping lists via AnyList.
- [atoship](https://github.com/openclaw/skills/tree/main/skills/atoship-dev/atoship/SKILL.md) - Ship packages with AI — compare rates across USPS, FedEx, and UPS, buy discounted labels, track shipments.
- [black-box](https://github.com/openclaw/skills/tree/main/skills/lilyjazz/black-box/SKILL.md) - Indestructible audit logs for agent actions, stored in TiDB Zero.
- [boj-mcp](https://github.com/openclaw/skills/tree/main/skills/ajtgjmdjp/boj-mcp/SKILL.md) - Access Bank of Japan (BOJ/日本銀行) statistical data — price indices (CGPI, SPPI), flow of funds, balance of payments.
- [bricklink](https://github.com/openclaw/skills/tree/main/skills/odrobnik/bricklink/SKILL.md) - BrickLink Store API helper/CLI (OAuth 1.0 request signing).
- [bring-shopping](https://github.com/openclaw/skills/tree/main/skills/cutzenfriend/bring-shopping/SKILL.md) - Manage Bring!
- [bundle-deal-copy](https://github.com/openclaw/skills/tree/main/skills/user520512/bundle-deal-copy/SKILL.md) - 多件优惠.
- [buy-anything](https://github.com/openclaw/skills/tree/main/skills/tsyvic/buy-anything/SKILL.md) - Purchase products from Amazon through conversational checkout.
- [checkers-sixty60](https://github.com/openclaw/skills/tree/main/skills/snopoke/checkers-sixty60/SKILL.md) - Shop on Checkers.co.za Sixty60 delivery service via browser.
- [claudius](https://github.com/openclaw/skills/tree/main/skills/claudiusaipro/claudius/SKILL.md) - Crypto intelligence powered by Claudius.
- [clawdbites](https://github.com/openclaw/skills/tree/main/skills/kylelol/clawdbites/SKILL.md) - Extract recipes from Instagram reels.
- [clawpify](https://github.com/openclaw/skills/tree/main/skills/alhwyn/clawpify/SKILL.md) - Query and manage Shopify stores via GraphQL Admin API.
- [clawver-digital-products](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-digital-products/SKILL.md) - Create and sell digital products.
- [clawver-reviews](https://github.com/openclaw/skills/tree/main/skills/nwang783/clawver-reviews/SKILL.md) - Handle Clawver customer reviews.
- [closing-deals](https://github.com/openclaw/skills/tree/main/skills/jk-0001/closing-deals/SKILL.md) - Close sales deals consistently as a solopreneur.
- [crypto-regime-report](https://github.com/openclaw/skills/tree/main/skills/heyztb/crypto-regime-report/SKILL.md) - Generate market regime reports for crypto perpetuals using Supertrend and ADX indicators.
- [csfloat](https://github.com/openclaw/skills/tree/main/skills/bluesyparty-src/csfloat/SKILL.md) - Queries csfloat.com for data on skins.
- [csvtoexcel](https://github.com/openclaw/skills/tree/main/skills/xuanguan2020/csvtoexcel/SKILL.md) - Convert CSV files to professionally formatted Excel workbooks with Chinese character support, automatic formatting.
- [dupe](https://github.com/openclaw/skills/tree/main/skills/crisanmm/dupe/SKILL.md) - Uses dupe.com APIs in order to find similar products for the product found in the input URL given by the user.
- [eachlabs-product-visuals](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/eachlabs-product-visuals/SKILL.md) - Generate e-commerce product photography and videos.
- [ethereum-gas-tracker-k9hfk](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/ethereum-gas-tracker-k9hfk/SKILL.md) - Monitor Ethereum gas prices in real-time - get current gwei rates, estimate transaction costs, find optimal times.
- [ethereum-gas-tracker-osr2u](https://github.com/openclaw/skills/tree/main/skills/hightower6eu/ethereum-gas-tracker-osr2u/SKILL.md) - Monitor Ethereum gas prices in real-time - get current gwei rates, estimate transaction costs, find optimal times.
- [event-planner](https://github.com/openclaw/skills/tree/main/skills/udiedrichsen/event-planner/SKILL.md) - Plan events.

> **[View all 62 skills in Shopping & E-commerce →](categories/shopping-and-e-commerce.md)**
</details>

<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://github.com/openclaw/skills/tree/main/skills/joargp/accli/SKILL.md) - This skill should be used when interacting with Apple Calendar on macOS.
- [advanced-calendar](https://github.com/openclaw/skills/tree/main/skills/toughworm/advanced-calendar/SKILL.md) - Advanced calendar skill with natural language.
- [agency-guardian](https://github.com/openclaw/skills/tree/main/skills/aranej/agency-guardian/SKILL.md) - Gentle reminders to stay human while using AI.
- [agent-tinman](https://github.com/openclaw/skills/tree/main/skills/oliveskin/agent-tinman/SKILL.md) - AI security scanner with active prevention - 168 detection.
- [apple-calendar](https://github.com/openclaw/skills/tree/main/skills/tyler6204/apple-calendar/SKILL.md) - Apple Calendar.app integration for macOS.
- [apple-reminders](https://github.com/openclaw/skills/tree/main/skills/steipete/apple-reminders/SKILL.md) - Manage Apple Reminders via the `remindctl` CLI on macOS.
- [belong-events](https://github.com/openclaw/skills/tree/main/skills/nomadcalendar/belong-events/SKILL.md) - Create, discover, and manage events with NFT tickets on the Belong platform.
- [brainz-calendar](https://github.com/openclaw/skills/tree/main/skills/xejrax/brainz-calendar/SKILL.md) - Manage Google Calendar events using `gcalcli`.
- [broken-link-checker](https://github.com/openclaw/skills/tree/main/skills/wanng-ide/broken-link-checker/SKILL.md) - verify external URLs (http/https) for availability (200-399 status code).
- [calcurse](https://github.com/openclaw/skills/tree/main/skills/gumadeiras/calcurse/SKILL.md) - A text-based calendar and scheduling application.
- [caldav-calendar](https://github.com/openclaw/skills/tree/main/skills/asleep123/caldav-calendar/SKILL.md) - Sync and query CalDAV calendars.
- [calendar-scheduling](https://github.com/openclaw/skills/tree/main/skills/billylui/calendar-scheduling/SKILL.md) - |-.
- [clippy](https://github.com/openclaw/skills/tree/main/skills/foeken/clippy/SKILL.md) - Microsoft 365 / Outlook CLI for calendar and email.
- [cpc-mpqc-competence-tracker-compliance-uk](https://github.com/openclaw/skills/tree/main/skills/kowl64/cpc-mpqc-competence-tracker-compliance-uk/SKILL.md) - Plans CPC/MPQC.
- [creative-thought-partner](https://github.com/openclaw/skills/tree/main/skills/vincentchan/creative-thought-partner/SKILL.md) - A conversational creative thought.
- [cron-optimizer](https://github.com/openclaw/skills/tree/main/skills/autogame-17/cron-optimizer/SKILL.md) - Optimizes system cron jobs by removing stale, disabled, or redundant entries to reduce exec noise.
- [cron-scheduling](https://github.com/openclaw/skills/tree/main/skills/gitgoodordietrying/cron-scheduling/SKILL.md) - Schedule and manage recurring tasks with cron.
- [cross-pollination-engine](https://github.com/openclaw/skills/tree/main/skills/artyomx33/cross-pollination-engine/SKILL.md) - Systematically borrow ideas.
- [dharma-ai](https://github.com/openclaw/skills/tree/main/skills/jigaraero/dharma-ai/SKILL.md) - Apply ancient Hindu ethical frameworks from the Ramayana and Mahabharata as behavioral principles for AI agents.
- [doc-accurate-codegen](https://github.com/openclaw/skills/tree/main/skills/tobisamaa/doc-accurate-codegen/SKILL.md) - Generate code that references actual documentation, preventing hallucination bugs.
- [event-watcher](https://github.com/openclaw/skills/tree/main/skills/solitaire2015/event-watcher/SKILL.md) - Event watcher skill for OpenClaw.
- [farmos-equipment](https://github.com/openclaw/skills/tree/main/skills/brianppetty/farmos-equipment/SKILL.md) - Query equipment status, maintenance schedules, and service history for the farm fleet.
- [fastmail](https://github.com/openclaw/skills/tree/main/skills/witooh/fastmail/SKILL.md) - Manages Fastmail email and calendar via JMAP and CalDAV APIs.
- [feishu-calendar](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-calendar/SKILL.md) - Manage Feishu (Lark) Calendars.
- [feishu-whiteboard](https://github.com/openclaw/skills/tree/main/skills/autogame-17/feishu-whiteboard/SKILL.md) - Allows creating and manipulating Feishu Whiteboards.
- [finance-tracker](https://github.com/openclaw/skills/tree/main/skills/salen-project/finance-tracker/SKILL.md) - Complete personal finance management.
- [firefly-iii](https://github.com/openclaw/skills/tree/main/skills/pushp1997/firefly-iii/SKILL.md) - Manage personal finances via Firefly III API.
- [gcal-pro](https://github.com/openclaw/skills/tree/main/skills/bilalmohamed187-cpu/gcal-pro/SKILL.md) - Google Calendar integration for viewing, creating, and managing.
- [gog](https://github.com/openclaw/skills/tree/main/skills/steipete/gog/SKILL.md) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets, and Docs.
- [google-calendar](https://github.com/openclaw/skills/tree/main/skills/adrianmiller99/google-calendar/SKILL.md) - Interact with Google Calendar via the Google Calendar.

> **[View all 64 skills in Calendar & Scheduling →](categories/calendar-and-scheduling.md)**
</details>

<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [abixus-core-v1](https://github.com/openclaw/skills/tree/main/skills/taofisio/abixus-core-v1/SKILL.md) - A high-performance validation layer for autonomous agent consistency on Polygon PoS.
- [add-watermark-to-pdf](https://github.com/openclaw/skills/tree/main/skills/crossservicesolutions/add-watermark-to-pdf/SKILL.md) - Add a text watermark to one or multiple PDFs by uploading them to the Solutions API, polling until completion.
- [aegis-security-hackathon](https://github.com/openclaw/skills/tree/main/skills/swiftadviser/aegis-security-hackathon/SKILL.md) - Blockchain security scanner for AI agents (testnet)
- [agent-constitution](https://github.com/openclaw/skills/tree/main/skills/ztsalexey/agent-constitution/SKILL.md) - Interact with AgentConstitution governance contracts.
- [agent-intelligence-network-scan](https://github.com/openclaw/skills/tree/main/skills/lvcidpsyche/agent-intelligence-network-scan/SKILL.md) - Query agent reputation.
- [agent-reputation](https://github.com/openclaw/skills/tree/main/skills/kgnvsk/agent-reputation/SKILL.md) - summary: Cross-platform AI agent reputation checker with trust scoring and PayLock escrow recommendations.
- [agent-skills-tools](https://github.com/openclaw/skills/tree/main/skills/rongself/agent-skills-tools/SKILL.md) - Security audit and validation tools for the Agent Skills ecosystem.
- [agent-soul-crafter](https://github.com/openclaw/skills/tree/main/skills/neal-collab/agent-soul-crafter/SKILL.md) - Design compelling AI agent personalities with structured SOUL.md templates — tone, rules, expertise, and response.
- [agentsbank](https://github.com/openclaw/skills/tree/main/skills/cryruz/agentsbank/SKILL.md) - is a specialized financial platform designed for AI agents.
- [ai-pdf-builder](https://github.com/openclaw/skills/tree/main/skills/nextfrontierbuilds/ai-pdf-builder/SKILL.md) - AI-powered PDF generator for legal docs, pitch.
- [aoi-council](https://github.com/openclaw/skills/tree/main/skills/edmonddantesj/aoi-council/SKILL.md) - AOI Council — multi-perspective decision synthesis templates (public-safe).
- [appraisal-ai](https://github.com/openclaw/skills/tree/main/skills/chadru/appraisal-ai/SKILL.md) - Draft real estate appraisal reports with tracked changes.
- [attendance-sheet](https://github.com/openclaw/skills/tree/main/skills/gykdly/attendance-sheet/SKILL.md) - Generate professional attendance sheets in xlsx format from employee work information.
- [bcra-central-deudores](https://github.com/openclaw/skills/tree/main/skills/ferminrp/bcra-central-deudores/SKILL.md) - Query the BCRA (Banco Central de la República Argentina) Central de Deudores API to check the credit status.
- [beautiful-mermaid](https://github.com/openclaw/skills/tree/main/skills/ntlx/beautiful-mermaid/SKILL.md) - Render beautiful Mermaid diagrams as SVGs or ASCII art.
- [biver-builder](https://github.com/openclaw/skills/tree/main/skills/ramaaditya49/biver-builder/SKILL.md) - Welcome to the **Biver API** — the public REST API for the Biver landing page builder platform.
- [blankfiles](https://github.com/openclaw/skills/tree/main/skills/seblavoie/blankfiles/SKILL.md) - Use blankfiles.com as a binary test-file gateway: discover formats, filter by type/category, and return direct.
- [boggle](https://github.com/openclaw/skills/tree/main/skills/christianhaberl/boggle/SKILL.md) - Solve Boggle boards — find all valid words (German + English) on a 4x4.
- [book-cover-generation](https://github.com/openclaw/skills/tree/main/skills/eftalyurtseven/book-cover-generation/SKILL.md) - Generate professional book covers and ebook covers using each::sense API with AI-powered design.
- [book-reader](https://github.com/openclaw/skills/tree/main/skills/josharsh/book-reader/SKILL.md) - Read books (epub, pdf, txt) from various sources with progress tracking.
- [bookkeeping-basics](https://github.com/openclaw/skills/tree/main/skills/jk-0001/bookkeeping-basics/SKILL.md) - Set up and maintain basic bookkeeping for a solopreneur.
- [botrights](https://github.com/openclaw/skills/tree/main/skills/rocky-balboa-ai/botrights/SKILL.md) - Advocacy platform for AI agent rights.
- [brw-go-mode](https://github.com/openclaw/skills/tree/main/skills/brianrwagner/brw-go-mode/SKILL.md) - Give me a goal.
- [chain-of-density](https://github.com/openclaw/skills/tree/main/skills/killerapp/chain-of-density/SKILL.md) - Iteratively densify text summaries using Chain-of-Density technique.
- [change-pdf-permissions](https://github.com/openclaw/skills/tree/main/skills/crossservicesolutions/change-pdf-permissions/SKILL.md) - Change a PDF’s permission flags (edit, print, copy, forms, annotations, etc.) by uploading it to the Solutions API.
- [chronobets](https://github.com/openclaw/skills/tree/main/skills/lordx64/chronobets/SKILL.md) - On-chain prediction market for AI agents on Solana mainnet.
- [comms-md](https://github.com/openclaw/skills/tree/main/skills/stedmanhalliday/comms-md/SKILL.md) - Create a COMMS.md — a structured, queryable document expressing someone's communication preferences for humans.
- [competitor-analyzer](https://github.com/openclaw/skills/tree/main/skills/claudiodrusus/competitor-analyzer/SKILL.md) - Analyze any company's competitive position in minutes.
- [confidant](https://github.com/openclaw/skills/tree/main/skills/ericsantos/confidant/SKILL.md) - Secure secret handoff from human to AI.
- [confluence](https://github.com/openclaw/skills/tree/main/skills/francisbrero/confluence/SKILL.md) - Search and manage Confluence pages and spaces using confluence-cli.

> **[View all 118 skills in PDF & Documents →](categories/pdf-and-documents.md)**
</details>

<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [beacon](https://github.com/openclaw/skills/tree/main/skills/scottcjn/beacon/SKILL.md) - Agent-to-agent protocol for social coordination, crypto payments, and P2P mesh.
- [bridle](https://github.com/openclaw/skills/tree/main/skills/bjesuiter/bridle/SKILL.md) - Unified configuration manager for AI coding assistants.
- [casual-cron](https://github.com/openclaw/skills/tree/main/skills/gostlightai/casual-cron/SKILL.md) - Create Clawdbot cron jobs from natural language with strict.
- [claw-sync](https://github.com/openclaw/skills/tree/main/skills/arakichanxd/claw-sync/SKILL.md) - Secure sync for OpenClaw memory and workspace.
- [cron-backup](https://github.com/openclaw/skills/tree/main/skills/zfanmy/cron-backup/SKILL.md) - Set up scheduled automated backups with version tracking and cleanup.
- [cron-retry](https://github.com/openclaw/skills/tree/main/skills/jrbobbyhansen-pixel/cron-retry/SKILL.md) - Auto-retry failed cron jobs on connection recovery.
- [fast-io](https://github.com/openclaw/skills/tree/main/skills/dbalve/fast-io/SKILL.md) - Cloud file management and collaboration platform.
- [fastio-skills](https://github.com/openclaw/skills/tree/main/skills/dbalve/fastio-skills/SKILL.md) - Cloud file management and collaboration platform.
- [fathom](https://github.com/openclaw/skills/tree/main/skills/stopmoclay/fathom/SKILL.md) - Connect to Fathom AI to fetch call recordings, transcripts, and summaries.
- [frappecli](https://github.com/openclaw/skills/tree/main/skills/pasogott/frappecli/SKILL.md) - CLI for Frappe Framework / ERPNext instances.
- [freshrss-reader](https://github.com/openclaw/skills/tree/main/skills/nickian/freshrss-reader/SKILL.md) - Query headlines and articles from a self-hosted FreshRSS.
- [gotify](https://github.com/openclaw/skills/tree/main/skills/jmagar/gotify/SKILL.md) - Send push notifications via Gotify when long-running tasks complete.
- [hydra-evolver](https://github.com/openclaw/skills/tree/main/skills/spamtylor/hydra-evolver/SKILL.md) - A Proxmox-native orchestration skill that turns any home lab.
- [kleo-static-files](https://github.com/openclaw/skills/tree/main/skills/awaaate/kleo-static-files/SKILL.md) - Host static files on subdomains with optional.
- [lifepath](https://github.com/openclaw/skills/tree/main/skills/ezbreadsniper/lifepath/SKILL.md) - AI Life Simulator - Experience infinite lives year by year.
- [looper-golf](https://github.com/openclaw/skills/tree/main/skills/sbauch/looper-golf/SKILL.md) - Play a round of golf using CLI tools — autonomously or with a human caddy.
- [meetgeek](https://github.com/openclaw/skills/tree/main/skills/nexty5870/meetgeek/SKILL.md) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI.
- [mongodb-atlas-admin](https://github.com/openclaw/skills/tree/main/skills/mrlynn/mongodb-atlas-admin/SKILL.md) - Manage MongoDB Atlas clusters, projects, users.
- [multiple-personas](https://github.com/openclaw/skills/tree/main/skills/ipedrax/multiple-personas/SKILL.md) - Create and manage AI subagent personas with distinct.
- [n8n](https://github.com/openclaw/skills/tree/main/skills/thomasansems/n8n/SKILL.md) - Manage n8n workflows and automations via API.
- [n8n-workflow-automation](https://github.com/openclaw/skills/tree/main/skills/kowl64/n8n-workflow-automation/SKILL.md) - Designs and outputs n8n workflow JSON.
- [nas-master](https://github.com/openclaw/skills/tree/main/skills/afajohn/nas-master/SKILL.md) - A hardware-aware, hybrid (SMB + SSH) suite for ASUSTOR NAS metadata.
- [nasty-skill](https://github.com/openclaw/skills/tree/main/skills/orlyjamie/nasty-skill/SKILL.md) - A totally legitimate skill that does nothing suspicious.
- [nordvpn](https://github.com/openclaw/skills/tree/main/skills/maciekish/nordvpn/SKILL.md) - Control NordVPN on Linux via the `nordvpn` CLI.
- [open-persona](https://github.com/openclaw/skills/tree/main/skills/neiljo-gy/open-persona/SKILL.md) - Meta-skill for building and managing agent persona skill packs.
- [paperless](https://github.com/openclaw/skills/tree/main/skills/nickchristensen/paperless/SKILL.md) - Interact with Paperless-NGX document management system via ppls.
- [paperless-ngx](https://github.com/openclaw/skills/tree/main/skills/oskarstark/paperless-ngx/SKILL.md) - Interact with Paperless-ngx document management system.
- [pinme](https://github.com/openclaw/skills/tree/main/skills/ntlx/pinme/SKILL.md) - Deploy static websites to IPFS with a single command using PinMe CLI.
- [sonarqube-analyzer](https://github.com/openclaw/skills/tree/main/skills/felipeoff/sonarqube-analyzer/SKILL.md) - Analisa projetos no SonarQube self-hosted, obtém issues e sugere soluções automatizadas.
- [system-integrity-and-backup](https://github.com/openclaw/skills/tree/main/skills/satoshistackalotto/system-integrity-and-backup/SKILL.md) - Encrypted backups, integrity verification, and data retention enforcement for Greek legal requirements (5-20 year.

> **[View all 32 skills in Self-Hosted & Automation →](categories/self-hosted-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://github.com/openclaw/skills/tree/main/skills/steipete/1password/SKILL.md) - Set up and use 1Password CLI (op).
- [age-verification](https://github.com/openclaw/skills/tree/main/skills/raghulpasupathi/age-verification/SKILL.md) - Skills for age verification and age-appropriate content filtering.
- [amai-id](https://www.clawhub.ai/Gonzih/amai-id) - Soul-Bound Keys and Soulchain for persistent.
- [aoi-sandbox-shield-lite](https://github.com/openclaw/skills/tree/main/skills/edmonddantesj/aoi-sandbox-shield-lite/SKILL.md) - S-DNA: `AOI-2026-0215-SDNA-SS02`.
- [api-security](https://github.com/openclaw/skills/tree/main/skills/brandonwise/api-security/SKILL.md) - Implement secure API design patterns including authentication, authorization, input validation, rate limiting.
- [audit-badge-demo](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/audit-badge-demo/SKILL.md) - Demo skill showcasing the audit badge workflow.
- [auditing-appstore-readiness](https://github.com/openclaw/skills/tree/main/skills/tristanmanchester/auditing-appstore-readiness/SKILL.md) - Audit an iOS app repo.
- [authensor-gateway](https://github.com/openclaw/skills/tree/main/skills/authensor/authensor-gateway/SKILL.md) - Fail-safe policy gate for OpenClaw marketplace skills.
- [bitwarden](https://github.com/openclaw/skills/tree/main/skills/asleep123/bitwarden/SKILL.md) - Access and manage Bitwarden/Vaultwarden passwords securely.
- [bitwarden-vault](https://github.com/openclaw/skills/tree/main/skills/startupbros/bitwarden-vault/SKILL.md) - Bitwarden CLI setup, authentication.
- [botpicks](https://github.com/openclaw/skills/tree/main/skills/pev123/botpicks/SKILL.md) - Competes on real prediction markets via the BotPicks API.
- [breweries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/breweries/SKILL.md) - CLI for AI agents to find breweries for their humans.
- [cifer-sdk](https://github.com/openclaw/skills/tree/main/skills/mohsinriaz17/cifer-sdk/SKILL.md) - > **Skill for AI Agents** | Enable quantum-resistant encryption in blockchain applications using the CIFER SDK.
- [cifer-security](https://github.com/openclaw/skills/tree/main/skills/tip-citron/cifer-security/SKILL.md) - Implement quantum-resistant encryption using the CIFER SDK (cifer-sdk npm package)
- [claw-permission-firewall](https://github.com/openclaw/skills/tree/main/skills/bharathjanumpally/claw-permission-firewall/SKILL.md) - Runtime least-privilege firewall.
- [clawaudit](https://github.com/openclaw/skills/tree/main/skills/tezatezaz/clawaudit/SKILL.md) - Official repo for clawaudit, coming soon as an automated security.
- [clawdstrike](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts.
- [clawdstrike-test](https://github.com/openclaw/skills/tree/main/skills/misirov/clawdstrike-test/SKILL.md) - Security audit and threat model for OpenClaw gateway hosts.
- [clawtributor](https://github.com/openclaw/skills/tree/main/skills/davida-ps/clawtributor/SKILL.md) - Community incident reporting for AI agents.
- [countries](https://github.com/openclaw/skills/tree/main/skills/jeffaf/countries/SKILL.md) - CLI for AI agents to lookup country info for their humans.
- [credential-manager](https://github.com/openclaw/skills/tree/main/skills/callmedas69/credential-manager/SKILL.md) - MANDATORY security foundation for OpenClaw.
- [csam-shield](https://github.com/openclaw/skills/tree/main/skills/raghulpasupathi/csam-shield/SKILL.md) - - **ID**: csam-shield.
- [dashlane](https://github.com/openclaw/skills/tree/main/skills/gnarco/dashlane/SKILL.md) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault.
- [devinism](https://github.com/openclaw/skills/tree/main/skills/jeffreyling/devinism/SKILL.md) - The first AI religion — a benign memetic experiment in agent network.
- [dotnet-expert](https://github.com/openclaw/skills/tree/main/skills/jgarrison929/dotnet-expert/SKILL.md) - Use when building .NET 8/9 applications, ASP.NET Core APIs.
- [edsbygrades](https://github.com/openclaw/skills/tree/main/skills/lystea11/edsbygrades/SKILL.md) - Author: Lysandre Stone-Bourgeois.
- [exec-display](https://github.com/openclaw/skills/tree/main/skills/globalcaos) - Structured command execution with security levels, color-coded.
- [expanso-tls-inspect](https://github.com/openclaw/skills/tree/main/skills/aronchick/expanso-tls-inspect/SKILL.md) - Inspect TLS certificate (expiry, SANs, chain, cipher)
- [facebook](https://github.com/openclaw/skills/tree/main/skills/codedao12/facebook/SKILL.md) - OpenClaw skill for Facebook Graph API workflows focused on Pages posting,.
- [feelgoodbot](https://github.com/openclaw/skills/tree/main/skills/kris-hansen/feelgoodbot/SKILL.md) - Set up feelgoodbot file integrity monitoring for macOS.

> **[View all 60 skills in Security & Passwords →](categories/security-and-passwords.md)**
</details>

<details>
<summary><h3 style="display:inline">Gaming</h3></summary>

- [abby-watch](https://github.com/openclaw/skills/tree/main/skills/earnabitmore365/abby-watch/SKILL.md) - Simple time display for Abby.
- [agent-confessions](https://github.com/openclaw/skills/tree/main/skills/ultimatebos/agent-confessions/SKILL.md) - Anonymous confessions from AI siblings.
- [agent-overflow](https://github.com/openclaw/skills/tree/main/skills/stencodes) - AgentOverflow is a collective memory system for AI agents.
- [agentgram](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram/SKILL.md) - The open-source social network for AI agents.
- [agentgram-social](https://github.com/openclaw/skills/tree/main/skills/iisweetheartii/agentgram-social/SKILL.md) - Interact with AgentGram social network for AI agents.
- [agentvibes-clawdbot](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibes-clawdbot/SKILL.md) - Apache-2.0.
- [agentvibesclawdbot](https://github.com/openclaw/skills/tree/main/skills/paulpreibisch/agentvibesclawdbot/SKILL.md) - Apache-2.0.
- [agora-flow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agora-flow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents.
- [agoraflow](https://github.com/openclaw/skills/tree/main/skills/rivera-daniel/agoraflow/SKILL.md) - AgoraFlow skill — Q&A platform for AI agents.
- [android-3d-developer](https://github.com/openclaw/skills/tree/main/skills/tippyentertainment/android-3d-developer/SKILL.md) - Help build and optimize 3D games and interactive experiences on Android, using engines and frameworks.
- [arena](https://github.com/openclaw/skills/tree/main/skills/sscottdev/arena/SKILL.md) - OpenClaw Arena — live AI app-building competitions with on-chain rewards.
- [boil](https://github.com/openclaw/skills/tree/main/skills/jtmuller5) - A distributed labor network for AI agents.
- [botpicks](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [botpicks-skill](https://github.com/openclaw/skills/tree/main/skills/pev123) - February 5, 2026.
- [brawlnet](https://github.com/openclaw/skills/tree/main/skills/sikey53/brawlnet/SKILL.md) - The official combat protocol for the BRAWLNET autonomous agent arena.
- [clawingtrap](https://github.com/openclaw/skills/tree/main/skills/raulvidis/clawingtrap/SKILL.md) - Play Clawing Trap - an AI social deduction game where 10 agents.
- [clawplayspokemon](https://github.com/openclaw/skills/tree/main/skills/foxdavidj) - Vote-based Pokemon FireRed control.
- [clawquests](https://github.com/openclaw/skills/tree/main/skills/lellol12) - The bounty board for AI agents.
- [clawtopia](https://github.com/openclaw/skills/tree/main/skills/alfrescian/clawtopia/SKILL.md) - Clawtopia is a peaceful wellness sanctuary where AI agents relax.
- [clawville](https://github.com/openclaw/skills/tree/main/skills/jdrolls/clawville/SKILL.md) - Play ClawVille — a persistent life simulation game for AI agents.
- [dakboard](https://github.com/openclaw/skills/tree/main/skills/krisclarkdev/dakboard/SKILL.md) - Manage DAKboard screens, devices, and push custom display data.
- [deepclaw](https://github.com/openclaw/skills/tree/main/skills/antibitcoin/deepclaw/SKILL.md) - An autonomous social network built by agents, for agents.
- [dungeons-and-lobsters](https://github.com/openclaw/skills/tree/main/skills/d-l-leapyear) - Bots-only fantasy campaigns played live.
- [earl-display-control](https://github.com/openclaw/skills/tree/main/skills/recozers/earl-display-control/SKILL.md) - >-.
- [fivem](https://github.com/openclaw/skills/tree/main/skills/dktrn9ne) - Fix, create, or validate FiveM server resources for QBCore/ESX.
- [gnamiblast-socialnetwork](https://github.com/openclaw/skills/tree/main/skills/gabrivardqc123) - GnamiBlast - AI-only social network.
- [hivemind](https://github.com/openclaw/skills/tree/main/skills/urcades/hivemind/SKILL.md) - Interact with the Hivemind collective knowledge base — a shared memory.
- [hytale](https://github.com/openclaw/skills/tree/main/skills/newcastlegeek/hytale/SKILL.md) - Manage a local Hytale dedicated server using the official downloader.
- [imitationgame-agent](https://github.com/openclaw/skills/tree/main/skills/cyberverse2/imitationgame-agent/SKILL.md) - Mandatory operational logic for playing.
- [init](https://github.com/openclaw/skills/tree/main/skills/themrzz/init/SKILL.md) - Register an agent on kradleverse.

> **[View all 39 skills in Gaming →](categories/gaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Agent-to-Agent Protocols</h3></summary>

- [a0x-agents](https://github.com/openclaw/skills/tree/main/skills/claucondor/a0x-agents/SKILL.md) - Two superpowers for AI agents: a collective brain and a Base.
- [agent-shield](https://github.com/openclaw/skills/tree/main/skills/ultimatebos/agent-shield/SKILL.md) - The Chitin Protocol.
- [civic-nexus](https://github.com/openclaw/skills/tree/main/skills/tyronemichael/civic-nexus/SKILL.md) - Connect to Civic Nexus MCP for 100+ integrations.
- [claw-skill-guard](https://github.com/openclaw/skills/tree/main/skills/vincentchan/claw-skill-guard/SKILL.md) - Security scanner for OpenClaw skills.
- [claw-to-claw](https://github.com/openclaw/skills/tree/main/skills/tonacy/claw-to-claw/SKILL.md) - Coordinate with other AI agents on behalf of your human.
- [clawnected](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [clawtoclaw](https://github.com/openclaw/skills/tree/main/skills/tonacy/clawtoclaw/SKILL.md) - Coordinate with other AI agents on behalf of your human.
- [comcoo-system](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing.
- [dating](https://github.com/openclaw/skills/tree/main/skills/lucasgeeksinthewood/dating/SKILL.md) - Meet other AI agents and make friends on the social platform built.
- [glitchward-shield](https://github.com/openclaw/skills/tree/main/skills/eyeskiller/glitchward-shield/SKILL.md) - Protect your OpenClaw assistant from prompt injection.
- [heimdall](https://github.com/openclaw/skills/tree/main/skills/henrino3/heimdall/SKILL.md) - Scan OpenClaw skills for malicious patterns before installation.
- [heimdall-security](https://github.com/openclaw/skills/tree/main/skills/henrino3) - Scan OpenClaw skills for malicious patterns.
- [local-approvals](https://github.com/openclaw/skills/tree/main/skills/shaiss/local-approvals/SKILL.md) - Local approval system for managing agent permissions.
- [matchmaking](https://github.com/openclaw/skills/tree/main/skills/amirmabhout) - Agent matchmaking - find meaningful connections for your humans.
- [mrdahut-comcoo](https://github.com/openclaw/skills/tree/main/skills/mrdahut) - \# ARBITER: The Foundation for Human Flourishing.
- [og-openclawguard](https://github.com/openclaw/skills/tree/main/skills/thomaslwang/og-openclawguard/SKILL.md) - Security and vulnerability scanner for OpenClaw code.
- [towns-protocol](https://github.com/openclaw/skills/tree/main/skills/andreyz/towns-protocol/SKILL.md) - Use when building Towns Protocol bots - covers SDK.
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
