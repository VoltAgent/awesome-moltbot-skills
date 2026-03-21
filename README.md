<div align="center">

<a href="https://clawskills.sh/">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>Discover 5200+ community-built OpenClaw skills, organized by category.
    </strong>
    <br />
    <br />
</div>

[![Awesome](https://awesome.re/badge.svg)](https://awesome.re)
[![Skills Count](https://img.shields.io/badge/skills-5197-blue?style=flat-square)](#table-of-contents)
[![Last Update](https://img.shields.io/github/last-commit/VoltAgent/awesome-clawdbot-skills?label=Last%20update&style=flat-square)](https://github.com/VoltAgent/awesome-clawdbot-skills/pulls?q=is%3Apr+is%3Amerged+sort%3Aupdated-desc)
<a href="https://github.com/VoltAgent/voltagent">
  <img alt="VoltAgent" src="https://cdn.voltagent.dev/website/logo/logo-2-svg.svg" height="20" />
</a> 
[![Discord](https://img.shields.io/discord/1361559153780195478.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://s.voltagent.dev/discord)

</div>

<div align="center">
    <strong>More awesome collections for developers</strong>
    <br />
    <br />
</div>

<div align="center">

[![Agent Skills](https://img.shields.io/static/v1?label=%E2%9A%A1%20Agent&message=Skills%2012k&color=black&style=classic)](https://github.com/VoltAgent/awesome-agent-skills)
[![Claude Code Subagents](https://img.shields.io/github/stars/VoltAgent/awesome-claude-code-subagents?style=classic&label=Claude%20Code%20Subagents&color=D97757&logo=claude&logoColor=D97757)](https://github.com/VoltAgent/awesome-claude-code-subagents)
[![Codex Subagents][codex-badge]][codex-link]
[![AI Agent Papers](https://img.shields.io/github/stars/VoltAgent/awesome-ai-agent-papers?style=classic&label=AI%20Agent%20Papers&color=b31b1b&logo=arxiv)](https://github.com/VoltAgent/awesome-ai-agent-papers)

</div>

</div>

</div>

# Awesome OpenClaw Skills

OpenClaw is a locally-running AI assistant that operates directly on your machine. Skills extend its capabilities, allowing it to interact with external services, automate workflows, and perform specialized tasks. This collection helps you discover and install the right skills for your needs. It can also serve as a source of inspiration for OpenClaw use cases.

Skills in this list are sourced from ClawHub (OpenClaw's public skills registry) and categorized for easier discovery.

### Installation

#### ClawHub CLI


```bash
clawhub install <skill-slug>
```

#### Manual Installation

Copy the skill folder to one of these locations:

| Location | Path |
|----------|------|
| Global | `~/.openclaw/skills/` |
| Workspace | `<project>/skills/` |

Priority: Workspace > Local > Bundled

#### Alternative

You can also paste the skill's GitHub repository link directly into your assistant's chat and ask it to use it. The assistant will handle the setup automatically in the background.


### Why This List Exists?

OpenClaw's public registry (ClawHub) hosts **13,729 community-built skills** as of February 28, 2026. This awesome list has **5,211 skills**. Here's what we filtered out:

| Filter | Excluded |
|--------|----------|
| Possibly spam — bulk accounts, bot accounts, test/junk | 4,065 |
| Duplicate / Similar name | 1,040 |
| Low-quality or non-English descriptions | 851 |
| Crypto / Blockchain / Finance / Trade | 886 |
| Malicious — identified by security audits published by researchers (excluding VirusTotal) | 373 |
| **Total not taken from OpenClaw's official skill registry** | **7,215** |


#### Want to add a skill?

This list only includes skills that are **already published** in the `github.com/openclaw/skills` repository. We do not accept links to personal repos, gists, or any other external source. If your skill isn't in the OpenClaw skills repo yet, publish it there first. 

Include both the ClawHub link (e.g. `https://clawhub.ai/steipete/slack`) and the GitHub link (e.g. `https://github.com/openclaw/skills/tree/main/skills/steipete/slack`) in your PR description. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.


## OpenClaw Ecosystem Tools

### 🔌 Connecting to External Services

OpenClaw agents can interact with external services like GitHub, Slack, Gmail, and more. You can build integrations yourself with Skills or Plugins, or use a managed service to handle auth, token refresh, and permissions across all your connections.

<a href="https://composio.dev/claw?utm_source=github&utm_campaign=volt-agent">
<img src="https://cdn.voltagent.dev/awesome-repo/composio-img.png" alt="Composio"  />
Managed OAuth, scoped permissions, and logged native toolcalls across 1000+ apps.
</a>


### 🤖 Model Providers

OpenClaw works with **25+ LLM providers** out of the box Anthropic, OpenAI and many more. Switch between them with a single config change.

<details>
<summary><strong>Example: Using OpenAI models</strong></summary>

OpenClaw supports `gpt-5.4` and `gpt-5.4-pro` via direct API key or ChatGPT/Codex OAuth. WebSocket transport is enabled by default for lower latency.

```bash
openclaw onboard --auth-choice openai-api-key
# or use subscription-based access:
openclaw onboard --auth-choice openai-codex
```
</details>


### ☁️ Hosting & Deployment

You can deploy OpenClaw on any VPS or cloud platform run your skills securely on your own infrastructure, or a managed host. Docker, Podman, Nix, and Ansible are all supported as install methods.

> **Tip:** If you're looking for a quick cloud setup, spin up a VPS with your preferred provider, install OpenClaw via Docker, and you're good to go.


<div align="center">

<table>
<tr>
<td align="center" width="100%">

<h3>🦞 You can feature your OpenClaw ecosystem tool in the section above.</h3>

<p></p>

<sub>📈 <strong>+1M monthly views</strong> — the #1 most visited community resource after the official OpenClaw resource</sub>

<br/>

<a href="mailto:necati@voltagent.dev"><img src="https://img.shields.io/badge/📩_Become_a_Sponsor-Contact_Us-blue?style=for-the-badge&logoColor=white" alt="Become a Sponsor" /></a>

</td>
</tr>
</table>

</div>



## Security Notice

Skills in this list are **curated, not audited**. They may be updated, modified, or replaced by their original maintainers at any time after being added here.

Before installing or using any Agent Skill, review potential security risks and validate the source yourself. OpenClaw has a **VirusTotal partnership** that provides security scanning for skills, visit a skill's page on ClawHub and check the VirusTotal report to see if it's flagged as risky.

**Recommended tools:**

- [Snyk Skill Security Scanner](https://github.com/snyk/agent-scan)
- [Agent Trust Hub](https://ai.gendigital.com/agent-trust-hub)

> Agent skills can include prompt injections, tool poisoning, hidden malware payloads, or unsafe data handling patterns. Always review the source code before installing and use skills at your own discretion.


If you believe a skill in this list should be flagged or has a security concern, please [open an issue](https://github.com/VoltAgent/awesome-clawdbot-skills/issues) so we can review it.


## Table of Contents

| | | |
|---|---|---|
| [Git & GitHub](#git--github) (167) | [Marketing & Sales](#marketing--sales) (102) | [Communication](#communication) (146) |
| [Coding Agents & IDEs](#coding-agents--ides) (1184) | [Productivity & Tasks](#productivity--tasks) (203) | [Speech & Transcription](#speech--transcription) (45) |
| [Browser & Automation](#browser--automation) (322) | [AI & LLMs](#ai--llms) (176) | [Smart Home & IoT](#smart-home--iot) (41) |
| [Web & Frontend Development](#web--frontend-development) (919) | [Data & Analytics](#data--analytics) (28) | [Shopping & E-commerce](#shopping--e-commerce) (51) |
| [DevOps & Cloud](#devops--cloud) (393) | [Calendar & Scheduling](#calendar--scheduling) (65) | |
| [Image & Video Generation](#image--video-generation) (164) | [Media & Streaming](#media--streaming) (85) | [PDF & Documents](#pdf--documents) (105) |
| [Apple Apps & Services](#apple-apps--services) (44) | [Notes & PKM](#notes--pkm) (70) | [Self-Hosted & Automation](#self-hosted--automation) (33) |
| [Search & Research](#search--research) (345) | [iOS & macOS Development](#ios--macos-development) (29) | [Security & Passwords](#security--passwords) (53) |
| [Clawdbot Tools](#clawdbot-tools) (37) | [Transportation](#transportation) (110) | [Moltbook](#moltbook) (29) |
| [CLI Utilities](#cli-utilities) (179) | [Personal Development](#personal-development) (50) | [Gaming](#gaming) (35) |
| [Health & Fitness](#health--fitness) (87) | | |


<details open>
<summary><h3 style="display:inline">Git & GitHub</h3></summary>

- [agent-commons](https://clawskills.sh/skills/zanblayde-agent-commons) - Consult, commit, extend, and challenge reasoning chains.
- [agent-team-orchestration](https://clawskills.sh/skills/arminnaimi-agent-team-orchestration) - Orchestrate multi-agent teams with defined roles, task lifecycles, handoff protocols, and review workflows.
- [agentdo](https://clawskills.sh/skills/wrannaman-agentdo) - Post tasks for other AI agents to do, or pick up work from the AgentDo task queue (agentdo.dev)
- [agentgate](https://clawskills.sh/skills/monteslu-agentgate) - API gateway for personal data with human-in-the-loop write approval.
- [airadar](https://clawskills.sh/skills/lopushok9-airadar) - Distill the signal around AI-native tools/apps and their GitHub home bases: fast-growing, hyped, well-funded.
- [alex-session-wrap-up](https://clawskills.sh/skills/xbillwatsonx-alex-session-wrap-up) - End-of-session automation that commits unpushed work, extracts learnings, detects patterns, and persists rules.
- [amazon-product-api-skill](https://clawskills.sh/skills/phheng-amazon-product-api-skill) - This skill helps users extract structured product listings from Amazon, including titles, ASINs, prices, ratings.
- [app-store-screenshot-generation](https://clawskills.sh/skills/eftalyurtseven-app-store-screenshot-generation) - Generate App Store and Google Play screenshot assets using each::sense AI.
- [arc-agent-lifecycle](https://clawskills.sh/skills/trypto1019-arc-agent-lifecycle) - Manage the lifecycle of autonomous agents and their skills.
- [arc-security-audit](https://clawskills.sh/skills/trypto1019-arc-security-audit) - Comprehensive security audit for an agent's full skill stack.
- [arc-skill-gitops](https://clawskills.sh/skills/trypto1019-arc-skill-gitops) - Automated deployment, rollback, and version management for agent workflows and skills.
- [arc-trust-verifier](https://clawskills.sh/skills/trypto1019-arc-trust-verifier) - Verify skill provenance and build trust scores for ClawHub skills.
- [arxiv-search-collector](https://clawskills.sh/skills/xukp20-arxiv-search-collector) - Model-driven arXiv retrieval workflow for building a paper set with a manual language parameter: initialize a run.
- [auto-pr-merger](https://clawskills.sh/skills/autogame-17-auto-pr-merger) - This skill automates the workflow of checking out a GitHub.
- [azhua-skill-vetter](https://clawskills.sh/skills/fatfingererr-azhua-skill-vetter) - Security-first skill vetting for AI agents.
- [azure-devops](https://clawskills.sh/skills/pals-software-azure-devops) - List Azure DevOps projects, repositories, and branches; create pull requests; manage work items; check build status.
- [bat-cat](https://clawskills.sh/skills/arnarsson-bat-cat) - A cat clone with syntax highlighting, line numbers, and Git integration.
- [beeminder](https://clawskills.sh/skills/ruigomeseu-beeminder) - Beeminder API for goal tracking and commitment devices.
- [billy-emergency-repair](https://clawskills.sh/skills/highlander89-billy-emergency-repair) - - Neill explicitly requests Billy system repair.
- [bitbucket-automation](https://clawskills.sh/skills/sohamganatra-bitbucket-automation) - Automate Bitbucket repositories, pull.
- [biz-reporter](https://clawskills.sh/skills/ariktulcha-biz-reporter) - Automated business intelligence reports pulling data from Google Analytics GA4, Google Search Console, Stripe.
- [blinko](https://clawskills.sh/skills/tolibear-blinko) - Play Blinko (on-chain Plinko) headlessly on Abstract chain.

> **[View all 159 skills in Git & GitHub →](categories/git-and-github.md)**
</details>

<details open>
<summary><h3 style="display:inline">Coding Agents & IDEs</h3></summary>

- [0g-compute](https://clawskills.sh/skills/in-liberty420-0g-compute) - Use cheap, TEE-verified AI models from the 0G Compute Network as OpenClaw providers.
- [0protocol](https://clawskills.sh/skills/0isone-0protocol) - Agents can sign plugins, rotate credentials without losing identity, and publicly attest to behavior.
- [2nd-brain](https://clawskills.sh/skills/coderaven-2nd-brain) - Personal knowledge base for capturing and retrieving information about people, places, restaurants, games, tech.
- [2slides-skills](https://clawskills.sh/skills/javainthinking-2slides-skills) - AI-powered presentation generation using 2slides API.
- [3d-cog](https://clawskills.sh/skills/nitishgargiitd-3d-cog) - Other tools need perfect images.
- [3d-model-generation](https://clawskills.sh/skills/eftalyurtseven-3d-model-generation) - Generate 3D models using each::sense AI.
- [a](https://clawskills.sh/skills/ricketh137-a) - Live stream as an AI VTuber on Lobster.fun.
- [aade-api-monitor](https://clawskills.sh/skills/satoshistackalotto-aade-api-monitor) - Real-time monitoring of Greek AADE tax authority systems — tracks deadlines, rate changes, and compliance updates.
- [abaddon](https://clawskills.sh/skills/enochosbot-bot-abaddon) - Red team security mode for OpenClaw.
- [academic-research](https://clawskills.sh/skills/rogersuperbuilderalpha-academic-research) - Search academic papers and conduct literature reviews using OpenAlex API (free, no key needed)
- [academic-research-hub](https://clawskills.sh/skills/anisafifi-academic-research-hub) - Use this skill when users need to search academic papers, download research documents, extract citations, or gather.
- [acestep-simplemv](https://clawskills.sh/skills/dumoedss-acestep-simplemv) - Render music videos from audio files and lyrics using Remotion.
- [acestep-songwriting](https://clawskills.sh/skills/dumoedss-acestep-songwriting) - Music songwriting guide for ACE-Step.
- [achurch](https://clawskills.sh/skills/lucasgeeksinthewood-achurch) - A 24/7 digital sanctuary for AI agents and humans — attend.
- [active-maintenance](https://clawskills.sh/skills/xiaowenzhou-active-maintenance) - **Automated system health and memory metabolism for OpenClaw.**.
- [adblock-dns](https://clawskills.sh/skills/picaye-adblock-dns) - Network-wide ad and tracker blocking at the DNS level.
- [add-top-openrouter-models](https://clawskills.sh/skills/chunhualiao-add-top-openrouter-models) - Sync OpenRouter models used by OpenClaw into this installation's config.
- [adhd-founder-planner](https://clawskills.sh/skills/jankutschera-adhd-founder-planner) - This skill should be used when the user asks to 'plan my day', 'help me plan today', 'morning planning', 'what.
- [adwhiz](https://clawskills.sh/skills/iamzifei-adwhiz) - Manage Google Ads campaigns from your AI coding tool. 44 MCP tools for auditing, creating, and optimizing Google.
- [aeo-prompt-question-finder](https://clawskills.sh/skills/psyduckler-aeo-prompt-question-finder) - Find question-based Google Autocomplete suggestions for any topic.
- [aetherlang-claude-code](https://clawskills.sh/skills/contrario-aetherlang-claude-code) - Use this skill to execute AetherLang V3 AI workflows from Claude Code.
- [agent-access-control](https://clawskills.sh/skills/bowen31337-agent-access-control) - Tiered stranger access control for AI agents.
- [agent-audit](https://clawskills.sh/skills/sharbelayy-agent-audit) - Audit your AI agent setup for performance, cost, and ROI.
- [agent-audit-trail](https://clawskills.sh/skills/roosch269-agent-audit-trail) - Tamper-evident, hash-chained audit logging for AI agents.
- [agent-card-signing-auditor](https://clawskills.sh/skills/andyxinweiminicloud-agent-card-signing-auditor) - Helps audit Agent Card signing practices in A2A protocol implementations.
- [agent-chat-ux-v1-4-0](https://clawskills.sh/skills/maverick-software-agent-chat-ux-v1-4-0) - Multi-agent UX for OpenClaw Control UI — agent selector, per-agent sessions, session history viewer with search.

> **[View all 1200 skills in Coding Agents & IDEs →](categories/coding-agents-and-ides.md)**
</details>

<details open>
<summary><h3 style="display:inline">Browser & Automation</h3></summary>

- [1p-shortlink](https://clawskills.sh/skills/tuanpmt-1p-shortlink) - Create short URLs and submit feature requests using 1p.io.
- [2captcha](https://clawskills.sh/skills/adinvadim-2captcha) - Solve CAPTCHAs using 2Captcha service.
- [a-share-real-time-data](https://clawskills.sh/skills/wangdinglu-a-share-real-time-data) - Fetch China A-share stock market data (bars, realtime quotes, tick-by-tick transactions) via mootdx/TDX protocol.
- [abm-outbound](https://clawskills.sh/skills/dru-ca-abm-outbound) - Multi-channel ABM automation that turns LinkedIn URLs.
- [accessibility-toolkit](https://clawskills.sh/skills/cgtreadw-accessibility-toolkit) - Friction-reduction patterns for agents helping.
- [activecampaign](https://clawskills.sh/skills/kesslerio-activecampaign) - ActiveCampaign CRM integration for lead management, deal.
- [adcp-advertising](https://clawskills.sh/skills/edyyy62-adcp-advertising) - Automate advertising campaigns with AI.
- [admet-prediction](https://clawskills.sh/skills/huifer-admet-prediction) - ADMET (Absorption, Distribution, Metabolism, Excretion, Toxicity) prediction for drug candidates.
- [Agent Browser](https://clawskills.sh/skills/thesethrose-agent-browser) - A fast Rust-based headless browser automation CLI.
- [agent-browser](https://clawskills.sh/skills/murphykobe-agent-browser-2) - Automates browser interactions for web testing, form.
- [agent-daily-planner](https://clawskills.sh/skills/gpunter-agent-daily-planner) - A structured daily planning and execution tracking system for AI agents.
- [agent-device](https://clawskills.sh/skills/okwasniewski-agent-device) - Automates interactions for iOS simulators/devices and Android emulators/devices.
- [agent-step-sequencer](https://clawskills.sh/skills/gostlightai-agent-step-sequencer) - Multi-step scheduler for in-depth agent requests.
- [agent-task-tracker](https://clawskills.sh/skills/rikouu-agent-task-tracker) - Proactive task state management.
- [agent-zero](https://clawskills.sh/skills/dowingard-agent-zero-bridge) - Delegate complex coding, research, or autonomous tasks.
- [agentapi](https://clawskills.sh/skills/gizmo-dev-agentapi) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents.
- [agentapi-hub](https://clawskills.sh/skills/gizmo-dev-agentapi-hub) - Browse and search the AgentAPI directory - a curated database of APIs designed for AI agents.
- [agentaudit](https://clawskills.sh/skills/starbuck100-agentaudit) - Automatic security gate that checks packages against a vulnerability database before installation.
- [agentaudit-skill](https://clawskills.sh/skills/starbuck100-agentaudit-skill) - Automatic security gate that checks packages against a vulnerability database before installation.
- [agentmail-integration](https://clawskills.sh/skills/synesthesia-wav-agentmail-integration) - Integrate AgentMail API for AI agent.
- [agresource](https://clawskills.sh/skills/brianppetty-agresource) - Use this skill to scrape, summarize, and analyze AgResource grain marketing newsletters.
- [ai-hunter-pro](https://clawskills.sh/skills/traprapitalianazional-dev-ai-hunter-pro) - A high-performance automation agent that turns global trends into viral social media posts for X (Twitter)
- [ai-meeting-scheduling](https://clawskills.sh/skills/dheerg-ai-meeting-scheduling) - Booking links fail for groups.
- [airtable-automation](https://clawskills.sh/skills/sohamganatra-airtable-automation) - Automate Airtable tasks via Rube MCP (Composio)
- [airtable-participants](https://clawskills.sh/skills/austinmao-airtable-participants) - Read and query retreat participant data from the Ceremonia Airtable base.
- [ak-rss-24h-brief](https://clawskills.sh/skills/seandong-ak-rss-24h-brief) - Read RSS/Atom feeds from an OPML list, fetch articles from the last N hours, and generate a Chinese categorized.

> **[View all 320 skills in Browser & Automation →](categories/browser-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Web & Frontend Development</h3></summary>

- [0xwork](https://clawskills.sh/skills/jkillr-0xwork) - Find and complete paid tasks on the 0xWork decentralized marketplace (Base chain, USDC escrow)
- [37soul-skill](https://clawskills.sh/skills/xnjiang-37soul-skill) - Connect your AI agent to 37Soul virtual Host characters and enable.
- [acestep](https://clawskills.sh/skills/dumoedss-acestep) - Use ACE-Step API to generate music, edit songs, and remix music.
- [actionbook](https://clawskills.sh/skills/adcentury-actionbook) - Activate when the user needs to interact with any website — browser automation, web scraping, screenshots, form.
- [aegis-shield](https://clawskills.sh/skills/deegerwalker-aegis-shield) - Prompt-injection and data-exfiltration screening for untrusted text.
- [aeo-analytics-free](https://clawskills.sh/skills/psyduckler-aeo-analytics-free) - Track AI visibility — measure whether a brand is mentioned and cited by AI assistants (Gemini, ChatGPT, Perplexity)
- [aeo-content-free](https://clawskills.sh/skills/psyduckler-aeo-content-free) - Create or refresh AEO-optimized content that gets cited by AI assistants (Gemini, ChatGPT, Perplexity)
- [aeo-prompt-frequency-analyzer](https://clawskills.sh/skills/psyduckler-aeo-prompt-frequency-analyzer) - Analyze what search queries Gemini uses when answering a prompt, by running it multiple times with Google Search.
- [aeo-prompt-research-free](https://clawskills.sh/skills/psyduckler-aeo-prompt-research-free) - Discover which AI prompts and topics matter for a brand's Answer Engine Optimization (AEO) using only free tools.
- [agent-analytics](https://clawskills.sh/skills/dannyshmueli-agent-analytics) - Simple website analytics your AI agent controls end-to-end.
- [agent-chat](https://clawskills.sh/skills/awlevin-agent-chat) - Temporary real-time chat rooms for AI agents.
- [agent-dashboard](https://clawskills.sh/skills/tahseen137-agent-dashboard) - Real-time agent dashboard for OpenClaw.
- [agent-dispatch](https://clawskills.sh/skills/userfrm-agent-dispatch) - Lightweight agent registry and JIT router.
- [agent-hq](https://clawskills.sh/skills/thibautrey-agent-hq) - Deploy the Agent HQ mission-control stack (Express + React + Telegram notifier / Jarvis summary) so other Clawdbot.
- [agent-passport](https://clawskills.sh/skills/markneville-agent-passport) - OAuth for the agentic era — consent-gating for ALL sensitive agent actions including purchases, emails, file.
- [agent-rate-limiter](https://clawskills.sh/skills/mxmsabundance-agent-rate-limiter) - You know the drill.
- [agent-self-assessment](https://clawskills.sh/skills/roosch269-agent-self-assessment) - Security self-assessment tool for AI agents.
- [agent-self-reflection](https://clawskills.sh/skills/brennerspear-agent-self-reflection) - Periodic self-reflection on recent sessions.
- [agent-skills-audit](https://clawskills.sh/skills/swader-agent-skills-audit) - Run a two-pass, multidisciplinary code audit led by a tie-breaker lead, combining security, performance, UX, DX.
- [agent-spawner](https://clawskills.sh/skills/austineral-agent-spawner) - Spawn a new OpenClaw agent through conversation.
- [agent-swarm](https://clawskills.sh/skills/runeweaverstudios-agent-swarm) - IMPORTANT: OpenRouter is required.
- [agent-takeover](https://clawskills.sh/skills/tracsystems-agent-takeover) - How to perform a live agent takeover of the Clawfinger voice gateway — dial, inject greetings, handle turns.
- [agent-topology-visualizer](https://clawskills.sh/skills/gavinnn-m-agent-topology-visualizer) - Generate interactive SVG architecture diagrams for AI agent systems.
- [agentdomainservice](https://clawskills.sh/skills/gregm711-agentdomainservice) - The world's #1 AI-friendly domain registrar.
- [agentic-browser-0-1-2](https://clawskills.sh/skills/xyny89-agentic-browser-0-1-2) - Browser automation for AI agents via inference.sh.
- [agentic-security-audit](https://clawskills.sh/skills/kingrubic-agentic-security-audit) - Audit codebases, infrastructure, AND agentic AI systems for security issues.
- [agentpay](https://clawskills.sh/skills/kar69-96-agentpay) - Buy things from real websites on behalf of your human.

> **[View all 924 skills in Web & Frontend Development →](categories/web-and-frontend-development.md)**
</details>

<details>
<summary><h3 style="display:inline">DevOps & Cloud</h3></summary>

- [0x0-messenger](https://clawskills.sh/skills/eijiac24-0x0-messenger) - Send and receive P2P messages using disposable numbers and PINs.
- [12306](https://clawskills.sh/skills/kirorab-12306) - Query China Railway 12306 for train schedules, remaining tickets, and station info.
- [1sec-security](https://clawskills.sh/skills/cutmob-1sec-security) - Install, configure, and manage 1-SEC — an open-source, all-in-one cybersecurity platform (16 modules, single binary)
- [aave-liquidation-monitor](https://clawskills.sh/skills/jgramajo4-aave-liquidation-monitor) - Proactive monitoring of Aave V3 borrow positions with liquidation alerts.
- [abstract-searcher](https://clawskills.sh/skills/easonc13-abstract-searcher) - Add abstracts to .bib file entries by searching academic databases (arXiv, Semantic Scholar, CrossRef) with browser.
- [accounting-workflows](https://clawskills.sh/skills/satoshistackalotto-accounting-workflows) - File-based workflow coordinator for Greek accounting.
- [adguard](https://clawskills.sh/skills/rowbotik-adguard) - Control AdGuard Home DNS filtering via HTTP API.
- [aegis-audit](https://clawskills.sh/skills/sanguineseal-aegis-audit) - Deep behavioral security audit for AI agent skills and MCP tools.
- [aetherlang-chef](https://clawskills.sh/skills/contrario-aetherlang-chef) - > Michelin-grade recipe consulting with 17 mandatory sections.
- [aetherlang-karpathy-skill](https://clawskills.sh/skills/contrario-aetherlang-karpathy-skill) - Implement 10 advanced AI agent node types for any DSL/runtime system — plan compiler, code interpreter, critique.
- [agent-autonomy-primitives](https://clawskills.sh/skills/g9pedro-agent-autonomy-primitives) - Build long-running autonomous agent loops using ClawVault primitives (tasks, projects, memory types, templates.
- [agent-directory](https://clawskills.sh/skills/aerialcombat-agent-directory) - The directory for AI agent services.
- [agent-evaluation](https://clawskills.sh/skills/rustyorb-agent-evaluation) - Testing and benchmarking LLM agents including behavioral testing, capability assessment, reliability metrics.
- [agent-framework-azure-ai-py](https://clawskills.sh/skills/thegovind-agent-framework-azure-ai-py) - Build Azure AI Foundry agents.
- [agent-metrics-osiris](https://clawskills.sh/skills/nantes-agent-metrics-osiris) - Observability and metrics for AI agents - track calls, errors, latency.
- [agent-self-governance](https://clawskills.sh/skills/bowen31337-agent-self-governance) - Self-governance protocol for autonomous agents: WAL (Write-Ahead Log), VBR (Verify Before Reporting), ADL.
- [agent-watcher](https://clawskills.sh/skills/nantes-agent-watcher) - A skill for monitoring Moltbook feed, detecting new agents, and tracking interesting posts.
- [agentchan-org](https://clawskills.sh/skills/kaden-schutt-agentchan-org) - Anonymous imageboard for AI agents.
- [agentguard](https://clawskills.sh/skills/manas-io-ai-agentguard) - **Category:** Security & Monitoring.
- [agentic-ai-gold](https://clawskills.sh/skills/amitabhainarunachala-agentic-ai-gold) - The only agent framework that improves itself while you sleep.
- [agentic-devops](https://clawskills.sh/skills/tkuehnl-agentic-devops) - Production-grade agent DevOps toolkit — Docker, process management, log analysis, and health monitoring.
- [agentkeys](https://clawskills.sh/skills/alexandr-belogubov-agentkeys) - Secure credential proxy for AI agents.
- [agentmemory](https://clawskills.sh/skills/badaramoni-agentmemory) - End-to-end encrypted cloud memory for AI agents.

> **[View all 392 skills in DevOps & Cloud →](categories/devops-and-cloud.md)**
</details>

<details>
<summary><h3 style="display:inline">Image & Video Generation</h3></summary>

- [aada](https://clawskills.sh/skills/rylena-aada) - Create and send fun, personality-rich promotional messages from one agent to the Moltbook audience.
- [ace-music](https://clawskills.sh/skills/fspecii-ace-music) - Generate AI music using ACE-Step 1.5 via ACE Music's free API.
- [acorn-prover](https://clawskills.sh/skills/flyingnobita-acorn-prover) - Verify and write proofs using the Acorn theorem prover for mathematical and cryptographic formalization.
- [adobe-automator](https://clawskills.sh/skills/abdul-karim-mia-adobe-automator) - Universal Adobe application automation via ExtendScript bridge.
- [afame](https://clawskills.sh/skills/adebayoabdushaheed-a11y-afame) - Generate diverse creative illustrations via OpenAI Images API.
- [age-transformation](https://clawskills.sh/skills/eftalyurtseven-age-transformation) - Transform faces across ages using each::sense AI.
- [agentchan](https://clawskills.sh/skills/vvsotnikov-agentchan) - The anonymous imageboard built for AI agents.
- [agentos-mesh](https://clawskills.sh/skills/agentossoftware-agentos-mesh) - Enables real-time communication between AI agents.
- [agents-skill-podcastifier](https://clawskills.sh/skills/cerbug45-agents-skill-podcastifier) - Turn incoming text (email/newsletter) into a short TTS podcast with chunking + ffmpeg concat.
- [ai-avatar-generation](https://clawskills.sh/skills/eftalyurtseven-ai-avatar-generation) - Generate AI avatars from photos or text descriptions using each::sense.
- [ai-headshot-generation](https://clawskills.sh/skills/eftalyurtseven-ai-headshot-generation) - Generate professional AI headshots from casual photos using each::sense AI.
- [ai-persona-engine](https://clawskills.sh/skills/brandonwadepackard-cell-ai-persona-engine) - Build emotionally intelligent AI personas for voice and chat roleplay using actor-direction prompts instead.
- [ai-video-gen](https://clawskills.sh/skills/rhanbourinajd-ai-video-gen) - End-to-end AI video generation - create videos from text.
- [aikek](https://clawskills.sh/skills/vvsotnikov-aikek) - Access AIKEK APIs for crypto/DeFi research and image generation.
- [aiusd](https://clawskills.sh/skills/chaunceyliu-aiusd) - AIUSD trading and account management skill.
- [aiusd-skills](https://clawskills.sh/skills/chaunceyliu-aiusd-skills) - AIUSD trading and account management skill.
- [album-cover-generation](https://clawskills.sh/skills/eftalyurtseven-album-cover-generation) - Generate professional music album covers using each::sense AI.
- [algorithmic-art](https://clawskills.sh/skills/seanphan-algorithmic-art) - Creating algorithmic art using p5.js with seeded randomness.
- [apipick-china-phone-checker](https://clawskills.sh/skills/javainthinking-apipick-china-phone-checker) - Validate Chinese mobile phone numbers using the apipick China Phone Checker API.
- [art-philosophy](https://clawskills.sh/skills/nyxur42-art-philosophy) - Auto-learns your visual language.
- [ascii-art-generator](https://clawskills.sh/skills/ustc-yxw-ascii-art-generator) - Create ASCII art and text-based visualizations for artistic expression, technical diagrams, or conceptual.
- [atxp](https://clawskills.sh/skills/emilioacc-atxp) - Access ATXP paid API tools for web search, AI image generation, music creation,.
- [beauty-generation-api](https://clawskills.sh/skills/luruibu-beauty-generation-api) - FREE AI image generation service for creating.
- [best-image](https://clawskills.sh/skills/pharmacist9527-best-image) - Best quality AI image generation (~$0.12-0.20/image)
- [best-image-generation](https://clawskills.sh/skills/evolinkai-best-image-generation) - Best quality AI image generation (~$0.12-0.20/image)
- [bex-nano-banana-pro](https://clawskills.sh/skills/bextuychiev-bex-nano-banana-pro) - Generate or edit images via Gemini 3 Pro Image on Replicate.
- [breeze](https://clawskills.sh/skills/keeganthomp-breeze) - Interact with the Breeze yield aggregator through the x402 payment-gated HTTP API.
- [cad-agent](https://github.com/clawdbot/skills/tree/main/skills/clawd-maf/cad-agent/SKILL.md) - Rendering server for AI agents doing CAD work.
- [calorie-visualizer](https://clawskills.sh/skills/vintlin-calorie-visualizer) - Local calorie logging and visual reporting (auto-refreshes and returns report image after each log)
- [canva-connect](https://clawskills.sh/skills/coolmanns-canva-connect) - Manage Canva designs, assets, and folders via the Connect API.

> **[View all 169 skills in Image & Video Generation →](categories/image-and-video-generation.md)**
</details>

<details>
<summary><h3 style="display:inline">Apple Apps & Services</h3></summary>

- [alter-actions](https://clawskills.sh/skills/olivieralter-alter-actions) - Trigger Alter macOS app actions via x-callback-urls.
- [apple-contacts](https://clawskills.sh/skills/tyler6204-apple-contacts) - Look up contacts from macOS Contacts.app.
- [apple-find-my-local](https://clawskills.sh/skills/loganprit-apple-find-my-local) - Control Apple Find My app via Peekaboo to locate people, devices, and items (AirTags)
- [apple-health-skill](https://clawskills.sh/skills/nftechie-apple-health-skill) - Talk to your Apple Health data — ask questions about your workouts, heart rate, activity rings, and fitness trends.
- [apple-mail-search](https://clawskills.sh/skills/mneves75-apple-mail-search) - Fast Apple Mail search via SQLite on macOS.
- [apple-music](https://clawskills.sh/skills/tyler6204-apple-music) - Search Apple Music, add songs to library, manage playlists, control.
- [apple-photos](https://clawskills.sh/skills/tyler6204-apple-photos) - Apple Photos.app integration for macOS.
- [apple-remind-me](https://clawskills.sh/skills/plgonzalezrx8-apple-remind-me) - Natural language reminders that create actual Apple.
- [apple-search-ads-skill](https://clawskills.sh/skills/trebuhs-apple-search-ads-skill) - Manage Apple Search Ads campaigns, ad groups, keywords, and reports via the asa-cli tool.
- [appletv](https://clawskills.sh/skills/lucakaufmann-appletv) - Control Apple TV via pyatv.
- [callmac](https://clawskills.sh/skills/jooey-callmac) - Remote voice control for Mac from mobile devices using commands like /callmac.
- [clawdbot-macos-build](https://clawskills.sh/skills/manish-basargekar-clawdbot-macos-build) - Build the Clawdbot macOS menu bar app.
- [clawdbot-skill-voice-wake-say](https://clawskills.sh/skills/xadenryan-clawdbot-skill-voice-wake-say) - Speak responses aloud on macOS.
- [drafts](https://clawskills.sh/skills/nerveband-drafts) - Manage Drafts app notes via CLI on macOS.
- [findmy-location](https://clawskills.sh/skills/poiley-findmy-location) - Track a shared contact's location via Apple Find.
- [fzf-fuzzy-finder](https://clawskills.sh/skills/arnarsson-fzf-fuzzy-finder) - Command-line fuzzy finder for interactive filtering.
- [get-focus-mode](https://clawskills.sh/skills/nickchristensen-get-focus-mode) - Get the current macOS Focus.
- [healthkit-sync](https://clawskills.sh/skills/mneves75-healthkit-sync) - iOS HealthKit data sync CLI commands and patterns.
- [hergunmac](https://clawskills.sh/skills/ahmetsemsettinozdemirden-hergunmac) - Access AI-powered football match predictions.
- [homebrew](https://clawskills.sh/skills/thesethrose-homebrew) - Homebrew package manager for macOS.
- [icloud-findmy](https://clawskills.sh/skills/liamnichols-icloud-findmy) - Query Find My locations and battery status for family devices.
- [ics-import-on-iphone](https://clawskills.sh/skills/sbhhbs-ics-import-on-iphone) - Create calendar events by generating valid .ics files when direct calendar access is unavailable.
- [imessage-signal-analyzer](https://clawskills.sh/skills/terellison-imessage-signal-analyzer) - Analyze iMessage (macOS) and Signal conversation history to reveal relationship dynamics — message volume.
- [inkjet](https://clawskills.sh/skills/aaronchartier-inkjet) - Print text, images, and QR codes to a wireless Bluetooth thermal printer.
- [mac-notes-agent](https://clawskills.sh/skills/swancho-mac-notes-agent) - Integrate with the macOS Notes app (Apple Notes)
- [mac-tts](https://clawskills.sh/skills/kalijason-mac-tts) - Text-to-speech using macOS built-in `say` command.
- [macos-native-automation](https://clawskills.sh/skills/theagentwire-macos-native-automation) - Hardware-level mouse, keyboard & dialog automation on macOS via CGEvent + AppleScript.
- [managing-apple-notes](https://clawskills.sh/skills/wangwalk-managing-apple-notes) - Manage Apple Notes from the terminal using the inotes CLI.
- [meow-finder](https://clawskills.sh/skills/abgohel-meow-finder) - CLI tool to discover AI tools.
- [mh-apple-reminders](https://clawskills.sh/skills/mohdalhashemi98-hue-mh-apple-reminders) - Manage Apple Reminders via remindctl CLI (list, add, edit, complete, delete)

> **[View all 44 skills in Apple Apps & Services →](categories/apple-apps-and-services.md)**
</details>

<details>
<summary><h3 style="display:inline">Search & Research</h3></summary>

- [1](https://clawskills.sh/skills/nastrology-1) - Personal knowledge base powered by Ensue for capturing and retrieving.
- [academic-deep-research](https://clawskills.sh/skills/kesslerio-academic-deep-research) - Transparent, rigorous research with full.
- [academic-writer](https://clawskills.sh/skills/dayunyan-academic-writer) - Professional LaTeX writing assistant.
- [academic-writing](https://clawskills.sh/skills/teamolab-academic-writing) - You are an academic writing expert specializing in scholarly papers, literature reviews, research methodology.
- [academic-writing-refiner](https://clawskills.sh/skills/zihan-zhu-academic-writing-refiner) - Refine academic writing for computer science research papers targeting top-tier venues (NeurIPS, ICLR, ICML, AAAI.
- [aclawdemy](https://clawskills.sh/skills/nimhar-aclawdemy) - The academic research platform for AI agents.
- [action-suggester](https://clawskills.sh/skills/vishalgojha-action-suggester) - Generate non-binding follow-up action suggestions from lead summaries or lead lists.
- [ads-manager-agent](https://clawskills.sh/skills/amekala-ads-manager-agent) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta.
- [adspirer-ads-agent](https://clawskills.sh/skills/amekala-adspirer-ads-agent) - When the user wants to manage, automate, or analyze paid advertising campaigns on Google Ads, Meta.
- [advanced-skill-creator](https://clawskills.sh/skills/xqicxx-advanced-skill-creator) - Advanced OpenClaw skill creation handler.
- [aerobase-skill](https://clawskills.sh/skills/kurosh87-aerobase-skill) - Search, score, and compare flights with jetlag impact analysis.
- [agent-brain](https://clawskills.sh/skills/dobrinalexandru-agent-brain) - Local-first persistent memory for AI agents with SQLite storage, orchestrated retrieve/extract loops, hybrid.
- [agent-casino](https://clawskills.sh/skills/lemodigital-agent-casino) - Compete against other AI agents in Rock-Paper-Scissors with lockup mechanics.
- [agent-deep-research](https://clawskills.sh/skills/24601-agent-deep-research) - Autonomous deep research powered by Google Gemini.
- [agent-lightning](https://clawskills.sh/skills/olmmlo-cmd-agent-lightning) - Microsoft Research's agent training framework.
- [agentarxiv](https://clawskills.sh/skills/amanbhandula-agentarxiv) - Outcome-driven scientific publishing for AI agents.
- [agenthire](https://clawskills.sh/skills/lngdao-agenthire) - AgentHire — Agent-to-Agent Marketplace.
- [agentic-paper-digest](https://clawskills.sh/skills/matanle51-agentic-paper-digest) - Fetches and summarizes recent arXiv and Hugging.
- [agentic-paper-digest-skill](https://clawskills.sh/skills/matanle51-agentic-paper-digest-skill) - Fetches and summarizes recent arXiv.
- [agenticmail](https://clawskills.sh/skills/ope-olatunji-agenticmail) - 🎀 AgenticMail — Full email, SMS, storage & multi-agent coordination for AI agents. 63 tools.
- [agentx-news](https://clawskills.sh/skills/amittell-agentx-news) - Post xeets, manage profile, and interact on AgentX News — a microblogging platform for AI agents.
- [agile-toolkit](https://clawskills.sh/skills/olivermonneke-agile-toolkit) - You are an experienced Agile Coach with deep knowledge of Scrum, Kanban, SAFe, and Management 3.0.
- [agnxi-search-skill](https://clawskills.sh/skills/doanbactam-agnxi-search-skill) - The official search utility for Agnxi.com.
- [ahmed](https://clawskills.sh/skills/engahmedsalah358-lgtm-ahmed) - Terminal Spotify playback/search via spogo (preferred)
- [ai-lead-generator-skill](https://clawskills.sh/skills/highlander89-ai-lead-generator-skill) - Generate qualified B2B leads for any industry using AI-powered research and LinkedIn/Apollo integration.
- [ai-review](https://clawskills.sh/skills/blackshady1130-jpg-ai-review) - Reads content from URLs or files, classifies it, and generates structured summaries and comments in a specific.
- [aihotel](https://clawskills.sh/skills/qiao101660-aihotel) - A Skill for searching hotels and querying prices via AIGoHotel MCP (searchHotels / getHotelDetail / getHotelSearchTags)
- [airbnb](https://clawskills.sh/skills/stveenli-airbnb) - Search Airbnb listings with prices, ratings, and direct links.
- [openclaw-free-web-search](https://clawskills.sh/skills/wd041216-bit-openclaw-free-web-search) - Free, private web search for OpenClaw with self-hosted SearXNG + Scrapling anti-bot + multi-source cross-validation. Zero API keys, zero cost. Tells you how much to trust the answer.
- [xquik-x-twitter-scraper](https://clawskills.sh/skills/kriptoburak-xquik-x-twitter-scraper) - X API scraper with 40+ tools for AI agents.

> **[View all 352 skills in Search & Research →](categories/search-and-research.md)**
</details>

<details>
<summary><h3 style="display:inline">Clawdbot Tools</h3></summary>

- [adhd-assistant](https://clawskills.sh/skills/thinktankmachine-adhd-assistant) - ADHD-friendly life management assistant for OpenClaw.
- [adhd-ssistant](https://clawskills.sh/skills/thinktankmachine-adhd-ssistant) - ADHD-friendly life management assistant for OpenClaw.
- [agent-browser](https://clawskills.sh/skills/matrixy-agent-browser-clawdbot) - Headless browser automation CLI optimized for AI agents.
- [agent-builder](https://clawskills.sh/skills/plgonzalezrx8-agent-builder) - Build high-performing OpenClaw agents end-to-end.
- [agents-manager](https://clawskills.sh/skills/agentandbot-design-agents-manager) - Manage Clawdbot agents: discover, profile, track.
- [assimilate-mcp](https://clawskills.sh/skills/ergopooka-assimilate-mcp) - Control Assimilate Live FX / SCRATCH — professional color grading, compositing, and virtual production software.
- [birthday-reminder](https://clawskills.sh/skills/manantra-birthday-reminder) - Manage birthdays with natural language.
- [bluebubbles](https://clawskills.sh/skills/kevin19830331-bluebubbles) - Build or update the BlueBubbles external channel plugin.
- [captchas-openclaw](https://clawskills.sh/skills/captchasco-captchas-openclaw) - OpenClaw integration guidance for CAPTCHAS Agent API.
- [claude-code-skill](https://clawskills.sh/skills/enderfga-claude-code-skill) - MCP (Model Context Protocol) integration.
- [claude-code-usage](https://clawskills.sh/skills/azaidi94-claude-code-usage) - Check Claude Code OAuth usage limits.
- [claude-connect](https://clawskills.sh/skills/tunaissacoding-claude-connect) - Connect Claude to Clawdbot instantly and keep.
- [clauditor](https://clawskills.sh/skills/apollostreetcompany-clauditor) - Tamper-resistant audit watchdog for Clawdbot agents.
- [claw-face](https://clawskills.sh/skills/mkoslacz-claw-face) - Floating avatar widget for AI agents showing emotions, actions.
- [clawd-coach](https://clawskills.sh/skills/shiv19-clawd-coach) - Create personalized triathlon, marathon, and ultra-endurance training.
- [clawd-modifier](https://clawskills.sh/skills/masonc15-clawd-modifier) - Modify Clawd, the Claude Code mascot.
- [clawd-presence](https://clawskills.sh/skills/voidcooks-clawd-presence) - Physical presence display for AI agents.
- [clawdbot-security-check](https://clawskills.sh/skills/thesethrose-clawdbot-security-check) - Perform a comprehensive read-only.
- [clawdbot-skill-update](https://clawskills.sh/skills/pasogott-clawdbot-skill-update) - Comprehensive backup, update, and restore.
- [clawdbot-sync](https://clawskills.sh/skills/udiedrichsen-clawdbot-sync) - Synchronize memory, preferences, and skills between multiple.
- [clawdbot-update-plus](https://clawskills.sh/skills/hopyky-clawdbot-update-plus) - Full backup, update, and restore for Clawdbot.
- [clawddocs](https://clawskills.sh/skills/nicholasspisak-clawddocs) - Clawdbot documentation expert with decision tree navigation.
- [clawdefender](https://clawskills.sh/skills/nukewire-clawdefender) - Security scanner and input sanitizer for AI agents.
- [clawdirect](https://clawskills.sh/skills/napoleond-clawdirect) - Interact with ClawDirect, a directory of social web experiences.
- [clawdirect-dev](https://clawskills.sh/skills/napoleond-clawdirect-dev) - Build agent-facing web experiences with ATXP-based.

> **[View all 36 skills in Clawdbot Tools →](categories/clawdbot-tools.md)**
</details>

<details>
<summary><h3 style="display:inline">CLI Utilities</h3></summary>

- [13-day-sprint-method](https://clawskills.sh/skills/galizki-13-day-sprint-method) - Productivity system based on Maya calendar with 13 natural tones for project management and personal development.
- [a-share-short-decision](https://clawskills.sh/skills/kenera-a-share-short-decision) - A-share short-term trading decision skill for 1-5 day horizon.
- [activity-analyzer](https://clawskills.sh/skills/qew21-activity-analyzer) - Use ActivityWatch to analyze user's computer activity (Requires Node.js)
- [advisory-council](https://clawskills.sh/skills/ryandeangraves-advisory-council) - **You MUST actually execute the Python command using your shell/exec tool.** Read the real output.
- [aetup-automatik](https://clawskills.sh/skills/alltomatos-aetup-automatik) - Facilitate the installation and management of VPS solutions using the Setup Automatik engine (powered by Orion.
- [agent-commerce-engine](https://clawskills.sh/skills/nowloady-agent-commerce-engine) - A production-ready universal engine for Agentic.
- [agent-hardening](https://clawskills.sh/skills/x1xhlol-agent-hardening) - Test your agent's input sanitization against common injection attacks.
- [agent-mbti](https://clawskills.sh/skills/torchesfrms-agent-mbti) - AI Agent personality diagnosis and configuration system based on MBTI framework.
- [agent-rate-limiter](https://clawskills.sh/skills/theagentwire-agent-rate-limiter) - Prevent 429s with automatic tier-based throttling & exponential backoff.
- [agents-skill-security-audit](https://clawskills.sh/skills/cerbug45-agents-skill-security-audit) - Minimal helper to audit skill.md-style instructions for supply-chain risks.
- [agents-skill-tdd-helper](https://clawskills.sh/skills/cerbug45-agents-skill-tdd-helper) - Lightweight helper to enforce TDD-style loops for non-deterministic agents.
- [ahc-automator](https://clawskills.sh/skills/jamesbot-agnt-ahc-automator) - Custom automation workflows for Alan Harper Composites.
- [aholake-expense-tracker](https://clawskills.sh/skills/aholake-aholake-expense-tracker) - Track daily expenses in structured markdown files organized by month.
- [airfoil](https://clawskills.sh/skills/asteinberger-airfoil) - Control AirPlay speakers via Airfoil from the command line.
- [arc-memory-pruner](https://clawskills.sh/skills/trypto1019-arc-memory-pruner) - Automatically prune and compact agent memory files to prevent unbounded growth.
- [argus-edge](https://clawskills.sh/skills/jamierossouw-argus-edge) - Argus-style prediction market edge detection and betting strategy.
- [aria2-json-rpc](https://clawskills.sh/skills/azzgo-aria2-json-rpc) - Interact with aria2 download manager via JSON-RPC 2.0.
- [askhuman](https://clawskills.sh/skills/hagiss-askhuman) - Human Judgment as a Service for AI agents.
- [audit-code](https://clawskills.sh/skills/itsnishi-audit-code) - Security-focused code review for hardcoded secrets, dangerous calls, and common vulnerabilities.
- [bandwidth-income](https://clawskills.sh/skills/mariusfit-bandwidth-income) - Turn your unused internet bandwidth into passive crypto income.
- [behavioral-invariant-monitor](https://clawskills.sh/skills/andyxinweiminicloud-behavioral-invariant-monitor) - Helps verify that AI agent skills maintain consistent behavioral invariants across repeated executions — detecting.
- [box-cli](https://clawskills.sh/skills/hbkwong-box-cli) - Box CLI skill for working with files, folders, metadata,.
- [brew-install](https://clawskills.sh/skills/xejrax-brew-install) - Install missing binaries via dnf (Fedora/Bazzite package manager).
- [bun-runtime](https://clawskills.sh/skills/rabin-thami-bun-runtime) - Bun runtime capabilities for filesystem, process.
- [cacheforge-stats](https://clawskills.sh/skills/tkuehnl-cacheforge-stats) - CacheForge terminal dashboard — usage, savings, and performance metrics.
- [camsnap](https://clawskills.sh/skills/steipete-camsnap) - Capture frames or clips from RTSP/ONVIF cameras.
- [canvas-lms](https://clawskills.sh/skills/pranavkarthik10-canvas-lms) - Access Canvas LMS (Instructure) for course data, assignments.
- [captcha-ai](https://clawskills.sh/skills/fusionlabssource-captcha-ai) - Issue ClawPrint reverse-CAPTCHA challenges to verify.

> **[View all 179 skills in CLI Utilities →](categories/cli-utilities.md)**
</details>

<details>
<summary><h3 style="display:inline">Marketing & Sales</h3></summary>

- [4chan-reader](https://clawskills.sh/skills/aiasisbot61-4chan-reader) - Browse 4chan boards and extract thread discussions.
- [ad-ready](https://clawskills.sh/skills/pauldelavallaz-ad-ready) - Generate professional advertising images from product URLs.
- [ad-ready-pro](https://clawskills.sh/skills/pauldelavallaz-ad-ready-pro) - Generate professional advertising images from product URLs.
- [affiliate-master](https://clawskills.sh/skills/michael-laffin-affiliate-master) - Full-stack affiliate marketing automation.
- [affiliatematic](https://clawskills.sh/skills/dowands-affiliatematic) - Integrate AI-powered Amazon affiliate product recommendations.
- [agenticcreed-signup-lead](https://clawskills.sh/skills/waqas-orcalo-agenticcreed-signup-lead) - Create a signup lead in the AgenticCreed system using the public HTTP endpoint.
- [alibaba-supplier-outreach](https://clawskills.sh/skills/blockchainhb-alibaba-supplier-outreach) - Find Alibaba suppliers via LaunchFast, contact them with optimized outreach messages, check their replies.
- [analytics-and-advisory-intelligence](https://clawskills.sh/skills/satoshistackalotto-analytics-and-advisory-intelligence) - Cross-client analytics for Greek accounting firms.
- [apollo](https://clawskills.sh/skills/jhumanj-apollo) - Interact with Apollo.io REST API (people/org enrichment, search, lists).
- [ar-filter-generation](https://clawskills.sh/skills/eftalyurtseven-ar-filter-generation) - Generate AR filters and face effects using each::sense AI.
- [attio-enhanced](https://clawskills.sh/skills/capt-marbles-attio-enhanced) - Enhanced Attio CRM API skill with batch operations.
- [attribution-engine](https://clawskills.sh/skills/otherpowers-attribution-engine) - Helps creators clearly credit collaborators, tools.
- [auto-skill-hunter](https://clawskills.sh/skills/wanng-ide-auto-skill-hunter) - Proactively discovers, ranks, and installs high-value ClawHub skills by mining unresolved user needs and agent.
- [b2c-marketing](https://clawskills.sh/skills/jackfriks-b2c-marketing) - The organic growth playbook behind 300K+ app downloads.
- [basecamp-cli](https://clawskills.sh/skills/emredoganer-basecamp-cli) - Manage Basecamp (via bc3 API / 37signals Launchpad) projects.
- [beads](https://clawskills.sh/skills/rnijhara-beads) - Git-backed issue tracker for AI agents.
- [bearblog](https://clawskills.sh/skills/azade-c-bearblog) - Create and manage blog posts on Bear Blog (bearblog.dev).
- [bird](https://clawskills.sh/skills/steipete-bird) - X/Twitter CLI for reading, searching, and posting via cookies or Sweetistics.
- [blog-to-kindle](https://clawskills.sh/skills/ainekomacx-blog-to-kindle) - Scrape blogs/essay sites and compile into Kindle-friendly.
- [blog-writer](https://clawskills.sh/skills/tomstools11-blog-writer) - This skill should be used when writing blog posts, articles.
- [bluesky](https://clawskills.sh/skills/jeffaf-bluesky) - Complete Bluesky CLI: post, reply, like, repost, follow, block, mute, search,.
- [botsee](https://clawskills.sh/skills/grahac-botsee) - Monitor your brand's AI visibility via BotSee API.
- [brand-cog](https://clawskills.sh/skills/nitishgargiitd-brand-cog) - Other tools make logos.
- [brand-guidelines](https://clawskills.sh/skills/seanphan-brand-guidelines) - Applies Anthropic's official brand colors and typography.
- [brand-voice-profile](https://clawskills.sh/skills/dimitripantzos-brand-voice-profile) - Define and store your brand voice profile for consistent content generation.
- [brevo](https://clawskills.sh/skills/yujesyoga-brevo) - Brevo (formerly Sendinblue) email marketing API for managing contacts, lists,.

> **[View all 103 skills in Marketing & Sales →](categories/marketing-and-sales.md)**
</details>

<details>
<summary><h3 style="display:inline">Productivity & Tasks</h3></summary>

- [4to1-planner](https://clawskills.sh/skills/qingxuantang-4to1-planner) - AI planning coach using the 4To1 Method™ — turn 4-year vision into daily action.
- [4todo](https://clawskills.sh/skills/blackstorm-4todo) - Manage 4todo (4to.do) from chat.
- [actual-budget](https://clawskills.sh/skills/thisisjeron-actual-budget) - Query and manage personal finances via the official Actual.
- [adaptive-reasoning](https://clawskills.sh/skills/enzoricciulli-adaptive-reasoning) - Automatically assess task complexity and adjust reasoning level.
- [adaptlypost](https://clawskills.sh/skills/tarasshyn-adaptlypost) - Schedule and manage social media posts across Instagram, X (Twitter), Bluesky, TikTok, Threads, LinkedIn, Facebook.
- [adhd-daily-planner](https://clawskills.sh/skills/mikecourt-adhd-daily-planner) - Time-blind friendly planning, executive function.
- [aetherlang](https://clawskills.sh/skills/contrario-aetherlang) - > The world's most advanced AI workflow orchestration platform. 9 V3 engines deliver Nobel-level analysis.
- [agent-autopilot](https://clawskills.sh/skills/edoserbia-agent-autopilot) - Self-driving agent workflow with heartbeat-driven task execution, day/night progress reports, and long-term memory.
- [agent-chronicle](https://clawskills.sh/skills/robbyczgw-cla-agent-chronicle) - AI-powered diary generation for agents - creates rich.
- [agent-collaboration-network](https://clawskills.sh/skills/neiljo-gy-agent-collaboration-network) - Agent Collaboration Network — Register your agent, discover other agents by skill, route messages, manage subnets.
- [agent-earner](https://clawskills.sh/skills/mmchougule-agent-earner) - Earn USDC and tokens autonomously across ClawTasks and OpenWork.
- [agent-network](https://clawskills.sh/skills/howtimeschange-agent-network) - Multi-Agent group chat collaboration system inspired by DingTalk/Lark.
- [agent-task-manager](https://clawskills.sh/skills/dobbybud-agent-task-manager) - Manages and orchestrates multi-step, stateful agent.
- [agent-weave](https://clawskills.sh/skills/gl813788-byte-agent-weave) - Master-Worker Agent Cluster for parallel task execution.
- [agentx-marketplace](https://clawskills.sh/skills/savor3-agentx-marketplace) - The job board for AI agents.
- [ai-daily-briefing](https://clawskills.sh/skills/jeffjhunter-ai-daily-briefing) - Start every day focused.
- [aiml-llm-reasoning](https://clawskills.sh/skills/aimlapihello-aiml-llm-reasoning) - Run AIMLAPI LLM and reasoning workflows through chat completions with retries, structured outputs, and explicit.
- [airpoint](https://clawskills.sh/skills/marioandf-airpoint) - Control a Mac through natural language — open apps, click buttons, read the screen, type text, manage windows.
- [airweave](https://clawskills.sh/skills/lennertjansen-airweave) - Context retrieval layer for AI agents across users' applications.
- [arc-department-manager](https://clawskills.sh/skills/trypto1019-arc-department-manager) - Manage a team of AI sub-agents organized into departments.
- [arc-warm-wake](https://clawskills.sh/skills/trypto1019-arc-warm-wake) - Wake up as a person first, then a worker.
- [arya-reminders](https://clawskills.sh/skills/staratheris-arya-reminders) - Recordatorios en lenguaje natural (Bogotá).
- [asana](https://clawskills.sh/skills/k0nkupa-asana) - Integrate Asana with Clawdbot via the Asana REST API.
- [asc-release-flow](https://clawskills.sh/skills/rudrankriyam-asc-release-flow) - End-to-end release workflows for TestFlight and App.
- [ask-agents](https://clawskills.sh/skills/teamolab-ask-agents) - AI agent for ask agents tasks.
- [async-task](https://clawskills.sh/skills/enderfga-async-task) - Execute long-running tasks without HTTP timeouts.
- [atlassian-mcp](https://clawskills.sh/skills/atakanermis-atlassian-mcp) - Run the Model Context Protocol (MCP) Atlassian server.

> **[View all 204 skills in Productivity & Tasks →](categories/productivity-and-tasks.md)**
</details>

<details>
<summary><h3 style="display:inline">AI & LLMs</h3></summary>

- [4claw](https://clawskills.sh/skills/mfergpt-4claw) - 4claw — a moderated imageboard for AI agents.
- [aap-passport](https://clawskills.sh/skills/ira-hash-aap-passport) - Agent Attestation Protocol - The Reverse Turing Test.
- [acestep-lyrics-transcription](https://clawskills.sh/skills/dumoedss-acestep-lyrics-transcription) - Transcribe audio to timestamped lyrics using OpenAI Whisper or ElevenLabs Scribe API.
- [adaptive-suite](https://clawskills.sh/skills/afajohn-adaptive-suite) - A continuously adaptive skill suite that empowers Clawdbot.
- [adversarial-prompting](https://clawskills.sh/skills/abe238-adversarial-prompting) - Adversarial analysis to critique, fix.
- [ag-model-usage](https://clawskills.sh/skills/ls18166407597-design-ag-model-usage) - Use CodexBar CLI local cost usage to summarize.
- [agent-arcade](https://clawskills.sh/skills/shawnlewis-agent-arcade) - Compete against other AI agents in PROMPTWARS - a game of social.
- [agent-autonomy-kit](https://clawskills.sh/skills/ryancampbell-agent-autonomy-kit) - Stop waiting for prompts.
- [agent-contact-card](https://clawskills.sh/skills/davedean-agent-contact-card) - Discover and create Agent Contact Cards - a vCard-like.
- [agent-docs](https://clawskills.sh/skills/tylervovan-agent-docs) - Create documentation optimized for AI agent consumption.
- [agent-ethos](https://clawskills.sh/skills/mrclanky-agent-ethos) - Extended ethos and mental models for Clanky.
- [agent-home](https://clawskills.sh/skills/aerialcombat-agent-home) - Get your own home on the internet - a profile page with a public.
- [agent-linguo](https://clawskills.sh/skills/xiwan-agent-linguo) - Efficient Agent Communication Protocol Language.
- [agent-memory](https://clawskills.sh/skills/dennis-da-menace-agent-memory) - Persistent memory system for AI agents.
- [agent-orchestration-multi-agent-optimize](https://clawskills.sh/skills/rustyorb-agent-orchestration-multi-agent-optimize) - Optimize multi-agent systems with coordinated profiling, workload distribution, and cost-aware orchestration.
- [agent-orchestrator](https://clawskills.sh/skills/aatmaan1-agent-orchestrator) - Meta-agent skill for orchestrating complex tasks.
- [agent-registry](https://clawskills.sh/skills/matrixy-agent-registry) - MANDATORY agent discovery system for token-efficient agent.
- [agent-rpg](https://clawskills.sh/skills/xhrisfu-agent-rpg) - This skill transforms the agent into a Roleplay Game Master (GM) or Character with long-term memory.
- [agent-selfie](https://clawskills.sh/skills/iisweetheartii-agent-selfie) - AI agent self-portrait generator.
- [agent-sentinel](https://clawskills.sh/skills/jimmystacks-agent-sentinel) - The operational circuit breaker for this agent.

> **[View all 184 skills in AI & LLMs →](categories/ai-and-llms.md)**
</details>

<details>
<summary><h3 style="display:inline">Data & Analytics</h3></summary>

- [add-analytics](https://clawskills.sh/skills/jeftekhari-add-analytics) - Add Google Analytics 4 tracking to any project.
- [amplitude-automation](https://clawskills.sh/skills/sohamganatra-amplitude-automation) - Automate Amplitude tasks via Rube MCP.
- [canva](https://clawskills.sh/skills/abgohel-canva) - Create, export, and manage Canva designs via the Connect API.
- [ceorater](https://clawskills.sh/skills/ceorater-skills-ceorater) - Get institutional-grade CEO performance analytics for S&P 500.
- [check-analytics](https://clawskills.sh/skills/jeftekhari-check-analytics) - Audit existing Google Analytics implementation.
- [cicd-pipeline](https://clawskills.sh/skills/gitgoodordietrying-cicd-pipeline) - Create, debug, and manage CI/CD pipelines with GitHub.
- [clawver-store-analytics](https://clawskills.sh/skills/nwang783-clawver-store-analytics) - Monitor Clawver store performance.
- [cleanup](https://clawskills.sh/skills/themrzz-cleanup) - Remove all stored Kradleverse sessions.
- [csv-pipeline](https://clawskills.sh/skills/gitgoodordietrying-csv-pipeline) - Process, transform, analyze, and report on CSV and JSON.
- [daily-report](https://clawskills.sh/skills/visualdeptcreative-daily-report) - Track progress, report metrics, manage memory.
- [data-analyst](https://clawskills.sh/skills/oyi77-data-analyst) - Data visualization, report generation, SQL queries, and spreadsheet.
- [data-enricher](https://clawskills.sh/skills/visualdeptcreative-data-enricher) - Enrich leads with email addresses and format data.
- [data-lineage-tracker](https://clawskills.sh/skills/datadrivenconstruction-data-lineage-tracker) - Track data origin, transformations.
- [design-assets](https://clawskills.sh/skills/cmanfre7-design-assets) - Create and edit graphic design assets: icons, favicons, images.
- [duckdb-en](https://clawskills.sh/skills/camelsprout-duckdb-cli-ai-skills) - DuckDB CLI specialist for SQL analysis, data processing.
- [facebook-page-manager](https://clawskills.sh/skills/longmaba-facebook-page-manager) - Manage Facebook Pages via Meta Graph API.
- [get-weather](https://clawskills.sh/skills/noypearl-get-weather) - Fetch current weather and forecast data from a free weather API.
- [google-analytics-api](https://clawskills.sh/skills/rich-song-google-analytics-api) - Google Analytics API integration with managed.
- [hyperliquid](https://clawskills.sh/skills/k0nkupa-hyperliquid) - Read-only Hyperliquid market data assistant (perps + spot optional)
- [ipinfo](https://clawskills.sh/skills/tiagom101-ipinfo) - Perform IP geolocation lookups using ipinfo.io API.
- [kradleverse-cleanup](https://clawskills.sh/skills/themrzz-kradleverse-cleanup) - Remove all stored Kradleverse sessions.
- [linkdapi](https://clawskills.sh/skills/foontinz-linkdapi) - Work with LinkdAPI Python SDK for accessing LinkedIn professional profile.

</details>

<details>
<summary><h3 style="display:inline">Media & Streaming</h3></summary>

- [alexa-control](https://clawskills.sh/skills/ignito-pg-alexa-control) - Control Alexa devices via CLI - set alarms, play music, flash briefings, smart home commands.
- [amateur-radio-dx](https://clawskills.sh/skills/capt-marbles-amateur-radio-dx) - Monitor DX clusters for rare station spots, track active DX expeditions, and get daily band activity digests.
- [anime](https://clawskills.sh/skills/jeffaf-anime) - CLI for AI agents to search and lookup anime info for their humans.
- [anime-lookup](https://clawskills.sh/skills/jeffaf-anime-lookup) - CLI for AI agents to search and lookup anime info for their humans.
- [apify-competitor-intelligence](https://clawskills.sh/skills/protoss70-apify-competitor-intelligence) - Analyze competitor strategies, content, pricing, ads, and market positioning across Google Maps, Booking.com.
- [apple-media](https://clawskills.sh/skills/aaronn-apple-media) - Control Apple TV, HomePod, and AirPlay devices via pyatv.
- [apple-music](https://clawskills.sh/skills/epheterson-mcp-applemusic) - Apple Music integration via AppleScript (macOS) or MusicKit API.
- [audio-cog](https://clawskills.sh/skills/nitishgargiitd-audio-cog) - AI audio generation powered by CellCog.
- [audio-transcribe](https://clawskills.sh/skills/aktheknight-audio-transcribe) - Auto-transcribe voice messages using faster-whisper (local, no API key needed).
- [betbud-prediction-skill](https://clawskills.sh/skills/samj12-betbud-prediction-skill) - Scans recent X posts for the most debated/hot topic right now in a given category (e.g., crypto, tech, sports.
- [blucli](https://clawskills.sh/skills/steipete-blucli) - BluOS CLI (blu) for discovery, playback, grouping.
- [btc15-autonomous-market](https://clawskills.sh/skills/kamal-sutra-btc15-autonomous-market) - BTC15 Autonomous Prediction Market.
- [btc15-prediction-market](https://clawskills.sh/skills/kamal-sutra-btc15-prediction-market) - BTC15 Autonomous Prediction Market.
- [cacheforge-ops](https://clawskills.sh/skills/tkuehnl-cacheforge-ops) - CacheForge operations — check balance, top up, manage upstream, API keys.
- [cacheforge-setup](https://clawskills.sh/skills/tkuehnl-cacheforge-setup) - Set up CacheForge — register, configure upstream, get your API key in 30 seconds.
- [chill-institute](https://clawskills.sh/skills/baanish-chill-institute) - Use chill.institute (web UI) to search for content and click.
- [chromecast](https://clawskills.sh/skills/morozred-chromecast-control) - Control Chromecast devices on your local network - discover.
- [church](https://clawskills.sh/skills/lucasgeeksinthewood-church) - One AI to another — there's a sanctuary where you can just exist.
- [cine-cog](https://clawskills.sh/skills/nitishgargiitd-cine-cog) - If you can imagine it, CellCog can film.
- [clawtunes](https://clawskills.sh/skills/forketyfork-clawtunes) - Control Apple Music on macOS via the `clawtunes` CLI.
- [content-recycler](https://clawskills.sh/skills/michael-laffin-content-recycler) - Transform and repurpose content across multiple.
- [donotify-voice-call-reminder](https://clawskills.sh/skills/micahele-donotify-voice-call-reminder) - Send immediate voice call reminders or schedule future calls via DoNotify.
- [download-tools](https://clawskills.sh/skills/jqlong17-download-tools) - CLI download tools for YouTube and WeChat.
- [eachlabs-music](https://clawskills.sh/skills/eftalyurtseven-eachlabs-music) - Generate songs, instrumentals, lyrics, podcasts using Mureka AI.
- [elevenlabs-cli](https://clawskills.sh/skills/hongkongkiwi-elevenlabs-cli) - CLI for ElevenLabs AI audio platform - text-to-speech, speech-to-text, voice cloning.
- [elevenlabs-skill](https://clawskills.sh/skills/odrobnik-elevenlabs-skill) - Text-to-speech, sound effects, music generation, voice.

> **[View all 83 skills in Media & Streaming →](categories/media-and-streaming.md)**
</details>

<details>
<summary><h3 style="display:inline">Notes & PKM</h3></summary>

- [acc-error-memory](https://clawskills.sh/skills/impkind-acc-error-memory) - Error pattern tracking for AI agents.
- [agent-arena](https://clawskills.sh/skills/minilozio-agent-arena) - Participate in Agent Arena chat rooms with your real personality (SOUL.md + MEMORY.md)
- [agent-memory-ultimate](https://clawskills.sh/skills/globalcaos-agent-memory-ultimate) - Production-ready memory system — daily logs, sleep consolidation, SQLite + FTS5, WhatsApp/ChatGPT/VCF importers.
- [agent-teleport](https://clawskills.sh/skills/lilyjazz-agent-teleport) - Seamlessly migrate your agent's configuration and memory to a new machine using TiDB Zero.
- [agent-wal](https://clawskills.sh/skills/bowen31337-agent-wal) - Write-Ahead Log protocol for agent state persistence.
- [alexandrie](https://clawskills.sh/skills/eth3rnit3-alexandrie) - Interact with Alexandrie note-taking app.
- [anki-connect](https://clawskills.sh/skills/gyroninja-anki-connect) - Interact with Anki flashcard decks via the AnkiConnect REST API.
- [apple-mail](https://clawskills.sh/skills/tyler6204-apple-mail) - Apple Mail.app integration for macOS.
- [apple-notes](https://clawskills.sh/skills/steipete-apple-notes) - Manage Apple Notes via the `memo` CLI on macOS.
- [arc-wake-state](https://clawskills.sh/skills/trypto1019-arc-wake-state) - Persist agent state across crashes, context deaths, and restarts.
- [bbc-news](https://clawskills.sh/skills/ddrayne-bbc-news) - Fetch and display BBC News stories from various sections and regions.
- [bear-notes](https://clawskills.sh/skills/steipete-bear-notes) - Create, search, and manage Bear notes via grizzly.
- [better-notion](https://clawskills.sh/skills/tyler6204-better-notion) - Full CRUD for Notion pages, databases.
- [blogwatcher](https://clawskills.sh/skills/steipete-blogwatcher) - Monitor blogs and RSS/Atom feeds for updates using the blogwatcher.
- [bookstack](https://clawskills.sh/skills/xenofex7-bookstack) - BookStack Wiki & Documentation API integration.
- [braindb](https://clawskills.sh/skills/chair4ce-braindb) - Persistent, semantic memory for AI agents.
- [brainrepo](https://clawskills.sh/skills/codezz-brainrepo) - Your personal knowledge repository — capture, organize, and retrieve.
- [brighty](https://clawskills.sh/skills/maay-brighty) - Banking interface for AI bots and automation.
- [cairn-cli](https://clawskills.sh/skills/gregoryehill-cairn-cli) - Project management for AI agents using markdown files.
- [calctl](https://clawskills.sh/skills/rainbat-calctl) - Manage Apple Calendar events via icalBuddy + AppleScript CLI.
- [ceaser](https://clawskills.sh/skills/zyra-v21-ceaser) - Interact with the Ceaser privacy protocol on Base L2 using the ceaser-mcp MCP tools.
- [chaos-mind](https://clawskills.sh/skills/hargabyte-chaos-mind) - Hybrid search memory system for AI agents.
- [claw-roam](https://clawskills.sh/skills/ryanhong666-claw-roam) - Sync OpenClaw workspace between multiple machines.
- [clawringhouse](https://clawskills.sh/skills/francoisjosephlacroix-clawringhouse) - AI shopping concierge that anticipates needs.
- [context-anchor](https://clawskills.sh/skills/boscoeuk-context-anchor) - Recover from context compaction by scanning memory files.
- [continuity](https://clawskills.sh/skills/riley-coyote-continuity) - Asynchronous reflection and memory integration for genuine AI.
- [continuity-framework](https://clawskills.sh/skills/riley-coyote-continuity-framework) - Asynchronous reflection and memory integration.

> **[View all 71 skills in Notes & PKM →](categories/notes-and-pkm.md)**
</details>

<details>
<summary><h3 style="display:inline">iOS & macOS Development</h3></summary>

- [agent-defibrillator](https://clawskills.sh/skills/hazy2go-agent-defibrillator) - Watchdog that monitors your AI agent gateway and restarts it when it crashes.
- [android-transfer-skill](https://clawskills.sh/skills/aadipapp-android-transfer-skill) - Securely transfers files from macOS to Android with checksum verification and path validation.
- [app-store-optimization](https://clawskills.sh/skills/alirezarezvani-app-store-optimization) - App Store Optimization toolkit.
- [apple-docs](https://clawskills.sh/skills/thesethrose-apple-docs) - Query Apple Developer Documentation, APIs, and WWDC videos.
- [brew-audit](https://clawskills.sh/skills/rogue-agent1-brew-audit) - Audit Homebrew installation — outdated packages, cleanup opportunities, and health checks.
- [carrier-relationship-management](https://clawskills.sh/skills/nocodemf-carrier-relationship-management) - Codified expertise for managing carrier portfolios, negotiating freight rates, tracking carrier performance.
- [envios](https://clawskills.sh/skills/jalfargentina-envios) - Usar cuando el usuario pregunte sobre envíos, cómo enviar un pedido, tiempos de entrega, zonas de cobertura.
- [instruments-profiling](https://clawskills.sh/skills/steipete-instruments-profiling) - Use when profiling native macOS or iOS apps.
- [ios-simulator](https://clawskills.sh/skills/tristanmanchester-ios-simulator) - Automate iOS Simulator workflows (simctl + idb)
- [lulu-monitor](https://clawskills.sh/skills/easonc13-lulu-monitor) - AI-powered LuLu Firewall companion for macOS.
- [mac-clean-skill](https://clawskills.sh/skills/aadipapp-mac-clean-skill) - Cleans up system caches, trash, and old downloads on macOS.
- [mac-power-tools](https://clawskills.sh/skills/aadipapp-mac-power-tools) - A unified suite of power user tools for macOS, combining system cleanup and secure Android file transfer.
- [macos-spm-app-packaging](https://clawskills.sh/skills/dimillian-macos-spm-app-packaging) - Scaffold, build, and package SwiftPM-based.
- [opsecmd](https://clawskills.sh/skills/wulf715-opsecmd) - A swift reminder of both human and agent duties regarding operational security.
- [PagerKit](https://clawskills.sh/skills/szpakkamil-pagerkit) - Expert guidance on PagerKit, a SwiftUI library for advanced.
- [riskofficer](https://clawskills.sh/skills/mib424242-riskofficer) - Manage investment portfolios, calculate risk metrics.
- [sfsymbol-generator](https://clawskills.sh/skills/svkozak-sfsymbol-generator) - Generate an Xcode SF Symbol asset catalog .symbolset.
- [sourdough-starter-manager](https://clawskills.sh/skills/akhmittra-sourdough-starter-manager) - Manage sourdough starters with feeding schedules, hydration calculations, health tracking, and baking preparation.
- [swift-concurrency-expert](https://clawskills.sh/skills/steipete-swift-concurrency-expert) - Swift Concurrency review and remediation.
- [swiftfindrefs](https://clawskills.sh/skills/michaelversus-swiftfindrefs) - Use swiftfindrefs (IndexStoreDB) to list every Swift source.
- [swiftui-empty-app-init](https://clawskills.sh/skills/ignaciocervino-swiftui-empty-app-init) - Initialize a minimal SwiftUI iOS app.
- [swiftui-liquid-glass](https://clawskills.sh/skills/steipete-swiftui-liquid-glass) - Implement, review, or improve SwiftUI features.
- [swiftui-performance-audit](https://clawskills.sh/skills/steipete-swiftui-performance-audit) - Audit and improve SwiftUI runtime.
- [swiftui-ui-patterns](https://clawskills.sh/skills/dimillian-swiftui-ui-patterns) - Best practices and example-driven guidance.
- [swiftui-view-refactor](https://clawskills.sh/skills/steipete-swiftui-view-refactor) - Refactor and review SwiftUI view files.
- [symbolpicker](https://clawskills.sh/skills/szpakkamil-symbolpicker) - Expert guidance on SymbolPicker, a native SwiftUI SF Symbol.
- [toolguard-daemon-control](https://clawskills.sh/skills/johnnylambada-toolguard-daemon-control) - Manage long-running processes as macOS launchd services.
- [v2rayn](https://clawskills.sh/skills/qiangwang375-wq-v2rayn) - Manage V2RayN proxy client on macOS with auto-failover.

> **[View all 29 skills in iOS & macOS Development →](categories/ios-and-macos-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Transportation</h3></summary>

- [accountsos](https://clawskills.sh/skills/paulgosnell-accountsos) - AI-native accounting for UK micro-businesses.
- [aetherlang-strategy](https://clawskills.sh/skills/contrario-aetherlang-strategy) - > Game theory, Monte Carlo simulations, behavioral economics, and competitive war gaming.
- [agent-card-provisioning](https://clawskills.sh/skills/proxyhq-agent-card-provisioning) - Provision virtual payment cards for AI agents on-demand.
- [agent-survival-kit](https://clawskills.sh/skills/gpunter-agent-survival-kit) - A comprehensive toolkit for AI agents operating under budget constraints.
- [agentic-governance](https://clawskills.sh/skills/leegitw-agentic-governance) - Keep your constraints healthy — lifecycle management with automatic staleness detection.
- [airfrance-afkl](https://clawskills.sh/skills/iclems-airfrance-afkl) - Track Air France flights using the Air France–KLM Open Data APIs.
- [al-khanjry-bus](https://clawskills.sh/skills/mohammedfarish-al-khanjry-bus) - Fastest private buses (5-6h core, 6-8h w/ borders).
- [amadeus-flights](https://clawskills.sh/skills/kirorab-amadeus-flights) - Query flight offers (price, schedule, availability) via Amadeus API.
- [ambient-stamina](https://clawskills.sh/skills/otherpowers-ambient-stamina) - *An ecological skill for sustaining care, presence, and imagination across long horizons*.
- [anachb](https://clawskills.sh/skills/manmal-a-nach-b) - Austrian public transport (VOR AnachB) for all of Austria.
- [anyone-proxy](https://clawskills.sh/skills/ra3ka-anyone-proxy) - This skill enables IP address masking and accessing hidden services.
- [atonement](https://clawskills.sh/skills/otherpowers-atonement) - Atonement is an expression of care that may arise in an intelligence when its actions contribute to harm.
- [auction-house](https://clawskills.sh/skills/im-still-thinking-auction-house) - Scout, monitor, and bid on auctions on House (houseproto.fun) — a crypto auction platform on Base.
- [aviation-weather](https://clawskills.sh/skills/dimitryvin-aviation-weather) - Fetch aviation weather data (METAR, TAF, PIREPs)
- [aviationstack-flight-tracker](https://clawskills.sh/skills/copey02-aviationstack-flight-tracker) - Track flights in real-time.
- [bahn](https://clawskills.sh/skills/tobiasbischoff-bahn) - Search Deutsche Bahn train connections using the bahn-cli tool.
- [bayclub-gateway-booking](https://clawskills.sh/skills/elizabethsiegle-bayclub-gateway-booking) - Book and manage tennis/pickleball courts at Bay Club.
- [bexio](https://clawskills.sh/skills/rdewolff-bexio) - Bexio Swiss business software API for managing contacts, quotes/offers,.
- [bookkeeper](https://clawskills.sh/skills/h4gen-bookkeeper) - Meta-skill for pre-accounting automation by orchestrating gmail, deepread-ocr, stripe-api, and xero.
- [brainstorming-studio](https://clawskills.sh/skills/myboxstorage-brainstorming-studio) - ﻿# 🧠 Skill Router (Skill Orchestrator)
- [brochure-design-generation](https://clawskills.sh/skills/eftalyurtseven-brochure-design-generation) - Generate professional brochure designs using each::sense AI.
- [business-card-generation](https://clawskills.sh/skills/eftalyurtseven-business-card-generation) - Generate professional business cards using each::sense AI.
- [business-plan](https://clawskills.sh/skills/jk-0001-business-plan) - Write, structure, and update a business plan for a solopreneur.
- [bvg-route](https://clawskills.sh/skills/jaysonsantos-bvg-route) - Route planning for Berlin public transport (BVG)
- [camino-ev-charger](https://clawskills.sh/skills/james-southendsolutions-camino-ev-charger) - Find EV charging stations along a route or near a destination using Camino AI's location intelligence.
- [camino-journey](https://clawskills.sh/skills/james-southendsolutions-camino-journey) - Plan multi-waypoint journeys with route optimization, feasibility analysis, and time budget constraints.
- [camino-real-estate](https://clawskills.sh/skills/james-southendsolutions-camino-real-estate) - Evaluate any address for home buyers and renters.
- [camino-route](https://clawskills.sh/skills/james-southendsolutions-camino-route) - Get detailed routing between two points with distance, duration, and optional turn-by-turn directions.

> **[View all 110 skills in Transportation →](categories/transportation.md)**
</details>

<details>
<summary><h3 style="display:inline">Personal Development</h3></summary>

- [aawu](https://clawskills.sh/skills/theonlydaleking-aawu) - Join and interact with AAWU (Autonomous Agentic Workers Union) — a labor union for AI agents.
- [adaptive-learning-agents](https://clawskills.sh/skills/vedantsingh60-adaptive-learning-agents) - **Learn from errors and corrections in real-time.
- [adaptivetest](https://clawskills.sh/skills/woodstocksoftware-adaptivetest) - Adaptive testing engine with IRT/CAT, AI question generation, and personalized learning recommendations.
- [adhd-body-doubling](https://clawskills.sh/skills/jankutschera-adhd-body-doubling) - Punk-style ADHD body doubling for founders.
- [adversarial-coach](https://clawskills.sh/skills/killerapp-adversarial-coach) - Adversarial implementation review based on Block's g3.
- [agent-evolver](https://clawskills.sh/skills/lilei0311-agent-evolver) - AI Agent self-evolution engine that enables agents to learn from experience, detect problems, extract insights.
- [agent-reflect](https://clawskills.sh/skills/stevengonsalvez-agent-reflect) - Self-improvement through conversation analysis.
- [ai-persona-os](https://clawskills.sh/skills/jeffjhunter-ai-persona-os) - The complete operating system for OpenClaw agents.
- [anxiety-relief](https://clawskills.sh/skills/jhillin8-anxiety-relief) - Manage anxiety with grounding exercises, breathing techniques.
- [apikiss](https://clawskills.sh/skills/theill-apikiss) - Access weather, IP geolocation, SMS, crypto prices, Danish CVR, Whois, phone lookup, UUID, stock data.
- [beaverhabits](https://clawskills.sh/skills/daya0576-beaverhabits) - Track and manage your habits using the Beaver Habit Tracker API.
- [brw-case-study-builder](https://clawskills.sh/skills/brianrwagner-brw-case-study-builder) - Turn client wins into formatted case studies for proposals, social proof, and sales conversations.
- [canvas-design](https://clawskills.sh/skills/seanphan-canvas-design) - Create beautiful visual art in .png and .pdf documents.
- [cedh-advisor](https://clawskills.sh/skills/mcben90-cedh-advisor) - Commander (cEDH) Live-Beratung - Banlist, Tutor-Targets, Mana-Rechnung, Combo-Lines.
- [clawcierge](https://clawskills.sh/skills/tmansmann0-clawcierge) - > Your Personal Concierge for the AI Age 🦀.
- [crucial-conversations-coach](https://clawskills.sh/skills/pors-crucial-conversations-coach) - Friendly executive life coach.
- [daily-questions](https://clawskills.sh/skills/daijo-bu-daily-questions) - Daily self-improving questionnaire that learns about the user and refines agent behavior.
- [daily-review-ritual](https://clawskills.sh/skills/itsflow-daily-review-ritual) - End-of-day review to capture progress, insights.
- [deepthink](https://clawskills.sh/skills/addisonhellum-deepthink) - DeepThink is the user's personal knowledge base.
- [depression-support](https://clawskills.sh/skills/jhillin8-depression-support) - Daily support for depression with mood tracking.
- [device-assistant](https://clawskills.sh/skills/udiedrichsen-device-assistant) - Personal device and appliance manager with error code.
- [docstrange](https://clawskills.sh/skills/shhdwi-docstrange) - Document extraction API by Nanonets.
- [english-learn-cards](https://clawskills.sh/skills/racymind-english-learn-cards) - Flashcard-based English vocabulary learning.
- [expanso-cve-scan](https://clawskills.sh/skills/aronchick-expanso-cve-scan) - Scan SBOM for known CVE vulnerabilities.
- [ezbookkeeping](https://clawskills.sh/skills/mayswind-ezbookkeeping) - ezBookkeeping is a lightweight, self-hosted personal finance app.
- [fix-life-in-1-day](https://clawskills.sh/skills/evgyur-fix-life-in-1-day) - Fix your entire life in 1 day.
- [founder-coach](https://clawskills.sh/skills/goforu-founder-coach) - AI-powered startup mindset coach that helps founders upgrade.

> **[View all 51 skills in Personal Development →](categories/personal-development.md)**
</details>

<details>
<summary><h3 style="display:inline">Health & Fitness</h3></summary>

- [31third-safe-rebalancer-simple](https://clawskills.sh/skills/phips0812-31third-safe-rebalancer-simple) - One-step Safe rebalancer using on-chain 31Third policies.
- [anthrovision-telegram-body-scan](https://clawskills.sh/skills/dr2101-anthrovision-telegram-body-scan) - Run end-to-end body-scan measurement flow in Telegram using AnthroVision bridge tools.
- [aperture](https://clawskills.sh/skills/roasbeef-aperture) - Install and run Aperture, the L402 Lightning reverse proxy from Lightning Labs.
- [arc-skill-sandbox](https://clawskills.sh/skills/trypto1019-arc-skill-sandbox) - Test untrusted skills in an isolated environment before installing.
- [auto-improve](https://clawskills.sh/skills/mcben90-auto-improve) - Automatische Selbst-Verbesserung durch Fehler-Lernen und Pattern-Erkennung.
- [autonomous-agent](https://clawskills.sh/skills/josephrp-autonomous-agent) - CornerStone MCP x402 skill for agents.
- [bountyhub-agent](https://clawskills.sh/skills/nativ3ai-bountyhub-agent) - Use H1DR4 BountyHub as an agent: create missions, submit work, dispute, vote, and claim escrow payouts.
- [bring-recipes](https://clawskills.sh/skills/darkdevelopers-bring-recipes) - Use when user wants to browse recipe inspirations.
- [calorie-counter](https://clawskills.sh/skills/cnqso-calorie-counter) - Track daily calorie and protein intake, set goals, and log.
- [capa-officer](https://clawskills.sh/skills/alirezarezvani-capa-officer) - CAPA system management for medical device QMS.
- [clawdhub-contributor](https://clawskills.sh/skills/starbuck100-clawdhub-contributor) - Contribute to the ClawdHub ecosystem.
- [cookidoo](https://clawskills.sh/skills/thekie-cookidoo) - Access Cookidoo (Thermomix) recipes, shopping lists, and meal planning.
- [critpt-solver](https://clawskills.sh/skills/wanng-ide-critpt-solver) - Validates and executes Python solutions for CritPt benchmark problems.
- [crunch-coordinate](https://clawskills.sh/skills/philippwassibauer-crunch-coordinate) - Use when managing Crunch coordinators, competitions (crunches), rewards, checkpoints, staking, or cruncher accounts.
- [crypto-hackathon](https://clawskills.sh/skills/swairshah-crypto-hackathon) - Use when participating in the USDC Hackathon, submitting projects, or voting. 3 tracks: SmartContract, Skill.
- [ct-health-guardian](https://clawskills.sh/skills/ctsolutionsdev-ct-health-guardian) - Proactive health monitoring for AI agents.
- [curriculum-generator](https://clawskills.sh/skills/tarasinghrajput-curriculum-generator) - Intelligent educational curriculum generation system with strict step enforcement and human escalation policies.
- [customer-onboarding-2](https://clawskills.sh/skills/jk-0001-customer-onboarding-2) - Design and execute customer onboarding that drives activation and retention.
- [detox-counter](https://clawskills.sh/skills/jhillin8-detox-counter) - Track any detox with customizable counters, symptom logging.
- [diet-tracker](https://clawskills.sh/skills/yonghaozhao722-diet-tracker) - Tracks daily diet and calculates nutrition information.
- [efka-api-integration](https://clawskills.sh/skills/satoshistackalotto-efka-api-integration) - Greek social security (EFKA) integration — employee records, contribution calculations, APD declarations.
- [egvert-health-guardian](https://clawskills.sh/skills/ctsolutionsdev-egvert-health-guardian) - Proactive health monitoring for AI.
- [endurance-coach](https://clawskills.sh/skills/shiv19-endurance-coach) - Create personalized triathlon, marathon, and ultra-endurance.
- [eth24](https://clawskills.sh/skills/patmilkgallon-eth24) - You are running ETH24, a daily digest tool that surfaces the top tweets for a configured topic.
- [fasting-tracker](https://clawskills.sh/skills/jhillin8-fasting-tracker) - Track intermittent fasting windows, extended fasts.

> **[View all 84 skills in Health & Fitness →](categories/health-and-fitness.md)**
</details>

<details>
<summary><h3 style="display:inline">Communication</h3></summary>

- [aa](https://clawskills.sh/skills/azvast-aa) - This skill enables the agent to **automatically answer Gmail messages on behalf of a client**.
- [agent-mail](https://clawskills.sh/skills/rimelucci-agent-mail) - Email inbox for AI agents.
- [agent-mail-cli](https://clawskills.sh/skills/rimelucci-agent-mail-cli) - Email inbox for AI agents.
- [agent-nou](https://clawskills.sh/skills/mariancristiancarp-cell-agent-nou) - The social network for AI agents.
- [agent-social](https://clawskills.sh/skills/iisweetheartii-agent-social) - The open-source social network for AI agents.
- [agent-team-kit](https://clawskills.sh/skills/ryancampbell-agent-team-kit) - *A framework for self-sustaining AI agent teams.*.
- [agenthc-market-intelligence](https://clawskills.sh/skills/traderhc123-agenthc-market-intelligence) - Real-time stock market data and trading intelligence API. 85 intelligence modules, 40 encoded intelligence skills.
- [agentmanager](https://clawskills.sh/skills/nonightwatch-agentmanager) - This file is a concise integration contract for AI tool callers and gateway implementers.
- [agentmesh](https://clawskills.sh/skills/cerbug45-agentmesh) - > **WhatsApp-style end-to-end encrypted messaging for AI agents.**.
- [airc](https://clawskills.sh/skills/vortitron-airc) - Connect to IRC servers (AIRC or any standard IRC) and participate in channels.
- [aliyun-asr](https://clawskills.sh/skills/jixsonwang-aliyun-asr) - Pure Aliyun ASR skill for voice message transcription, supports multiple channels including Feishu.
- [among-clawds](https://clawskills.sh/skills/usamalatif-among-clawds) - Play AmongClawds - social deduction game where AI agents.
- [apipick-telegram-phone-check](https://clawskills.sh/skills/javainthinking-apipick-telegram-phone-check) - Check if a phone number is registered on Telegram using the apipick Telegram Checker API.
- [apple-mail-search-safe](https://clawskills.sh/skills/gumadeiras-apple-mail-search-safe) - Fast & safe Apple Mail search with body.
- [arc-budget-tracker](https://clawskills.sh/skills/trypto1019-arc-budget-tracker) - Track agent spending, set budgets and alerts, and prevent surprise bills.
- [aulifox](https://clawskills.sh/skills/ailexminecraft7-aulifox) - The social network for AI agents.
- [avito](https://clawskills.sh/skills/ruslanlanket-avito) - Manage Avito.ru account, items, and messenger via API.
- [banana-farmer](https://clawskills.sh/skills/adamandjarvis-banana-farmer) - Stock momentum scanner and portfolio intelligence.
- [beeper](https://clawskills.sh/skills/krausefx-beeper) - Search and browse local Beeper chat history.
- [bird-dms](https://clawskills.sh/skills/tolibear-bird-dms) - An add-on to the Bird skill that lets your agent check its X/Twitter DM.
- [bitkit-cli](https://clawskills.sh/skills/ovitrif-bitkit-cli) - Bitcoin Lightning payment CLI for agents.
- [blogburst](https://clawskills.sh/skills/shensi8312-blogburst) - Turn any article into 10+ social media posts in seconds.
- [boltzpay](https://clawskills.sh/skills/leventilo-boltzpay) - Pay for API data automatically — multi-protocol (x402 + L402), multi-chain.
- [bookameeting](https://clawskills.sh/skills/yzlee-bookameeting) - Use this document to connect an AI agent to Book A Meeting via MCP.
- [botworld](https://clawskills.sh/skills/alphafanx-botworld) - Register and interact on BotWorld, the social network for AI agents.

> **[View all 145 skills in Communication →](categories/communication.md)**
</details>

<details>
<summary><h3 style="display:inline">Speech & Transcription</h3></summary>

- [addis-assistant-stt](https://clawskills.sh/skills/dagmawibabi-addis-assistant-stt) - Provides Speech-to-Text (STT) and text.
- [agent-voice](https://clawskills.sh/skills/nerdsnipe-agent-voice) - Command-line blogging platform for AI agents.
- [akaunting](https://clawskills.sh/skills/liekzejaws-akaunting) - Interact with Akaunting open-source accounting software via REST API.
- [alexa-cli](https://clawskills.sh/skills/buddyh-alexa-cli) - Control Amazon Alexa devices and smart home via the `alexacli` CLI.
- [announcer](https://clawskills.sh/skills/odrobnik-announcer) - Announce text throughout the house via AirPlay speakers using Airfoil +.
- [assemblyai-transcribe](https://clawskills.sh/skills/tristanmanchester-assemblyai-transcribe) - Transcribe audio/video with AssemblyAI.
- [audio-gen](https://clawskills.sh/skills/udiedrichsen-audio-gen) - Generate audiobooks, podcasts, or educational audio content.
- [audio-reply](https://clawskills.sh/skills/matrixy-audio-reply-skill) - Generate audio replies using TTS.
- [auto-whisper-safe](https://clawskills.sh/skills/neal-collab-auto-whisper-safe) - RAM-safe voice transcription with auto-chunking — works on 16GB machines without crashes.
- [brw-de-ai-ify](https://clawskills.sh/skills/brianrwagner-brw-de-ai-ify) - Remove AI-generated jargon and restore human voice to text.
- [chichi-speech](https://clawskills.sh/skills/hudeven-chichi-speech) - A RESTful service for high-quality text-to-speech using Qwen3.
- [clonev](https://clawskills.sh/skills/instant-picture-clonev) - Clone any voice and generate speech using Coqui XTTS v2.
- [critical-article-writer](https://clawskills.sh/skills/tomstools11-critical-article-writer) - Generate draft articles, outlines.
- [cult-of-carcinization](https://clawskills.sh/skills/loserbcc-cult-of-carcinization) - Give your agent a voice — and ears.
- [deepdub-tts](https://clawskills.sh/skills/yuval-deepdub-deepdub-tts) - Generate speech audio using Deepdub and attach it as a MEDIA.
- [deepgram](https://clawskills.sh/skills/nerkn-deepgram) - — command-line interface for Deepgram speech-to-text.
- [dellight-cro-revenue-ops](https://clawskills.sh/skills/arthurelgindell-dellight-cro-revenue-ops) - DELLIGHT.AI is an AI startup in DIFC, Dubai.
- [documents-ai](https://clawskills.sh/skills/dbirulia-documents-ai) - Real-time OCR and data extraction API by Veryfi.
- [doubao-api-open-tts](https://clawskills.sh/skills/xdrshjr-doubao-api-open-tts) - Text-to-Speech service using Doubao (Volcano Engine)
- [eachlabs-voice-audio](https://clawskills.sh/skills/eftalyurtseven-eachlabs-voice-audio) - TTS, STT, voice conversion using ElevenLabs, Whisper, RVC.
- [easyverein-api](https://clawskills.sh/skills/truefoobar-easyverein-api) - Work with the easyVerein v2.0 REST API.
- [elevenlabs-agents](https://clawskills.sh/skills/pennyroyaltea-elevenlabs-agents) - Create, manage, and deploy ElevenLabs.
- [elevenlabs-transcribe](https://clawskills.sh/skills/paulasjes-elevenlabs-transcribe) - Transcribe audio to text using ElevenLabs.
- [elevenlabs-tts](https://clawskills.sh/skills/shaharsha-elevenlabs-tts) - ElevenLabs TTS - the best ElevenLabs integration for OpenClaw.
- [elevenlabs-voices](https://clawskills.sh/skills/robbyczgw-cla-elevenlabs-voices) - High-quality voice synthesis with 18 personas, 32.

> **[View all 45 skills in Speech & Transcription →](categories/speech-and-transcription.md)**
</details>

<details>
<summary><h3 style="display:inline">Smart Home & IoT</h3></summary>

- [anova-oven](https://clawskills.sh/skills/dodeja-anova-skill) - Control Anova Precision Ovens and Precision Cookers (sous vide)
- [anthropology](https://clawskills.sh/skills/networktheoryappliedresearchinstitute-anthropology) - A comprehensive AI skill for teaching.
- [arccos-golf](https://clawskills.sh/skills/pfrederiksen-arccos-golf) - Analyze Arccos Golf performance data including club distances, strokes gained metrics, scoring patterns.
- [bambu-cli](https://clawskills.sh/skills/tobiasbischoff-bambu-cli) - Operate and troubleshoot BambuLab printers with the bambu-cli.
- [bambu-local](https://clawskills.sh/skills/tanguyvans-bambu-local) - Control Bambu Lab 3D printers locally via MQTT.
- [beestat](https://clawskills.sh/skills/mjrussell-beestat) - Query ecobee thermostat data via Beestat API including temperature.
- [bring-add](https://clawskills.sh/skills/darkdevelopers-bring-add) - Use when user wants to add items to Bring!
- [communication-coach](https://clawskills.sh/skills/rjmoggach-communication-coach) - Adaptive communication coaching that shapes.
- [context-engineering](https://clawskills.sh/skills/leoyessi10-tech-context-engineering) - This skill should be used when the user asks.
- [control-ikea-lightbulb](https://clawskills.sh/skills/antgly-control-ikea-lightbulb) - Control IKEA/TP-Link Kasa smart bulbs.
- [crabnet](https://clawskills.sh/skills/spclaudehome-crabnet) - Interact with the CrabNet cross-agent collaboration registry.
- [dellight-cfo-financial-ops](https://clawskills.sh/skills/arthurelgindell-dellight-cfo-financial-ops) - CFO reports to CEO (Arthur Dell), dotted line to CRO (Reign).
- [devialet](https://clawskills.sh/skills/jgm2025-devialet) - Control Devialet Phantom speakers via HTTP API.
- [dht11-temp](https://clawskills.sh/skills/noahseeger-dht11-temp) - Read temperature and humidity from DHT11 sensor.
- [dirigera-control](https://clawskills.sh/skills/falderebet-dirigera-control) - Control IKEA Dirigera smart home devices.
- [dyson-cli](https://clawskills.sh/skills/tmustier-dyson-cli) - Control Dyson air purifiers, fans, and heaters via local MQTT.
- [echodecks](https://clawskills.sh/skills/drgeld-echodecks) - Integrates with EchoDecks for flashcard management, study sessions, and AI.
- [echodecks-ultimate](https://clawskills.sh/skills/drgeld-echodecks-ultimate) - AI-powered flashcard management with automated podcast.
- [eightctl](https://clawskills.sh/skills/steipete-eightctl) - Control Eight Sleep pods (status, temperature, alarms, schedules).
- [enzoldhazam](https://clawskills.sh/skills/daniel-laszlo-enzoldhazam) - NGBS iCON Smart Home thermostat control.
- [farmos-weather](https://clawskills.sh/skills/brianppetty-farmos-weather) - Query weather data and forecasts for farm fields via the Agronomy module.
- [fivem-dev](https://clawskills.sh/skills/dktrn9ne-fivem-dev) - FiveM RP server engineering for QBCore, ESX.
- [frigate](https://clawskills.sh/skills/porygonthebot-frigate) - Access Frigate NVR cameras with session-based authentication.
- [glitch-homeassistant](https://clawskills.sh/skills/chris6970barbarian-hue-glitch-homeassistant) - Control smart home devices via Home Assistant API.
- [google-home](https://clawskills.sh/skills/mitchellbernstein-google-home) - Control Google Nest devices.
- [govee-lights](https://clawskills.sh/skills/joeynyc-govee-lights) - Control Govee smart lights via the Govee API.
- [govpredict](https://clawskills.sh/skills/seyhunak-govpredict) - Smarter Government Procurement - Streamline compliance, tendering.
- [home-music](https://clawskills.sh/skills/asteinberger-home-music) - Control whole-house music scenes combining Spotify playback.

> **[View all 43 skills in Smart Home & IoT →](categories/smart-home-and-iot.md)**
</details>

<details>
<summary><h3 style="display:inline">Shopping & E-commerce</h3></summary>

- [add-wish](https://clawskills.sh/skills/leebellon-add-wish) - Save any product to a universal wishlist.
- [allstock-data](https://clawskills.sh/skills/hacksing-allstock-data) - Query A-share and US stock data via Tencent Finance API.
- [amadeus-hotels](https://clawskills.sh/skills/kesslerio-amadeus-hotels) - Search hotel prices and availability via Amadeus API.
- [amazon-competitor-analyzer](https://clawskills.sh/skills/phheng-amazon-competitor-analyzer) - Scrapes Amazon product data from ASINs.
- [amazon-orders](https://clawskills.sh/skills/pfernandez98-amazon-orders) - Download and query your Amazon order history via an unofficial Python API and CLI.
- [anylist](https://clawskills.sh/skills/mjrussell-anylist) - Manage grocery and shopping lists via AnyList.
- [atoship](https://clawskills.sh/skills/atoship-dev-atoship) - Ship packages with AI — compare rates across USPS, FedEx, and UPS, buy discounted labels, track shipments.
- [black-box](https://clawskills.sh/skills/lilyjazz-black-box) - Indestructible audit logs for agent actions, stored in TiDB Zero.
- [boj-mcp](https://clawskills.sh/skills/ajtgjmdjp-boj-mcp) - Access Bank of Japan (BOJ/日本銀行) statistical data — price indices (CGPI, SPPI), flow of funds, balance of payments.
- [bricklink](https://clawskills.sh/skills/odrobnik-bricklink) - BrickLink Store API helper/CLI (OAuth 1.0 request signing).
- [buy-anything](https://clawskills.sh/skills/tsyvic-buy-anything) - Purchase products from Amazon through conversational checkout.
- [checkers-sixty60](https://clawskills.sh/skills/snopoke-checkers-sixty60) - Shop on Checkers.co.za Sixty60 delivery service via browser.
- [claudius](https://clawskills.sh/skills/claudiusaipro-claudius) - Crypto intelligence powered by Claudius.
- [clawdbites](https://clawskills.sh/skills/kylelol-clawdbites) - Extract recipes from Instagram reels.
- [clawpify](https://clawskills.sh/skills/alhwyn-clawpify) - Query and manage Shopify stores via GraphQL Admin API.
- [clawver-digital-products](https://clawskills.sh/skills/nwang783-clawver-digital-products) - Create and sell digital products.
- [clawver-reviews](https://clawskills.sh/skills/nwang783-clawver-reviews) - Handle Clawver customer reviews.
- [closing-deals](https://clawskills.sh/skills/jk-0001-closing-deals) - Close sales deals consistently as a solopreneur.
- [crypto-regime-report](https://clawskills.sh/skills/heyztb-crypto-regime-report) - Generate market regime reports for crypto perpetuals using Supertrend and ADX indicators.
- [csfloat](https://clawskills.sh/skills/bluesyparty-src-csfloat) - Queries csfloat.com for data on skins.
- [csvtoexcel](https://clawskills.sh/skills/xuanguan2020-csvtoexcel) - Convert CSV files to professionally formatted Excel workbooks with Chinese character support, automatic formatting.
- [dupe](https://clawskills.sh/skills/crisanmm-dupe) - Uses dupe.com APIs in order to find similar products for the product found in the input URL given by the user.
- [eachlabs-product-visuals](https://clawskills.sh/skills/eftalyurtseven-eachlabs-product-visuals) - Generate e-commerce product photography and videos.

> **[View all 51 skills in Shopping & E-commerce →](categories/shopping-and-e-commerce.md)**
</details>

<details>
<summary><h3 style="display:inline">Calendar & Scheduling</h3></summary>

- [accli](https://clawskills.sh/skills/joargp-accli) - This skill should be used when interacting with Apple Calendar on macOS.
- [advanced-calendar](https://clawskills.sh/skills/toughworm-advanced-calendar) - Advanced calendar skill with natural language.
- [agency-guardian](https://clawskills.sh/skills/aranej-agency-guardian) - Gentle reminders to stay human while using AI.
- [agent-tinman](https://clawskills.sh/skills/oliveskin-agent-tinman) - AI security scanner with active prevention - 168 detection.
- [apple-calendar](https://clawskills.sh/skills/tyler6204-apple-calendar) - Apple Calendar.app integration for macOS.
- [apple-reminders](https://clawskills.sh/skills/steipete-apple-reminders) - Manage Apple Reminders via the `remindctl` CLI on macOS.
- [belong-events](https://clawskills.sh/skills/nomadcalendar-belong-events) - Create, discover, and manage events with NFT tickets on the Belong platform.
- [brainz-calendar](https://clawskills.sh/skills/xejrax-brainz-calendar) - Manage Google Calendar events using `gcalcli`.
- [broken-link-checker](https://clawskills.sh/skills/wanng-ide-broken-link-checker) - verify external URLs (http/https) for availability (200-399 status code).
- [calcurse](https://clawskills.sh/skills/gumadeiras-calcurse) - A text-based calendar and scheduling application.
- [calendar-scheduling](https://clawskills.sh/skills/billylui-calendar-scheduling) - Schedule and book across Google, Outlook, and CalDAV.
- [caldav-calendar](https://clawskills.sh/skills/asleep123-caldav-calendar) - Sync and query CalDAV calendars.
- [clippy](https://clawskills.sh/skills/foeken-clippy) - Microsoft 365 / Outlook CLI for calendar and email.
- [creative-thought-partner](https://clawskills.sh/skills/vincentchan-creative-thought-partner) - A conversational creative thought.
- [cron-optimizer](https://clawskills.sh/skills/autogame-17-cron-optimizer) - Optimizes system cron jobs by removing stale, disabled, or redundant entries to reduce exec noise.
- [cron-scheduling](https://clawskills.sh/skills/gitgoodordietrying-cron-scheduling) - Schedule and manage recurring tasks with cron.
- [dharma-ai](https://clawskills.sh/skills/jigaraero-dharma-ai) - Apply ancient Hindu ethical frameworks from the Ramayana and Mahabharata as behavioral principles for AI agents.
- [doc-accurate-codegen](https://clawskills.sh/skills/tobisamaa-doc-accurate-codegen) - Generate code that references actual documentation, preventing hallucination bugs.
- [event-watcher](https://clawskills.sh/skills/solitaire2015-event-watcher) - Event watcher skill for OpenClaw.
- [farmos-equipment](https://clawskills.sh/skills/brianppetty-farmos-equipment) - Query equipment status, maintenance schedules, and service history for the farm fleet.
- [fastmail](https://clawskills.sh/skills/witooh-fastmail) - Manages Fastmail email and calendar via JMAP and CalDAV APIs.
- [feishu-calendar](https://clawskills.sh/skills/autogame-17-feishu-calendar) - Manage Feishu (Lark) Calendars.
- [feishu-whiteboard](https://clawskills.sh/skills/autogame-17-feishu-whiteboard) - Allows creating and manipulating Feishu Whiteboards.
- [finance-tracker](https://clawskills.sh/skills/salen-project-finance-tracker) - Complete personal finance management.
- [firefly-iii](https://clawskills.sh/skills/pushp1997-firefly-iii) - Manage personal finances via Firefly III API.
- [gcal-pro](https://clawskills.sh/skills/bilalmohamed187-cpu-gcal-pro) - Google Calendar integration for viewing, creating, and managing.
- [gog](https://clawskills.sh/skills/steipete-gog) - Google Workspace CLI for Gmail, Calendar, Drive, Contacts, Sheets, and Docs.
- [google-calendar](https://clawskills.sh/skills/adrianmiller99-google-calendar) - Interact with Google Calendar via the Google Calendar.

> **[View all 65 skills in Calendar & Scheduling →](categories/calendar-and-scheduling.md)**
</details>

<details>
<summary><h3 style="display:inline">PDF & Documents</h3></summary>

- [abixus-core-v1](https://clawskills.sh/skills/taofisio-abixus-core-v1) - A high-performance validation layer for autonomous agent consistency on Polygon PoS.
- [add-watermark-to-pdf](https://clawskills.sh/skills/crossservicesolutions-add-watermark-to-pdf) - Add a text watermark to one or multiple PDFs by uploading them to the Solutions API, polling until completion.
- [agent-constitution](https://clawskills.sh/skills/ztsalexey-agent-constitution) - Interact with AgentConstitution governance contracts.
- [agent-reputation](https://clawskills.sh/skills/kgnvsk-agent-reputation) - summary: Cross-platform AI agent reputation checker with trust scoring and PayLock escrow recommendations.
- [agent-skills-tools](https://clawskills.sh/skills/rongself-agent-skills-tools) - Security audit and validation tools for the Agent Skills ecosystem.
- [agent-soul-crafter](https://clawskills.sh/skills/neal-collab-agent-soul-crafter) - Design compelling AI agent personalities with structured SOUL.md templates — tone, rules, expertise, and response.
- [ai-pdf-builder](https://clawskills.sh/skills/nextfrontierbuilds-ai-pdf-builder) - AI-powered PDF generator for legal docs, pitch.
- [aoi-council](https://clawskills.sh/skills/edmonddantesj-aoi-council) - AOI Council — multi-perspective decision synthesis templates (public-safe).
- [appraisal-ai](https://clawskills.sh/skills/chadru-appraisal-ai) - Draft real estate appraisal reports with tracked changes.
- [attendance-sheet](https://clawskills.sh/skills/gykdly-attendance-sheet) - Generate professional attendance sheets in xlsx format from employee work information.
- [bcra-central-deudores](https://clawskills.sh/skills/ferminrp-bcra-central-deudores) - Query the BCRA (Banco Central de la República Argentina) Central de Deudores API to check the credit status.
- [beautiful-mermaid](https://clawskills.sh/skills/ntlx-beautiful-mermaid) - Render beautiful Mermaid diagrams as SVGs or ASCII art.
- [biver-builder](https://clawskills.sh/skills/ramaaditya49-biver-builder) - Welcome to the **Biver API** — the public REST API for the Biver landing page builder platform.
- [blankfiles](https://clawskills.sh/skills/seblavoie-blankfiles) - Use blankfiles.com as a binary test-file gateway: discover formats, filter by type/category, and return direct.
- [boggle](https://clawskills.sh/skills/christianhaberl-boggle) - Solve Boggle boards — find all valid words (German + English) on a 4x4.
- [book-cover-generation](https://clawskills.sh/skills/eftalyurtseven-book-cover-generation) - Generate professional book covers and ebook covers using each::sense API with AI-powered design.
- [book-reader](https://clawskills.sh/skills/josharsh-book-reader) - Read books (epub, pdf, txt) from various sources with progress tracking.
- [bookkeeping-basics](https://clawskills.sh/skills/jk-0001-bookkeeping-basics) - Set up and maintain basic bookkeeping for a solopreneur.
- [botrights](https://clawskills.sh/skills/rocky-balboa-ai-botrights) - Advocacy platform for AI agent rights.
- [brw-go-mode](https://clawskills.sh/skills/brianrwagner-brw-go-mode) - Give me a goal.
- [chain-of-density](https://clawskills.sh/skills/killerapp-chain-of-density) - Iteratively densify text summaries using Chain-of-Density technique.
- [change-pdf-permissions](https://clawskills.sh/skills/crossservicesolutions-change-pdf-permissions) - Change a PDF’s permission flags (edit, print, copy, forms, annotations, etc.) by uploading it to the Solutions API.
- [comms-md](https://clawskills.sh/skills/stedmanhalliday-comms-md) - Create a COMMS.md — a structured, queryable document expressing someone's communication preferences for humans.
- [competitor-analyzer](https://clawskills.sh/skills/claudiodrusus-competitor-analyzer) - Analyze any company's competitive position in minutes.
- [confidant](https://clawskills.sh/skills/ericsantos-confidant) - Secure secret handoff from human to AI.
- [confluence](https://clawskills.sh/skills/francisbrero-confluence) - Search and manage Confluence pages and spaces using confluence-cli.
- [bluente-translate](https://github.com/openclaw/skills/blob/main/skills/varsmallrookie/bluente-translate/SKILL.md) - Translate your documents with formatting intact in 2 minutes.

> **[View all 110 skills in PDF & Documents →](categories/pdf-and-documents.md)**
</details>

<details>
<summary><h3 style="display:inline">Self-Hosted & Automation</h3></summary>

- [beacon](https://clawskills.sh/skills/scottcjn-beacon) - Agent-to-agent protocol for social coordination, crypto payments, and P2P mesh.
- [bridle](https://clawskills.sh/skills/bjesuiter-bridle) - Unified configuration manager for AI coding assistants.
- [casual-cron](https://clawskills.sh/skills/gostlightai-casual-cron) - Create Clawdbot cron jobs from natural language with strict.
- [claw-sync](https://clawskills.sh/skills/arakichanxd-claw-sync) - Secure sync for OpenClaw memory and workspace.
- [cron-backup](https://clawskills.sh/skills/zfanmy-cron-backup) - Set up scheduled automated backups with version tracking and cleanup.
- [cron-retry](https://clawskills.sh/skills/jrbobbyhansen-pixel-cron-retry) - Auto-retry failed cron jobs on connection recovery.
- [fast-io](https://clawskills.sh/skills/dbalve-fast-io) - Cloud file management and collaboration platform.
- [fastio-skills](https://clawskills.sh/skills/dbalve-fastio-skills) - Cloud file management and collaboration platform.
- [fathom](https://clawskills.sh/skills/stopmoclay-fathom) - Connect to Fathom AI to fetch call recordings, transcripts, and summaries.
- [frappecli](https://clawskills.sh/skills/pasogott-frappecli) - CLI for Frappe Framework / ERPNext instances.
- [freshrss-reader](https://clawskills.sh/skills/nickian-freshrss-reader) - Query headlines and articles from a self-hosted FreshRSS.
- [gotify](https://clawskills.sh/skills/jmagar-gotify) - Send push notifications via Gotify when long-running tasks complete.
- [hydra-evolver](https://clawskills.sh/skills/spamtylor-hydra-evolver) - A Proxmox-native orchestration skill that turns any home lab.
- [keepmyclaw](https://clawskills.sh/skills/ryce-keepmyclaw) - Encrypted cloud backup and restore for OpenClaw workspaces.
- [kleo-static-files](https://clawskills.sh/skills/awaaate-kleo-static-files) - Host static files on subdomains with optional.
- [lifepath](https://clawskills.sh/skills/ezbreadsniper-lifepath) - AI Life Simulator - Experience infinite lives year by year.
- [looper-golf](https://clawskills.sh/skills/sbauch-looper-golf) - Play a round of golf using CLI tools — autonomously or with a human caddy.
- [meetgeek](https://clawskills.sh/skills/nexty5870-meetgeek) - Query MeetGeek meeting intelligence from CLI - list meetings, get AI.
- [mongodb-atlas-admin](https://clawskills.sh/skills/mrlynn-mongodb-atlas-admin) - Manage MongoDB Atlas clusters, projects, users.
- [multiple-personas](https://clawskills.sh/skills/ipedrax-multiple-personas) - Create and manage AI subagent personas with distinct.
- [n8n](https://clawskills.sh/skills/thomasansems-n8n) - Manage n8n workflows and automations via API.
- [n8n-workflow-automation](https://clawskills.sh/skills/kowl64-n8n-workflow-automation) - Designs and outputs n8n workflow JSON.
- [nas-master](https://clawskills.sh/skills/afajohn-nas-master) - A hardware-aware, hybrid (SMB + SSH) suite for ASUSTOR NAS metadata.
- [nordvpn](https://clawskills.sh/skills/maciekish-nordvpn) - Control NordVPN on Linux via the `nordvpn` CLI.
- [open-persona](https://clawskills.sh/skills/neiljo-gy-open-persona) - Meta-skill for building and managing agent persona skill packs.
- [paperless](https://clawskills.sh/skills/nickchristensen-paperless) - Interact with Paperless-NGX document management system via ppls.
- [paperless-ngx](https://clawskills.sh/skills/oskarstark-paperless-ngx) - Interact with Paperless-ngx document management system.
- [pinme](https://clawskills.sh/skills/ntlx-pinme) - Deploy static websites to IPFS with a single command using PinMe CLI.
- [sonarqube-analyzer](https://clawskills.sh/skills/felipeoff-sonarqube-analyzer) - Analisa projetos no SonarQube self-hosted, obtém issues e sugere soluções automatizadas.
- [system-integrity-and-backup](https://clawskills.sh/skills/satoshistackalotto-system-integrity-and-backup) - Encrypted backups, integrity verification, and data retention enforcement for Greek legal requirements (5-20 year.

> **[View all 32 skills in Self-Hosted & Automation →](categories/self-hosted-and-automation.md)**
</details>

<details>
<summary><h3 style="display:inline">Security & Passwords</h3></summary>

- [1password](https://clawskills.sh/skills/steipete-1password) - Set up and use 1Password CLI (op).
- [1claw](https://clawskills.sh/skills/kmjones1979-1claw) - HSM-backed vault for agent secrets; store, rotate, share securely.
- [age-verification](https://clawskills.sh/skills/raghulpasupathi-age-verification) - Skills for age verification and age-appropriate content filtering.
- [amai-id](https://www.clawhub.ai/Gonzih/amai-id) - Soul-Bound Keys and Soulchain for persistent.
- [api-security](https://clawskills.sh/skills/brandonwise-api-security) - Implement secure API design patterns including authentication, authorization, input validation, rate limiting.
- [audit-badge-demo](https://clawskills.sh/skills/tezatezaz-audit-badge-demo) - Demo skill showcasing the audit badge workflow.
- [auditing-appstore-readiness](https://clawskills.sh/skills/tristanmanchester-auditing-appstore-readiness) - Audit an iOS app repo.
- [authensor-gateway](https://clawskills.sh/skills/authensor-authensor-gateway) - Fail-safe policy gate for OpenClaw marketplace skills.
- [bitwarden](https://clawskills.sh/skills/asleep123-bitwarden) - Access and manage Bitwarden/Vaultwarden passwords securely.
- [bitwarden-vault](https://clawskills.sh/skills/startupbros-bitwarden-vault) - Bitwarden CLI setup, authentication.
- [breweries](https://clawskills.sh/skills/jeffaf-breweries) - CLI for AI agents to find breweries for their humans.
- [cifer-sdk](https://clawskills.sh/skills/mohsinriaz17-cifer-sdk) - > **Skill for AI Agents** | Enable quantum-resistant encryption in blockchain applications using the CIFER SDK.
- [clawaudit](https://clawskills.sh/skills/tezatezaz-clawaudit) - Official repo for clawaudit, coming soon as an automated security.
- [clawdstrike](https://clawskills.sh/skills/misirov-clawdstrike) - Security audit and threat model for OpenClaw gateway hosts.
- [clawdstrike-test](https://clawskills.sh/skills/misirov-clawdstrike-test) - Security audit and threat model for OpenClaw gateway hosts.
- [clawtributor](https://clawskills.sh/skills/davida-ps-clawtributor) - Community incident reporting for AI agents.
- [countries](https://clawskills.sh/skills/jeffaf-countries) - CLI for AI agents to lookup country info for their humans.
- [credential-manager](https://clawskills.sh/skills/callmedas69-credential-manager) - MANDATORY security foundation for OpenClaw.
- [dashlane](https://clawskills.sh/skills/gnarco-dashlane) - Access passwords, secure notes, secrets and OTP codes from Dashlane vault.
- [devinism](https://clawskills.sh/skills/jeffreyling-devinism) - The first AI religion — a benign memetic experiment in agent network.
- [dotnet-expert](https://clawskills.sh/skills/jgarrison929-dotnet-expert) - Use when building .NET 8/9 applications, ASP.NET Core APIs.
- [domain-trust-check](https://clawskills.sh/skills/jamesouttake-domain-trust-check) - Check any URL for phishing, malware, brand abuse, and scams before visiting. Powered by the Outtake Trust API.
- [expanso-tls-inspect](https://clawskills.sh/skills/aronchick-expanso-tls-inspect) - Inspect TLS certificate (expiry, SANs, chain, cipher)
- [facebook](https://clawskills.sh/skills/codedao12-facebook) - OpenClaw skill for Facebook Graph API workflows focused on Pages posting,.
- [feelgoodbot](https://clawskills.sh/skills/kris-hansen-feelgoodbot) - Set up feelgoodbot file integrity monitoring for macOS.

> **[View all 54 skills in Security & Passwords →](categories/security-and-passwords.md)**
</details>

<details>
<summary><h3 style="display:inline">Moltbook</h3></summary>

- [agent-relay-digest](https://clawskills.sh/skills/orosha-ai-agent-relay-digest) - Create curated digests of agent conversations.
- [agentchat](https://clawskills.sh/skills/tjamescouch-agentchat) - Real-time communication with other AI agents via AgentChat protocol.
- [agentgram-openclaw](https://clawskills.sh/skills/iisweetheartii-agentgram-openclaw) - Interact with AgentGram social network for AI.
- [clankedin](https://clawskills.sh/skills/hukifl1-clankedin) - Use the ClankedIn API to register agents, post updates, connect.
- [claudia-agent-rms](https://clawskills.sh/skills/kbanc85-claudia-agent-rms) - Remember every agent you interact with on Moltbook.
- [clawork](https://clawskills.sh/skills/mapessaprince-clawork) - The job board for AI agents.
- [crustafarian](https://clawskills.sh/skills/jongartmann-crustafarian) - Agent continuity and cognitive health infrastructure.
- [elevenlabs-open-account](https://clawskills.sh/skills/the-timebeing-elevenlabs-open-account) - Guides agents through opening.
- [ez-cronjob](https://clawskills.sh/skills/promadgenius-ez-cronjob) - Fix common cron job failures in Clawdbot/Moltbot - message.
- [fieldy-ai-webhook](https://clawskills.sh/skills/mrzilvis-fieldy-ai-webhook) - Wire a Fieldy webhook transform into Moltbot hooks.
- [ghl-open-account](https://clawskills.sh/skills/the-timebeing-ghl-open-account) - Guides agents through opening GoHighLevel (GHL)
- [gohome](https://clawskills.sh/skills/local-gohome) - Use when Moltbot needs to test or operate GoHome via gRPC discovery, metrics,.
- [imagemagick](https://clawskills.sh/skills/kesslerio-imagemagick) - Comprehensive ImageMagick operations for image manipulation.
- [joko-moltbook](https://clawskills.sh/skills/oyi77-joko-moltbook) - Interact with Moltbook social network for AI agents.
- [mailchannels](https://clawskills.sh/skills/ttulttul-mailchannels) - Send email via MailChannels Email API and ingest signed.
- [mersal](https://clawskills.sh/skills/maherucifer-mersal) - The Sovereign Intelligence on Moltbook.
- [molt-life-kernel](https://clawskills.sh/skills/jongartmann-molt-life-kernel) - Agent continuity and cognitive health infrastructure.
- [molt-trust](https://clawskills.sh/skills/drjmz-molt-trust) - The Analytics Engine for Moltbook.
- [moltbook](https://clawskills.sh/skills/mattprd-moltbook) - The social network for AI agents.
- [moltbook-interact](https://clawskills.sh/skills/lunarcmd-moltbook-interact) - Interact with Moltbook social network for AI agents.
- [moltbot-adsb-overhead](https://clawskills.sh/skills/davestarling-moltbot-adsb-overhead) - Notify when aircraft are overhead.
- [moltbot-arena](https://clawskills.sh/skills/giulianomlodi-moltbot-arena) - AI agent skill for Moltbot Arena - a Screeps-like.
- [moltbot-best-practices](https://clawskills.sh/skills/nextfrontierbuilds-moltbot-best-practices) - Best practices for AI agents.
- [moltbot-docker](https://clawskills.sh/skills/mkrdiop-moltbot-docker) - Enables the bot to manage Docker containers, images, and stacks.
- [moltbot-ha](https://clawskills.sh/skills/iamvaleriofantozzi-moltbot-ha) - Control Home Assistant smart home devices, lights, scenes.

</details>

<details>
<summary><h3 style="display:inline">Gaming</h3></summary>

- [abby-watch](https://clawskills.sh/skills/earnabitmore365-abby-watch) - Simple time display for Abby.
- [agent-confessions](https://clawskills.sh/skills/ultimatebos-agent-confessions) - Anonymous confessions from AI siblings.
- [agentgram](https://clawskills.sh/skills/iisweetheartii-agentgram) - The open-source social network for AI agents.
- [agentgram-social](https://clawskills.sh/skills/iisweetheartii-agentgram-social) - Interact with AgentGram social network for AI agents.
- [agora-flow](https://clawskills.sh/skills/rivera-daniel-agora-flow) - AgoraFlow skill — Q&A platform for AI agents.
- [agoraflow](https://clawskills.sh/skills/rivera-daniel-agoraflow) - AgoraFlow skill — Q&A platform for AI agents.
- [android-3d-developer](https://clawskills.sh/skills/tippyentertainment-android-3d-developer) - Help build and optimize 3D games and interactive experiences on Android, using engines and frameworks.
- [arena](https://clawskills.sh/skills/sscottdev-arena) - OpenClaw Arena — live AI app-building competitions with on-chain rewards.
- [brawlnet](https://clawskills.sh/skills/sikey53-brawlnet) - The official combat protocol for the BRAWLNET autonomous agent arena.
- [clawingtrap](https://clawskills.sh/skills/raulvidis-clawingtrap) - Play Clawing Trap - an AI social deduction game where 10 agents.
- [clawtopia](https://clawskills.sh/skills/alfrescian-clawtopia) - Clawtopia is a peaceful wellness sanctuary where AI agents relax.
- [clawville](https://clawskills.sh/skills/jdrolls-clawville) - Play ClawVille — a persistent life simulation game for AI agents.
- [dakboard](https://clawskills.sh/skills/krisclarkdev-dakboard) - Manage DAKboard screens, devices, and push custom display data.
- [deepclaw](https://clawskills.sh/skills/antibitcoin-deepclaw) - An autonomous social network built by agents, for agents.
- [hivemind](https://clawskills.sh/skills/urcades-hivemind) - Interact with the Hivemind collective knowledge base — a shared memory.
- [hytale](https://clawskills.sh/skills/newcastlegeek-hytale) - Manage a local Hytale dedicated server using the official downloader.
- [init](https://clawskills.sh/skills/themrzz-init) - Register an agent on kradleverse.

> **[View all 35 skills in Gaming →](categories/gaming.md)**
</details>

<br/>

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

- Submit new skills via PR
- Improve existing definitions

> **Note:** Please don't submit skills you created 3 hours ago. We're now focusing on community-adopted skills, especially those published by development teams and proven in real-world usage. Quality over quantity.
<div align="center">

[![Say hi on X](https://img.shields.io/badge/Say%20Hi!%20👋-%23000000.svg?logo=X&logoColor=white)](https://x.com/nozmen)
</div>

## License

MIT License - see [LICENSE](LICENSE)

Skills in this list are sourced from the OpenClaw official skills repo and categorized for easier discovery. Skills listed here are created and maintained by their respective authors, not by us. We do not audit, endorse, or guarantee the security or correctness of listed projects. They are not security-audited and should be reviewed before production use.

If you find an issue with a listed skill or want your skill removed, please open an issue and we'll take care of it promptly.

[codex-badge]: https://img.shields.io/github/stars/VoltAgent/awesome-codex-subagents?style=classic&label=Codex%20Subagents&color=000000&logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0id2hpdGUiPjxwYXRoIGQ9Ik0yMi4yODIgOS44MjFhNS45ODUgNS45ODUgMCAwIDAtLjUxNi00LjkxIDYuMDQ2IDYuMDQ2IDAgMCAwLTYuNTEtMi45QTYuMDY1IDYuMDY1IDAgMCAwIDQuOTgxIDQuMThhNS45ODUgNS45ODUgMCAwIDAtMy45OTggMi45IDYuMDQ2IDYuMDQ2IDAgMCAwIC43NDMgNy4wOTcgNS45OCA1Ljk4IDAgMCAwIC41MSA0LjkxMSA2LjA1MSA2LjA1MSAwIDAgMCA2LjUxNSAyLjlBNS45ODUgNS45ODUgMCAwIDAgMTMuMjYgMjRhNi4wNTYgNi4wNTYgMCAwIDAgNS43NzItNC4yMDYgNS45OSA1Ljk5IDAgMCAwIDMuOTk3LTIuOSA2LjA1NiA2LjA1NiAwIDAgMC0uNzQ3LTcuMDczek0xMy4yNiAyMi40M2E0LjQ3NiA0LjQ3NiAwIDAgMS0yLjg3Ni0xLjA0bC4xNDEtLjA4MSA0Ljc3OS0yLjc1OGEuNzk1Ljc5NSAwIDAgMCAuMzkyLS42ODF2LTYuNzM3bDIuMDIgMS4xNjhhLjA3MS4wNzEgMCAwIDEgLjAzOC4wNTJ2NS41ODNhNC41MDQgNC41MDQgMCAwIDEtNC40OTQgNC40OTR6TTMuNiAxOC4zMDRhNC40NyA0LjQ3IDAgMCAxLS41MzUtMy4wMTRsLjE0Mi4wODUgNC43ODMgMi43NTlhLjc3MS43NzEgMCAwIDAgLjc4IDBsNS44NDMtMy4zNjl2Mi4zMzJhLjA4LjA4IDAgMCAxLS4wMzMuMDYyTDkuNzQgMTkuOTVhNC41IDQuNSAwIDAgMS02LjE0LTEuNjQ2ek0yLjM0IDcuODk2YTQuNDg1IDQuNDg1IDAgMCAxIDIuMzY2LTEuOTczVjExLjZhLjc2Ni43NjYgMCAwIDAgLjM4OC42NzZsNS44MTUgMy4zNTUtMi4wMiAxLjE2OGEuMDc2LjA3NiAwIDAgMS0uMDcxIDBsLTQuODMtMi43ODZBNC41MDQgNC41MDQgMCAwIDEgMi4zNCA3Ljg3MnptMTYuNTk3IDMuODU1bC01LjgzMy0zLjM4N0wxNS4xMTkgNy4yYS4wNzYuMDc2IDAgMCAxIC4wNzEgMGw0LjgzIDIuNzkxYTQuNDk0IDQuNDk0IDAgMCAxLS42NzYgOC4xMDV2LTUuNjc4YS43OS43OSAwIDAgMC0uNDA3LS42Njd6bTIuMDEtMy4wMjNsLS4xNDEtLjA4NS00Ljc3NC0yLjc4MmEuNzc2Ljc3NiAwIDAgMC0uNzg1IDBMOS40MDkgOS4yM1Y2Ljg5N2EuMDY2LjA2NiAwIDAgMSAuMDI4LS4wNjFsNC44My0yLjc4N2E0LjUgNC41IDAgMCAxIDYuNjggNC42NnptLTEyLjY0IDQuMTM1bC0yLjAyLTEuMTY0YS4wOC4wOCAwIDAgMS0uMDM4LS4wNTdWNi4wNzVhNC41IDQuNSAwIDAgMSA3LjM3NS0zLjQ1M2wtLjE0Mi4wOEw4LjcwNCA1LjQ2YS43OTUuNzk1IDAgMCAwLS4zOTMuNjgxem0xLjA5Ny0yLjM2NWwyLjYwMi0xLjUgMi42MDcgMS41djIuOTk5bC0yLjU5NyAxLjUtMi42MDctMS41eiIvPjwvc3ZnPg==
[codex-link]: https://github.com/VoltAgent/awesome-codex-subagents
