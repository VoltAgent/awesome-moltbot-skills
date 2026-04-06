# Coding Agent Review — Burgess-Enhanced Skill

A calm, structured skill for reviewing AI coding agent outputs before they ship. Grounded in the Burgess Principle, it flags any code change that affects real people — accessibility, privacy, security, user-facing language, billing, or automated decisions — and ensures a human reviews the specific implications before the change goes live.

Designed for developers who use AI coding agents and want confidence that nothing human-impacting slips through without a real person checking it.

---

## The Burgess Principle

> **"Was a human member of the team able to personally review the specific implications of this code change for the people it affects?"**

If the answer is no — or there is any doubt — the change is flagged for human review. That is the whole point: see the human first.

---

## How It Works

1. **Share the changes your AI coding agent made** — a diff, pull request, commit log, or plain description of what changed.
2. The skill assesses each change against **seven human-impact areas**: accessibility, privacy & data handling, security, user-facing language, pricing & billing, automated decisions, and deployment & infrastructure.
3. Any change that touches a human-impact area is **automatically flagged** with:
   - **Impact area** — which category it falls under
   - **Risk level** — Low / Medium / High
   - **Plain English explanation** — what it does and who it affects
   - **Suggested action** — what a human reviewer should check
4. You receive a **clear summary** and suggested next steps.

---

## Example Prompts

| You say | What happens |
|---------|--------------|
| *"Review the changes my coding agent made to the login flow"* | Flags any security, privacy, or accessibility concerns in the authentication changes. |
| *"Check this diff for anything that needs a human to look at"* | Scans all changes and highlights those with human impact. |
| *"My agent updated the pricing page — apply the Burgess check"* | Reviews billing and user-facing language changes for human-review needs. |
| *"An AI agent refactored our accessibility module — is that safe to merge?"* | Checks whether accessibility features were preserved and flags anything that needs a human eye. |
| *"Review this PR — the bot changed our data collection code"* | Identifies privacy and data handling implications and flags them for human review. |

---

## Human-Impact Areas

| Area | What gets flagged |
|------|-------------------|
| **Accessibility** | Changes to UI components, screen-reader support, ARIA attributes, colour contrast, keyboard navigation, alt text. |
| **Privacy & data handling** | Changes to data collection, storage, sharing, consent flows, cookies, analytics, or tracking. |
| **Security** | Changes to authentication, authorisation, input validation, encryption, secrets, or access control. |
| **User-facing language** | Changes to error messages, onboarding copy, notifications, emails, or terms of service. |
| **Pricing & billing** | Changes to payment logic, subscription handling, refund flows, or free-tier limits. |
| **Automated decisions** | Changes to algorithms that accept, reject, rank, score, or classify people. |
| **Deployment & infrastructure** | Production deployments, database migrations, or feature flags affecting live users. |

---

## When to Use This Skill

- An AI coding agent has made changes you want to review before merging
- A bot opened a pull request and you want to know what needs human attention
- You want to ensure accessibility, privacy, or security changes get a real person's review
- Your team uses AI agents for routine coding and you need a human-review checkpoint
- You want to apply the Burgess Principle to your development workflow

---

## Important Notes

- **This is not a substitute for human code review.** It helps you identify where human attention matters most.
- Designed for developers with limited time or energy — clear, structured, no noise.
- Every flagged change is an invitation to pause and involve a real person who understands the implications.
- Works with any AI coding agent output — diffs, PRs, commit logs, or plain descriptions.

---

Source: [Advocate Companion](https://github.com/ljbudgie/advocate-companion)
