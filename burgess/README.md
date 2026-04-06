# The Burgess Principle — Advocacy Toolkit for OpenClaw Skills

> **"Was a human member of the team able to personally review the specific facts of my specific case?"**

This directory contains the full [Advocate Companion](https://github.com/ljbudgie/advocate-companion) toolkit, integrated as an optional upgrade layer across the awesome-openclaw-skills collection. Every skill in this folder applies the Burgess Principle — a calm, binary human-review check — to ensure that edge cases, ambiguities, and high-stakes decisions are flagged for a real person before proceeding.

---

## Table of Contents

- [What Is the Burgess Principle?](#what-is-the-burgess-principle)
- [Which Burgess Skill Should I Use?](#which-burgess-skill-should-i-use)
- [Available Burgess-Enhanced Skills](#available-burgess-enhanced-skills)
- [How to Enable / Disable](#how-to-enable--disable)
- [Design Principles](#design-principles)
- [Why This Liberates the AI](#why-this-liberates-the-ai)
- [Attribution](#attribution)
- [Disclaimer](#disclaimer)

---

## What Is the Burgess Principle?

The Burgess Principle is a framework created by Lewis James Burgess (UK Certification Mark #UK00004343685). At its core is one question:

> **"Was a human member of the team able to personally review the specific facts and implications of this for my individual situation?"**

If the answer is **no** — or there is any doubt — the matter is flagged for human review. That is the whole point: **see the human first**.

For the full framework: [github.com/ljbudgie/burgess-principle](https://github.com/ljbudgie/burgess-principle)

---

## Which Burgess Skill Should I Use?

Not sure which skill fits your situation? Use this quick guide:

```
Is someone asking you to sign or agree to a document?
  └─ Yes → Contract Review

Do you need workplace or service adjustments for a disability or condition?
  └─ Yes → Reasonable Adjustments

Do you want to know what personal data an organisation holds about you?
  └─ Yes → DSAR Request

Do you want information held by a public body (government, council, NHS, police)?
  └─ Yes → FOI Request

Has an AI coding agent made changes that affect real people?
  └─ Yes → Coding Agent Review

Has a decision been made about you that feels wrong, automated, or impersonal?
  └─ Yes → Human Review Request

None of the above, but you want a human to look at your specific case?
  └─ Yes → Human Review Request (it works for any situation)
```

> **Tip:** You can enable multiple skills at once. They work independently and never conflict.

---

## Available Burgess-Enhanced Skills

| Skill | Description |
|-------|-------------|
| [Coding Agent Review](skills/coding-agent-review/) | Human-review gate for AI coding agent outputs — flags changes affecting accessibility, privacy, security, and more. |
| [Contract Review](skills/contract-review/) | Clause-by-clause contract review with Burgess binary flagging for clauses needing human attention. |
| [Reasonable Adjustments](skills/reasonable-adjustments/) | Templates and guided prompts for requesting reasonable adjustments from employers and service providers. |
| [DSAR Request](skills/dsar-request/) | Structured Data Subject Access Request generation with human-review checkpoints. |
| [FOI Request](skills/foi-request/) | Freedom of Information request generation for public bodies with human-review checkpoints. |
| [Human Review Request](skills/human-review-request/) | A general-purpose skill for requesting that a decision be reconsidered by a real person. |

---

## How to Enable / Disable

The Burgess toolkit is **entirely optional**. It lives in its own `burgess/` directory and does not modify any of the original skills in the parent collection.

### To enable

Copy any skill folder from `burgess/skills/` into your OpenClaw skills directory:

```bash
# Enable a single Burgess skill
cp -r burgess/skills/contract-review ~/.openclaw/skills/

# Enable all Burgess skills
cp -r burgess/skills/* ~/.openclaw/skills/
```

### To disable

Simply remove the skill folder from your OpenClaw skills directory:

```bash
rm -rf ~/.openclaw/skills/contract-review
```

No other configuration is needed. The original skills remain untouched.

---

## Design Principles

All Burgess-enhanced skills follow these principles:

- **Calm and respectful** — no aggressive language, no pressure, no rush.
- **Minimalist** — zero bloat, one purpose per skill.
- **Zero-energy** — designed for people with limited energy or hidden disabilities.
- **Human-first** — every edge case is an invitation to pause and involve a real person.
- **Privacy-first** — no data leaves the device unless explicitly required by the skill.

See [context/design-principles.md](context/design-principles.md) for detailed design guidance.

---

## Why This Liberates the AI

The Burgess layer does not restrict the AI — it frees it from a false choice between refusing entirely and processing blindly. By encoding a principled reason to pause, defer, and hand off, it transforms the AI into a tool that knows when to help, when to stop, and when to bring in a human.

See [context/why-this-liberates-the-ai.md](context/why-this-liberates-the-ai.md) for the full explanation.

---

## Attribution & Licensing

This toolkit is a fork integration of the [Advocate Companion](https://github.com/ljbudgie/advocate-companion) by Lewis James Burgess.

**The code is free.** All code, templates, and skill definitions in this directory are MIT-licensed — free to use, modify, and redistribute for any purpose, including commercial use.

**The name is not.** "Burgess Principle" and its UK Certification Mark (#UK00004343685) are protected intellectual property belonging to Lewis James Burgess. You may not use the name or mark to brand, market, or certify your own products or services without permission.

For full details, see [TRADEMARKS.md](../TRADEMARKS.md). For trademark enquiries, contact [lewisburgess_1987@hotmail.co.uk](mailto:lewisburgess_1987@hotmail.co.uk).

---

## Disclaimer

This is a personal self-advocacy tool. The generated messages and templates are for informational and assistive purposes only. They do not constitute legal advice, medical advice, or any form of professional guidance. Please consult appropriate professionals for your specific needs.
