# Burgess Toolkit Configuration

The Burgess-enhanced skills are **optional** and **independent**. Each skill can be enabled or disabled individually without affecting any other skill in this collection.

---

## Enabling Skills

Copy the skill folder(s) you want into your OpenClaw skills directory:

```bash
# Enable a specific skill
cp -r burgess/skills/benefits-claim-assistance ~/.openclaw/skills/
cp -r burgess/skills/coding-agent-review ~/.openclaw/skills/
cp -r burgess/skills/contract-review ~/.openclaw/skills/
cp -r burgess/skills/copyright-dmca-counter-notice ~/.openclaw/skills/
cp -r burgess/skills/council-tax-pcn-dispute ~/.openclaw/skills/
cp -r burgess/skills/direct-debit-refund ~/.openclaw/skills/
cp -r burgess/skills/dsar-request ~/.openclaw/skills/
cp -r burgess/skills/enforcement-agent-response ~/.openclaw/skills/
cp -r burgess/skills/foi-request ~/.openclaw/skills/
cp -r burgess/skills/human-review-request ~/.openclaw/skills/
cp -r burgess/skills/media-libel-review ~/.openclaw/skills/
cp -r burgess/skills/medical-device-review ~/.openclaw/skills/
cp -r burgess/skills/music-copyright-dispute ~/.openclaw/skills/
cp -r burgess/skills/reasonable-adjustments ~/.openclaw/skills/

# Enable all Burgess skills at once
cp -r burgess/skills/* ~/.openclaw/skills/
```

## Disabling Skills

Remove the skill folder from your OpenClaw skills directory:

```bash
rm -rf ~/.openclaw/skills/contract-review
```

## Workspace-Level Installation

You can also install Burgess skills at the workspace level for a specific project:

```bash
cp -r burgess/skills/contract-review <your-project>/skills/
```

Workspace skills take priority over global skills.

---

## No Other Configuration Required

- No environment variables needed for local use.
- No API keys required for template-based features.
- No database or backend setup.
- The original skills in the parent collection are **never modified**.

---

## Compatibility

These skills work with any OpenClaw installation that supports the standard skills format. They are plain-text SKILL.md files with no external dependencies.
