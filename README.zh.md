<div align="center">

<a href="https://github.com/VoltAgent/voltagent">
<img width="1500" height="500" alt="social" src="https://github.com/user-attachments/assets/a6f310af-8fed-4766-9649-b190575b399d" />
</a>

<br/>
<br/>

<div align="center">
    <strong>发现 2868 个社区构建的 OpenClaw 技能，按类别组织。
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

# Awesome OpenClaw 技能

OpenClaw（以前称为 Moltbot，最初叫 Clawdbot... 身份认同危机包含在内，不额外收费）是一个直接在你的机器上运行的本地 AI 助手。技能扩展了它的能力，让它可以与外部服务交互、自动化工作流程并执行专门任务。这个精选列表帮助你发现和安装适合你需求的技能。

本列表中的技能来自 [ClawHub](https://www.clawhub.ai/)（OpenClaw 的公共技能注册表），并按类别组织以便于发现。

## 安装

### ClawHub CLI

> **注意：** 你可能知道，他们一直在改名。这反映了当前的官方文档。当他们再次改名时，我们会更新。

```bash
npx clawhub@latest install <skill-slug>
```

### 手动安装

将技能文件夹复制到以下位置之一：

| 位置 | 路径 |
|------|------|
| 全局 | `~/.openclaw/skills/` |
| 工作区 | `<project>/skills/` |

优先级：工作区 > 本地 > 捆绑

### 替代方案

你也可以将技能的 GitHub 仓库链接直接粘贴到你的助手中，并要求它使用。助手会在后台自动处理设置。

## 为什么存在这个列表？

截至 2026 年 2 月 7 日，OpenClaw 的公共注册表（ClawHub）托管了 **5,705 个社区构建的技能**。这个精选列表有 **2,868 个技能**。以下是我们过滤掉的内容：

| 过滤 | 排除数量 |
|------|----------|
| 可能是垃圾邮件 — 批量账户、机器人账户、测试/垃圾 | 1,180 |
| 加密 / 区块链 / 金融 / 交易 | 672 |
| 重复 / 相似名称 | 492 |
| 恶意 — 由研究人员发布的安全审计识别（不包括 VirusTotal） | 396 |
| 非英语 — 描述不是英文 | 8 |
| **未从 OpenClaw 官方技能注册表中获取的总数** | **2,748** |

## 安全说明

本列表中的技能是**精选的，未经过审计**。它们可能随时被其原始维护者更新、修改或替换。

在安装或使用任何 Agent Skill 之前，请自己审查潜在的安全风险并验证来源。OpenClaw 与 **VirusTotal 建立了合作伙伴关系**，提供技能安全扫描，请访问 ClawHub 上的技能页面并查看 VirusTotal 报告，看看是否被标记为有风险。

**推荐工具：**

- [Snyk Skill Security Scanner](https://github.com/snyk/agent-scan)
- [Agent Trust Hub](https://ai.gendigital.com/agent-trust-hub)

> Agent 技能可能包含提示注入、工具中毒、隐藏的恶意软件有效载荷或不安全的数据处理模式。安装前请务必检查源代码，使用技能需自行承担风险。

**想添加技能吗？** 此列表仅包含已发布在 `github.com/openclaw/skills` 仓库中的技能。我们不接受个人仓库、gist 或任何其他外部来源的链接。如果你的技能还不在 OpenClaw 技能仓库中，请先在那里发布。有关详细信息，请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。

如果你认为此列表中的某个技能应该被标记或存在安全问题，请[提出 issue](https://github.com/VoltAgent/awesome-clawdbot-skills/issues)，以便我们审查。

## 目录

| | | |
|---|---|---|
| [编程代理与 IDEs](#编程代理与-ides) (133) | [营销与销售](#营销与销售) (143) | [沟通](#沟通) (133) |
| [Git 与 GitHub](#git-与-github) (66) | [生产力与任务](#生产力与任务) (135) | [语音与转录](#语音与转录) (65) |
| [Moltbook](#moltbook) (51) | [AI 与大语言模型](#ai-与大语言模型) (287) | [智能家居与物联网](#智能家居与物联网) (56) |
| [Web 与前端开发](#web-与前端开发) (202) | [数据与分析](#数据与分析) (46) | [购物与电商](#购物与电商) (51) |
| [DevOps 与云](#devops-与云) (212) | [金融](#金融) (22) | [日历与日程安排](#日历与日程安排) (50) |
| [浏览器与自动化](#浏览器与自动化) (139) | [媒体与流媒体](#媒体与流媒体) (80) | [PDF 与文档](#pdf-与文档) (67) |
| [图像与视频生成](#图像与视频生成) (60) | [笔记与个人知识管理](#笔记与个人知识管理) (100) | [自托管与自动化](#自托管与自动化) (25) |
| [Apple 应用与服务](#apple-应用与服务) (35) | [iOS 与 macOS 开发](#ios-与-macos-开发) (17) | [安全与密码](#安全与密码) (64) |
| [搜索与研究](#搜索与研究) (253) | [交通](#交通) (76) | [游戏](#游戏) (61) |
| [Clawdbot 工具](#clawdbot-工具) (120) | [个人发展](#个人发展) (56) | [代理间协议](#代理间协议) (18) |
| [CLI 实用工具](#cli-实用工具) (129) | [健康与健身](#健康与健身) (55) |

## OpenClaw 部署技术栈

OpenClaw 代理的设置、托管和部署提供商。

赞助商位置保留给为 OpenClaw 开发者和用户提供服务的托管、部署和设置提供商。

📩 有关赞助查询，请联系 [necati@voltagent.dev](mailto:necati@voltagent.dev)

#your-link-here
你的产品描述在这里 — 关于你为 OpenClaw 开发者提供的服务的一句话介绍。

#your-link-here   #your-link-here
简短描述在这里。                          简短描述在这里。

#your-link-here  #your-link-here  #your-link-here

---

*这是 Awesome OpenClaw 技能的中文翻译版本。如需原始英文版本，请查看 [README.md](README.md)。*
