# Marketing Research Agent

An automated Python agent that downloads, analyzes, and generates insights from papers published in top marketing journals.

## What It Does

1. **Collects** paper metadata (titles, authors, abstracts, citations) from 6 top marketing journals via free APIs (CrossRef + OpenAlex)
2. **Downloads** full-text PDFs through your university library proxy (optional — works with metadata/abstracts alone)
3. **Analyzes** each paper to extract what made it publishable: methodology, data sources, analytical techniques, theoretical frameworks, and more
4. **Discovers patterns** across papers — what methods are trending, what journals prefer, what makes highly-cited papers different
5. **Generates research ideas** based on identified gaps, emerging trends, and cross-domain opportunities
6. **Finds datasets** from online repositories (Harvard Dataverse, data.gov, Zenodo, ICPSR) plus curated marketing-specific sources

## Supported Journals

| Code | Journal | Publisher |
|------|---------|-----------|
| JM | Journal of Marketing | SAGE / AMA |
| JMR | Journal of Marketing Research | SAGE / AMA |
| MKSC | Marketing Science | INFORMS |
| JCR | Journal of Consumer Research | Oxford |
| JAMS | Journal of the Academy of Marketing Science | Springer |
| IJRM | International Journal of Research in Marketing | Elsevier |

## Quick Start

### 1. Install Dependencies

```bash
cd marketing_research_agent
pip install -r requirements.txt
```

### 2. Run the Agent (No Credentials Needed)

The agent works out of the box using free APIs for metadata and abstracts:

```bash
# Run the full pipeline
python -m marketing_research_agent

# Or with custom parameters
python -m marketing_research_agent --from-year 2022 --to-year 2025 --max-papers 50
```

### 3. Check Status

```bash
python -m marketing_research_agent --status
```

## Usage

### Full Pipeline

```bash
# Default: all journals, 2020-2025, up to 200 papers per journal
python -m marketing_research_agent

# Custom year range and paper limit
python -m marketing_research_agent --from-year 2023 --to-year 2025 --max-papers 100

# Specific journals only
python -m marketing_research_agent --journals JM JMR MKSC

# Generate more ideas
python -m marketing_research_agent --num-ideas 20

# Verbose logging
python -m marketing_research_agent -v
```

### Individual Stages

Run stages independently (useful for large collections or debugging):

```bash
# Stage 1: Collect metadata
python -m marketing_research_agent --stage collect

# Stage 4: Analyze papers (requires collected papers)
python -m marketing_research_agent --stage analyze

# Stage 5: Find patterns (requires analyzed papers)
python -m marketing_research_agent --stage patterns

# Stage 6: Generate ideas (requires patterns)
python -m marketing_research_agent --stage ideas --num-ideas 15

# Stage 7: Find datasets (requires ideas)
python -m marketing_research_agent --stage datasets

# Stage 8: Generate report
python -m marketing_research_agent --stage report
```

### With PDF Downloads (Optional)

To download full-text PDFs through your university library:

```bash
export LIBRARY_PROXY_URL="https://proxy.library.youruniversity.edu/login?url="
export LIBRARY_USERNAME="your_username"
export LIBRARY_PASSWORD="your_password"

python -m marketing_research_agent --download-pdfs
```

> **Note:** PDF downloads require Selenium and Chrome/ChromeDriver. The agent works perfectly fine without PDFs — it analyzes abstracts and metadata instead.

### With LLM Enhancement (Optional)

For deeper AI-powered analysis and idea generation:

```bash
export OPENAI_API_KEY="sk-..."
python -m marketing_research_agent
```

## Output

The agent produces:

| File | Description |
|------|-------------|
| `data/knowledge.db` | SQLite database with all papers, analyses, patterns, ideas, and datasets |
| `data/report.txt` | Human-readable comprehensive report |
| `data/report.json` | Machine-readable structured report |
| `data/agent.log` | Execution log |
| `data/pdfs/` | Downloaded PDFs (if enabled) |

## Architecture

```
marketing_research_agent/
├── config.py              # Configuration & journal definitions
├── database.py            # SQLite database manager
├── agent.py               # Main orchestrator
├── run_agent.py           # CLI entry point
├── scrapers/
│   ├── base_scraper.py    # Rate-limited HTTP client
│   ├── crossref_scraper.py # CrossRef API (DOIs, metadata)
│   ├── openalex_scraper.py # OpenAlex API (citations, concepts)
│   └── browser_downloader.py # Selenium PDF downloader
├── analyzers/
│   ├── pdf_parser.py      # PDF text extraction (PyMuPDF)
│   ├── paper_analyzer.py  # Publishability factor analysis
│   └── pattern_finder.py  # Cross-paper pattern discovery
└── generators/
    ├── idea_generator.py  # Research idea generation
    └── dataset_finder.py  # Online dataset discovery
```

## Analysis Details

### What the Agent Extracts from Each Paper

- **Methodology type**: experiment, survey, archival, qualitative, meta-analysis, analytical modeling, simulation, machine learning
- **Data characteristics**: primary vs. secondary, specific sources (MTurk, Nielsen, Yelp, etc.), sample sizes
- **Analytical techniques**: regression, SEM, causal inference, Bayesian, panel methods, choice modeling, text analysis
- **Theoretical frameworks**: 40+ known marketing theories detected
- **Research domain**: consumer behavior, digital marketing, branding, pricing, advertising, etc.
- **Publishability factors**: methodological rigor, multi-study design, sample quality, theoretical contribution, practical relevance, novelty, robustness checks, citation impact

### Pattern Discovery

The agent identifies:
- Most common methodologies per journal
- Trending vs. declining methods over time
- Popular analytical technique combinations
- Journal-specific preferences
- What distinguishes highly-cited papers

### Idea Generation Strategies

1. **Method transfer**: Apply methods common in one domain to another where they're rare
2. **Emerging trends**: Ride growing methodological trends in underexplored domains
3. **Gap filling**: Address future directions explicitly stated by published authors
4. **Multi-method**: Propose novel combinations of complementary methods
5. **LLM-generated**: Creative ideas from AI analysis of patterns (requires OpenAI key)

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CROSSREF_MAILTO` | Recommended | Email for CrossRef polite pool (faster API access) |
| `OPENALEX_MAILTO` | Recommended | Email for OpenAlex API |
| `OPENAI_API_KEY` | Optional | OpenAI key for LLM-enhanced analysis |
| `LIBRARY_PROXY_URL` | For PDFs | University library proxy URL |
| `LIBRARY_USERNAME` | For PDFs | Library login username |
| `LIBRARY_PASSWORD` | For PDFs | Library login password |

## Database Schema

The SQLite database (`data/knowledge.db`) contains:

- **papers**: Full metadata for each paper (DOI, title, authors, abstract, citations, etc.)
- **analyses**: Extracted publishability factors per paper
- **patterns**: Discovered cross-paper patterns
- **ideas**: Generated research ideas with scores
- **datasets**: Found online datasets linked to ideas

You can query it directly:

```python
import sqlite3
conn = sqlite3.connect("marketing_research_agent/data/knowledge.db")
conn.row_factory = sqlite3.Row

# Top cited papers
for row in conn.execute("SELECT title, citation_count, journal_code FROM papers ORDER BY citation_count DESC LIMIT 10"):
    print(f"{row['citation_count']:5d} | {row['journal_code']} | {row['title'][:80]}")

# Most common methodologies
for row in conn.execute("SELECT methodology_types, COUNT(*) as cnt FROM analyses GROUP BY methodology_types ORDER BY cnt DESC LIMIT 10"):
    print(f"{row['cnt']:3d} | {row['methodology_types']}")
```

## Programmatic Usage

```python
from marketing_research_agent.agent import MarketingResearchAgent

with MarketingResearchAgent() as agent:
    # Collect papers from specific journals
    agent.collect_papers(from_year=2023, to_year=2025, journals=["JM", "JMR"])

    # Analyze
    agent.analyze_papers()

    # Find patterns
    patterns = agent.find_patterns()

    # Generate ideas
    ideas = agent.generate_ideas(num_ideas=10)

    # Find datasets
    agent.find_datasets()

    # Get report
    report = agent.generate_report()
    print(report)
```

## License

MIT
