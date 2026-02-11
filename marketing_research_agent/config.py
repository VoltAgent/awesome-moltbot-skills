"""
Configuration for the Marketing Research Agent.
Defines journal metadata, API endpoints, analysis parameters, and paths.
"""

import os
from pathlib import Path

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PDF_DIR = DATA_DIR / "pdfs"
DB_PATH = DATA_DIR / "knowledge.db"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

# ─── Journal Definitions ─────────────────────────────────────────────────────
JOURNALS = {
    "JM": {
        "name": "Journal of Marketing",
        "issn": "0022-2429",
        "eissn": "1547-7185",
        "publisher": "SAGE / American Marketing Association",
        "openalex_id": "S205187041",
        "url": "https://journals.sagepub.com/home/jmx",
    },
    "JMR": {
        "name": "Journal of Marketing Research",
        "issn": "0022-2437",
        "eissn": "1547-7193",
        "publisher": "SAGE / American Marketing Association",
        "openalex_id": "S49861276",
        "url": "https://journals.sagepub.com/home/mrj",
    },
    "MKSC": {
        "name": "Marketing Science",
        "issn": "0732-2399",
        "eissn": "1526-548X",
        "publisher": "INFORMS",
        "openalex_id": "S16512636",
        "url": "https://pubsonline.informs.org/journal/mksc",
    },
    "JCR": {
        "name": "Journal of Consumer Research",
        "issn": "0093-5301",
        "eissn": "1537-5277",
        "publisher": "Oxford University Press",
        "openalex_id": "S144285006",
        "url": "https://academic.oup.com/jcr",
    },
    "JAMS": {
        "name": "Journal of the Academy of Marketing Science",
        "issn": "0092-0703",
        "eissn": "1552-7824",
        "publisher": "Springer",
        "openalex_id": "S122842597",
        "url": "https://link.springer.com/journal/11747",
    },
    "IJRM": {
        "name": "International Journal of Research in Marketing",
        "issn": "0167-8116",
        "eissn": "1872-8383",
        "publisher": "Elsevier",
        "openalex_id": "S204335043",
        "url": "https://www.sciencedirect.com/journal/international-journal-of-research-in-marketing",
    },
}

# ─── API Configuration ────────────────────────────────────────────────────────
CROSSREF_API_URL = "https://api.crossref.org/works"
CROSSREF_MAILTO = os.environ.get("CROSSREF_MAILTO", "researcher@university.edu")

OPENALEX_API_URL = "https://api.openalex.org"
OPENALEX_MAILTO = os.environ.get("OPENALEX_MAILTO", CROSSREF_MAILTO)

# Optional: OpenAI for LLM-powered analysis
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# ─── Scraping Parameters ─────────────────────────────────────────────────────
DEFAULT_YEARS = (2020, 2025)  # Default year range for paper collection
MAX_PAPERS_PER_JOURNAL = 200  # Max papers to fetch per journal per run
REQUEST_DELAY = 1.0  # Seconds between API requests (be polite)
REQUEST_TIMEOUT = 30  # Seconds

# ─── Browser / Library Access ─────────────────────────────────────────────────
# Set these environment variables or update here for authenticated PDF downloads
LIBRARY_PROXY_URL = os.environ.get("LIBRARY_PROXY_URL", "")
# Example: "https://proxy.library.university.edu/login?url="
LIBRARY_USERNAME = os.environ.get("LIBRARY_USERNAME", "")
LIBRARY_PASSWORD = os.environ.get("LIBRARY_PASSWORD", "")

# ─── Analysis Parameters ─────────────────────────────────────────────────────
# Methodology keywords to detect in papers
METHODOLOGY_KEYWORDS = {
    "experiment": [
        "experiment", "experimental design", "between-subjects", "within-subjects",
        "control group", "treatment group", "manipulation", "random assignment",
        "randomized", "lab study", "field experiment", "A/B test",
    ],
    "survey": [
        "survey", "questionnaire", "likert scale", "self-report", "respondents",
        "sample size", "cross-sectional", "longitudinal survey", "panel survey",
    ],
    "archival": [
        "archival data", "secondary data", "panel data", "scanner data",
        "transaction data", "administrative data", "observational data",
        "database", "records", "log data", "clickstream",
    ],
    "qualitative": [
        "qualitative", "interview", "focus group", "ethnography", "grounded theory",
        "thematic analysis", "content analysis", "case study", "netnography",
    ],
    "meta_analysis": [
        "meta-analysis", "meta analysis", "systematic review", "effect size",
        "publication bias", "funnel plot",
    ],
    "analytical_modeling": [
        "analytical model", "game theory", "equilibrium", "optimization",
        "structural model", "dynamic programming", "mechanism design",
    ],
    "simulation": [
        "simulation", "agent-based model", "monte carlo", "computational model",
    ],
    "machine_learning": [
        "machine learning", "deep learning", "neural network", "random forest",
        "NLP", "natural language processing", "text mining", "sentiment analysis",
        "topic model", "LDA", "BERT", "transformer", "classification",
        "clustering", "prediction model",
    ],
}

ANALYTICAL_TECHNIQUES = {
    "regression": [
        "regression", "OLS", "linear regression", "logistic regression",
        "probit", "tobit", "poisson regression", "negative binomial",
    ],
    "SEM": [
        "structural equation", "SEM", "path analysis", "confirmatory factor",
        "CFA", "latent variable",
    ],
    "causal_inference": [
        "instrumental variable", "IV", "difference-in-difference", "DID",
        "regression discontinuity", "RDD", "propensity score", "matching",
        "synthetic control", "causal forest", "double machine learning",
    ],
    "bayesian": [
        "bayesian", "MCMC", "posterior", "prior distribution", "hierarchical bayes",
    ],
    "panel_methods": [
        "fixed effect", "random effect", "panel data", "GMM",
        "dynamic panel", "Arellano-Bond",
    ],
    "time_series": [
        "time series", "VAR", "ARIMA", "Granger causality",
        "cointegration", "impulse response",
    ],
    "choice_modeling": [
        "discrete choice", "conjoint", "multinomial logit", "mixed logit",
        "nested logit", "BLP", "demand estimation",
    ],
    "text_analysis": [
        "text analysis", "sentiment", "topic model", "word embedding",
        "LDA", "word2vec", "BERT", "transformer",
    ],
}

# ─── Dataset Repositories for Idea Validation ─────────────────────────────────
DATASET_SOURCES = [
    {
        "name": "Harvard Dataverse",
        "search_url": "https://dataverse.harvard.edu/api/search",
        "type": "api",
    },
    {
        "name": "ICPSR",
        "search_url": "https://www.icpsr.umich.edu/web/ICPSR/search/studies",
        "type": "web",
    },
    {
        "name": "Kaggle",
        "search_url": "https://www.kaggle.com/api/v1/datasets/list",
        "type": "api",
    },
    {
        "name": "data.gov",
        "search_url": "https://catalog.data.gov/api/3/action/package_search",
        "type": "api",
    },
    {
        "name": "Zenodo",
        "search_url": "https://zenodo.org/api/records",
        "type": "api",
    },
    {
        "name": "Google Dataset Search",
        "search_url": "https://datasetsearch.research.google.com/",
        "type": "web",
    },
]
