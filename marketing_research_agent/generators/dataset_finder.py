"""
Dataset finder that searches online repositories for datasets
matching generated research ideas.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

import requests

from .. import config
from ..database import Database

logger = logging.getLogger(__name__)


class DatasetFinder:
    """Find online datasets relevant to generated research ideas."""

    def __init__(self, db: Database):
        self.db = db
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "MarketingResearchAgent/1.0",
        })

    def find_datasets_for_all_ideas(self, max_per_idea: int = 5) -> int:
        """
        Search for datasets for all generated ideas.
        Returns total number of datasets found.
        """
        ideas = self.db.get_ideas()
        if not ideas:
            logger.warning("No ideas found. Generate ideas first.")
            return 0

        self.db.clear_datasets()
        total = 0

        for idea in ideas:
            datasets = self.find_datasets_for_idea(idea, max_results=max_per_idea)
            total += len(datasets)

        logger.info("Found %d datasets across %d ideas", total, len(ideas))
        return total

    def find_datasets_for_idea(
        self, idea: Dict, max_results: int = 5
    ) -> List[Dict]:
        """Find datasets relevant to a specific research idea."""
        # Build search queries from the idea
        queries = self._build_search_queries(idea)
        all_datasets = []

        for query in queries[:3]:  # Limit to 3 queries per idea
            # Search each repository
            datasets = []
            datasets.extend(self._search_harvard_dataverse(query))
            datasets.extend(self._search_data_gov(query))
            datasets.extend(self._search_zenodo(query))
            datasets.extend(self._search_icpsr_web(query))

            all_datasets.extend(datasets)

        # Deduplicate and score
        unique_datasets = self._deduplicate(all_datasets)
        scored = self._score_relevance(unique_datasets, idea)
        top_datasets = sorted(
            scored, key=lambda x: x.get("relevance_score", 0), reverse=True
        )[:max_results]

        # Save to database
        for ds in top_datasets:
            ds["idea_id"] = idea.get("id")
            self.db.save_dataset(ds)

        return top_datasets

    # ─── Query Building ───────────────────────────────────────────────────────

    def _build_search_queries(self, idea: Dict) -> List[str]:
        """Build search queries from an idea."""
        queries = []

        title = idea.get("title", "")
        description = idea.get("description", "")

        # Extract key terms from title
        if title:
            # Remove common academic words
            stop_words = {
                "applying", "the", "of", "in", "to", "and", "a", "an",
                "for", "with", "using", "study", "research", "examining",
                "investigating", "role", "effect", "impact", "how",
            }
            words = re.findall(r"\b[a-zA-Z]{3,}\b", title.lower())
            key_words = [w for w in words if w not in stop_words]
            if key_words:
                queries.append(" ".join(key_words[:5]))

        # Extract domain-specific terms
        methods = idea.get("suggested_methodology", [])
        if isinstance(methods, str):
            try:
                methods = json.loads(methods)
            except (json.JSONDecodeError, TypeError):
                methods = []

        # Build domain-specific queries
        domain_terms = {
            "consumer_behavior": "consumer behavior purchase decision",
            "digital_marketing": "digital marketing online advertising social media",
            "branding": "brand equity brand perception",
            "pricing": "pricing strategy price elasticity",
            "advertising": "advertising effectiveness media exposure",
            "marketing_strategy": "marketing strategy competitive advantage",
            "marketing_analytics": "marketing analytics customer data",
            "services_marketing": "service quality customer satisfaction",
            "retailing": "retail sales store data",
            "sustainability": "sustainable consumption green marketing",
        }

        # Try to match domain from description
        desc_lower = description.lower()
        for domain, terms in domain_terms.items():
            if domain.replace("_", " ") in desc_lower:
                queries.append(terms)
                break

        # Add method-specific data queries
        method_data_queries = {
            "experiment": "experimental data consumer behavior",
            "survey": "survey data consumer attitudes marketing",
            "archival": "firm-level data marketing performance",
            "machine_learning": "text data reviews social media marketing",
        }
        for method in methods:
            if method in method_data_queries:
                queries.append(method_data_queries[method])

        # Fallback generic query
        if not queries:
            queries.append("marketing consumer data")

        return queries

    # ─── Repository Searches ──────────────────────────────────────────────────

    def _search_harvard_dataverse(self, query: str) -> List[Dict]:
        """Search Harvard Dataverse for datasets."""
        datasets = []
        try:
            response = self.session.get(
                "https://dataverse.harvard.edu/api/search",
                params={
                    "q": query,
                    "type": "dataset",
                    "per_page": 5,
                    "sort": "score",
                    "order": "desc",
                },
                timeout=15,
            )
            if response.status_code == 200:
                data = response.json()
                for item in data.get("data", {}).get("items", []):
                    datasets.append({
                        "name": item.get("name", "Unknown"),
                        "source": "Harvard Dataverse",
                        "source_url": item.get("url", ""),
                        "description": item.get("description", "")[:500],
                        "variables": [],
                        "size_description": "",
                        "access_type": "open",
                    })
        except Exception as e:
            logger.debug("Harvard Dataverse search failed: %s", e)

        return datasets

    def _search_data_gov(self, query: str) -> List[Dict]:
        """Search data.gov for datasets."""
        datasets = []
        try:
            response = self.session.get(
                "https://catalog.data.gov/api/3/action/package_search",
                params={
                    "q": query,
                    "rows": 5,
                    "sort": "score desc",
                },
                timeout=15,
            )
            if response.status_code == 200:
                data = response.json()
                for result in data.get("result", {}).get("results", []):
                    resources = result.get("resources", [])
                    url = resources[0].get("url", "") if resources else ""

                    datasets.append({
                        "name": result.get("title", "Unknown"),
                        "source": "data.gov",
                        "source_url": url or f"https://catalog.data.gov/dataset/{result.get('name', '')}",
                        "description": result.get("notes", "")[:500],
                        "variables": [],
                        "size_description": "",
                        "access_type": "open",
                    })
        except Exception as e:
            logger.debug("data.gov search failed: %s", e)

        return datasets

    def _search_zenodo(self, query: str) -> List[Dict]:
        """Search Zenodo for datasets."""
        datasets = []
        try:
            response = self.session.get(
                "https://zenodo.org/api/records",
                params={
                    "q": query,
                    "type": "dataset",
                    "size": 5,
                    "sort": "bestmatch",
                },
                timeout=15,
            )
            if response.status_code == 200:
                data = response.json()
                for hit in data.get("hits", {}).get("hits", []):
                    metadata = hit.get("metadata", {})
                    datasets.append({
                        "name": metadata.get("title", "Unknown"),
                        "source": "Zenodo",
                        "source_url": hit.get("links", {}).get("html", ""),
                        "description": metadata.get("description", "")[:500],
                        "variables": [
                            kw for kw in metadata.get("keywords", [])
                        ],
                        "size_description": "",
                        "access_type": metadata.get("access_right", "open"),
                    })
        except Exception as e:
            logger.debug("Zenodo search failed: %s", e)

        return datasets

    def _search_icpsr_web(self, query: str) -> List[Dict]:
        """
        Search ICPSR for datasets.
        ICPSR doesn't have a simple public API, so we construct search URLs.
        """
        datasets = []

        # Provide a direct search link for the user
        search_url = (
            f"https://www.icpsr.umich.edu/web/ICPSR/search/studies"
            f"?q={requests.utils.quote(query)}"
        )

        datasets.append({
            "name": f"ICPSR search results for: {query}",
            "source": "ICPSR",
            "source_url": search_url,
            "description": (
                f"Search ICPSR (Inter-university Consortium for Political and Social Research) "
                f"for datasets related to: {query}. ICPSR hosts thousands of social science "
                f"datasets including consumer surveys, economic data, and behavioral studies."
            ),
            "variables": [],
            "size_description": "Varies by dataset",
            "access_type": "restricted",
            "notes": "Requires institutional access (most universities have ICPSR membership)",
        })

        return datasets

    # ─── Well-Known Marketing Datasets ────────────────────────────────────────

    def _get_known_marketing_datasets(self) -> List[Dict]:
        """Return a curated list of well-known marketing research datasets."""
        return [
            {
                "name": "American Customer Satisfaction Index (ACSI)",
                "source": "ACSI / University of Michigan",
                "source_url": "https://www.theacsi.org/",
                "description": "National economic indicator of customer satisfaction. Covers 400+ companies across 47 industries.",
                "variables": ["customer satisfaction", "expectations", "perceived quality", "perceived value", "complaints", "loyalty"],
                "access_type": "restricted",
            },
            {
                "name": "Nielsen Consumer Panel / Homescan",
                "source": "Nielsen / Kilts Center (Chicago Booth)",
                "source_url": "https://www.chicagobooth.edu/research/kilts/datasets/nielsenIQ-nielsen",
                "description": "Household-level purchase data across consumer packaged goods categories.",
                "variables": ["UPC", "quantity", "price", "store", "household demographics"],
                "access_type": "restricted",
                "notes": "Available through Kilts Center at Chicago Booth for academic researchers",
            },
            {
                "name": "Yelp Open Dataset",
                "source": "Yelp",
                "source_url": "https://www.yelp.com/dataset",
                "description": "Business reviews, ratings, and metadata. Great for text analysis and service quality research.",
                "variables": ["reviews", "ratings", "business attributes", "user data", "check-ins"],
                "access_type": "open",
            },
            {
                "name": "Amazon Product Reviews",
                "source": "UCSD / Julian McAuley",
                "source_url": "https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/",
                "description": "Millions of Amazon product reviews across categories. Widely used in marketing research.",
                "variables": ["review text", "rating", "product metadata", "reviewer info", "helpfulness votes"],
                "access_type": "open",
            },
            {
                "name": "Google Trends",
                "source": "Google",
                "source_url": "https://trends.google.com/",
                "description": "Search interest data over time and across regions. Useful for demand estimation and brand tracking.",
                "variables": ["search volume index", "related queries", "geographic breakdown", "temporal trends"],
                "access_type": "open",
            },
            {
                "name": "World Values Survey",
                "source": "WVS Association",
                "source_url": "https://www.worldvaluessurvey.org/",
                "description": "Cross-national survey on values, beliefs, and attitudes. Covers 100+ countries.",
                "variables": ["cultural values", "social attitudes", "economic perceptions", "demographics"],
                "access_type": "open",
            },
            {
                "name": "Compustat / WRDS",
                "source": "S&P Global / Wharton",
                "source_url": "https://wrds-www.wharton.upenn.edu/",
                "description": "Financial and marketing data for public firms. Includes advertising expenditure, R&D, sales.",
                "variables": ["advertising spend", "R&D", "sales", "market share", "financial metrics"],
                "access_type": "restricted",
                "notes": "Available through WRDS with institutional subscription",
            },
            {
                "name": "Twitter/X Academic Research API Data",
                "source": "Various archives",
                "source_url": "https://archive.org/details/twitterstream",
                "description": "Social media posts for text analysis, sentiment analysis, and brand monitoring research.",
                "variables": ["tweet text", "user info", "engagement metrics", "hashtags", "timestamps"],
                "access_type": "open",
            },
            {
                "name": "General Social Survey (GSS)",
                "source": "NORC at University of Chicago",
                "source_url": "https://gss.norc.org/",
                "description": "Long-running US survey on social attitudes, behaviors, and demographics.",
                "variables": ["attitudes", "behaviors", "demographics", "spending patterns"],
                "access_type": "open",
            },
            {
                "name": "Bureau of Labor Statistics Consumer Expenditure Survey",
                "source": "BLS",
                "source_url": "https://www.bls.gov/cex/",
                "description": "Detailed consumer spending data across categories, demographics, and regions.",
                "variables": ["expenditure categories", "income", "demographics", "geographic data"],
                "access_type": "open",
            },
        ]

    # ─── Scoring & Utilities ──────────────────────────────────────────────────

    def _score_relevance(self, datasets: List[Dict], idea: Dict) -> List[Dict]:
        """Score dataset relevance to the idea."""
        idea_text = (
            idea.get("title", "") + " " +
            idea.get("description", "")
        ).lower()

        idea_words = set(re.findall(r"\b[a-z]{3,}\b", idea_text))

        for ds in datasets:
            ds_text = (
                ds.get("name", "") + " " +
                ds.get("description", "")
            ).lower()
            ds_words = set(re.findall(r"\b[a-z]{3,}\b", ds_text))

            # Simple word overlap score
            overlap = len(idea_words & ds_words)
            total = max(len(idea_words), 1)
            ds["relevance_score"] = min(1.0, overlap / total * 3)

            # Boost open access datasets
            if ds.get("access_type") == "open":
                ds["relevance_score"] = min(1.0, ds["relevance_score"] + 0.1)

        return datasets

    def _deduplicate(self, datasets: List[Dict]) -> List[Dict]:
        """Remove duplicate datasets based on name similarity."""
        seen_names = set()
        unique = []
        for ds in datasets:
            name_key = ds.get("name", "").lower().strip()[:50]
            if name_key not in seen_names:
                seen_names.add(name_key)
                unique.append(ds)
        return unique

    def find_datasets_with_known_sources(self, idea: Dict) -> List[Dict]:
        """
        Combine API search results with curated known marketing datasets.
        """
        # Get API results
        api_datasets = self.find_datasets_for_idea(idea, max_results=5)

        # Get relevant known datasets
        known = self._get_known_marketing_datasets()
        scored_known = self._score_relevance(known, idea)
        relevant_known = [
            ds for ds in scored_known if ds.get("relevance_score", 0) > 0.1
        ]

        # Combine and sort
        all_datasets = api_datasets + relevant_known
        all_datasets.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

        return all_datasets[:10]

    def get_datasets_summary(self) -> str:
        """Generate a human-readable summary of found datasets."""
        ideas = self.db.get_ideas()
        if not ideas:
            return "No ideas found. Generate ideas first."

        parts = []
        parts.append("=" * 70)
        parts.append("DATASET RECOMMENDATIONS")
        parts.append("=" * 70)

        for idea in ideas:
            datasets = self.db.get_datasets(idea_id=idea.get("id"))
            if not datasets:
                continue

            parts.append(f"\n{'─' * 60}")
            parts.append(f"  FOR IDEA: {idea['title']}")
            parts.append(f"{'─' * 60}")

            for ds in datasets:
                relevance = ds.get("relevance_score", 0)
                access = ds.get("access_type", "unknown")
                parts.append(f"\n  📊 {ds['name']}")
                parts.append(f"     Source: {ds['source']}")
                parts.append(f"     URL: {ds.get('source_url', 'N/A')}")
                parts.append(f"     Access: {access} | Relevance: {relevance:.2f}")
                if ds.get("description"):
                    desc = ds["description"][:200]
                    parts.append(f"     {desc}")
                if ds.get("notes"):
                    parts.append(f"     Note: {ds['notes']}")

        return "\n".join(parts)

    def close(self):
        """Close the HTTP session."""
        self.session.close()
