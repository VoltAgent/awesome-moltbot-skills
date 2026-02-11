import OpenAI from 'openai';
import axios from 'axios';
import * as cheerio from 'cheerio';
import dotenv from 'dotenv';
import KnowledgeDatabase from './database.js';

dotenv.config();

class DatasetFinder {
  constructor() {
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.model = process.env.ANALYSIS_MODEL || 'gpt-4-turbo-preview';
    this.db = null;

    // Dataset repositories
    this.repositories = [
      {
        name: 'Google Dataset Search',
        url: 'https://datasetsearch.research.google.com',
        searchUrl: 'https://datasetsearch.research.google.com/search?query='
      },
      {
        name: 'Kaggle',
        url: 'https://www.kaggle.com',
        apiUrl: 'https://www.kaggle.com/api/v1/datasets/list'
      },
      {
        name: 'Data.gov',
        url: 'https://data.gov',
        apiUrl: 'https://catalog.data.gov/api/3/action/package_search'
      },
      {
        name: 'UCI Machine Learning Repository',
        url: 'https://archive.ics.uci.edu/ml/index.php',
        searchUrl: 'https://archive.ics.uci.edu/ml/datasets.php'
      },
      {
        name: 'Hugging Face Datasets',
        url: 'https://huggingface.co/datasets',
        apiUrl: 'https://huggingface.co/api/datasets'
      },
      {
        name: 'Awesome Public Datasets (GitHub)',
        url: 'https://github.com/awesomedata/awesome-public-datasets',
        type: 'github'
      },
      {
        name: 'World Bank Open Data',
        url: 'https://data.worldbank.org',
        apiUrl: 'https://api.worldbank.org/v2/datacatalog'
      },
      {
        name: 'OECD Data',
        url: 'https://data.oecd.org',
        type: 'curated'
      }
    ];
  }

  async initialize() {
    this.db = new KnowledgeDatabase();
    await this.db.initialize();
    console.log('Dataset Finder initialized');
  }

  async findDatasetsForIdea(idea) {
    console.log(`\nFinding datasets for: ${idea.title}`);

    // Parse required data from the idea
    let requiredData = [];
    try {
      requiredData = typeof idea.required_data === 'string'
        ? JSON.parse(idea.required_data)
        : idea.required_data;
    } catch (e) {
      requiredData = [idea.required_data];
    }

    // Generate search queries using AI
    const searchQueries = await this.generateSearchQueries(idea, requiredData);

    const allDatasets = [];

    // Search each repository
    for (const query of searchQueries) {
      console.log(`  Searching for: ${query}`);

      // Search multiple repositories
      const datasets = await Promise.all([
        this.searchDataGov(query),
        this.searchKaggle(query),
        this.searchHuggingFace(query),
        this.searchGeneric(query)
      ]);

      allDatasets.push(...datasets.flat());

      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Remove duplicates
    const uniqueDatasets = this.deduplicateDatasets(allDatasets);

    // Score and rank datasets by relevance
    const scoredDatasets = await this.scoreDatasets(uniqueDatasets, idea);

    // Save top datasets to database
    const topDatasets = scoredDatasets.slice(0, 10);
    for (const dataset of topDatasets) {
      await this.db.insertDataset({
        name: dataset.name,
        source: dataset.source,
        url: dataset.url,
        description: dataset.description,
        dataType: dataset.dataType,
        variables: dataset.variables || [],
        timePeriod: dataset.timePeriod,
        geography: dataset.geography,
        accessibility: dataset.accessibility,
        matchedIdeaId: idea.id,
        relevanceScore: dataset.relevanceScore
      });
    }

    console.log(`✓ Found ${topDatasets.length} relevant datasets`);

    return topDatasets;
  }

  async generateSearchQueries(idea, requiredData) {
    const prompt = `Generate 5-7 effective search queries to find datasets for this research idea:

Title: ${idea.title}
Research Question: ${idea.research_question}
Required Data: ${JSON.stringify(requiredData)}

Generate search queries that will find relevant datasets in public repositories.
Focus on:
- Specific data types needed
- Variables of interest
- Context/domain
- Time period if relevant

Provide response as JSON array of strings.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a data scientist expert at finding relevant datasets for research projects.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.5
      });

      const result = JSON.parse(response.choices[0].message.content);
      return result.queries || result.search_queries || [];

    } catch (error) {
      console.error('Error generating search queries:', error.message);
      return [idea.title, ...requiredData.slice(0, 3)];
    }
  }

  async searchDataGov(query) {
    try {
      const response = await axios.get('https://catalog.data.gov/api/3/action/package_search', {
        params: {
          q: query,
          rows: 10
        },
        timeout: 10000
      });

      if (response.data.success && response.data.result.results) {
        return response.data.result.results.map(dataset => ({
          name: dataset.title,
          source: 'Data.gov',
          url: `https://catalog.data.gov/dataset/${dataset.name}`,
          description: dataset.notes || '',
          dataType: dataset.type || 'dataset',
          variables: dataset.resources?.map(r => r.name) || [],
          timePeriod: dataset.temporal || 'Unknown',
          geography: dataset.spatial || 'United States',
          accessibility: 'Public',
          metadata: dataset
        }));
      }
    } catch (error) {
      console.error('Error searching Data.gov:', error.message);
    }

    return [];
  }

  async searchKaggle(query) {
    // Note: Kaggle API requires authentication
    // This is a simplified version using web scraping as fallback
    try {
      const searchUrl = `https://www.kaggle.com/search?q=${encodeURIComponent(query)}`;
      const response = await axios.get(searchUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        timeout: 10000
      });

      // Note: Kaggle's structure changes frequently
      // This is a placeholder - in production, use Kaggle API
      return [];

    } catch (error) {
      console.error('Error searching Kaggle:', error.message);
      return [];
    }
  }

  async searchHuggingFace(query) {
    try {
      const response = await axios.get('https://huggingface.co/api/datasets', {
        params: {
          search: query,
          limit: 10
        },
        timeout: 10000
      });

      if (Array.isArray(response.data)) {
        return response.data.map(dataset => ({
          name: dataset.id,
          source: 'Hugging Face',
          url: `https://huggingface.co/datasets/${dataset.id}`,
          description: dataset.description || '',
          dataType: dataset.task || 'dataset',
          variables: dataset.tags || [],
          timePeriod: 'Varies',
          geography: 'Global',
          accessibility: 'Public',
          metadata: dataset
        }));
      }
    } catch (error) {
      console.error('Error searching Hugging Face:', error.message);
    }

    return [];
  }

  async searchGeneric(query) {
    // Use AI to suggest potential datasets based on query
    const prompt = `Suggest 3-5 publicly available datasets that would be relevant for this search query: "${query}"

For each dataset provide:
- Name
- Source/Repository
- URL (if known, or repository where it can be found)
- Brief description
- Data type
- Key variables
- Accessibility

Focus on real, publicly available datasets.

Provide response as JSON array.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a data librarian with extensive knowledge of public datasets across various domains.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.6
      });

      const result = JSON.parse(response.choices[0].message.content);
      const datasets = result.datasets || [];

      return datasets.map(d => ({
        name: d.name,
        source: d.source || 'AI Suggested',
        url: d.url || '',
        description: d.description,
        dataType: d.data_type || d.dataType,
        variables: d.variables || d.key_variables || [],
        timePeriod: d.time_period || 'Varies',
        geography: d.geography || 'Global',
        accessibility: d.accessibility || 'Public'
      }));

    } catch (error) {
      console.error('Error with AI dataset suggestions:', error.message);
      return [];
    }
  }

  deduplicateDatasets(datasets) {
    const seen = new Set();
    const unique = [];

    for (const dataset of datasets) {
      const key = `${dataset.name}_${dataset.source}`.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(dataset);
      }
    }

    return unique;
  }

  async scoreDatasets(datasets, idea) {
    console.log(`\n  Scoring ${datasets.length} datasets for relevance...`);

    const prompt = `Score how relevant each dataset is for this research idea:

RESEARCH IDEA:
${idea.title}
${idea.research_question}
Required Data: ${idea.required_data}

DATASETS:
${JSON.stringify(datasets.map(d => ({
  name: d.name,
  source: d.source,
  description: d.description,
  variables: d.variables
})), null, 2)}

For each dataset, provide a relevance score (0-100) based on:
- Match with required data
- Availability of needed variables
- Data quality and completeness
- Accessibility
- Fit with research design

Provide response as JSON object with dataset names as keys and scores as values.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a research data expert evaluating dataset fit for research projects.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.3
      });

      const scores = JSON.parse(response.choices[0].message.content);

      // Add scores to datasets
      datasets.forEach(dataset => {
        dataset.relevanceScore = scores[dataset.name] || scores.scores?.[dataset.name] || 50;
      });

      // Sort by relevance
      datasets.sort((a, b) => b.relevanceScore - a.relevanceScore);

      return datasets;

    } catch (error) {
      console.error('Error scoring datasets:', error.message);
      return datasets;
    }
  }

  async findDatasetsForAllIdeas() {
    const ideas = await this.db.getResearchIdeas('generated');
    console.log(`\nFinding datasets for ${ideas.length} ideas...`);

    for (const idea of ideas) {
      try {
        await this.findDatasetsForIdea(idea);

        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 3000));
      } catch (error) {
        console.error(`Error finding datasets for idea ${idea.id}:`, error.message);
      }
    }

    console.log('\n✓ Dataset discovery complete');
  }

  async generateDatasetReport(ideaId) {
    const ideas = await this.db.getResearchIdeas();
    const idea = ideas.find(i => i.id === ideaId);

    if (!idea) {
      console.error('Idea not found');
      return;
    }

    const datasets = await this.db.getDatasetsByIdeaId(ideaId);

    console.log('\n' + '='.repeat(80));
    console.log(`DATASET REPORT FOR: ${idea.title}`);
    console.log('='.repeat(80));

    console.log(`\nResearch Question: ${idea.research_question}`);
    console.log(`\nFound ${datasets.length} relevant datasets:\n`);

    datasets.forEach((dataset, index) => {
      console.log(`${index + 1}. ${dataset.name}`);
      console.log(`   Source: ${dataset.source}`);
      console.log(`   URL: ${dataset.url}`);
      console.log(`   Relevance: ${dataset.relevance_score}/100`);
      console.log(`   Description: ${dataset.description?.substring(0, 100)}...`);
      console.log('');
    });
  }

  async close() {
    if (this.db) {
      await this.db.close();
    }
  }
}

// CLI usage
if (import.meta.url === `file://${process.argv[1]}`) {
  const finder = new DatasetFinder();

  (async () => {
    try {
      await finder.initialize();
      await finder.findDatasetsForAllIdeas();

      console.log('\n✓ Dataset finding complete');
    } catch (error) {
      console.error('Error:', error);
    } finally {
      await finder.close();
    }
  })();
}

export default DatasetFinder;
