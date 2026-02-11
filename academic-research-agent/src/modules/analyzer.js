import OpenAI from 'openai';
import fs from 'fs';
import pdf from 'pdf-parse';
import natural from 'natural';
import dotenv from 'dotenv';
import KnowledgeDatabase from './database.js';

dotenv.config();

const tokenizer = new natural.WordTokenizer();
const TfIdf = natural.TfIdf;

class PaperAnalyzer {
  constructor() {
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.model = process.env.ANALYSIS_MODEL || 'gpt-4-turbo-preview';
    this.db = null;
  }

  async initialize() {
    this.db = new KnowledgeDatabase();
    await this.db.initialize();
    console.log('Paper Analyzer initialized');
  }

  async extractTextFromPDF(pdfPath) {
    try {
      const dataBuffer = fs.readFileSync(pdfPath);
      const data = await pdf(dataBuffer);
      return data.text;
    } catch (error) {
      console.error(`Error extracting PDF text: ${error.message}`);
      return null;
    }
  }

  async analyzePaperStructure(text) {
    // Extract key sections using heuristics
    const sections = {
      abstract: this.extractSection(text, ['abstract', 'summary']),
      introduction: this.extractSection(text, ['introduction', '1. introduction']),
      methodology: this.extractSection(text, ['method', 'methodology', 'research design', 'data and methods']),
      results: this.extractSection(text, ['results', 'findings', 'analysis']),
      discussion: this.extractSection(text, ['discussion', 'conclusion']),
      references: this.extractSection(text, ['references', 'bibliography'])
    };

    return sections;
  }

  extractSection(text, keywords) {
    const lines = text.split('\n');
    let sectionText = '';
    let capturing = false;
    let sectionStart = -1;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].toLowerCase().trim();

      // Check if line starts a section
      if (keywords.some(kw => line.startsWith(kw) || line === kw)) {
        capturing = true;
        sectionStart = i;
        continue;
      }

      // Stop capturing at next major section
      if (capturing && line.match(/^(introduction|method|results|discussion|references|conclusion)/i)
          && !keywords.some(kw => line.includes(kw))) {
        break;
      }

      if (capturing) {
        sectionText += lines[i] + '\n';
      }
    }

    return sectionText.trim();
  }

  async analyzeMethodology(methodologyText) {
    const prompt = `Analyze this methodology section from an academic marketing paper and extract:

1. Research Design (e.g., experimental, survey, observational, mixed-methods)
2. Data Sources (e.g., primary/secondary, specific datasets used)
3. Sample Size and Characteristics
4. Analytical Methods (e.g., regression, SEM, experiments, qualitative analysis)
5. Statistical Techniques
6. Variables and Measures

Methodology Text:
${methodologyText.substring(0, 3000)}

Provide a structured JSON response.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are an expert in academic research methodology, specializing in marketing research. Provide detailed, structured analysis.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.3
      });

      return JSON.parse(response.choices[0].message.content);
    } catch (error) {
      console.error('Error analyzing methodology:', error.message);
      return null;
    }
  }

  async assessSuccessFactors(paper, fullText) {
    const prompt = `Analyze this academic marketing paper and assess what makes it publishable and successful in top-tier journals.

Title: ${paper.title}
Journal: ${paper.journal}
Citations: ${paper.citations || 'Unknown'}

Full Text Sample:
${fullText.substring(0, 4000)}

Evaluate and score (0-100) the following dimensions:

1. DATA QUALITY & NOVELTY
   - Uniqueness of dataset
   - Data richness and granularity
   - Access to proprietary/novel data

2. METHODOLOGY RIGOR
   - Appropriateness of methods
   - Statistical sophistication
   - Validity of research design

3. THEORETICAL CONTRIBUTION
   - Novel theoretical insights
   - Extension of existing theory
   - Cross-disciplinary integration

4. PRACTICAL RELEVANCE
   - Managerial implications
   - Industry applicability
   - Policy implications

5. NOVELTY & INNOVATION
   - Research question originality
   - Methodological innovation
   - Counterintuitive findings

6. IMPACT POTENTIAL
   - Significance of findings
   - Generalizability
   - Future research opportunities

For each dimension, provide:
- Score (0-100)
- Justification
- Specific examples from the paper

Also identify:
- Key success factors (what made this paper stand out)
- Replicable patterns for future research
- Data characteristics that contributed to success

Provide response as JSON.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a senior academic editor with expertise in evaluating research papers for top marketing journals. You understand what makes papers publishable in JM, JMR, Marketing Science, JCR, JAMS, and IJRM.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.4
      });

      return JSON.parse(response.choices[0].message.content);
    } catch (error) {
      console.error('Error assessing success factors:', error.message);
      return null;
    }
  }

  async extractKeywords(text) {
    const tfidf = new TfIdf();
    tfidf.addDocument(text);

    const keywords = [];
    tfidf.listTerms(0).forEach((item, index) => {
      if (index < 20) {
        keywords.push({ term: item.term, score: item.tfidf });
      }
    });

    return keywords;
  }

  async analyzePaper(paper) {
    console.log(`\nAnalyzing: ${paper.title}`);

    let fullText = '';
    let sections = null;

    // Extract text from PDF if available
    if (paper.pdf_path && fs.existsSync(paper.pdf_path)) {
      fullText = await this.extractTextFromPDF(paper.pdf_path);
      if (fullText) {
        sections = await this.analyzePaperStructure(fullText);
      }
    }

    // If no PDF, use abstract
    if (!fullText && paper.abstract) {
      fullText = paper.abstract;
    }

    if (!fullText) {
      console.log('No text available for analysis');
      return null;
    }

    // Analyze methodology
    const methodologyAnalysis = sections?.methodology
      ? await this.analyzeMethodology(sections.methodology)
      : null;

    // Assess success factors
    const successAssessment = await this.assessSuccessFactors(paper, fullText);

    if (!successAssessment) {
      return null;
    }

    // Calculate overall success score
    const successScore = (
      (successAssessment.data_quality_score || 0) * 0.25 +
      (successAssessment.methodology_rigor || 0) * 0.20 +
      (successAssessment.theoretical_contribution || 0) * 0.20 +
      (successAssessment.novelty_score || 0) * 0.20 +
      (successAssessment.impact_potential || 0) * 0.15
    );

    // Extract keywords
    const keywords = await this.extractKeywords(fullText);

    // Prepare analysis result
    const analysisResult = {
      paperId: paper.id,
      successScore: successScore,
      dataQualityScore: successAssessment.data_quality_score || 0,
      methodologyRigor: successAssessment.methodology_rigor || 0,
      noveltyScore: successAssessment.novelty_score || 0,
      impactScore: successAssessment.impact_potential || 0,
      keySuccessFactors: successAssessment.key_success_factors || [],
      methodologyDetails: methodologyAnalysis || {},
      dataCharacteristics: successAssessment.data_characteristics || {},
      analyticalApproach: successAssessment.analytical_approach || {},
      recommendations: successAssessment.replicable_patterns || []
    };

    // Save to database
    await this.db.insertAnalysis(analysisResult);

    // Update paper metadata
    if (methodologyAnalysis) {
      await this.db.updatePaperMetadata(paper.id, {
        methodology: methodologyAnalysis.research_design || '',
        dataSources: methodologyAnalysis.data_sources || [],
        analysisMethods: methodologyAnalysis.analytical_methods || [],
        keyFindings: fullText.substring(0, 500),
        theoreticalFramework: sections?.introduction?.substring(0, 500) || ''
      });
    }

    console.log(`✓ Analysis complete. Success Score: ${successScore.toFixed(2)}`);

    return analysisResult;
  }

  async analyzeAllPapers() {
    const papers = await this.db.getPapersWithoutAnalysis();
    console.log(`\nFound ${papers.length} papers to analyze`);

    const results = [];

    for (const paper of papers) {
      try {
        const result = await this.analyzePaper(paper);
        if (result) {
          results.push(result);
        }

        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        console.error(`Error analyzing paper ${paper.id}:`, error.message);
      }
    }

    return results;
  }

  async identifySuccessPatterns() {
    console.log('\nIdentifying success patterns across papers...');

    const topPapers = await this.db.getTopPapers(30);

    if (topPapers.length === 0) {
      console.log('No analyzed papers found');
      return;
    }

    const prompt = `Analyze these top-performing marketing research papers and identify common success patterns:

${topPapers.map((p, i) => `
${i + 1}. ${p.title}
   Journal: ${p.journal}
   Citations: ${p.citations}
   Success Score: ${p.success_score}
`).join('\n')}

Identify patterns in:
1. Data types and sources commonly used
2. Methodological approaches that succeed
3. Research design patterns
4. Theoretical frameworks
5. Types of research questions

For each pattern, provide:
- Pattern description
- Frequency/prevalence
- Example papers
- Why this pattern leads to success

Provide response as JSON array of patterns.`;

    try {
      const response = await this.openai.chat.completions.create({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a research methodology expert analyzing publication patterns in top marketing journals.'
          },
          { role: 'user', content: prompt }
        ],
        response_format: { type: 'json_object' },
        temperature: 0.5
      });

      const patterns = JSON.parse(response.choices[0].message.content);

      // Save patterns to database
      if (patterns.patterns) {
        for (const pattern of patterns.patterns) {
          await this.db.insertSuccessPattern({
            patternType: pattern.type,
            patternDescription: pattern.description,
            frequency: pattern.frequency || 1,
            examplePapers: pattern.examples || [],
            journals: pattern.journals || []
          });
        }
      }

      console.log(`✓ Identified ${patterns.patterns?.length || 0} success patterns`);

      return patterns;
    } catch (error) {
      console.error('Error identifying patterns:', error.message);
      return null;
    }
  }

  async close() {
    if (this.db) {
      await this.db.close();
    }
  }
}

// CLI usage
if (import.meta.url === `file://${process.argv[1]}`) {
  const analyzer = new PaperAnalyzer();

  (async () => {
    try {
      await analyzer.initialize();
      await analyzer.analyzeAllPapers();
      await analyzer.identifySuccessPatterns();

      console.log('\n✓ Analysis complete');
    } catch (error) {
      console.error('Error:', error);
    } finally {
      await analyzer.close();
    }
  })();
}

export default PaperAnalyzer;
