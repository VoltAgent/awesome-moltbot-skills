import sqlite3 from 'sqlite3';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

class KnowledgeDatabase {
  constructor(dbPath = null) {
    this.dbPath = dbPath || process.env.DATABASE_PATH || './data/papers.db';
    this.db = null;
    this.ensureDataDirectory();
  }

  ensureDataDirectory() {
    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }

  async initialize() {
    return new Promise((resolve, reject) => {
      this.db = new sqlite3.Database(this.dbPath, (err) => {
        if (err) {
          console.error('Error opening database:', err);
          reject(err);
        } else {
          console.log('Database connected successfully');
          this.createTables().then(resolve).catch(reject);
        }
      });
    });
  }

  async createTables() {
    const schemas = [
      `CREATE TABLE IF NOT EXISTS papers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        authors TEXT,
        journal TEXT,
        journal_key TEXT,
        year INTEGER,
        abstract TEXT,
        url TEXT,
        pdf_path TEXT,
        download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        citations INTEGER DEFAULT 0,
        methodology TEXT,
        data_sources TEXT,
        analysis_methods TEXT,
        key_findings TEXT,
        theoretical_framework TEXT,
        UNIQUE(title, journal)
      )`,

      `CREATE TABLE IF NOT EXISTS analysis_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        paper_id INTEGER,
        analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        success_score REAL,
        data_quality_score REAL,
        methodology_rigor REAL,
        novelty_score REAL,
        impact_score REAL,
        key_success_factors TEXT,
        methodology_details TEXT,
        data_characteristics TEXT,
        analytical_approach TEXT,
        recommendations TEXT,
        FOREIGN KEY(paper_id) REFERENCES papers(id)
      )`,

      `CREATE TABLE IF NOT EXISTS research_ideas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        based_on_papers TEXT,
        research_question TEXT,
        proposed_methodology TEXT,
        required_data TEXT,
        expected_contribution TEXT,
        generation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        feasibility_score REAL,
        novelty_score REAL,
        status TEXT DEFAULT 'generated'
      )`,

      `CREATE TABLE IF NOT EXISTS datasets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        source TEXT,
        url TEXT,
        description TEXT,
        data_type TEXT,
        variables TEXT,
        time_period TEXT,
        geography TEXT,
        accessibility TEXT,
        matched_idea_id INTEGER,
        relevance_score REAL,
        discovery_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(matched_idea_id) REFERENCES research_ideas(id)
      )`,

      `CREATE TABLE IF NOT EXISTS success_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_type TEXT,
        pattern_description TEXT,
        frequency INTEGER DEFAULT 1,
        example_papers TEXT,
        journals TEXT,
        identified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`
    ];

    for (const schema of schemas) {
      await this.run(schema);
    }

    console.log('Database tables created successfully');
  }

  run(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.run(sql, params, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve({ id: this.lastID, changes: this.changes });
        }
      });
    });
  }

  get(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve(row);
        }
      });
    });
  }

  all(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }

  async insertPaper(paper) {
    const sql = `INSERT OR IGNORE INTO papers
      (title, authors, journal, journal_key, year, abstract, url, pdf_path, citations)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const params = [
      paper.title,
      paper.authors,
      paper.journal,
      paper.journalKey,
      paper.year,
      paper.abstract || '',
      paper.url,
      paper.pdfPath || null,
      paper.citations || 0
    ];

    return await this.run(sql, params);
  }

  async insertAnalysis(analysis) {
    const sql = `INSERT INTO analysis_results
      (paper_id, success_score, data_quality_score, methodology_rigor,
       novelty_score, impact_score, key_success_factors, methodology_details,
       data_characteristics, analytical_approach, recommendations)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const params = [
      analysis.paperId,
      analysis.successScore,
      analysis.dataQualityScore,
      analysis.methodologyRigor,
      analysis.noveltyScore,
      analysis.impactScore,
      JSON.stringify(analysis.keySuccessFactors),
      JSON.stringify(analysis.methodologyDetails),
      JSON.stringify(analysis.dataCharacteristics),
      JSON.stringify(analysis.analyticalApproach),
      JSON.stringify(analysis.recommendations)
    ];

    return await this.run(sql, params);
  }

  async insertResearchIdea(idea) {
    const sql = `INSERT INTO research_ideas
      (title, description, based_on_papers, research_question, proposed_methodology,
       required_data, expected_contribution, feasibility_score, novelty_score, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const params = [
      idea.title,
      idea.description,
      JSON.stringify(idea.basedOnPapers),
      idea.researchQuestion,
      idea.proposedMethodology,
      JSON.stringify(idea.requiredData),
      idea.expectedContribution,
      idea.feasibilityScore,
      idea.noveltyScore,
      idea.status || 'generated'
    ];

    return await this.run(sql, params);
  }

  async insertDataset(dataset) {
    const sql = `INSERT INTO datasets
      (name, source, url, description, data_type, variables, time_period,
       geography, accessibility, matched_idea_id, relevance_score)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const params = [
      dataset.name,
      dataset.source,
      dataset.url,
      dataset.description,
      dataset.dataType,
      JSON.stringify(dataset.variables),
      dataset.timePeriod,
      dataset.geography,
      dataset.accessibility,
      dataset.matchedIdeaId,
      dataset.relevanceScore
    ];

    return await this.run(sql, params);
  }

  async insertSuccessPattern(pattern) {
    const sql = `INSERT INTO success_patterns
      (pattern_type, pattern_description, frequency, example_papers, journals)
      VALUES (?, ?, ?, ?, ?)`;

    const params = [
      pattern.patternType,
      pattern.patternDescription,
      pattern.frequency,
      JSON.stringify(pattern.examplePapers),
      JSON.stringify(pattern.journals)
    ];

    return await this.run(sql, params);
  }

  async getPaperByTitle(title) {
    return await this.get('SELECT * FROM papers WHERE title = ?', [title]);
  }

  async getAllPapers() {
    return await this.all('SELECT * FROM papers ORDER BY year DESC, citations DESC');
  }

  async getPapersWithoutAnalysis() {
    const sql = `SELECT p.* FROM papers p
      LEFT JOIN analysis_results a ON p.id = a.paper_id
      WHERE a.id IS NULL`;
    return await this.all(sql);
  }

  async getTopPapers(limit = 50) {
    const sql = `SELECT p.*, a.success_score, a.impact_score
      FROM papers p
      LEFT JOIN analysis_results a ON p.id = a.paper_id
      ORDER BY a.success_score DESC, p.citations DESC
      LIMIT ?`;
    return await this.all(sql, [limit]);
  }

  async getSuccessPatterns() {
    return await this.all('SELECT * FROM success_patterns ORDER BY frequency DESC');
  }

  async getResearchIdeas(status = null) {
    if (status) {
      return await this.all('SELECT * FROM research_ideas WHERE status = ? ORDER BY novelty_score DESC', [status]);
    }
    return await this.all('SELECT * FROM research_ideas ORDER BY novelty_score DESC');
  }

  async getDatasetsByIdeaId(ideaId) {
    return await this.all('SELECT * FROM datasets WHERE matched_idea_id = ? ORDER BY relevance_score DESC', [ideaId]);
  }

  async updatePaperMetadata(paperId, metadata) {
    const sql = `UPDATE papers
      SET methodology = ?, data_sources = ?, analysis_methods = ?,
          key_findings = ?, theoretical_framework = ?
      WHERE id = ?`;

    const params = [
      metadata.methodology,
      JSON.stringify(metadata.dataSources),
      JSON.stringify(metadata.analysisMethods),
      metadata.keyFindings,
      metadata.theoreticalFramework,
      paperId
    ];

    return await this.run(sql, params);
  }

  async close() {
    return new Promise((resolve, reject) => {
      this.db.close((err) => {
        if (err) {
          reject(err);
        } else {
          console.log('Database connection closed');
          resolve();
        }
      });
    });
  }
}

export default KnowledgeDatabase;
