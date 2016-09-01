package com.orientechnologies.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by frank on 10/12/15.
 */
public class OLucenePerFieldAnalyzerWrapper extends DelegatingAnalyzerWrapper {
  private final Analyzer              defaultAnalyzer;
  private final Map<String, Analyzer> fieldAnalyzers;

  /**
   * Constructs with default analyzer.
   *
   * @param defaultAnalyzer
   *          Any fields not specifically defined to use a different analyzer will use the one provided here.
   */
  public OLucenePerFieldAnalyzerWrapper(Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, null);
  }

  /**
   * Constructs with default analyzer and a map of analyzers to use for specific fields.
   *
   * @param defaultAnalyzer
   *          Any fields not specifically defined to use a different analyzer will use the one provided here.
   * @param fieldAnalyzers
   *          a Map (String field name to the Analyzer) to be used for those fields
   */
  public OLucenePerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, Map<String, Analyzer> fieldAnalyzers) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.defaultAnalyzer = defaultAnalyzer;
    this.fieldAnalyzers = new HashMap<String, Analyzer>();

    if (fieldAnalyzers != null && !fieldAnalyzers.isEmpty()) {
      this.fieldAnalyzers.putAll(fieldAnalyzers);
    }

    this.fieldAnalyzers.put("RID", new KeywordAnalyzer());
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    Analyzer analyzer = fieldAnalyzers.get(fieldName);
    return (analyzer != null) ? analyzer : defaultAnalyzer;
  }

  @Override
  public String toString() {
    return "PerFieldAnalyzerWrapper(" + fieldAnalyzers + ", default=" + defaultAnalyzer + ")";
  }

  public OLucenePerFieldAnalyzerWrapper add(String field, Analyzer analyzer) {
    fieldAnalyzers.put(field, analyzer);
    return this;
  }

  public OLucenePerFieldAnalyzerWrapper remove(String field) {
    fieldAnalyzers.remove(field);
    return this;
  }
}