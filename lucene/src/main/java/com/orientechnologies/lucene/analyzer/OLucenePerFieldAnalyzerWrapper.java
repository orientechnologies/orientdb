package com.orientechnologies.lucene.analyzer;

import static com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract.RID;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

/**
 * Created by frank on 10/12/15.
 *
 * <p>Doesn't allow to wrap components or readers. Thread local resources can be delegated to the
 * delegate analyzer, but not allocated on this analyzer (limit memory consumption). Uses a per
 * field reuse strategy.
 */
public class OLucenePerFieldAnalyzerWrapper extends DelegatingAnalyzerWrapper {
  private final Analyzer defaultDelegateAnalyzer;
  private final Map<String, Analyzer> fieldAnalyzers;

  /**
   * Constructs with default analyzer.
   *
   * @param defaultAnalyzer Any fields not specifically defined to use a different analyzer will use
   *     the one provided here.
   */
  public OLucenePerFieldAnalyzerWrapper(final Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, new HashMap<>());
  }

  /**
   * Constructs with default analyzer and a map of analyzers to use for specific fields.
   *
   * @param defaultAnalyzer Any fields not specifically defined to use a different analyzer will use
   *     the one provided here.
   * @param fieldAnalyzers a Map (String field name to the Analyzer) to be used for those fields
   */
  public OLucenePerFieldAnalyzerWrapper(
      final Analyzer defaultAnalyzer, final Map<String, Analyzer> fieldAnalyzers) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.defaultDelegateAnalyzer = defaultAnalyzer;
    this.fieldAnalyzers = new HashMap<>();

    this.fieldAnalyzers.putAll(fieldAnalyzers);

    this.fieldAnalyzers.put(RID, new KeywordAnalyzer());
    this.fieldAnalyzers.put("_CLASS", new KeywordAnalyzer());
    this.fieldAnalyzers.put("_CLUSTER", new KeywordAnalyzer());
    this.fieldAnalyzers.put("_JSON", new KeywordAnalyzer());
  }

  @Override
  protected Analyzer getWrappedAnalyzer(final String fieldName) {
    final Analyzer analyzer = fieldAnalyzers.get(fieldName);
    return (analyzer != null) ? analyzer : defaultDelegateAnalyzer;
  }

  @Override
  public String toString() {
    return "PerFieldAnalyzerWrapper("
        + fieldAnalyzers
        + ", default="
        + defaultDelegateAnalyzer
        + ")";
  }

  public OLucenePerFieldAnalyzerWrapper add(final String field, final Analyzer analyzer) {
    fieldAnalyzers.put(field, analyzer);
    return this;
  }

  public OLucenePerFieldAnalyzerWrapper add(final OLucenePerFieldAnalyzerWrapper analyzer) {
    fieldAnalyzers.putAll(analyzer.getAnalyzers());
    return this;
  }

  public OLucenePerFieldAnalyzerWrapper remove(final String field) {
    fieldAnalyzers.remove(field);
    return this;
  }

  protected Map<String, Analyzer> getAnalyzers() {
    return fieldAnalyzers;
  }
}
