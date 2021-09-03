package com.orientechnologies.lucene.analyzer;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Locale;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/** Created by frank on 30/10/2015. */
public class OLuceneAnalyzerFactory {
  public Analyzer createAnalyzer(
      final OIndexDefinition index, final AnalyzerKind kind, final ODocument metadata) {
    if (index == null) {
      throw new IllegalArgumentException("Index must not be null");
    }
    if (kind == null) {
      throw new IllegalArgumentException("Analyzer kind must not be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("Metadata must not be null");
    }
    final String defaultAnalyzerFQN = metadata.field("default");
    final String prefix = index.getClassName() + ".";

    final OLucenePerFieldAnalyzerWrapper analyzer =
        geLucenePerFieldPresetAnalyzerWrapperForAllFields(defaultAnalyzerFQN);
    setDefaultAnalyzerForRequestedKind(index, kind, metadata, prefix, analyzer);
    setSpecializedAnalyzersForEachField(index, kind, metadata, prefix, analyzer);
    return analyzer;
  }

  private OLucenePerFieldAnalyzerWrapper geLucenePerFieldPresetAnalyzerWrapperForAllFields(
      final String defaultAnalyzerFQN) {
    if (defaultAnalyzerFQN == null) {
      return new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
    } else {
      return new OLucenePerFieldAnalyzerWrapper(buildAnalyzer(defaultAnalyzerFQN));
    }
  }

  private void setDefaultAnalyzerForRequestedKind(
      final OIndexDefinition index,
      final AnalyzerKind kind,
      final ODocument metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) {
    final String specializedAnalyzerFQN = metadata.field(kind.toString());
    if (specializedAnalyzerFQN != null) {
      for (final String field : index.getFields()) {
        analyzer.add(field, buildAnalyzer(specializedAnalyzerFQN));
        analyzer.add(prefix + field, buildAnalyzer(specializedAnalyzerFQN));
      }
    }
  }

  private void setSpecializedAnalyzersForEachField(
      final OIndexDefinition index,
      final AnalyzerKind kind,
      final ODocument metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) {
    for (final String field : index.getFields()) {
      final String analyzerName = field + "_" + kind.toString();
      final String analyzerStopwords = analyzerName + "_stopwords";

      if (metadata.containsField(analyzerName) && metadata.containsField(analyzerStopwords)) {
        final Collection<String> stopWords = metadata.field(analyzerStopwords, OType.EMBEDDEDLIST);
        analyzer.add(field, buildAnalyzer(metadata.field(analyzerName), stopWords));
        analyzer.add(prefix + field, buildAnalyzer(metadata.field(analyzerName), stopWords));
      } else if (metadata.containsField(analyzerName)) {
        analyzer.add(field, buildAnalyzer(metadata.field(analyzerName)));
        analyzer.add(prefix + field, buildAnalyzer(metadata.field(analyzerName)));
      }
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN) {
    try {
      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getConstructor();
      return (Analyzer) constructor.newInstance();
    } catch (final ClassNotFoundException e) {
      throw OException.wrapException(
          new OIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (final NoSuchMethodException e) {
      Class classAnalyzer = null;
      try {
        classAnalyzer = Class.forName(analyzerFQN);
        return (Analyzer) classAnalyzer.newInstance();
      } catch (Exception e1) {
        OLogManager.instance().error(this, "Exception is suppressed, original exception is ", e);
        //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
        throw OException.wrapException(
            new OIndexException("Couldn't instantiate analyzer:  public constructor  not found"),
            e1);
      }
    } catch (Exception e) {
      OLogManager.instance()
          .error(
              this,
              "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)",
              e);
      return new StandardAnalyzer();
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN, final Collection<String> stopwords) {
    try {
      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getDeclaredConstructor(CharArraySet.class);
      return (Analyzer) constructor.newInstance(new CharArraySet(stopwords, true));
    } catch (final ClassNotFoundException e) {
      throw OException.wrapException(
          new OIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (final NoSuchMethodException e) {
      throw OException.wrapException(
          new OIndexException("Couldn't instantiate analyzer: public constructor not found"), e);
    } catch (final Exception e) {
      OLogManager.instance()
          .error(
              this,
              "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)",
              e);
      return new StandardAnalyzer();
    }
  }

  public enum AnalyzerKind {
    INDEX,
    QUERY;

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }
}
