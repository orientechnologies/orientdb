package com.orientechnologies.lucene.analyzer;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * Created by frank on 30/10/2015.
 */
public class OLuceneAnalyzerFactory {

  public enum AnalyzerKind {
    INDEX, QUERY;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  public Analyzer createAnalyzer(OIndexDefinition index, AnalyzerKind kind, ODocument metadata) {
    final String defaultAnalyzerFQN = metadata.field("default");

    final String prefix = index.getClassName() + ".";

    //preset default analyzer for all fields
    OLucenePerFieldAnalyzerWrapper analyzer;
    if (defaultAnalyzerFQN == null) {
      analyzer = new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
    } else {
      analyzer = new OLucenePerFieldAnalyzerWrapper(buildAnalyzer(defaultAnalyzerFQN));
    }

    //default analyzer for requested kind
    final String specializedAnalyzerFQN = metadata.field(kind.toString());
    if (specializedAnalyzerFQN != null) {
      for (String field : index.getFields()) {
        analyzer.add(field, buildAnalyzer(specializedAnalyzerFQN));
        analyzer.add(prefix + field, buildAnalyzer(specializedAnalyzerFQN));
      }
    }

    //specialized for each field
    for (String field : index.getFields()) {

      final String analyzerName = field + "_" + kind.toString();

      final String analyzerStopwords = analyzerName + "_stopwords";

      if (metadata.containsField(analyzerName) && metadata.containsField(analyzerStopwords)) {
        final Collection<String> stopWords = metadata.field(analyzerStopwords, OType.EMBEDDEDLIST);
        analyzer.add(field, buildAnalyzer(metadata.<String>field(analyzerName), stopWords));
        analyzer.add(prefix + field, buildAnalyzer(metadata.<String>field(analyzerName), stopWords));
      } else if (metadata.containsField(analyzerName)) {
        analyzer.add(field, buildAnalyzer(metadata.<String>field(analyzerName)));
        analyzer.add(prefix + field, buildAnalyzer(metadata.<String>field(analyzerName)));
      }
    }

    return analyzer;

  }

  private Analyzer buildAnalyzer(String analyzerFQN) {

    try {

      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getConstructor();

      return (Analyzer) constructor.newInstance();
    } catch (ClassNotFoundException e) {
      throw OException.wrapException(new OIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (NoSuchMethodException e) {
      Class classAnalyzer = null;
      try {
        classAnalyzer = Class.forName(analyzerFQN);
        return (Analyzer) classAnalyzer.newInstance();

      } catch (Throwable e1) {
        throw OException.wrapException(new OIndexException("Couldn't instantiate analyzer:  public constructor  not found"), e);
      }

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on getting analyzer for Lucene index", e);
    }
    return new StandardAnalyzer();
  }

  private Analyzer buildAnalyzer(String analyzerFQN, Collection<String> stopwords) {

    try {

      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getDeclaredConstructor(CharArraySet.class);

      return (Analyzer) constructor.newInstance(new CharArraySet(stopwords, true));
    } catch (ClassNotFoundException e) {
      throw OException.wrapException(new OIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (NoSuchMethodException e) {
      throw OException.wrapException(new OIndexException("Couldn't instantiate analyzer:  public constructor  not found"), e);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on getting analyzer for Lucene index", e);
    }
    return new StandardAnalyzer();
  }

}
