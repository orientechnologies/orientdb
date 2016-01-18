package com.orientechnologies.lucene.analyzer;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.lang.reflect.Constructor;

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
    String defaultAnalyzerFQN = metadata.field("analyzer");

    //preset default analyzer for all fields
    OLucenePerFieldAnalyzerWrapper analyzer;
    if (defaultAnalyzerFQN == null) {
      analyzer = new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
    } else {
      analyzer = new OLucenePerFieldAnalyzerWrapper(buildAnalyzer(defaultAnalyzerFQN));
    }

    //default analyzer for indexing
    String indexAnalyzerFQN = metadata.field(kind + "_analyzer");
    if (indexAnalyzerFQN != null) {
      for (String field : index.getFields()) {
        analyzer.add(field, buildAnalyzer(indexAnalyzerFQN));
      }
    }

    //specialized for each field
    for (String field : index.getFields()) {
      for (String meta : metadata.fieldNames()) {
        if (meta.startsWith(field) && meta.contains(kind + "_analyzer")) {
          analyzer.add(field, buildAnalyzer(metadata.<String>field(meta)));
        }
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
      OLogManager.instance()
                 .error(this, "Error on getting analyzer for Lucene index", e);
    }
    return new StandardAnalyzer();
  }

}
