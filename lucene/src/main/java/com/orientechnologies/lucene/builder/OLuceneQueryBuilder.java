/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.builder;

import com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory;
import com.orientechnologies.lucene.parser.OLuceneMultiFieldQueryParser;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OLuceneQueryBuilder {

  public static final ODocument EMPTY_METADATA = new ODocument();

  private final boolean                allowLeadingWildcard;
  private final boolean                lowercaseExpandedTerms;
  private final OLuceneAnalyzerFactory analyzerFactory;

  public OLuceneQueryBuilder(ODocument metadata) {
    this(Optional.ofNullable(metadata.<Boolean>field("allowLeadingWildcard"))
            .orElse(false),
        Optional.ofNullable(metadata.<Boolean>field("lowercaseExpandedTerms"))
            .orElse(true));
  }

  public OLuceneQueryBuilder(boolean allowLeadingWildcard, boolean lowercaseExpandedTerms) {
    this.allowLeadingWildcard = allowLeadingWildcard;
    this.lowercaseExpandedTerms = lowercaseExpandedTerms;

    analyzerFactory = new OLuceneAnalyzerFactory();
  }

  public Query query(OIndexDefinition index, Object key, ODocument metadata, Analyzer analyzer) throws ParseException {

    String query;
    if (key instanceof OCompositeKey) {
      Object params = ((OCompositeKey) key).getKeys().get(0);
      query = params.toString();

    } else {

      query = key.toString();
    }

    if (query.isEmpty())
      return new MatchNoDocsQuery();
    return buildQuery(index, query, metadata, analyzer);
  }

  protected Query buildQuery(OIndexDefinition index, String query, ODocument metadata, Analyzer queryAnalyzer)
      throws ParseException {

    String[] fields;
    if (index.isAutomatic()) {
      fields = index.getFields().toArray(new String[index.getFields().size()]);
    } else {
      int length = index.getTypes().length;

      fields = new String[length];
      for (int i = 0; i < length; i++) {
        fields[i] = "k" + i;
      }
    }

    Map<String, OType> types = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      types.put(field, index.getTypes()[i]);
    }

    return getQuery(index, query, metadata, queryAnalyzer, fields, types);

  }

  private Query getQuery(OIndexDefinition index, String query, ODocument metadata, Analyzer queryAnalyzer, String[] fields,
      Map<String, OType> types) throws ParseException {

    Map<String, Float> boost = Optional.ofNullable(metadata.<Map<String, Float>>getProperty("boost"))
        .orElse(new HashMap<>());

    Analyzer analyzer = Optional.ofNullable(metadata.<Boolean>getProperty("customAnalysis"))
        .filter(b -> b == true)
        .map(b -> analyzerFactory.createAnalyzer(index, OLuceneAnalyzerFactory.AnalyzerKind.QUERY, metadata))
        .orElse(queryAnalyzer);

    final OLuceneMultiFieldQueryParser queryParser = new OLuceneMultiFieldQueryParser(types, fields, analyzer, boost);

    queryParser.setAllowLeadingWildcard(
        Optional.ofNullable(metadata.<Boolean>getProperty("allowLeadingWildcard"))
            .orElse(allowLeadingWildcard));

    queryParser.setLowercaseExpandedTerms(
        Optional.ofNullable(metadata.<Boolean>getProperty("lowercaseExpandedTerms"))
            .orElse(lowercaseExpandedTerms));

    try {
      return queryParser.parse(query);

    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      throw new ParseException(e.getMessage());
    }
  }

}
