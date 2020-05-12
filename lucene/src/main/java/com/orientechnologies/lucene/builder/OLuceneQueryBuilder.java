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

import com.orientechnologies.common.log.OLogManager;
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
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OLuceneQueryBuilder {
  public static final ODocument EMPTY_METADATA = new ODocument();

  private final boolean                allowLeadingWildcard;
  // private final boolean                lowercaseExpandedTerms;
  private final boolean                splitOnWhitespace;
  private final OLuceneAnalyzerFactory analyzerFactory;

  public OLuceneQueryBuilder(final ODocument metadata) {
    this(Optional.ofNullable(metadata.<Boolean>field("allowLeadingWildcard")).orElse(false),
        Optional.ofNullable(metadata.<Boolean>field("lowercaseExpandedTerms")).orElse(true),
        Optional.ofNullable(metadata.<Boolean>field("splitOnWhitespace")).orElse(true));
  }

  public OLuceneQueryBuilder(final boolean allowLeadingWildcard, final boolean lowercaseExpandedTerms, final boolean splitOnWhitespace) {
    this.allowLeadingWildcard = allowLeadingWildcard;
    // this.lowercaseExpandedTerms = lowercaseExpandedTerms;
    this.splitOnWhitespace = splitOnWhitespace;
    analyzerFactory = new OLuceneAnalyzerFactory();
  }

  public Query query(final OIndexDefinition index, final Object key, final ODocument metadata, final Analyzer analyzer)
      throws ParseException {
    final String query = constructQueryString(key);
    if (query.isEmpty()) {
      return new MatchNoDocsQuery();
    }
    return buildQuery(index, query, metadata, analyzer);
  }

  private String constructQueryString(final Object key) {
    if (key instanceof OCompositeKey) {
      final Object params = ((OCompositeKey) key).getKeys().get(0);
      return params.toString();
    } else {
      return key.toString();
    }
  }

  protected Query buildQuery(final OIndexDefinition index, final String query, final ODocument metadata, final Analyzer queryAnalyzer)
      throws ParseException {
    String[] fields;
    if (index.isAutomatic()) {
      fields = index.getFields().toArray(new String[index.getFields().size()]);
    } else {
      final int length = index.getTypes().length;
      fields = new String[length];
      for (int i = 0; i < length; i++) {
        fields[i] = "k" + i;
      }
    }
    final Map<String, OType> types = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      final String field = fields[i];
      types.put(field, index.getTypes()[i]);
    }
    return getQuery(index, query, metadata, queryAnalyzer, fields, types);
  }

  private Query getQuery(final OIndexDefinition index, final String query, final ODocument metadata, final Analyzer queryAnalyzer,
      final String[] fields, final Map<String, OType> types) throws ParseException {
    final Map<String, Float> boost = Optional.ofNullable(metadata.<Map<String, Number>>getProperty("boost")).orElse(new HashMap<>())
        .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().floatValue()));
    final Analyzer analyzer = Optional.ofNullable(metadata.<Boolean>getProperty("customAnalysis")).filter(b -> b == true)
        .map(b -> analyzerFactory.createAnalyzer(index, OLuceneAnalyzerFactory.AnalyzerKind.QUERY, metadata)).orElse(queryAnalyzer);
    final OLuceneMultiFieldQueryParser queryParser = new OLuceneMultiFieldQueryParser(types, fields, analyzer, boost);
    queryParser.setAllowLeadingWildcard(
        Optional.ofNullable(metadata.<Boolean>getProperty("allowLeadingWildcard")).orElse(allowLeadingWildcard));
    queryParser
        .setSplitOnWhitespace(Optional.ofNullable(metadata.<Boolean>getProperty("splitOnWhitespace")).orElse(splitOnWhitespace));
    //  TODO   REMOVED
    //    queryParser.setLowercaseExpandedTerms(
    //        Optional.ofNullable(metadata.<Boolean>getProperty("lowercaseExpandedTerms"))
    //            .orElse(lowercaseExpandedTerms));
    try {
      return queryParser.parse(query);
    } catch (final org.apache.lucene.queryparser.classic.ParseException e) {
      OLogManager.instance().error(this, "Exception is suppressed, original exception is ", e);
      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw new ParseException(e.getMessage());
    }
  }
}
