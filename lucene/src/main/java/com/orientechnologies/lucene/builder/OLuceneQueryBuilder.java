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
import com.orientechnologies.lucene.parser.OLuceneMultiFieldQueryParser;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OLuceneQueryBuilder {

  private final boolean allowLeadingWildcard;
  private final boolean lowercaseExpandedTerms;

  public OLuceneQueryBuilder(ODocument metadata) {

    Boolean allowLeadingWildcard = false;
    if (metadata.containsField("allowLeadingWildcard")) {
      allowLeadingWildcard = metadata.<Boolean>field("allowLeadingWildcard");
    }

    Boolean lowercaseExpandedTerms = true;
    if (metadata.containsField("lowercaseExpandedTerms")) {
      lowercaseExpandedTerms = metadata.<Boolean>field("lowercaseExpandedTerms");
    }
    this.allowLeadingWildcard = allowLeadingWildcard;
    this.lowercaseExpandedTerms = lowercaseExpandedTerms;

  }

  public OLuceneQueryBuilder(boolean allowLeadingWildcard, boolean lowercaseExpandedTerms) {
    this.allowLeadingWildcard = allowLeadingWildcard;
    this.lowercaseExpandedTerms = lowercaseExpandedTerms;

    OLogManager.instance().info(this, "allowLeadingWildcard::  " + allowLeadingWildcard);
  }

  public Query query(OIndexDefinition index, Object key, Analyzer analyzer) throws ParseException {
    String query = "";
    if (key instanceof OCompositeKey) {
      Object params = ((OCompositeKey) key).getKeys().get(0);
      if (params instanceof Map) {
        Object q = ((Map) params).get("q");
        if (q != null) {
          query = q.toString();
        }
      } else {
        query = params.toString();

      }
    } else {
      query = key.toString();
    }

    return getQueryParser(index, query, analyzer);
  }
  public Query query(OIndexDefinition index, String query, Analyzer analyzer) throws ParseException {

    return getQueryParser(index, query, analyzer);
  }

  protected Query getQueryParser(OIndexDefinition index, String query, Analyzer analyzer) throws ParseException {

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

    Map<String, OType> types = new HashMap<String, OType>();
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      types.put(field, index.getTypes()[i]);
    }

    final OLuceneMultiFieldQueryParser queryParser = new OLuceneMultiFieldQueryParser(types, fields, analyzer, new HashMap<>());
    queryParser.setAllowLeadingWildcard(allowLeadingWildcard);

    queryParser.setLowercaseExpandedTerms(lowercaseExpandedTerms);

    try {
      return queryParser.parse(query);

    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      throw new ParseException(e.getMessage());
    }

  }

}
