/*
 *
 *  * Copyright 2014 Orient Technologies.
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

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.Map;

/**
 * Created by Enrico Risa on 02/09/15.
 */
public class OQueryBuilderImpl implements OQueryBuilder {
  @Override
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

  protected static Query getQueryParser(OIndexDefinition index, String key, Analyzer analyzer)
      throws ParseException {
    QueryParser queryParser;
    if ((key).startsWith("(")) {
      queryParser = new QueryParser( "", analyzer);

    } else {
      String[] fields = null;
      if (index.isAutomatic()) {
        fields = index.getFields().toArray(new String[index.getFields().size()]);
      } else {
        int length = index.getTypes().length;

        fields = new String[length];
        for (int i = 0; i < length; i++) {
          fields[i] = "k" + i;
        }
      }
      queryParser = new MultiFieldQueryParser(fields, analyzer);
    }

    try {
      return queryParser.parse(key);

    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      throw new ParseException(e.getMessage());
    }

  }
}
