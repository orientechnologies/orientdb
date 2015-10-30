/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene;

import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneIndexType {

  public static Field createField(String fieldName, Object value, Field.Store store,
      Field.Index analyzed) {
    Field field = null;

    if (value instanceof Number) {
      Number number = (Number) value;
      if (value instanceof Long) {
        field = new LongField(fieldName, number.longValue(), store);
      } else if (value instanceof Float) {
        field = new FloatField(fieldName, number.floatValue(), store);
      } else if (value instanceof Double) {
        field = new DoubleField(fieldName, number.doubleValue(), store);
      } else {
        field = new IntField(fieldName, number.intValue(), store);
      }
    } else if (value instanceof Date) {
      field = new LongField(fieldName, ((Date) value).getTime(), store);

    } else {
      field = new Field(fieldName, value.toString(), store, analyzed);

    }
    return field;
  }

  public static Query createExactQuery(OIndexDefinition index, Object key) {

    Query query = null;
    if (key instanceof String) {
      BooleanQuery booleanQ = new BooleanQuery();
      if (index.getFields().size() > 0) {
        for (String idx : index.getFields()) {
          booleanQ.add(new TermQuery(new Term(idx, key.toString())), BooleanClause.Occur.SHOULD);
        }
      } else {
        booleanQ.add(new TermQuery(new Term(OLuceneIndexManagerAbstract.KEY, key.toString())), BooleanClause.Occur.SHOULD);
      }
      query = booleanQ;
    } else if (key instanceof OCompositeKey) {
      BooleanQuery booleanQ = new BooleanQuery();
      int i = 0;
      OCompositeKey keys = (OCompositeKey) key;
      for (String idx : index.getFields()) {
        String val = (String) keys.getKeys().get(i);
        booleanQ.add(new TermQuery(new Term(idx, val)), BooleanClause.Occur.MUST);
        i++;

      }
      query = booleanQ;
    }
    return query;
  }

  public static Query createQueryId(OIdentifiable value) {
    return new TermQuery(new Term(OLuceneIndexManagerAbstract.RID, value.toString()));
  }

  public static Query createDeleteQuery(OIdentifiable value, List<String> fields, Object key) {

    BooleanQuery booleanQuery = new BooleanQuery();

    booleanQuery.add(new TermQuery(new Term(OLuceneIndexManagerAbstract.RID, value.toString())), BooleanClause.Occur.MUST);

    Map<String, String> values = new HashMap<String, String>();
    // TODO Implementation of Composite keys with Collection
    if (key instanceof OCompositeKey) {

    } else {
      values.put(fields.iterator().next(), key.toString());
    }
    for (String s : values.keySet()) {
      booleanQuery.add(new TermQuery(new Term(s + OLuceneIndexManagerAbstract.STORED, values.get(s))), BooleanClause.Occur.MUST);
    }
    return booleanQuery;
  }

  public static Query createFullQuery(OIndexDefinition index, Object key, Analyzer analyzer, Version version) throws ParseException {

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

    return getQueryParser(index, query, analyzer, version);

  }

  protected static Query getQueryParser(OIndexDefinition index, String key, Analyzer analyzer, Version version)
      throws ParseException {
    QueryParser queryParser;
    if ((key).startsWith("(")) {
      queryParser = new QueryParser(version, "", analyzer);

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
      queryParser = new MultiFieldQueryParser(version, fields, analyzer);
    }

    return queryParser.parse(key);
  }

  public static Sort sort(Query query, OIndexDefinition index, boolean ascSortOrder) {
    String key = index.getFields().iterator().next();
    Number number = ((NumericRangeQuery) query).getMin();
    number = number != null ? number : ((NumericRangeQuery) query).getMax();
    SortField.Type fieldType = SortField.Type.INT;
    if (number instanceof Long) {
      fieldType = SortField.Type.LONG;
    } else if (number instanceof Float) {
      fieldType = SortField.Type.FLOAT;
    } else if (number instanceof Double) {
      fieldType = SortField.Type.DOUBLE;
    }

    return new Sort(new SortField(key, fieldType, ascSortOrder));
  }

}
