/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.builder;

import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;

import java.util.*;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneIndexType {

  public static Field createField(String fieldName, Object value, Field.Store store /*,Field.Index index*/) {

    if (value instanceof Number) {
      Number number = (Number) value;
      if (value instanceof Long)
        return new LongPoint(fieldName, number.longValue());
      else if (value instanceof Float)
        return new FloatPoint(fieldName, number.floatValue());
      else if (value instanceof Double)
        return new DoublePoint(fieldName, number.doubleValue());
      return new IntPoint(fieldName, number.intValue());

    } else if (value instanceof Date) {
      Date date = (Date) value;
      return new LongPoint(fieldName, date.getTime());
    }

    if (fieldName.equalsIgnoreCase(OLuceneIndexEngineAbstract.RID)) {
      StringField ridField = new StringField(fieldName, value.toString(), store);
      return ridField;
    }

    //metadata fileds: _CLASS, _CLUSTER
    if (fieldName.startsWith("_")) {
      StringField ridField = new StringField(fieldName, value.toString(), store);
      return ridField;
    }

    return new TextField(fieldName, value.toString(), Field.Store.YES);

  }

  public static Query createExactQuery(OIndexDefinition index, Object key) {

    Query query = null;
    if (key instanceof String) {
      final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      if (index.getFields().size() > 0) {
        for (String idx : index.getFields()) {
          queryBuilder.add(new TermQuery(new Term(idx, key.toString())), BooleanClause.Occur.SHOULD);
        }
      } else {
        queryBuilder.add(new TermQuery(new Term(OLuceneIndexEngineAbstract.KEY, key.toString())), BooleanClause.Occur.SHOULD);
      }
      query = queryBuilder.build();
    } else if (key instanceof OCompositeKey) {
      final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      int i = 0;
      OCompositeKey keys = (OCompositeKey) key;
      for (String idx : index.getFields()) {
        String val = (String) keys.getKeys().get(i);
        queryBuilder.add(new TermQuery(new Term(idx, val)), BooleanClause.Occur.MUST);
        i++;

      }
      query = queryBuilder.build();
    }
    return query;
  }

  public static Query createQueryId(OIdentifiable value) {
    return new TermQuery(new Term(OLuceneIndexEngineAbstract.RID, value.getIdentity().toString()));
  }

  public static Query createDeleteQuery(OIdentifiable value, List<String> fields, Object key) {

    final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

    queryBuilder.add(createQueryId(value), BooleanClause.Occur.MUST);

    Map<String, String> values = new HashMap<>();
    // TODO Implementation of Composite keys with Collection
    if (key instanceof OCompositeKey) {

    } else {
      values.put(fields.iterator().next(), key.toString());
    }
    for (String s : values.keySet()) {
      queryBuilder.add(new TermQuery(new Term(s, values.get(s).toLowerCase(Locale.ENGLISH))), Occur.MUST);
    }
    return queryBuilder.build();
  }

  public static Sort sort(Query query, OIndexDefinition index, boolean ascSortOrder) {
    String key = index.getFields().iterator().next();
    Number number = ((LegacyNumericRangeQuery<Number>) query).getMin();
    number = number != null ? number : ((LegacyNumericRangeQuery<Number>) query).getMax();
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
