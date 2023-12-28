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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/** Created by enricorisa on 21/03/14. */
public class OLuceneIndexType {
  public static final String RID_HASH = "_RID_HASH";

  public static Field createField(
      final String fieldName, final Object value, final Field.Store store /*,Field.Index index*/) {
    // metadata fields: _CLASS, _CLUSTER
    if (fieldName.startsWith("_CLASS") || fieldName.startsWith("_CLUSTER")) {
      return new StringField(fieldName, value.toString(), store);
    }
    return new TextField(fieldName, value.toString(), Field.Store.YES);
  }

  public static String extractId(Document doc) {
    String value = doc.get(RID_HASH);
    if (value != null) {
      int pos = value.indexOf("|");
      if (pos > 0) {
        return value.substring(0, pos);
      } else {
        return value;
      }
    } else {
      return null;
    }
  }

  public static Field createIdField(final OIdentifiable id, final Object key) {
    return new StringField(RID_HASH, genValueId(id, key), Field.Store.YES);
  }

  public static Field createOldIdField(final OIdentifiable id) {
    return new StringField(
        OLuceneIndexEngineAbstract.RID, id.getIdentity().toString(), Field.Store.YES);
  }

  public static String genValueId(final OIdentifiable id, final Object key) {
    String value = id.getIdentity().toString() + "|";
    value += hashKey(key);
    return value;
  }

  public static List<Field> createFields(
      String fieldName, Object value, Field.Store store, Boolean sort) {
    List<Field> fields = new ArrayList<>();
    if (value instanceof Number) {
      Number number = (Number) value;
      if (value instanceof Long) {
        fields.add(new NumericDocValuesField(fieldName, number.longValue()));
        fields.add(new LongPoint(fieldName, number.longValue()));
        return fields;
      } else if (value instanceof Float) {
        fields.add(new FloatDocValuesField(fieldName, number.floatValue()));
        fields.add(new FloatPoint(fieldName, number.floatValue()));
        return fields;
      } else if (value instanceof Double) {
        fields.add(new DoubleDocValuesField(fieldName, number.doubleValue()));
        fields.add(new DoublePoint(fieldName, number.doubleValue()));
        return fields;
      }
      fields.add(new NumericDocValuesField(fieldName, number.longValue()));
      fields.add(new IntPoint(fieldName, number.intValue()));
      return fields;
    } else if (value instanceof Date) {
      Date date = (Date) value;
      fields.add(new NumericDocValuesField(fieldName, date.getTime()));
      fields.add(new LongPoint(fieldName, date.getTime()));
      return fields;
    }
    if (Boolean.TRUE.equals(sort)) {
      fields.add(new SortedDocValuesField(fieldName, new BytesRef(value.toString())));
    }
    fields.add(new TextField(fieldName, value.toString(), Field.Store.YES));
    return fields;
  }

  public static Query createExactQuery(OIndexDefinition index, Object key) {
    Query query = null;
    if (key instanceof String) {
      final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      if (index.getFields().size() > 0) {
        for (String idx : index.getFields()) {
          queryBuilder.add(
              new TermQuery(new Term(idx, key.toString())), BooleanClause.Occur.SHOULD);
        }
      } else {
        queryBuilder.add(
            new TermQuery(new Term(OLuceneIndexEngineAbstract.KEY, key.toString())),
            BooleanClause.Occur.SHOULD);
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

  public static Query createQueryId(OIdentifiable value, Object key) {
    return new TermQuery(new Term(RID_HASH, genValueId(value, key)));
  }

  public static String hashKey(Object key) {
    try {
      String keyString;
      if (key instanceof ODocument) {
        keyString = ((ODocument) key).toJSON();
      } else {
        keyString = key.toString();
      }
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] bytes = sha256.digest(keyString.getBytes("UTF-8"));
      return Base64.getEncoder().encodeToString(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw OException.wrapException(new OLuceneIndexException("fail to find sha algorithm"), e);

    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OLuceneIndexException("fail to find utf-8 encoding"), e);
    }
  }

  public static Query createDeleteQuery(
      OIdentifiable value, List<String> fields, Object key, ODocument metadata) {

    // TODO Implementation of Composite keys with Collection
    final BooleanQuery.Builder filter = new BooleanQuery.Builder();
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    // TODO: Condition on Id and field key only for backward compatibility
    if (value != null) {
      builder.add(createQueryId(value), BooleanClause.Occur.MUST);
    }
    String field = fields.iterator().next();
    builder.add(
        new TermQuery(new Term(field, key.toString().toLowerCase(Locale.ENGLISH))),
        BooleanClause.Occur.MUST);

    filter.add(builder.build(), BooleanClause.Occur.SHOULD);
    if (value != null) {
      filter.add(createQueryId(value, key), BooleanClause.Occur.SHOULD);
    }

    return filter.build();
  }
}
