package com.orientechnologies.lucene;

import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Version;

import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexDefinition;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OLuceneIndexType {

  public static Field createField(String fieldName, OIdentifiable oIdentifiable, Object value, Field.Store store,
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

    } else if (value instanceof String) {
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
    } else if (key instanceof Number) {
      String idx = index.getFields().iterator().next();
      Number number = (Number) key;
      if (key instanceof Long) {
        query = NumericRangeQuery.newLongRange(idx, number.longValue(), number.longValue(), true, true);
      } else if (key instanceof Float) {
        query = NumericRangeQuery.newFloatRange(idx, number.floatValue(), number.floatValue(), true, true);
      } else if (key instanceof Double) {
        query = NumericRangeQuery.newDoubleRange(idx, number.doubleValue(), number.doubleValue(), true, true);
      } else {
        query = NumericRangeQuery.newIntRange(idx, number.intValue(), number.intValue(), true, true);
      }
    } else if (key instanceof Date) {
      String idx = index.getFields().iterator().next();
      query = NumericRangeQuery.newLongRange(idx, ((Date) key).getTime(), ((Date) key).getTime(), true, true);
    }
    return query;
  }

  public static Query createRangeQuery(OIndexDefinition index, Object fromValue, Object toValue, boolean includeFrom,
      boolean includeTo) {

    Query query = null;
    if (fromValue instanceof Number || toValue instanceof Number) {
      String idx = index.getFields().iterator().next();
      Number from = (Number) fromValue;
      Number to = (Number) toValue;

      if (from instanceof Long || to instanceof Long) {
        return NumericRangeQuery.newLongRange(idx, from != null ? from.longValue() : 0, to != null ? to.longValue()
            : Long.MAX_VALUE, includeFrom, includeTo);
      } else if (from instanceof Double || to instanceof Double) {
        return NumericRangeQuery.newDoubleRange(idx, from != null ? from.doubleValue() : 0, to != null ? to.doubleValue()
            : Double.MAX_VALUE, includeFrom, includeTo);
      } else if (from instanceof Float || to instanceof Float) {
        return NumericRangeQuery.newFloatRange(idx, from != null ? from.floatValue() : 0, to != null ? to.floatValue()
            : Float.MAX_VALUE, includeFrom, includeTo);
      } else {
        return NumericRangeQuery.newIntRange(idx, from != null ? from.intValue() : 0, to != null ? to.intValue()
            : Integer.MAX_VALUE, includeFrom, includeTo);
      }

    } else if (fromValue instanceof Date && toValue instanceof Date) {
      String idx = index.getFields().iterator().next();
      query = NumericRangeQuery.newLongRange(idx, ((Date) fromValue).getTime(), ((Date) toValue).getTime(), includeFrom, includeTo);
    }
    return query;
  }

  public static Query createQueryId(OIdentifiable value) {
    return new TermQuery(new Term(OLuceneIndexManagerAbstract.RID, value.toString()));
  }

  public static Query createFullQuery(OIndexDefinition index, Object key, Analyzer analyzer) throws ParseException {
    QueryParser queryParser = null;
    String query = null;
    if (key instanceof String) {
      query = (String) key;
      if (((String) key).startsWith("(")) {
        queryParser = new QueryParser(Version.LUCENE_47, "", analyzer);
      } else {
        queryParser = new MultiFieldQueryParser(Version.LUCENE_47, index.getFields().toArray(new String[0]), analyzer);
      }
    } else {
      query = key.toString();
    }
    return queryParser.parse(query);
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
