package com.orientechnologies.lucene.parser;

import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;

import java.util.Map;

/**
 * Created by frank on 13/12/2016.
 */
public class OLuceneMultiFieldQueryParser extends MultiFieldQueryParser {
  private final Map<String, OType> types;

  public OLuceneMultiFieldQueryParser(Map<String, OType> types, String[] fields,
      Analyzer analyzer,
      Map<String, Float> boosts) {
    super(fields, analyzer, boosts);
    this.types = types;
  }

  public OLuceneMultiFieldQueryParser(Map<String, OType> types, String[] fields, Analyzer analyzer) {
    super(fields, analyzer);
    this.types = types;
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
    Query query = getQuery(field, queryText, queryText, true, true);
    if (query != null)
      return query;

    return super.getFieldQuery(field, queryText, slop);
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {

    Query query = getQuery(field, queryText, queryText, true, true);
    if (query != null)
      return query;

    return super.getFieldQuery(field, queryText, quoted);
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {

    Query query = getQuery(field, part1, part2, startInclusive, endInclusive);
    if (query != null)
      return query;

    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }

  private Query getQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {

    if (types.containsKey(field)) {
      switch (types.get(field)) {
      case LONG:
        return NumericRangeQuery.newLongRange(field, Long.parseLong(part1), Long.parseLong(part2),
            startInclusive, endInclusive);
      case INTEGER:
        return NumericRangeQuery.newIntRange(field, Integer.parseInt(part1), Integer.parseInt(part2),
            startInclusive, endInclusive);
      case DOUBLE:
        return NumericRangeQuery.newDoubleRange(field, Double.parseDouble(part1), Double.parseDouble(part2),
            startInclusive, endInclusive);
      case DATE:
      case DATETIME:
        try {
          return NumericRangeQuery.newLongRange(field, DateTools.stringToTime(part1), DateTools.stringToTime(part2),
              startInclusive, endInclusive);
        } catch (java.text.ParseException e) {
          throw new ParseException(e.getMessage());
        }
      }
    }
    return null;
  }

}
