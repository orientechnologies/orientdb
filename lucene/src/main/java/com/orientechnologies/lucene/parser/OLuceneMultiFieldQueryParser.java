package com.orientechnologies.lucene.parser;

import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import java.util.Map;
import java.util.Optional;

/**
 * Created by frank on 13/12/2016.
 */
public class OLuceneMultiFieldQueryParser extends MultiFieldQueryParser {
  private final Map<String, OType> types;

  public OLuceneMultiFieldQueryParser(Map<String, OType> types, String[] fields, Analyzer analyzer) {
    this(types, fields, analyzer, null);
  }

  public OLuceneMultiFieldQueryParser(Map<String, OType> types, String[] fields, Analyzer analyzer, Map<String, Float> boosts) {
    super(fields, analyzer, boosts);
    this.types = types;

  }

  @Override
  protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
    Optional<Query> query = getQuery(field, queryText, queryText, true, true);

    return query.orElse(super.getFieldQuery(field, queryText, slop));
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {

    Optional<Query> query = getQuery(field, queryText, queryText, true, true);

    return query.orElse(super.getFieldQuery(field, queryText, quoted));
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {

    Optional<Query> query = getQuery(field, part1, part2, startInclusive, endInclusive);

    return query.orElse(super.getRangeQuery(field, part1, part2, startInclusive, endInclusive));
  }

  private Optional<Query> getQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {

    int start = 0;
    int end = 0;
    if (!startInclusive)
      start = 1;

    if (!endInclusive)
      end = -1;

    if (types.containsKey(field)) {
      switch (types.get(field)) {
      case LONG:
        return Optional.of(LongPoint
            .newRangeQuery(field, Math.addExact(Long.parseLong(part1), start), Math.addExact(Long.parseLong(part2), end)));
      case INTEGER:
        return Optional.of(IntPoint
            .newRangeQuery(field, Math.addExact(Integer.parseInt(part1), start), Math.addExact(Integer.parseInt(part2), end)));
      case DOUBLE:
        return Optional.of(DoublePoint
            .newRangeQuery(field, Double.parseDouble(part1) - start, Double.parseDouble(part2) + end));
      case DATE:
      case DATETIME:
        try {
          return Optional.of(LongPoint
              .newRangeQuery(field, Math.addExact(DateTools.stringToTime(part1), start),
                  Math.addExact(DateTools.stringToTime(part2), end)));
        } catch (java.text.ParseException e) {
          throw new ParseException(e.getMessage());
        }
      }
    }
    return Optional.empty();
  }

}
