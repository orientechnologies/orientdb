package com.orientechnologies.lucene.parser;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.builder.OLuceneDateTools;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;

/** Created by frank on 13/12/2016. */
public class OLuceneMultiFieldQueryParser extends MultiFieldQueryParser {
  private final Map<String, OType> types;

  public OLuceneMultiFieldQueryParser(
      final Map<String, OType> types, final String[] fields, final Analyzer analyzer) {
    this(types, fields, analyzer, null);
  }

  public OLuceneMultiFieldQueryParser(
      final Map<String, OType> types,
      final String[] fields,
      final Analyzer analyzer,
      final Map<String, Float> boosts) {
    super(fields, analyzer, boosts);
    this.types = types;
  }

  @Override
  protected Query getFieldQuery(final String field, final String queryText, final int slop)
      throws ParseException {
    final Optional<Query> query = getQuery(field, queryText, queryText, true, true);
    return handleBoost(field, query.orElse(super.getFieldQuery(field, queryText, slop)));
  }

  @Override
  protected Query getFieldQuery(final String field, final String queryText, final boolean quoted)
      throws ParseException {
    final Optional<Query> query = getQuery(field, queryText, queryText, true, true);
    final Query q = query.orElse(super.getFieldQuery(field, queryText, quoted));
    return handleBoost(field, q);
  }

  private Query handleBoost(final String field, final Query query) {
    if (field != null && boosts.containsKey(field)) {
      return new BoostQuery(query, boosts.get(field));
    }
    return query;
  }

  @Override
  protected Query getRangeQuery(
      final String field,
      final String part1,
      final String part2,
      final boolean startInclusive,
      final boolean endInclusive)
      throws ParseException {
    final Optional<Query> query = getQuery(field, part1, part2, startInclusive, endInclusive);
    return query.orElse(super.getRangeQuery(field, part1, part2, startInclusive, endInclusive));
  }

  private Optional<Query> getQuery(
      final String field,
      final String part1,
      final String part2,
      final boolean startInclusive,
      final boolean endInclusive)
      throws ParseException {
    int start = 0;
    int end = 0;
    if (!startInclusive) {
      start = 1;
    }
    if (!endInclusive) {
      end = -1;
    }

    if (types.containsKey(field)) {
      switch (types.get(field)) {
        case LONG:
          return Optional.of(
              LongPoint.newRangeQuery(
                  field,
                  Math.addExact(Long.parseLong(part1), start),
                  Math.addExact(Long.parseLong(part2), end)));
        case INTEGER:
          return Optional.of(
              IntPoint.newRangeQuery(
                  field,
                  Math.addExact(Integer.parseInt(part1), start),
                  Math.addExact(Integer.parseInt(part2), end)));
        case FLOAT:
          return Optional.of(
              FloatPoint.newRangeQuery(
                  field, Float.parseFloat(part1) - start, Float.parseFloat(part2) + end));
        case DOUBLE:
          return Optional.of(
              DoublePoint.newRangeQuery(
                  field, Double.parseDouble(part1) - start, Double.parseDouble(part2) + end));
        case DATE:
        case DATETIME:
          try {
            return Optional.of(
                LongPoint.newRangeQuery(
                    field,
                    Math.addExact(OLuceneDateTools.stringToTime(part1), start),
                    Math.addExact(OLuceneDateTools.stringToTime(part2), end)));

          } catch (final java.text.ParseException e) {
            OLogManager.instance()
                .error(this, "Exception is suppressed, original exception exception is ", e);
            //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
            throw new ParseException(e.getMessage());
          }
      }
    }
    return Optional.empty();
  }
}
