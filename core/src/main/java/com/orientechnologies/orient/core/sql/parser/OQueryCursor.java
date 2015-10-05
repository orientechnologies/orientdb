package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 02/10/15.
 */
public class OQueryCursor implements Iterator<OIdentifiable> {
  private int                     limit;
  private int                     skip;
  private OWhereClause            filter;
  private Iterator<OIdentifiable> iterator;
  private OOrderBy                orderBy;
  private OCommandContext         ctx;

  private OIdentifiable           next         = null;
  private long                    countFetched = 0;

  public OQueryCursor() {

  }

  public OQueryCursor(Iterator<OIdentifiable> oIdentifiableIterator, OWhereClause filter, OOrderBy orderBy, int skip, int limit,
      OCommandContext ctx) {
    this.iterator = oIdentifiableIterator;
    this.filter = filter;
    this.skip = skip;
    this.limit = limit;
    this.orderBy = orderBy;
    this.ctx = ctx;
    loadNext();
  }

  private void loadNext() {
    if (iterator == null) {
      next = null;
      return;
    }
    if (limit > 0 && countFetched >= limit) {
      next = null;
      return;
    }
    if (countFetched == 0 && skip > 0) {
      for (int i = 0; i < skip; i++) {
        next = getNextFromIterator();
        if (next == null) {
          return;
        }
      }
    }
    next = getNextFromIterator();
    countFetched++;
  }

  private OIdentifiable getNextFromIterator() {
    while (true) {
      if (iterator == null || !iterator.hasNext()) {
        return null;
      }

      OIdentifiable result = iterator.next();
      if (filter==null || filter.matchesFilters(result, ctx)) {
        return result;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public OIdentifiable next() {
    OIdentifiable result = next;
    if (result == null) {
      throw new NoSuchElementException();
    }
    loadNext();
    return result;
  }
}
