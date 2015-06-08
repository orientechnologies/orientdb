package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;

import java.util.Iterator;

/**
 * Created by luigidellaquila on 08/06/15.
 */
public class FilteredIterator implements Iterable<OIdentifiable>, Iterator<OIdentifiable> {
  private final OWhereClause      whereClause;
  private final Iterator<ORecord> baseIterator;

  private OIdentifiable           next;

  public FilteredIterator(Iterable<ORecord> baseIterable, OWhereClause oWhereClause) {
    this.baseIterator = baseIterable.iterator();
    this.whereClause = oWhereClause;
    fetchNext();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (next == null) {
      fetchNext();
    }
    return next != null;
  }

  public void fetchNext() {
    if (!baseIterator.hasNext()) {
      return;
    }
    OIdentifiable nextElement = baseIterator.next();
    while (!whereClause.matchesFilters(nextElement)) {
      if (!baseIterator.hasNext()) {
        return;
      }
      nextElement = baseIterator.next();

      if (nextElement == null) {
        return;
      }
    }
    this.next = nextElement;
  }

  @Override
  public OIdentifiable next() {
    OIdentifiable result = next;
    next = null;
    fetchNext();
    return result;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
