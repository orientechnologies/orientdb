package com.orientechnologies.lucene.collections;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.IndexCursor;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Created by frank on 03/05/2017.
 */
public final class LuceneIndexCursor implements IndexCursor {
  private final Object                  key;
  private final Iterator<OIdentifiable> iterator;

  public LuceneIndexCursor(OLuceneResultSet resultSet, Object key) {
    this.iterator = resultSet.iterator();
    this.key = key;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    if (iterator.hasNext()) {
      final OIdentifiable rid = iterator.next();
      action.accept(new ORawPair<>(key, rid.getIdentity()));
      return true;
    }

    return false;
  }

  @Override
  public Spliterator<ORawPair<Object, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return NONNULL | ORDERED;
  }
}

