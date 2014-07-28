package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/24/14
 */
public abstract class OIndexAbstractCursor implements OIndexCursor {
  protected int prefetchSize = -1;

  @Override
  public Set<OIdentifiable> toValues() {
    final HashSet<OIdentifiable> result = new HashSet<OIdentifiable>();
    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getValue());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    final HashSet<Map.Entry<Object, OIdentifiable>> result = new HashSet<Map.Entry<Object, OIdentifiable>>();

    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry);
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Object> toKeys() {
    final HashSet<Object> result = new HashSet<Object>();

    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getKey());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public OIdentifiable next() {
    final Map.Entry<Object, OIdentifiable> entry = nextEntry();
    return entry != null ? entry.getValue() : null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public int getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize(final int prefetchSize) {
    this.prefetchSize = prefetchSize;
  }
}
