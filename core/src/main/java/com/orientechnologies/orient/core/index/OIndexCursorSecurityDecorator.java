package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OIndexCursorSecurityDecorator implements OIndexCursor {

  OIndexCursor delegate;
  OIndex originalIndex;

  OIdentifiable next;


  public OIndexCursorSecurityDecorator(OIndexCursor delegate, OIndex originalIndex) {
    this.delegate = delegate;
    this.originalIndex = originalIndex;
  }

  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {
    while (true) {
      Map.Entry<Object, OIdentifiable> result = delegate.nextEntry();
      if (result == null) {
        return null;
      }
      OIdentifiable val = OIndexInternal.securityFilterOnRead(originalIndex, result.getValue());
      if (val != null) {
        return result;
      }
    }

  }

  @Override
  public Set<OIdentifiable> toValues() {
    return new HashSet<>(OIndexInternal.securityFilterOnRead(originalIndex, delegate.toValues()));
  }

  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    return delegate.toEntries().stream()
            .filter(x -> OIndexInternal.securityFilterOnRead(originalIndex, x.getValue()) != null)
            .collect(Collectors.toSet());
  }

  @Override
  public Set<Object> toKeys() {
    return delegate.toKeys();
  }

  @Override
  public void setPrefetchSize(int prefetchSize) {
    delegate.setPrefetchSize(prefetchSize);
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }
    if (!delegate.hasNext()) {
      return false;
    }
    while (delegate.hasNext()) {
      next = OIndexInternal.securityFilterOnRead(originalIndex, delegate.next());
      if (next != null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public OIdentifiable next() {
    if (hasNext()) {
      OIdentifiable result = next;
      next = null;
      return result;
    }
    return delegate.next();
  }
}