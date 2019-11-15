package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.id.ORID;

import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

public class IndexCursorSecurityDecorator implements IndexCursor {

  private final IndexCursor delegate;
  private final OIndex      originalIndex;

  public IndexCursorSecurityDecorator(IndexCursor delegate, OIndex originalIndex) {
    this.delegate = delegate;
    this.originalIndex = originalIndex;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    final Optional<ORawPair<Object, ORID>> result = StreamSupport.stream(delegate, false)
        .filter((pair) -> OIndexInternal.securityFilterOnRead(originalIndex, pair.second) != null).findFirst();

    result.ifPresent(action);
    return result.isPresent();
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