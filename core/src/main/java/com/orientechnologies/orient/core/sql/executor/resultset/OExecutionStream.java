package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OExecutionStep;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface OExecutionStream {
  public static final OExecutionStream EMPTY = new OEmptyExecutionStream();

  boolean hasNext(OCommandContext ctx);

  OResult next(OCommandContext ctx);

  void close(OCommandContext ctx);

  public default OExecutionStream map(OResultMapper mapper) {
    return new OMapperExecutionStream(this, mapper);
  }

  public default OExecutionStream filter(OFilterResult filter) {
    return new OFilterExecutionStream(this, filter);
  }

  public default OExecutionStream flatMap(OMapExecutionStream map) {
    return new OFlatMapExecutionStream(this, map);
  }

  public default OExecutionStream interruptable() {
    return new OInterruptResultSet(this);
  }

  public default OExecutionStream limit(long limit) {
    return new OLimitedExecutionStream(this, limit);
  }

  public static OExecutionStream iterator(Iterator<Object> iterator) {
    return new OIteratorExecutionStream(iterator);
  }

  public static OExecutionStream resultIterator(Iterator<OResult> iterator) {
    return new OResultIteratorExecutionStream(iterator);
  }

  public default OCostMeasureResultSet profile(OExecutionStep step, long baseCost) {
    return new OCostMeasureResultSet(this, baseCost);
  }

  public static OExecutionStream loadIterator(Iterator<OIdentifiable> iterator) {
    return new OLoaderExecutionStream(iterator);
  }

  public static OExecutionStream empty() {
    return EMPTY;
  }

  static OExecutionStream singleton(OResult result) {
    return new OSingletonExecutionStream(result);
  }

  public default Stream<OResult> stream(OCommandContext ctx) {
    return StreamSupport.stream(
            new Spliterator<OResult>() {

              @Override
              public boolean tryAdvance(Consumer<? super OResult> action) {
                if (hasNext(ctx)) {
                  action.accept(next(ctx));
                  return true;
                }
                return false;
              }

              @Override
              public Spliterator<OResult> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return 0;
              }
            },
            false)
        .onClose(() -> this.close(ctx));
  }
}
