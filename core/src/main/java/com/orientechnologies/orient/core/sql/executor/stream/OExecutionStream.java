package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OExecutionStepInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.stream.OTimeoutExecutionStream.TimedOut;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface OExecutionStream {
  boolean hasNext(OCommandContext ctx);

  OResult next(OCommandContext ctx);

  void close(OCommandContext ctx);

  /**
   * Flag used to terminate scripts early in the execution, used by the return statement via
   * the terminate execution stream
   *
   * The only implementation that tour true is {@link OTerminationExecutionStream} created with {@link OExecutionStream#terminate()} the
   * other implementation return false if they provide content themselves, or delegate the method if are wrapper (independently of the modifications) of another stream.
   *
   * @param ctx context of the query
   * @return true if this is last execution stream, false if there are more
   */
  boolean isTermination(OCommandContext ctx);

  public static OExecutionStream produce(OProduceResult producer) {
    return new OProduceExecutionStream(producer);
  }

  public static OExecutionStream multipleStreams(OExecutionStreamProducer producer) {
    return new OMultipleExecutionStream(producer);
  }

  public static <T> OExecutionStream streamsFromIterator(
      Iterator<T> producer, OExecutionStreamProducerValueMap<T> map) {
    return new OMultipleExecutionStream(new OExcutionStreamProducerFromIterator<T>(producer, map));
  }

  public default OExecutionStream map(OMapResult mapper) {
    return new OMapExecutionStream(this, mapper);
  }

  public default OExecutionStream filter(OFilterResult filter) {
    return new OFilterExecutionStream(this, filter);
  }

  public default OExecutionStream flatMap(OFlatMapResult map) {
    return new OFlatMapExecutionStream(this, map);
  }

  public default OExecutionStream interruptable() {
    return new OInterruptExecutionStream(this);
  }

  public default OExecutionStream timeout(long timeInMills, TimedOut timedout) {
    return new OTimeoutExecutionStream(this, timeInMills, timedout);
  }

  public default OExecutionStream limit(long limit) {
    return new OLimitedExecutionStream(this, limit);
  }

  public default OExecutionStream terminate() {
    return new OTerminationExecutionStream(this);
  }

  public static OExecutionStream iterator(Iterator<Object> iterator) {
    return new OIteratorExecutionStream(iterator);
  }

  public static OExecutionStream resultIterator(Iterator<OResult> iterator) {
    return new OResultIteratorExecutionStream(iterator);
  }

  public static OExecutionStream resultCollection(Collection<OResult> iterator) {
    return new OResultCollectionExecutionStream(iterator);
  }

  public default OCostMeasureExecutionStream profile(OExecutionStepInternal step) {
    return new OCostMeasureExecutionStream(this, step);
  }

  public static OExecutionStream loadIterator(Iterator<OIdentifiable> iterator) {
    return new OLoaderExecutionStream(iterator);
  }

  public static OExecutionStream empty() {
    return OEmptyExecutionStream.EMPTY;
  }

  public static OExecutionStream singleton(OResult result) {
    return new OSingletonExecutionStream(result);
  }

  /**
   * Check if the current stream has all the data in memory, without much computation
   * need to get the final result.
   *
   * Only implementation with all the content inside can return true, wrappers with no computation can just delegate, if
   * the wrapper do a logic computation should return false.
   *
   * @param ctx the current query context
   * @return true if the data is all in memory
   */
  public default boolean isFullInMemory(OCommandContext ctx) {
    return false;
  }

  public interface OnClose {
    void close(OCommandContext ctx);
  }

  public default OExecutionStream onClose(OnClose onClose) {
    return new OnCloseExecutionStream(this, onClose);
  }

  public static OExecutionStream collectAll(OExecutionStream from, OCommandContext ctx) {
    if (!from.hasNext(ctx) || from.isFullInMemory(ctx)) {
      return from;
    }
    List<OResult> result = new ArrayList<>();
    while (from.hasNext(ctx)) {
      result.add(from.next(ctx));
    }
    from.close(ctx);
    OExecutionStream fullStream = OExecutionStream.resultCollection(result);
    if (from.isTermination(ctx)) {
      fullStream = fullStream.terminate();
    }
    return fullStream;
  }

  public static void consume(OExecutionStream toConsume, OCommandContext ctx) {
    while (toConsume.hasNext(ctx)) {
      toConsume.next(ctx);
    }
    toConsume.close(ctx);
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
