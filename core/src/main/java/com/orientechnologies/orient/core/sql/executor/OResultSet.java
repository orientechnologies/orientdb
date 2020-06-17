package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Created by luigidellaquila on 07/07/16. */
public interface OResultSet extends Spliterator<OResult>, Iterator<OResult>, AutoCloseable {

  @Override
  boolean hasNext();

  @Override
  OResult next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Long> getQueryStats();

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }

  default boolean tryAdvance(Consumer<? super OResult> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  default void forEachRemaining(Consumer<? super OResult> action) {
    Spliterator.super.forEachRemaining(action);
  }

  default OResultSet trySplit() {
    return null;
  }

  default long estimateSize() {
    return Long.MAX_VALUE;
  }

  default int characteristics() {
    return ORDERED;
  }

  /**
   * Returns the result set as a stream. IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<OResult> stream() {
    return StreamSupport.stream(this, false).onClose(() -> this.close());
  }

  /**
   * Returns the result set as a stream of elements (filters only the results that are elements -
   * where the isElement() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<OElement> elementStream() {
    return StreamSupport.stream(
            new Spliterator<OElement>() {
              @Override
              public boolean tryAdvance(Consumer<? super OElement> action) {
                while (hasNext()) {
                  OResult elem = next();
                  if (elem.isElement()) {
                    action.accept(elem.getElement().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<OElement> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(() -> this.close());
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are vertices -
   * where the isVertex() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<OVertex> vertexStream() {
    return StreamSupport.stream(
            new Spliterator<OVertex>() {
              @Override
              public boolean tryAdvance(Consumer<? super OVertex> action) {
                while (hasNext()) {
                  OResult elem = next();
                  if (elem.isVertex()) {
                    action.accept(elem.getVertex().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<OVertex> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(() -> this.close());
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are edges - where
   * the isEdge() method returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<OEdge> edgeStream() {
    return StreamSupport.stream(
            new Spliterator<OEdge>() {
              @Override
              public boolean tryAdvance(Consumer<? super OEdge> action) {
                while (hasNext()) {
                  OResult nextElem = next();
                  if (nextElem != null && nextElem.isEdge()) {
                    action.accept(nextElem.getEdge().get());
                    return true;
                  }
                }
                return false;
              }

              @Override
              public Spliterator<OEdge> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return Long.MAX_VALUE;
              }

              @Override
              public int characteristics() {
                return ORDERED;
              }
            },
            false)
        .onClose(() -> this.close());
  }
}
