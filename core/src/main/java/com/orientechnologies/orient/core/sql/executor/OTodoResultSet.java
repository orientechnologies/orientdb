package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
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

/**
 * Created by luigidellaquila on 07/07/16.
 */
public interface OTodoResultSet extends Spliterator<OResult>, Iterator<OResult> {

  @Override boolean hasNext();

  @Override OResult next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Object> getQueryStats();

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

  default OTodoResultSet trySplit() {
    return null;
  }

  default long estimateSize() {
    return Long.MAX_VALUE;
  }

  default int characteristics() {
    return ORDERED;
  }

  default Stream<OResult> stream() {
    return StreamSupport.stream(this, false);
  }

  default Stream<OElement> elementStream() {
    return StreamSupport.stream(new Spliterator<OElement>() {
      @Override public boolean tryAdvance(Consumer<? super OElement> action) {
        while (hasNext()) {
          OIdentifiable elem = next().getElement();
          if (elem instanceof OElement) {
            action.accept((OElement) elem);
            return true;
          }
        }
        return false;
      }

      @Override public Spliterator<OElement> trySplit() {
        return null;
      }

      @Override public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  default Stream<OVertex> vertexStream() {
    return StreamSupport.stream(new Spliterator<OVertex>() {
      @Override public boolean tryAdvance(Consumer<? super OVertex> action) {
        while (hasNext()) {
          OElement elem = (OElement) next().getElement();
          if (elem != null && elem.isVertex()) {
            action.accept((OVertex) elem);
            return true;
          }
        }
        return false;
      }

      @Override public Spliterator<OVertex> trySplit() {
        return null;
      }

      @Override public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  default Stream<OEdge> edgeStream() {
    return StreamSupport.stream(new Spliterator<OEdge>() {
      @Override public boolean tryAdvance(Consumer<? super OEdge> action) {
        while (hasNext()) {
          OElement elem = (OElement) next().getElement();
          if (elem != null && elem.isEdge()) {
            action.accept((OEdge) elem);
            return true;
          }
        }
        return false;
      }

      @Override public Spliterator<OEdge> trySplit() {
        return null;
      }

      @Override public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override public int characteristics() {
        return ORDERED;
      }
    }, false);
  }
}
