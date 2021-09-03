package com.orientechnologies.common.stream;

import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class BreakingForEach {
  public static class Breaker {
    private boolean shouldBreak = false;

    public void stop() {
      shouldBreak = true;
    }
  }

  public static <T> void forEach(Stream<T> stream, BiConsumer<T, Breaker> consumer) {
    Spliterator<T> spliterator = stream.spliterator();
    boolean hadNext = true;
    Breaker breaker = new Breaker();

    while (hadNext && !breaker.shouldBreak) {
      hadNext =
          spliterator.tryAdvance(
              elem -> {
                consumer.accept(elem, breaker);
              });
    }
  }
}
