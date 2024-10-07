package org.apache.tinkerpop.gremlin.orientdb;

import static com.google.common.collect.Lists.newArrayList;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {

  public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
    return asStream(sourceIterator, false);
  }

  public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
    Iterable<T> iterable = () -> sourceIterator;
    return StreamSupport.stream(iterable.spliterator(), parallel);
  }

  public static Stream<String> asStream(String[] fieldNames) {
    return newArrayList(fieldNames).stream();
  }
}
