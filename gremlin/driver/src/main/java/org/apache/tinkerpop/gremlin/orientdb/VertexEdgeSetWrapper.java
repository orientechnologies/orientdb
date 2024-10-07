package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VertexEdgeSetWrapper implements Set {
  private final Set wrapped;
  private final OrientElement parent;

  public VertexEdgeSetWrapper(Set wrapped, OrientElement parentElement) {
    this.wrapped = wrapped;
    this.parent = parentElement;
  }

  private Object unbox(Object next) {
    if (next instanceof OrientElement) {
      return ((OrientElement) next).getRawElement();
    }
    return next;
  }

  private Object box(Object elem) {
    if (elem instanceof ORID) {
      elem = ((ORID) elem).getRecord();
    }
    if (elem instanceof OElement) {
      if (((OElement) elem).isVertex()) {
        elem = parent.getGraph().elementFactory().wrapVertex(((OElement) elem).asVertex().get());
      } else if (((OElement) elem).isEdge()) {
        elem = parent.getGraph().elementFactory().wrapEdge(((OElement) elem).asEdge().get());
      }
    }
    return elem;
  }

  public int size() {
    return wrapped.size();
  }

  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  public boolean contains(Object o) {
    if (o instanceof OrientElement && wrapped.contains(((OrientElement) o).getRawElement())) {
      return true;
    }
    return wrapped.contains(o);
  }

  public Iterator iterator() {

    return new Iterator() {
      Iterator baseIter = wrapped.iterator();

      @Override
      public boolean hasNext() {
        return baseIter.hasNext();
      }

      @Override
      public Object next() {
        return box(baseIter.next());
      }
    };
  }

  public Object[] toArray() {
    return wrapped.stream().map(x -> box(x)).toArray();
  }

  public Object[] toArray(Object[] a) {
    return wrapped.stream().map(x -> box(x)).toArray();
  }

  public boolean add(Object o) {
    return wrapped.add(unbox(o));
  }

  public boolean remove(Object o) {
    return wrapped.remove(unbox(o));
  }

  public boolean containsAll(Collection c) {

    return wrapped.containsAll((List) c.stream().map(x -> unbox(x)).collect(Collectors.toList()));
  }

  public boolean addAll(Collection c) {
    boolean changed = false;
    for (Object o : c) {
      changed = changed || wrapped.add(unbox(o));
    }
    return changed;
  }

  @Override
  public void clear() {
    wrapped.clear();
  }

  public boolean removeAll(Collection c) {
    boolean changed = false;
    for (Object o : c) {
      changed = changed || wrapped.remove(unbox(o));
    }
    return changed;
  }

  public boolean retainAll(Collection c) {
    return wrapped.retainAll(
        (Collection<?>) c.stream().map(x -> unbox(x)).collect(Collectors.toList()));
  }

  public Spliterator spliterator() {
    throw new UnsupportedOperationException();
  }

  public boolean removeIf(Predicate filter) {
    return wrapped.removeIf(filter);
  }

  public Stream stream() {
    return wrapped.stream().map(x -> box(x));
  }

  public Stream parallelStream() {
    return wrapped.parallelStream().map(x -> box(x));
  }

  public void forEach(Consumer action) {
    wrapped.forEach(action);
  }
}
