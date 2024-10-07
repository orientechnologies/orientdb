package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VertexEdgeListWrapper implements List {
  private final List wrapped;
  private final OrientElement parent;

  public VertexEdgeListWrapper(List wrapped, OrientElement parentElement) {
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

  public boolean addAll(int index, Collection c) {
    for (Object o : c) {
      wrapped.add(index++, unbox(o));
    }
    return true;
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

  public void replaceAll(UnaryOperator operator) {
    wrapped.replaceAll(operator);
  }

  public void sort(Comparator c) {
    wrapped.sort(c);
  }

  public void clear() {
    wrapped.clear();
  }

  public Object get(int index) {
    return box(wrapped.get(index));
  }

  public Object set(int index, Object element) {
    return wrapped.set(index, unbox(element));
  }

  public void add(int index, Object element) {
    wrapped.add(index, unbox(element));
  }

  public Object remove(int index) {
    return wrapped.remove(index);
  }

  public int indexOf(Object o) {
    return wrapped.indexOf(unbox(o));
  }

  public int lastIndexOf(Object o) {
    return wrapped.lastIndexOf(unbox(o));
  }

  public ListIterator listIterator() {
    return new ListIterator() {
      ListIterator baseIter = wrapped.listIterator();

      @Override
      public boolean hasNext() {
        return baseIter.hasNext();
      }

      @Override
      public Object next() {
        return box(baseIter.next());
      }

      @Override
      public boolean hasPrevious() {
        return baseIter.hasNext();
      }

      @Override
      public Object previous() {
        return box(baseIter.previous());
      }

      @Override
      public int nextIndex() {
        return baseIter.nextIndex();
      }

      @Override
      public int previousIndex() {
        return baseIter.previousIndex();
      }

      @Override
      public void remove() {
        baseIter.remove();
      }

      @Override
      public void set(Object o) {
        baseIter.set(unbox(o));
      }

      @Override
      public void add(Object o) {
        baseIter.add(unbox(o));
      }
    };
  }

  public ListIterator listIterator(int index) {
    return new ListIterator() {
      ListIterator baseIter = wrapped.listIterator(index);

      @Override
      public boolean hasNext() {
        return baseIter.hasNext();
      }

      @Override
      public Object next() {
        return box(baseIter.next());
      }

      @Override
      public boolean hasPrevious() {
        return baseIter.hasNext();
      }

      @Override
      public Object previous() {
        return box(baseIter.previous());
      }

      @Override
      public int nextIndex() {
        return baseIter.nextIndex();
      }

      @Override
      public int previousIndex() {
        return baseIter.previousIndex();
      }

      @Override
      public void remove() {
        baseIter.remove();
      }

      @Override
      public void set(Object o) {
        baseIter.set(unbox(o));
      }

      @Override
      public void add(Object o) {
        baseIter.add(unbox(o));
      }
    };
  }

  public List subList(int fromIndex, int toIndex) {
    return new VertexEdgeListWrapper(wrapped.subList(fromIndex, toIndex), parent);
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
