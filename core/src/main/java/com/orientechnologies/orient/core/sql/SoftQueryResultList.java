package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by luigidellaquila on 04/09/17.
 */
public class SoftQueryResultList<T> implements List<T> {

  protected List<SoftReference<T>> buffer = new ArrayList<SoftReference<T>>();
  protected ReferenceQueue<T>      queue  = new ReferenceQueue<T>();
  protected String query;

  public SoftQueryResultList() {
    this((String) null);
  }

  public SoftQueryResultList(String query) {
    this.query = query;
  }

  public SoftQueryResultList(List other) {
    this(other, null);
  }

  public SoftQueryResultList(List other, String query) {
    this(query);
    for (Object x : other) {
      buffer.add(new SoftReference(x, queue));
    }
  }

  protected void checkQueue() {
    if (queue.poll() != null) {
      if (query != null) {
        throw new OCommandExecutionException("Cannot execute query \"" + query + "\": low heap memory");
      } else {
        throw new OCommandExecutionException("Cannot execute query: low heap memory");
      }
    }
  }

  @Override
  public int size() {
    checkQueue();
    return buffer.size();
  }

  @Override
  public boolean isEmpty() {
    checkQueue();
    return buffer.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    checkQueue();
    for (SoftReference<T> ref : buffer) {
      T item = getItem(ref);
      if (item.equals(o)) {
        return true;
      }
    }
    return false;
  }

  private T getItem(SoftReference<T> ref) {
    checkQueue();
    T result = ref.get();
    if (result == null) {
      throw new OCommandExecutionException("Cannot execute query: low heap memory");
    }
    return result;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      Iterator<SoftReference<T>> innerIterator = buffer.iterator();

      @Override
      public boolean hasNext() {
        return innerIterator.hasNext();
      }

      @Override
      public T next() {
        return getItem(innerIterator.next());
      }

      @Override
      public void remove() {
        checkQueue();
        innerIterator.remove();
      }
    };
  }

  @Override
  public Object[] toArray() {
    Object[] result = new Object[buffer.size()];
    for (int i = 0; i < buffer.size(); i++) {
      result[i] = getItem(buffer.get(i));
    }
    return result;
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    T1[] result;
    if (a.length >= buffer.size()) {
      result = a;
    } else {
      result = (T1[]) Array.newInstance(a.getClass(), buffer.size());
    }
    for (int i = 0; i < buffer.size(); i++) {
      result[i] = (T1) getItem(buffer.get(i));
    }
    return result;
  }

  @Override
  public boolean add(T t) {
    checkQueue();
    if (t == null) {
      throw new IllegalArgumentException();
    }
    return buffer.add(new SoftReference<T>(t, queue));
  }

  @Override
  public boolean remove(Object o) {
    for (SoftReference<T> ref : buffer) {
      T item = getItem(ref);
      if (item.equals(o)) {
        buffer.remove(ref);
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(c)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean added = false;
    for (T o : c) {
      added = added || add(o);
    }
    return added;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    boolean added = false;
    for (T o : c) {
      added = true;
      add(index++, o);
    }
    return added;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean updated = false;
    Iterator<SoftReference<T>> iter = buffer.iterator();
    while (iter.hasNext()) {
      SoftReference<T> item = iter.next();
      T val = getItem(item);
      for (Object o : c) {
        if (val.equals(o)) {
          updated = true;
          iter.remove();
        }
      }
    }
    return updated;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean updated = false;
    Iterator<SoftReference<T>> iter = buffer.iterator();
    while (iter.hasNext()) {
      SoftReference<T> item = iter.next();
      T val = getItem(item);
      boolean found = false;
      for (Object o : c) {
        if (val.equals(o)) {
          found = true;
        }
      }
      if (!found) {
        iter.remove();
        updated = true;
      }
    }
    return updated;
  }

  @Override
  public void clear() {
    buffer.clear();
  }

  @Override
  public T get(int index) {
    SoftReference<T> res = buffer.get(index);
    if (res == null) {
      return null;
    }
    return getItem(res);
  }

  @Override
  public T set(int index, T element) {
    if (element == null) {
      throw new IllegalArgumentException();
    }
    SoftReference<T> res = buffer.set(index, new SoftReference<T>(element, queue));
    if (res == null) {
      return null;
    }
    return getItem(res);
  }

  @Override
  public void add(int index, T element) {
    checkQueue();
    if (element == null) {
      throw new IllegalArgumentException();
    }
    buffer.add(index, new SoftReference<T>(element, queue));
  }

  @Override
  public T remove(int index) {
    SoftReference<T> res = buffer.remove(index);
    if (res == null) {
      return null;
    }
    return getItem(res);
  }

  @Override
  public int indexOf(Object o) {
    for (int i = 0; i < buffer.size(); i++) {
      T item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    for (int i = buffer.size() - 1; i >= 0; i--) {
      T item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public ListIterator<T> listIterator() {
    return new ListIterator<T>() {
      ListIterator<SoftReference<T>> innerIterator = buffer.listIterator();

      @Override
      public boolean hasNext() {
        return innerIterator.hasNext();
      }

      @Override
      public T next() {
        return getItem(innerIterator.next());
      }

      @Override
      public boolean hasPrevious() {
        return innerIterator.hasPrevious();
      }

      @Override
      public T previous() {
        return getItem(innerIterator.previous());
      }

      @Override
      public int nextIndex() {
        return innerIterator.nextIndex();
      }

      @Override
      public int previousIndex() {
        return innerIterator.previousIndex();
      }

      @Override
      public void remove() {
        innerIterator.remove();
      }

      @Override
      public void set(T t) {
        innerIterator.set(new SoftReference<T>(t, queue));
      }

      @Override
      public void add(T t) {
        innerIterator.add(new SoftReference<T>(t, queue));
      }
    };
  }

  @Override
  public ListIterator<T> listIterator(final int index) {
    return new ListIterator<T>() {
      ListIterator<SoftReference<T>> innerIterator = buffer.listIterator(index);

      @Override
      public boolean hasNext() {
        return innerIterator.hasNext();
      }

      @Override
      public T next() {
        return getItem(innerIterator.next());
      }

      @Override
      public boolean hasPrevious() {
        return innerIterator.hasPrevious();
      }

      @Override
      public T previous() {
        return getItem(innerIterator.previous());
      }

      @Override
      public int nextIndex() {
        return innerIterator.nextIndex();
      }

      @Override
      public int previousIndex() {
        return innerIterator.previousIndex();
      }

      @Override
      public void remove() {
        innerIterator.remove();
      }

      @Override
      public void set(T t) {
        innerIterator.set(new SoftReference<T>(t, queue));
      }

      @Override
      public void add(T t) {
        innerIterator.add(new SoftReference<T>(t, queue));
      }
    };
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    SoftQueryResultList<T> result = new SoftQueryResultList<T>(query);
    result.buffer = buffer.subList(fromIndex, toIndex);
    return result;
  }
}
