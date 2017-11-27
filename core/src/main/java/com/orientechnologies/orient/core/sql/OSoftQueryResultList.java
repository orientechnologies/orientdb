/*
  *
  *  *  Copyright 2017 OrientDB LTD (info(at)orientdb.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientdb.com
  *
  */

package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.*;

/**
 * Implementation which is similar to {@link WeakHashMap} but uses {@link SoftReference}s instead of {@link
 * java.lang.ref.WeakReference}s. Once it is detected that some of the records inside list are processed by GC {@link
 * OCommandExecutionException} will be thrown.
 */
public class OSoftQueryResultList<E> implements List<E> {

  public static <T> List<T> createResultList(String query) {
    if (OGlobalConfiguration.QUERY_USE_SOFT_REFENCES_IN_RESULT_SET.getValueAsBoolean()) {
      return new OSoftQueryResultList<T>(query);
    } else {
      return new ArrayList<T>();
    }
  }

  static <T> List<T> createResultList(String query, List<T> other) {
    if (OGlobalConfiguration.QUERY_USE_SOFT_REFENCES_IN_RESULT_SET.getValueAsBoolean()) {
      return new OSoftQueryResultList<T>(other, query);
    } else {
      return new ArrayList<T>(other);
    }
  }

  private final List<SoftReference<E>> buffer;
  private final ReferenceQueue<E> queue = new ReferenceQueue<E>();
  private final String query;

  @SuppressWarnings("WeakerAccess")
  OSoftQueryResultList(String query) {
    this.query = query;
    this.buffer = new ArrayList<SoftReference<E>>();
  }

  @SuppressWarnings("WeakerAccess")
  OSoftQueryResultList(List<? extends E> other, String query) {
    this(query);
    for (E x : other) {
      buffer.add(new SoftReference<E>(x, queue));
    }
  }

  private OSoftQueryResultList(String query, List<SoftReference<E>> buffer) {
    this.buffer = buffer;
    this.query = query;
  }

  private void checkQueue() {
    if (queue.poll() != null) {
      throwCanExecuteException();
    }
  }

  /**
   * The same as {@link List#size()} but throws {@link OCommandExecutionException} if some of items inside the list are collected by
   * GC.
   */
  @Override
  public int size() {
    checkQueue();
    return buffer.size();
  }

  /**
   * The same as {@link List#isEmpty()} but throws {@link OCommandExecutionException} if some of items inside the list are collected
   * by GC.
   */

  @Override
  public boolean isEmpty() {
    checkQueue();
    return buffer.isEmpty();
  }

  /**
   * The same as {@link List#contains(Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   */
  @Override
  public boolean contains(Object o) {
    checkQueue();

    for (SoftReference<E> ref : buffer) {
      final E item = getItem(ref);
      if (item.equals(o)) {
        return true;
      }
    }
    return false;
  }

  private E getItem(SoftReference<E> ref) {
    checkQueue();
    final E result = ref.get();

    if (result == null) {
      throwCanExecuteException();
    }
    return result;
  }

  /**
   * The same as {@link List#iterator()} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC. All methods of iterator also check presence of collected items inside of the list and throw {@link
   * OCommandExecutionException} if such items are detected.
   */
  @Override
  public Iterator<E> iterator() {
    checkQueue();
    return new Iterator<E>() {
      private SoftReference<E> current = null;
      private Iterator<SoftReference<E>> innerIterator = buffer.iterator();

      @Override
      public boolean hasNext() {
        checkQueue();
        return innerIterator.hasNext();
      }

      @Override
      public E next() {
        checkQueue();
        current = innerIterator.next();

        return getItem(current);
      }

      @Override
      public void remove() {
        checkQueue();

        if (current != null) {
          current.clear();
        } else {
          throw new IllegalStateException("Can not clear currently processed soft reference");
        }

        innerIterator.remove();
      }
    };
  }

  /**
   * Not supported because may lead to generation of OOM.
   */
  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Operation is not supported because may cause OOM exception");
  }

  /**
   * Not supported because may lead to generation of OOM.
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Operation is not supported because may cause OOM exception");
  }

  /**
   * The same as {@link List#add(Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   *
   * @throws NullPointerException if passed in value is <code>null</code>
   */
  @Override
  public boolean add(E e) {
    checkQueue();

    if (e == null) {
      throw new NullPointerException("Null value is passed");
    }

    return buffer.add(new SoftReference<E>(e, queue));
  }

  /**
   * The same as {@link List#remove(Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   */
  @Override
  public boolean remove(Object o) {
    checkQueue();
    for (SoftReference<E> ref : buffer) {
      final E item = getItem(ref);

      if (item.equals(o)) {
        ref.clear();

        buffer.remove(ref);
        return true;
      }
    }

    return false;
  }

  /**
   * The same as {@link List#containsAll(Collection)} (Object)} but throws {@link OCommandExecutionException} if some of items
   * inside the list are collected by GC.
   */
  @Override
  public boolean containsAll(Collection<?> c) {
    checkQueue();

    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }

    return true;
  }

  /**
   * The same as {@link List#addAll(Collection)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   */
  @Override
  public boolean addAll(Collection<? extends E> c) {
    checkQueue();

    boolean added = false;

    for (final E o : c) {
      added = added | add(o);
    }

    return added;
  }

  /**
   * The same as {@link List#addAll(int, Collection)} but throws {@link OCommandExecutionException} if some of items inside the list
   * are collected by GC.
   */
  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    checkQueue();

    boolean added = false;

    for (E o : c) {
      added = true;
      add(index++, o);
    }

    return added;
  }

  /**
   * The same as {@link List#removeAll(Collection)} but throws {@link OCommandExecutionException} if some of items inside the list
   * are collected by GC.
   */
  @Override
  public boolean removeAll(Collection<?> c) {
    checkQueue();

    boolean updated = false;
    final Iterator<SoftReference<E>> iter = buffer.iterator();

    while (iter.hasNext()) {
      final SoftReference<E> item = iter.next();
      final E val = getItem(item);

      for (Object o : c) {
        if (val.equals(o)) {
          updated = true;
          item.clear();
          iter.remove();
          break;
        }
      }
    }

    return updated;
  }

  /**
   * The same as {@link List#retainAll(Collection)} but throws {@link OCommandExecutionException} if some of items inside the list
   * are collected by GC.
   */
  @Override
  public boolean retainAll(Collection<?> c) {
    checkQueue();

    boolean updated = false;
    final Iterator<SoftReference<E>> iter = buffer.iterator();

    while (iter.hasNext()) {
      final SoftReference<E> item = iter.next();

      final E val = getItem(item);
      boolean found = false;

      for (Object o : c) {
        if (val.equals(o)) {
          found = true;
          break;
        }
      }

      if (!found) {
        item.clear();
        iter.remove();
        updated = true;
      }
    }

    return updated;
  }

  /**
   * The same as {@link List#clear()} but throws {@link OCommandExecutionException} if some of items inside the list are collected
   * by GC.
   */
  @Override
  public void clear() {
    checkQueue();

    for (SoftReference<E> ref : buffer) {
      ref.clear();
    }

    buffer.clear();
  }

  /**
   * The same as {@link List#get(int)} but throws {@link OCommandExecutionException} if some of items inside the list are collected
   * by GC.
   */
  @Override
  public E get(int index) {
    checkQueue();

    final SoftReference<E> res = buffer.get(index);
    assert res != null;

    return getItem(res);
  }

  /**
   * The same as {@link List#set(int, Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   *
   * @throws NullPointerException if passed in value is <code>null</code>
   */
  @Override
  public E set(int index, E element) {
    checkQueue();

    if (element == null) {
      throw new NullPointerException("Null value passed");
    }

    final SoftReference<E> res = buffer.set(index, new SoftReference<E>(element, queue));
    assert res != null;

    final E item = getItem(res);
    res.clear();
    return item;
  }

  /**
   * The same as {@link List#add(int, Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   *
   * @throws NullPointerException if passed in value equals to <code>null</code>
   */
  @Override
  public void add(int index, E element) {
    checkQueue();

    if (element == null) {
      throw new NullPointerException("Null value passed");
    }

    buffer.add(index, new SoftReference<E>(element, queue));
  }

  /**
   * The same as {@link List#remove(int)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   */
  @Override
  public E remove(int index) {
    checkQueue();

    final SoftReference<E> res = buffer.remove(index);
    assert res != null;

    final E item = getItem(res);
    res.clear();

    return item;
  }

  /**
   * The same as {@link List#indexOf(Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   *
   * @throws NullPointerException if passed in value equals to <code>null</code>
   */
  @Override
  public int indexOf(Object o) {
    checkQueue();

    if (o == null) {
      throw new NullPointerException("Null value passed");
    }

    for (int i = 0; i < buffer.size(); i++) {
      E item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The same as {@link List#lastIndexOf(Object)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   *
   * @throws NullPointerException if argument value is <code>null</code>
   */
  @Override
  public int lastIndexOf(Object o) {
    checkQueue();

    if (o == null)
      throw new NullPointerException("Null value passed");

    for (int i = buffer.size() - 1; i >= 0; i--) {
      E item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The same as {@link List#listIterator()} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC. All methods of iterator also check presence of collected items inside of the list and throw {@link
   * OCommandExecutionException} if such items are detected.
   */
  @Override
  public ListIterator<E> listIterator() {
    checkQueue();

    return new ListIterator<E>() {
      private SoftReference<E> current = null;
      private ListIterator<SoftReference<E>> innerIterator = buffer.listIterator();

      @Override
      public boolean hasNext() {
        checkQueue();
        return innerIterator.hasNext();
      }

      @Override
      public E next() {
        current = innerIterator.next();
        return getItem(current);
      }

      @Override
      public boolean hasPrevious() {
        checkQueue();
        return innerIterator.hasPrevious();
      }

      @Override
      public E previous() {
        current = innerIterator.previous();

        return getItem(current);
      }

      @Override
      public int nextIndex() {
        checkQueue();
        return innerIterator.nextIndex();
      }

      @Override
      public int previousIndex() {
        checkQueue();
        return innerIterator.previousIndex();
      }

      @Override
      public void remove() {
        checkQueue();

        if (current != null) {
          current.clear();
        } else {
          throw new IllegalStateException("Can not clear currently processed soft reference");
        }

        innerIterator.remove();
      }

      @Override
      public void set(E e) {
        checkQueue();

        if (e == null) {
          throw new NullPointerException("Null value is passed as argument");
        }

        if (current != null) {
          current.clear();
        } else {
          throw new IllegalStateException("Can not clear currently processed soft reference");
        }

        innerIterator.set(new SoftReference<E>(e, queue));
      }

      @Override
      public void add(E e) {
        checkQueue();

        if (e == null) {
          throw new NullPointerException("Null value is passed as argument");
        }

        innerIterator.add(new SoftReference<E>(e, queue));
      }
    };
  }

  /**
   * The same as {@link List#listIterator(int)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC. All methods of iterator also check presence of collected items inside of the list and throw {@link
   * OCommandExecutionException} if such items are detected.
   */
  @Override
  public ListIterator<E> listIterator(final int index) {
    checkQueue();

    return new ListIterator<E>() {
      private SoftReference<E> current = null;
      private ListIterator<SoftReference<E>> innerIterator = buffer.listIterator(index);

      @Override
      public boolean hasNext() {
        checkQueue();
        return innerIterator.hasNext();
      }

      @Override
      public E next() {
        current = innerIterator.next();
        return getItem(current);
      }

      @Override
      public boolean hasPrevious() {
        checkQueue();
        return innerIterator.hasPrevious();
      }

      @Override
      public E previous() {
        current = innerIterator.previous();
        return getItem(current);
      }

      @Override
      public int nextIndex() {
        checkQueue();
        return innerIterator.nextIndex();
      }

      @Override
      public int previousIndex() {
        checkQueue();
        return innerIterator.previousIndex();
      }

      @Override
      public void remove() {
        checkQueue();
        if (current != null) {
          current.clear();
        } else {
          throw new IllegalStateException("Can not clear currently processed soft reference");
        }

        innerIterator.remove();
      }

      @Override
      public void set(E e) {
        checkQueue();

        if (e == null) {
          throw new NullPointerException("Null value is passed as argument");
        }

        if (current != null) {
          current.clear();
        } else {
          throw new IllegalStateException("Can not clear currently processed soft reference");
        }
        innerIterator.set(new SoftReference<E>(e, queue));
      }

      @Override
      public void add(E e) {
        checkQueue();

        if (e == null) {
          throw new NullPointerException("Null value is passed as argument");
        }

        innerIterator.add(new SoftReference<E>(e, queue));
      }
    };
  }

  /**
   * The same as {@link List#subList(int, int)} but throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC.
   */
  @Override
  public OSoftQueryResultList<E> subList(int fromIndex, int toIndex) {
    checkQueue();

    return new OSoftQueryResultList<E>(query, buffer.subList(fromIndex, toIndex));
  }

  /**
   * Sort elements according to passed in comparator, throws {@link OCommandExecutionException} if some of items inside the list are
   * collected by GC. This method is implemented because default method of sorting collections in Java converts {@link List} to
   * array which is not supported by current implementation.
   */
  public void sort(Comparator<? super E> comparator) {
    checkQueue();

    @SuppressWarnings("unchecked")
    final SoftReference<E>[] softRefs = new SoftReference[buffer.size()];
    for (int i = 0; i < softRefs.length; i++) {
      softRefs[i] = buffer.get(i);
    }

    final OSoftQueryResultTimSort<E> sort = new OSoftQueryResultTimSort<E>(query, queue);
    sort.sort(softRefs, 0, buffer.size(), comparator);

    for (int i = 0; i < buffer.size(); i++) {
      buffer.set(i, softRefs[i]);
    }
  }

  private void throwCanExecuteException() {
    if (query != null) {
      throw new OCommandExecutionException("Cannot execute query \"" + query + "\": low heap memory");
    } else {
      throw new OCommandExecutionException("Cannot execute query: low heap memory");
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (!(obj instanceof List))
      return false;

    final ListIterator<E> e1 = listIterator();
    final ListIterator<?> e2 = ((List<?>) obj).listIterator();

    while (e1.hasNext() && e2.hasNext()) {
      E o1 = e1.next();
      Object o2 = e2.next();
      if (!(o1 == null ? o2 == null : o1.equals(o2)))
        return false;
    }
    return !(e1.hasNext() || e2.hasNext());
  }

  @Override
  public int hashCode() {
    int hashCode = 1;
    for (E e : this)
      hashCode = 31 * hashCode + (e == null ? 0 : e.hashCode());
    return hashCode;
  }
}
