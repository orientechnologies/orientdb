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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.*;

/**
 * Implementation which is similar to {@link WeakHashMap} but uses {@link SoftReference}s instead of {@link
 * java.lang.ref.WeakReference}s. Once it is detected that some of the records inside list are processed by GC {@link
 * OCommandExecutionException} will be thrown.
 */
public class OSoftQueryResultList<E extends OIdentifiable> implements List<E> {
  private final List<SoftReference<E>> buffer;
  private final ReferenceQueue<E> queue = new ReferenceQueue<E>();
  private final String query;

  @SuppressWarnings({ "WeakerAccess", "unused" })
  public OSoftQueryResultList() {
    this((String) null);
  }

  @SuppressWarnings("WeakerAccess")
  public OSoftQueryResultList(String query) {
    this.query = query;
    this.buffer = new ArrayList<SoftReference<E>>();
  }

  @SuppressWarnings({ "unused", "WeakerAccess" })
  public OSoftQueryResultList(List<? extends E> other) {
    this(other, null);
  }

  @SuppressWarnings("WeakerAccess")
  public OSoftQueryResultList(List<? extends E> other, String query) {
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
        }

        innerIterator.remove();
      }
    };
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Operation is not supported because may cause OOM exception");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Operation is not supported because may cause OOM exception");
  }

  @Override
  public boolean add(E e) {
    checkQueue();
    if (e == null) {
      throw new IllegalArgumentException("Null value is passed");
    }

    return buffer.add(new SoftReference<E>(e, queue));
  }

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

  @Override
  public boolean addAll(Collection<? extends E> c) {
    checkQueue();

    boolean added = false;

    for (final E o : c) {
      added = added | add(o);
    }

    return added;
  }

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
        }
      }
    }

    return updated;
  }

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

  @Override
  public void clear() {
    checkQueue();

    for (SoftReference<E> ref : buffer) {
      ref.clear();
    }

    buffer.clear();
  }

  @Override
  public E get(int index) {
    checkQueue();

    final SoftReference<E> res = buffer.get(index);

    if (res == null) {
      return null;
    }
    return getItem(res);
  }

  @Override
  public E set(int index, E element) {
    checkQueue();

    if (element == null) {
      throw new IllegalArgumentException("Null value passed");
    }

    final SoftReference<E> res = buffer.set(index, new SoftReference<E>(element, queue));
    if (res == null) {
      return null;
    }

    final E item = getItem(res);
    res.clear();
    return item;
  }

  @Override
  public void add(int index, E element) {
    checkQueue();

    if (element == null) {
      throw new IllegalArgumentException();
    }

    buffer.add(index, new SoftReference<E>(element, queue));
  }

  @Override
  public E remove(int index) {
    checkQueue();

    final SoftReference<E> res = buffer.remove(index);
    if (res == null) {
      return null;
    }

    final E item = getItem(res);
    res.clear();

    return item;
  }

  @Override
  public int indexOf(Object o) {
    checkQueue();

    for (int i = 0; i < buffer.size(); i++) {
      E item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    checkQueue();

    for (int i = buffer.size() - 1; i >= 0; i--) {
      E item = getItem(buffer.get(i));
      if (item.equals(o)) {
        return i;
      }
    }
    return -1;
  }

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
        }
        innerIterator.remove();
      }

      @Override
      public void set(E e) {
        checkQueue();
        if (current != null) {
          current.clear();
        }
        innerIterator.set(new SoftReference<E>(e, queue));
      }

      @Override
      public void add(E e) {
        checkQueue();
        innerIterator.add(new SoftReference<E>(e, queue));
      }
    };
  }

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
        }
        innerIterator.remove();
      }

      @Override
      public void set(E e) {
        checkQueue();
        if (current != null) {
          current.clear();
        }
        innerIterator.set(new SoftReference<E>(e, queue));
      }

      @Override
      public void add(E e) {
        checkQueue();
        innerIterator.add(new SoftReference<E>(e, queue));
      }
    };
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    checkQueue();

    return new OSoftQueryResultList<E>(query, buffer.subList(fromIndex, toIndex));
  }

  public void sort(Comparator<? super E> comparator) {
    checkQueue();

    @SuppressWarnings("unchecked")
    final SoftReference<E>[] softRefs = new SoftReference[buffer.size()];
    for (int i = 0; i < softRefs.length; i++) {
      softRefs[i] = buffer.get(i);
    }

    OSoftQueryResultTimSort.sort(softRefs, size(), comparator, queue, query);

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
