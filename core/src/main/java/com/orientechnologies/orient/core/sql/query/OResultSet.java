/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * ResultSet class that implements List interface for retro compatibility.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 * @see OSQLAsynchQuery
 */
public class OResultSet<T> implements List<T>, Externalizable {
  protected List<T>                    underlying;
  protected transient volatile boolean completed         = false;
  protected transient Object           waitForNextItem   = new Object();
  protected transient Object           waitForCompletion = new Object();
  protected transient int              limit             = -1;

  public OResultSet() {
    underlying = Collections.synchronizedList(new ArrayList<T>());
  }

  public OResultSet(final int iCapacity) {
    underlying = Collections.synchronizedList(new ArrayList<T>(iCapacity));
  }

  public OResultSet<T> setCompleted() {
    completed = true;
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
    synchronized (waitForCompletion) {
      waitForCompletion.notifyAll();
    }
    return this;
  }

  public T set(int index, T element) {
    return underlying.set(index, element);
  }

  @Override
  public int size() {
    waitForCompletion();
    return underlying.size();
  }

  @Override
  public boolean isEmpty() {
    boolean empty = underlying.isEmpty();
    if (empty) {
      waitForCompletion();
      empty = underlying.isEmpty();
    }
    return empty;
  }

  @Override
  public boolean contains(final Object o) {
    waitForCompletion();
    return underlying.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        while (!completed) {
          if (index < size())
            return true;

          waitForNewItemOrCompleted();
        }

        return index < size();
      }

      @Override
      public T next() {
        while (!completed) {
          if (index < size())
            break;

          waitForNewItemOrCompleted();
        }

        if (index > size())
          throw new NoSuchElementException("Error on browsing at element " + index + " while the resultset contains only " + size()
              + " items");

        return underlying.get(index++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("OResultSet.iterator.remove()");
      }
    };
  }

  @Override
  public Object[] toArray() {
    return underlying.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return underlying.toArray(a);
  }

  public boolean add(T t) {
    if (limit > -1 && underlying.size() >= limit)
      return false;

    final boolean result = underlying.add(t);
    notifyNewItem();
    return result;
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  public boolean addAll(Collection<? extends T> c) {
    return underlying.addAll(c);
  }

  public boolean addAll(int index, Collection<? extends T> c) {
    return underlying.addAll(index, c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void clear() {
    underlying.clear();
  }

  @Override
  public boolean equals(Object o) {
    return underlying.equals(o);
  }

  @Override
  public int hashCode() {
    return underlying.hashCode();
  }

  @Override
  public T get(int index) {
    return underlying.get(index);
  }

  public void add(int index, T element) {
    underlying.add(index, element);
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException("indexOf");
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException("lastIndexOf");
  }

  @Override
  public ListIterator<T> listIterator() {
    return underlying.listIterator();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    return underlying.listIterator(index);
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return underlying.subList(fromIndex, toIndex);
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(underlying);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    underlying = (List<T>) in.readObject();
    completed = true;
  }

  protected void waitForCompletion() {
    synchronized (waitForCompletion) {
      if (!completed)
        try {
          waitForCompletion.wait();
        } catch (InterruptedException e) {
        }
    }
  }

  protected void waitForNewItemOrCompleted() {
    synchronized (waitForNextItem) {
      try {
        waitForNextItem.wait();
      } catch (InterruptedException e) {
      }
    }
  }

  protected void notifyNewItem() {
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
  }
}
