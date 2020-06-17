/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.query;

import com.orientechnologies.common.log.OLogManager;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * ResultSet implementation that allows concurrent population.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @param <T>
 * @see OSQLAsynchQuery
 */
public class OConcurrentLegacyResultSet<T> implements OLegacyResultSet<T> {
  protected final transient Object waitForNextItem = new Object();
  protected final transient Object waitForCompletion = new Object();
  protected final transient OBasicLegacyResultSet<T> wrapped;
  protected transient volatile boolean completed = false;

  public OConcurrentLegacyResultSet() {
    this.wrapped = new OBasicLegacyResultSet<T>();
  }

  public OConcurrentLegacyResultSet(final OBasicLegacyResultSet<T> wrapped) {
    this.wrapped = wrapped;
  }

  public OConcurrentLegacyResultSet<T> setCompleted() {
    completed = true;
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
    synchronized (waitForCompletion) {
      waitForCompletion.notifyAll();
    }
    return this;
  }

  @Override
  public int getLimit() {
    return wrapped.getLimit();
  }

  @Override
  public OLegacyResultSet<T> setLimit(final int limit) {
    return wrapped.setLimit(limit);
  }

  @Override
  public OLegacyResultSet<T> copy() {
    synchronized (wrapped) {
      final OConcurrentLegacyResultSet<T> copy = new OConcurrentLegacyResultSet<T>(wrapped.copy());
      copy.completed = true;
      return copy;
    }
  }

  @Override
  public boolean isEmptyNoWait() {
    synchronized (wrapped) {
      return wrapped.isEmpty();
    }
  }

  public T set(final int index, final T element) {
    synchronized (wrapped) {
      return wrapped.set(index, element);
    }
  }

  @Override
  public void add(int index, T element) {
    synchronized (wrapped) {
      wrapped.add(index, element);
    }
    notifyNewItem();
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<T> listIterator() {
    synchronized (wrapped) {
      return wrapped.listIterator();
    }
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    synchronized (wrapped) {
      return wrapped.listIterator(index);
    }
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    synchronized (wrapped) {
      return wrapped.subList(fromIndex, toIndex);
    }
  }

  @Override
  public int size() {
    waitForCompletion();
    synchronized (wrapped) {
      return wrapped.size();
    }
  }

  @Override
  public int currentSize() {
    synchronized (wrapped) {
      return wrapped.size();
    }
  }

  @Override
  public boolean isEmpty() {
    boolean empty;
    synchronized (wrapped) {
      empty = wrapped.isEmpty();
    }

    if (empty) {
      waitForNewItemOrCompleted();
      synchronized (wrapped) {
        empty = wrapped.isEmpty();
      }
    }
    return empty;
  }

  @Override
  public boolean contains(final Object o) {
    waitForCompletion();
    synchronized (wrapped) {
      return wrapped.contains(o);
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        int size;
        synchronized (wrapped) {
          size = wrapped.size();
        }
        while (!completed) {
          if (index < size) return true;

          waitForNewItemOrCompleted();

          synchronized (wrapped) {
            size = wrapped.size();
          }
        }

        return index < wrapped.size();
      }

      @Override
      public T next() {
        int size;
        synchronized (wrapped) {
          size = wrapped.size();
        }

        while (!completed) {
          if (index < size) break;

          waitForNewItemOrCompleted();

          synchronized (wrapped) {
            size = wrapped.size();
          }
        }

        if (index > size || size == 0)
          throw new NoSuchElementException(
              "Error on browsing at element "
                  + index
                  + " while the resultset contains only "
                  + size
                  + " items");

        synchronized (wrapped) {
          return wrapped.get(index++);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("OLegacyResultSet.iterator.remove()");
      }
    };
  }

  @Override
  public Object[] toArray() {
    waitForCompletion();
    synchronized (wrapped) {
      return wrapped.toArray();
    }
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    waitForCompletion();
    synchronized (wrapped) {
      return wrapped.toArray(a);
    }
  }

  public boolean add(final T t) {
    final boolean result;

    synchronized (wrapped) {
      result = wrapped.add(t);
    }

    notifyNewItem();
    return result;
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean result;
    synchronized (wrapped) {
      result = wrapped.addAll(c);
    }
    notifyNewItem();
    return result;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    boolean result;
    synchronized (wrapped) {
      result = wrapped.addAll(index, c);
    }
    notifyNewItem();
    return result;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    synchronized (wrapped) {
      wrapped.clear();
    }
  }

  @Override
  public T get(int index) {
    synchronized (wrapped) {
      return wrapped.get(index);
    }
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    synchronized (wrapped) {
      wrapped.writeExternal(out);
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    synchronized (wrapped) {
      wrapped.readExternal(in);
    }
    completed = true;
  }

  @Override
  public int hashCode() {
    synchronized (wrapped) {
      return wrapped.hashCode();
    }
  }

  @Override
  public boolean equals(final Object obj) {
    synchronized (wrapped) {
      return wrapped.equals(obj);
    }
  }

  @Override
  public String toString() {
    return "size=" + wrapped.size();
  }

  protected void waitForCompletion() {
    synchronized (waitForCompletion) {
      if (!completed)
        try {
          waitForCompletion.wait();
        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Thread was interrupted", e);
        }
    }
  }

  protected void waitForNewItemOrCompleted() {
    synchronized (waitForNextItem) {
      if (!completed)
        try {
          waitForNextItem.wait();
        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "Thread was interrupted", e);
        }
    }
  }

  protected void notifyNewItem() {
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
  }
}
