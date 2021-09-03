package com.orientechnologies.orient.core.sql.query;

import com.orientechnologies.common.log.OLogManager;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Created by luigidellaquila on 18/03/15. */
public class OLiveLegacyResultSet<T> extends OConcurrentLegacyResultSet<T> {

  private final BlockingQueue<T> queue = new LinkedBlockingQueue<T>();

  public OLiveLegacyResultSet() {}

  public OConcurrentLegacyResultSet<T> setCompleted() {
    // completed = true;
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
    synchronized (waitForCompletion) {
      waitForCompletion.notifyAll();
    }
    return this;
  }

  public void complete() {
    completed = true;
    synchronized (waitForNextItem) {
      waitForNextItem.notifyAll();
    }
    synchronized (waitForCompletion) {
      waitForCompletion.notifyAll();
    }
  }

  public T set(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(final Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        try {

          T result = queue.take();
          return result;
        } catch (InterruptedException e) {
          setCompleted();
          Thread.currentThread().interrupt();
          return null;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public <T1> T1[] toArray(final T1[] a) {
    throw new UnsupportedOperationException();
  }

  public boolean add(final T t) {
    queue.offer(t);
    return true;
  }

  @Override
  public boolean remove(final Object o) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  public boolean addAll(final Collection<? extends T> c) {
    for (T t : c) {
      add(t);
    }
    return true;
  }

  public boolean addAll(final int index, final Collection<? extends T> c) {
    for (T t : c) {
      add(t);
    }
    return true;
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(final int index) {
    throw new UnsupportedOperationException();
  }

  public void add(final int index, T element) {
    add(element);
  }

  @Override
  public T remove(final int index) {
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
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }

  public int getLimit() {
    return wrapped.getLimit();
  }

  public OLegacyResultSet<T> setLimit(final int limit) {
    wrapped.setLimit(limit);
    return null;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
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

  public OLiveLegacyResultSet<T> copy() {
    OLiveLegacyResultSet<T> newValue = new OLiveLegacyResultSet<T>();
    return newValue;
  }
}
