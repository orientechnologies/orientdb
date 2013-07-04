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
package com.orientechnologies.common.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.orientechnologies.common.util.OResettable;

/**
 * Iterator that allow to iterate against multiple collection of elements.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OMultiCollectionIterator<T> implements Iterator<T>, Iterable<T>, OResettable {
  private Collection<Object> sources;
  private Iterator<?>        iteratorOfInternalCollections;
  private Iterator<T>        partialIterator;
  private List<T>            temp     = null;

  private int                browsed  = 0;
  private int                limit    = -1;
  private boolean            embedded = false;

  public OMultiCollectionIterator() {
    sources = new ArrayList<Object>();
  }

  public OMultiCollectionIterator(final Collection<Object> iSources) {
    sources = iSources;
    iteratorOfInternalCollections = iSources.iterator();
    getNextPartial();
  }

  public OMultiCollectionIterator(final Iterator<? extends Collection<?>> iterator) {
    iteratorOfInternalCollections = iterator;
    getNextPartial();
  }

  @Override
  public boolean hasNext() {
    if (iteratorOfInternalCollections == null) {
      if (sources == null || sources.isEmpty())
        return false;

      // THE FIRST TIME CREATE THE ITERATOR
      iteratorOfInternalCollections = sources.iterator();
      getNextPartial();
    }

    if (partialIterator == null)
      return false;

    if (limit > -1 && browsed >= limit)
      return false;

    if (partialIterator.hasNext())
      return true;
    else if (iteratorOfInternalCollections.hasNext())
      return getNextPartial();

    return false;
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();

    browsed++;
    return partialIterator.next();
  }

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  @Override
  public void reset() {
    iteratorOfInternalCollections = null;
    partialIterator = null;
    browsed = 0;
  }

  public void add(final Object iValue) {
    if (iteratorOfInternalCollections != null)
      throw new IllegalStateException("MultiCollection iterator is in use and new collections cannot be added");

    sources.add(iValue);
  }

  public int size() {
    // SUM ALL THE COLLECTION SIZES
    int size = 0;
    for (Object o : sources) {
      if (o != null)
        if (o instanceof Collection<?>)
          size += ((Collection<?>) o).size();
        else if (o instanceof Map<?, ?>)
          size += ((Map<?, ?>) o).size();
        else if (o instanceof OMultiCollectionIterator<?>)
          size += ((OMultiCollectionIterator<?>) o).size();
        else if (o instanceof Iterator<?> && o instanceof OResettable) {
          while (((Iterator<?>) o).hasNext()) {
            size++;
            ((Iterator<?>) o).next();
          }
          ((OResettable) o).reset();
        } else
          size++;
    }
    return size;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("OMultiCollectionIterator.remove()");
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  @SuppressWarnings("unchecked")
  protected boolean getNextPartial() {
    if (iteratorOfInternalCollections != null)
      while (iteratorOfInternalCollections.hasNext()) {
        final Object next = iteratorOfInternalCollections.next();
        if (next != null) {
          if (next instanceof Iterator<?>) {
            if (next instanceof OResettable)
              ((OResettable) next).reset();

            if (((Iterator<T>) next).hasNext()) {
              partialIterator = (Iterator<T>) next;
              return true;
            }
          } else if (next instanceof Collection<?>) {
            if (!((Collection<T>) next).isEmpty()) {
              partialIterator = ((Collection<T>) next).iterator();
              return true;
            }
          } else {
            if (temp == null)
              temp = new ArrayList<T>(1);
            else
              temp.clear();

            temp.add((T) next);
            partialIterator = temp.iterator();
            return true;
          }
        }
      }

    return false;
  }

  public boolean isEmbedded() {
    return embedded;
  }

  public OMultiCollectionIterator<T> setEmbedded(final boolean embedded) {
    this.embedded = embedded;
    return this;
  }
}
