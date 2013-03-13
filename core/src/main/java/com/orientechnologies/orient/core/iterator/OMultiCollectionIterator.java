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
package com.orientechnologies.orient.core.iterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Iterator that allow to iterate against multiple collection of elements.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OMultiCollectionIterator<T> implements Iterator<T>, Iterable<T> {
  private List<Object> internalCollections;
  private Iterator<?>  iteratorOfInternalCollections;
  private Iterator<T>  partialIterator;

  private int          browsed = 0;
  private int          limit   = -1;

  public OMultiCollectionIterator() {
    internalCollections = new ArrayList<Object>();
  }

  public OMultiCollectionIterator(final Collection<Collection<OIdentifiable>> iterators) {
    iteratorOfInternalCollections = iterators.iterator();
    getNextPartial();
  }

  public OMultiCollectionIterator(final Iterator<? extends Collection<OIdentifiable>> iterator) {
    iteratorOfInternalCollections = iterator;
    getNextPartial();
  }

  @Override
  public boolean hasNext() {
    if (internalCollections != null) {
      // THE FIRST TIME CREATE THE ITERATOR
      iteratorOfInternalCollections = internalCollections.iterator();
      internalCollections = null;
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
    return this;
  }

  public void add(final Object iValue) {
    if (internalCollections == null)
      throw new IllegalStateException("Flatten iterator is in use and new collections cannot be added");

    internalCollections.add(iValue);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("OFlattenIterator.remove()");
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
            if (((Iterator<OIdentifiable>) next).hasNext()) {
              partialIterator = (Iterator<T>) next;
              return true;
            }
          } else if (next instanceof Collection<?>) {
            if (!((Collection<OIdentifiable>) next).isEmpty()) {
              partialIterator = ((Collection<T>) next).iterator();
              return true;
            }
          } else if (next instanceof OIdentifiable) {
            final List<T> list = new ArrayList<T>();
            list.add((T) next);
            partialIterator = list.iterator();
            return true;
          }
        }
      }

    return false;
  }
}
