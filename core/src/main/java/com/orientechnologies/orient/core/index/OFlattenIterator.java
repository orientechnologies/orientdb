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
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Iterator that allow to iterate against multiple collection of elements.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <T>
 */
public class OFlattenIterator<T> implements Iterator<OIdentifiable> {
  private Iterator<? extends Collection<OIdentifiable>> subIterator;
  private Iterator<OIdentifiable>                       partialIterator;

  public OFlattenIterator(final Iterator<? extends Collection<OIdentifiable>> iterator) {
    subIterator = iterator;
    getNextPartial();
  }

  @Override
  public boolean hasNext() {
    if (partialIterator == null)
      return false;

    if (partialIterator.hasNext())
      return true;
    else if (subIterator.hasNext())
      return getNextPartial();

    return false;
  }

  @Override
  public OIdentifiable next() {
    if (!hasNext())
      throw new NoSuchElementException();

    return partialIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("OFlattenIterator.remove()");
  }

  protected boolean getNextPartial() {
    if (subIterator != null)
      while (subIterator.hasNext()) {
        final Collection<OIdentifiable> next = subIterator.next();
        if (next != null && !next.isEmpty()) {
          partialIterator = next.iterator();
          return true;
        }
      }

    return false;
  }

}
