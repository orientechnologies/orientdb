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

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;

/**
 * Iterator that created wrapped objects during browsing.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public abstract class OLazyWrapperIterator<T> implements Iterator<T>, Iterable<T>, OResettable, OSizeable {
  protected final Iterator<?> iterator;
  protected T                 nextElement;
  protected final int         size;       // -1 = UNKNOWN

  public OLazyWrapperIterator(final Iterator<?> iterator) {
    this.iterator = iterator;
    this.size = -1;
  }

  public OLazyWrapperIterator(final Iterator<?> iterator, final int iSize) {
    this.iterator = iterator;
    this.size = iSize;
  }

  public abstract boolean filter(T iObject);

  public abstract T createWrapper(Object iObject);

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  public int size() {
    if (size > -1)
      return size;

    if (iterator instanceof OSizeable)
      return ((OSizeable) iterator).size();

    return 0;
  }

  @Override
  public void reset() {
    if (iterator instanceof OResettable)
      // RESET IT FOR MULTIPLE ITERATIONS
      ((OResettable) iterator).reset();
    nextElement = null;
  }

  @Override
  public boolean hasNext() {
    while (nextElement == null && iterator.hasNext()) {
      nextElement = createWrapper(iterator.next());
      if (nextElement != null && !filter(nextElement))
        nextElement = null;
    }

    return nextElement != null;
  }

  @Override
  public T next() {
    if (hasNext())
      try {
        return nextElement;
      } finally {
        nextElement = null;
      }

    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    iterator.remove();
  }
}
