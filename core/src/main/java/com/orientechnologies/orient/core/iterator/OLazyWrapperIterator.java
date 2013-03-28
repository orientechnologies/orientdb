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

import com.orientechnologies.common.util.OResettable;

/**
 * Iterator that created wrapped objects during browsing.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public abstract class OLazyWrapperIterator<T> implements Iterator<T>, Iterable<T>, OResettable {
  protected final Iterator<?> iterator;
  protected final Object      additionalData;

  public OLazyWrapperIterator(final Iterator<?> iterator) {
    this.iterator = iterator;
    this.additionalData = null;
  }

  public OLazyWrapperIterator(final Iterator<?> iterator, final Object iAdditionalData) {
    this.iterator = iterator;
    this.additionalData = iAdditionalData;
  }

  public abstract T createWrapper(Object iObject);

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  @Override
  public void reset() {
    if (iterator instanceof OResettable)
      // RESET IT FOR MULTIPLE ITERATIONS
      ((OResettable) iterator).reset();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return createWrapper(iterator.next());
  }

  @Override
  public void remove() {
    iterator.remove();
  }
}
