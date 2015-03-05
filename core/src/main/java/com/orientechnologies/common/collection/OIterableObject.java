/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.common.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.util.OResettable;

/**
 * Allows to iterate over a single object
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OIterableObject<T> implements Iterable<T>, OResettable, Iterator<T> {

  private final T object;
  private boolean alreadyRead = false;

  public OIterableObject(T o) {
    object = o;
  }

  /**
   * Returns an iterator over a set of elements of type T.
   * 
   * @return an Iterator.
   */
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public void reset() {
    alreadyRead = false;
  }

  @Override
  public boolean hasNext() {
    return !alreadyRead;
  }

  @Override
  public T next() {
    if (!alreadyRead) {
      alreadyRead = true;
      return object;
    } else {
        throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
