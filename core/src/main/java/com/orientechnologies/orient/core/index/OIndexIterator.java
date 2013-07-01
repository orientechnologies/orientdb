/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Iterator;

/**
 * Index iterator that locks while fetching.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexIterator<T> implements Iterator<T> {
  protected final OIndexMVRBTreeAbstract<?> index;
  protected Iterator<?>                     iterator;

  public OIndexIterator(final OIndexMVRBTreeAbstract<?> index, final Iterator<?> iIterator) {
    this.index = index;
    this.iterator = iIterator;
  }

  @Override
  public boolean hasNext() {
    index.acquireExclusiveLock();
    try {
      return iterator.hasNext();
    } finally {
      index.releaseExclusiveLock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() {
    index.acquireExclusiveLock();
    try {
      return (T) iterator.next();
    } finally {
      index.releaseExclusiveLock();
    }
  }

  @Override
  public void remove() {
    index.acquireExclusiveLock();
    try {
      iterator.remove();
    } finally {
      index.releaseExclusiveLock();
    }
  }
}
