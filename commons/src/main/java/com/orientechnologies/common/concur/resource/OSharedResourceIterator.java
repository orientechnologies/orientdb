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
package com.orientechnologies.common.concur.resource;

import java.util.Iterator;

import com.orientechnologies.common.util.OResettable;

/**
 * Iterator against a shared resource: locks the resource while fetching.
 * 
 * @author Luca Garulli
 * 
 */
public class OSharedResourceIterator<T> implements Iterator<T>, OResettable {
  protected final OSharedResourceAdaptiveExternal resource;
  protected Iterator<?>                           iterator;

  public OSharedResourceIterator(final OSharedResourceAdaptiveExternal iResource, final Iterator<?> iIterator) {
    this.resource = iResource;
    this.iterator = iIterator;
  }

  @Override
  public boolean hasNext() {
    resource.acquireExclusiveLock();
    try {
      return iterator.hasNext();
    } finally {
      resource.releaseExclusiveLock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() {
    resource.acquireExclusiveLock();
    try {
      return (T) iterator.next();
    } finally {
      resource.releaseExclusiveLock();
    }
  }

  @Override
  public void remove() {
    resource.acquireExclusiveLock();
    try {
      iterator.remove();
    } finally {
      resource.releaseExclusiveLock();
    }
  }

  @Override
  public void reset() {
    if( !( iterator instanceof OResettable) )
      return;
    
    resource.acquireExclusiveLock();
    try {
      ((OResettable) iterator).reset();
    } finally {
      resource.releaseExclusiveLock();
    }
  }
}
