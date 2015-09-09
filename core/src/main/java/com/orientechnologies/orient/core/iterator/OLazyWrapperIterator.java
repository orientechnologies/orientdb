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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that created wrapped objects during browsing.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public abstract class OLazyWrapperIterator<T> implements OAutoConvertToRecord, Iterator<T>, Iterable<T>, OResettable, OSizeable {
  protected final Iterator<?> iterator;
  protected OIdentifiable     nextRecord;
  protected T                 nextElement;
  protected final int         size;                      // -1 = UNKNOWN
  protected boolean           autoConvertToRecord = true;
  protected Object            multiValue;

  public OLazyWrapperIterator(final Iterator<?> iterator) {
    this.iterator = iterator;
    this.size = -1;
  }

  public OLazyWrapperIterator(final Iterator<?> iterator, final int iSize, final Object iOriginalValue) {
    this.iterator = iterator;
    this.size = iSize;
    this.multiValue = iOriginalValue;
  }

  public abstract boolean filter(T iObject);

  public abstract boolean canUseMultiValueDirectly();

  public abstract T createGraphElement(Object iObject);

  public OIdentifiable getGraphElementRecord(final Object iObject) {
    return (OIdentifiable) iObject;
  }

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
    if (autoConvertToRecord) {
      // ACT ON WRAPPER
      while (nextElement == null && iterator.hasNext()) {
        nextElement = createGraphElement(iterator.next());
        if (nextElement != null && !filter(nextElement))
          nextElement = null;
      }

      return nextElement != null;
    }

    // ACT ON RECORDS (FASTER & LIGHTER)
    while (nextRecord == null && iterator.hasNext()) {
      nextRecord = getGraphElementRecord(iterator.next());
    }

    return nextRecord != null;
  }

  @Override
  public T next() {
    if (hasNext())
      if (autoConvertToRecord)
        // ACT ON WRAPPER
        try {
          return nextElement;
        } finally {
          nextElement = null;
        }
      else
        // ACT ON RECORDS (FASTER & LIGHTER)
        try {
          return (T) nextRecord;
        } finally {
          nextRecord = null;
        }

    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void setAutoConvertToRecord(final boolean convertToRecord) {
    autoConvertToRecord = convertToRecord;
    if (iterator instanceof OAutoConvertToRecord)
      ((OAutoConvertToRecord) iterator).setAutoConvertToRecord(autoConvertToRecord);
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  public Object getMultiValue() {
    return multiValue;
  }
}
