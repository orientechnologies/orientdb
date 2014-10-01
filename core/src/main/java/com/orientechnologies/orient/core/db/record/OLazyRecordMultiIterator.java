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
package com.orientechnologies.orient.core.db.record;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of changes to the source record
 * avoiding to call setDirty() by hand. The main difference with OLazyRecordIterator is that this iterator handles multiple
 * iterators of collections as they are just one.
 * 
 * @see OLazyRecordIterator
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLazyRecordMultiIterator implements OLazyIterator<OIdentifiable>, OResettable {
  final private ORecord sourceRecord;
  final private Object[]   underlyingSources;
  final private Object[]   underlyingIterators;
  final private boolean    convertToRecord;
  private int              iteratorIndex = 0;

  public OLazyRecordMultiIterator(final ORecord iSourceRecord, final Object[] iIterators, final boolean iConvertToRecord) {
    this.sourceRecord = iSourceRecord;
    this.underlyingSources = iIterators;
    this.underlyingIterators = new Object[iIterators.length];
    this.convertToRecord = iConvertToRecord;
  }

  @Override
  public void reset() {
    iteratorIndex = 0;
    for (int i = 0; i < underlyingIterators.length; ++i)
      underlyingIterators[i] = null;
  }

  public OIdentifiable next() {
    if (!hasNext())
      throw new NoSuchElementException();

    final Iterator<OIdentifiable> underlying = getCurrentIterator();
    OIdentifiable value = underlying.next();

    if (value == null)
      return null;

    if (value instanceof ORecordId && convertToRecord) {
      value = ((ORecordId) value).getRecord();

      if (underlying instanceof OLazyIterator<?>)
        ((OLazyIterator<OIdentifiable>) underlying).update(value);
    }

    return value;
  }

  public boolean hasNext() {
    final Iterator<OIdentifiable> underlying = getCurrentIterator();
    boolean again = underlying.hasNext();

    while (!again && iteratorIndex < underlyingIterators.length - 1) {
      iteratorIndex++;
      again = getCurrentIterator().hasNext();
    }

    return again;
  }

  public OIdentifiable update(final OIdentifiable iValue) {
    final Iterator<OIdentifiable> underlying = getCurrentIterator();
    if (underlying instanceof OLazyIterator) {
      final OIdentifiable old = ((OLazyIterator<OIdentifiable>) underlying).update(iValue);
      if (sourceRecord != null && !old.equals(iValue))
        sourceRecord.setDirty();
      return old;
    } else
      throw new UnsupportedOperationException("Underlying iterator not supports lazy updates (Interface OLazyIterator");
  }

  public void remove() {
    final Iterator<OIdentifiable> underlying = getCurrentIterator();
    underlying.remove();
    if (sourceRecord != null)
      sourceRecord.setDirty();
  }

  @SuppressWarnings("unchecked")
  private Iterator<OIdentifiable> getCurrentIterator() {
    if (iteratorIndex > underlyingIterators.length)
      throw new NoSuchElementException();

    Object next = underlyingIterators[iteratorIndex];
    if (next == null) {
      // GET THE ITERATOR
      if (underlyingSources[iteratorIndex] instanceof OResettable) {
        // REUSE IT
        ((OResettable) underlyingSources[iteratorIndex]).reset();
        underlyingIterators[iteratorIndex] = underlyingSources[iteratorIndex];
      } else if (underlyingSources[iteratorIndex] instanceof Iterable<?>) {
        // CREATE A NEW ONE FROM THE COLLECTION
        underlyingIterators[iteratorIndex] = ((Iterable<?>) underlyingSources[iteratorIndex]).iterator();
      } else if (underlyingSources[iteratorIndex] instanceof Iterator<?>) {
        // COPY IT
        underlyingIterators[iteratorIndex] = underlyingSources[iteratorIndex];
      } else
        throw new IllegalStateException("Unsupported iteration source: " + underlyingSources[iteratorIndex]);
      
      next = underlyingIterators[iteratorIndex];
    }

    if (next instanceof Iterator<?>)
      return (Iterator<OIdentifiable>) next;

    return ((Collection<OIdentifiable>) next).iterator();
  }
}
