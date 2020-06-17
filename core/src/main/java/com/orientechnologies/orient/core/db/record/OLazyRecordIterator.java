/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import java.util.Iterator;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of
 * changes to the source record avoiding to call setDirty() by hand.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OLazyRecordIterator implements OLazyIterator<OIdentifiable>, OResettable {
  private final ORecordElement sourceRecord;
  private final Iterable<? extends OIdentifiable> source;
  private Iterator<? extends OIdentifiable> underlying;
  private final boolean autoConvert2Record;

  public OLazyRecordIterator(
      final Iterator<? extends OIdentifiable> iIterator, final boolean iConvertToRecord) {
    this.sourceRecord = null;
    this.underlying = iIterator;
    this.autoConvert2Record = iConvertToRecord;
    this.source = null;
  }

  public OLazyRecordIterator(
      final ORecordElement iSourceRecord,
      final Iterator<? extends OIdentifiable> iIterator,
      final boolean iConvertToRecord) {
    this.sourceRecord = iSourceRecord;
    this.underlying = iIterator;
    this.autoConvert2Record = iConvertToRecord;
    this.source = null;
  }

  public OLazyRecordIterator(
      final Iterable<? extends OIdentifiable> iSource, final boolean iConvertToRecord) {
    this.sourceRecord = null;
    this.autoConvert2Record = iConvertToRecord;
    this.source = iSource;
    this.underlying = iSource.iterator();
  }

  @SuppressWarnings("unchecked")
  public OIdentifiable next() {
    OIdentifiable value = underlying.next();

    if (value == null) return null;

    if (value instanceof ORecordId
        && autoConvert2Record
        && ODatabaseRecordThreadLocal.instance().isDefined()) {
      try {
        final ORecord rec = ((ORecordId) value).getRecord();
        if (sourceRecord != null && rec != null) ORecordInternal.track(sourceRecord, rec);
        if (underlying instanceof OLazyIterator<?>)
          ((OLazyIterator<OIdentifiable>) underlying).update(rec);
        value = rec;
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on iterating record collection", e);
        value = null;
      }
    }

    return value;
  }

  public boolean hasNext() {
    return underlying.hasNext();
  }

  @SuppressWarnings("unchecked")
  public OIdentifiable update(final OIdentifiable iValue) {
    if (underlying instanceof OLazyIterator) {
      final OIdentifiable old = ((OLazyIterator<OIdentifiable>) underlying).update(iValue);
      if (sourceRecord != null && !old.equals(iValue)) sourceRecord.setDirty();
      return old;
    } else
      throw new UnsupportedOperationException(
          "Underlying iterator not supports lazy updates (Interface OLazyIterator");
  }

  public void remove() {
    underlying.remove();
    if (sourceRecord != null) sourceRecord.setDirty();
  }

  @Override
  public void reset() {
    if (underlying instanceof OResettable) ((OResettable) underlying).reset();
    else if (source != null) {
      underlying = source.iterator();
    }
  }
}
