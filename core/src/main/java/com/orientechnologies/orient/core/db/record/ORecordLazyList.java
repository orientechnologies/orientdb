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
import com.orientechnologies.common.collection.OLazyIteratorListWrapper;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes.
 * This avoid to call the makeDirty() by hand when the list is changed. It handles an internal
 * contentType to speed up some operations like conversion to/from record/links.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings({"serial"})
public class ORecordLazyList extends OTrackedList<OIdentifiable> implements ORecordLazyMultiValue {
  protected ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE contentType =
      MULTIVALUE_CONTENT_TYPE.EMPTY;
  protected boolean autoConvertToRecord = true;
  protected boolean ridOnly = false;

  public ORecordLazyList() {
    super(null);
  }

  public ORecordLazyList(final ORecordElement iSourceRecord) {
    super(iSourceRecord);
    if (iSourceRecord != null) {
      ORecordElement source = iSourceRecord;
      while (!(source instanceof ODocument)) {
        source = source.getOwner();
      }
      if (!((ODocument) source).isLazyLoad())
        // SET AS NON-LAZY LOAD THE COLLECTION TOO
        autoConvertToRecord = false;
    }
  }

  public ORecordLazyList(
      final ORecordElement iSourceRecord, final Collection<? extends OIdentifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) addAll(iOrigin);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    final Iterator it =
        (Iterator)
            (c instanceof ORecordLazyMultiValue
                ? ((ORecordLazyMultiValue) c).rawIterator()
                : c.iterator());

    while (it.hasNext()) {
      Object o = it.next();
      if (o == null) add(null);
      else if (o instanceof OIdentifiable) add((OIdentifiable) o);
      else OMultiValue.add(this, o);
    }

    return true;
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty();
  }

  /** @return iterator that just returns the elements without conversion. */
  public Iterator<OIdentifiable> rawIterator() {
    lazyLoad(false);
    final Iterator<OIdentifiable> subIterator =
        new OLazyIterator<OIdentifiable>() {
          private int pos = -1;

          public boolean hasNext() {
            return pos < size() - 1;
          }

          public OIdentifiable next() {
            return ORecordLazyList.this.rawGet(++pos);
          }

          public void remove() {
            ORecordLazyList.this.remove(pos);
          }

          public OIdentifiable update(final OIdentifiable iValue) {
            return ORecordLazyList.this.set(pos, iValue);
          }
        };
    return new OLazyRecordIterator(sourceRecord, subIterator, false);
  }

  public OIdentifiable rawGet(final int index) {
    lazyLoad(false);
    return super.get(index);
  }

  @Override
  public OLazyIterator<OIdentifiable> iterator() {
    lazyLoad(false);
    return new OLazyRecordIterator(
        sourceRecord,
        new OLazyIteratorListWrapper<OIdentifiable>(super.listIterator()),
        autoConvertToRecord);
  }

  @Override
  public ListIterator<OIdentifiable> listIterator() {
    lazyLoad(false);
    return super.listIterator();
  }

  @Override
  public ListIterator<OIdentifiable> listIterator(int index) {
    lazyLoad(false);
    return super.listIterator(index);
  }

  @Override
  public boolean contains(final Object o) {
    lazyLoad(false);
    return super.contains(o);
  }

  @Override
  public boolean add(OIdentifiable e) {
    preAdd(e);
    lazyLoad(true);
    return super.add(e);
  }

  @Override
  public void add(int index, OIdentifiable e) {
    preAdd(e);
    lazyLoad(true);
    super.add(index, e);
  }

  @Override
  public boolean addInternal(OIdentifiable e) {
    preAdd(e);
    return super.addInternal(e);
  }

  private void preAdd(OIdentifiable e) {
    if (e != null) {
      ORecordInternal.track(sourceRecord, e);
      if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
          && e.getIdentity().isPersistent()
          && (e instanceof ODocument && !((ODocument) e).isDirty()))
        // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
        e = e.getIdentity();
      else contentType = ORecordMultiValueHelper.updateContentType(contentType, e);
    }
  }

  @Override
  public OIdentifiable set(int index, OIdentifiable e) {
    lazyLoad(true);

    if (e != null) {
      ORecordInternal.track(sourceRecord, e);
      if (e != null)
        if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
            && e.getIdentity().isPersistent()
            && (e instanceof ODocument && !((ODocument) e).isDirty()))
          // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
          e = e.getIdentity();
        else contentType = ORecordMultiValueHelper.updateContentType(contentType, e);
    }
    return super.set(index, e);
  }

  @Override
  public OIdentifiable get(final int index) {
    lazyLoad(false);
    if (autoConvertToRecord) convertLink2Record(index);
    return super.get(index);
  }

  @Override
  public int indexOf(final Object o) {
    lazyLoad(false);
    return super.indexOf(o);
  }

  @Override
  public int lastIndexOf(final Object o) {
    lazyLoad(false);
    return super.lastIndexOf(o);
  }

  @Override
  public OIdentifiable remove(final int iIndex) {
    lazyLoad(true);
    return super.remove(iIndex);
  }

  @Override
  public boolean remove(final Object iElement) {
    if (iElement == null) {
      return clearDeletedRecords();
    }
    final boolean result;
    lazyLoad(true);
    result = super.remove(iElement);

    if (isEmpty()) contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;

    return result;
  }

  @Override
  public void clear() {
    lazyLoad(true);
    super.clear();
    contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;
  }

  @Override
  public int size() {
    lazyLoad(false);
    return super.size();
  }

  @Override
  public Object[] toArray() {
    convertLinks2Records();
    return super.toArray();
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    lazyLoad(false);
    convertLinks2Records();
    return super.toArray(a);
  }

  public void convertLinks2Records() {
    lazyLoad(false);
    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS || !autoConvertToRecord)
      // PRECONDITIONS
      return;

    for (int i = 0; i < size(); ++i) {
      try {
        convertLink2Record(i);
      } catch (ORecordNotFoundException ignore) {
        // LEAVE THE RID DIRTY
      }
    }

    contentType = MULTIVALUE_CONTENT_TYPE.ALL_RECORDS;
  }

  public boolean convertRecords2Links() {
    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || sourceRecord == null)
      // PRECONDITIONS
      return true;

    boolean allConverted = true;
    for (int i = 0; i < super.size(); ++i) {
      try {
        if (!convertRecord2Link(i)) allConverted = false;
      } catch (ORecordNotFoundException ignore) {
        // LEAVE THE RID DIRTY
      }
    }

    if (allConverted) contentType = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;

    return allConverted;
  }

  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  public void setAutoConvertToRecord(boolean convertToDocument) {
    this.autoConvertToRecord = convertToDocument;
  }

  @Override
  public String toString() {
    return ORecordMultiValueHelper.toString(this);
  }

  public ORecordLazyList copy(final ODocument iSourceRecord) {
    final ORecordLazyList copy = new ORecordLazyList(iSourceRecord);
    copy.contentType = contentType;
    copy.autoConvertToRecord = autoConvertToRecord;

    final int tot = super.size();
    for (int i = 0; i < tot; ++i) copy.add(rawGet(i));

    return copy;
  }

  public boolean lazyLoad(final boolean iInvalidateStream) {
    return true;
  }

  public boolean detach() {
    return convertRecords2Links();
  }

  /**
   * Convert the item requested from link to record.
   *
   * @param iIndex Position of the item to convert
   */
  private void convertLink2Record(final int iIndex) {
    if (ridOnly || !autoConvertToRecord)
      // PRECONDITIONS
      return;

    final OIdentifiable o = super.get(iIndex);

    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS && !o.getIdentity().isNew())
      // ALL RECORDS AND THE OBJECT IS NOT NEW, DO NOTHING
      return;

    if (o != null && o instanceof ORecordId) {
      final ORecordId rid = (ORecordId) o;

      try {
        ORecord record = rid.getRecord();
        if (record != null) {
          ORecordInternal.unTrack(sourceRecord, rid);
          ORecordInternal.track(sourceRecord, record);
        }
        super.set(iIndex, record);

      } catch (ORecordNotFoundException ignore) {
        // IGNORE THIS
      }
    }
  }

  /**
   * Convert the item requested from record to link.
   *
   * @param iIndex Position of the item to convert
   * @return <code>true</code> if conversion was successful.
   */
  private boolean convertRecord2Link(final int iIndex) {
    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
      // PRECONDITIONS
      return true;

    final Object o = super.get(iIndex);

    if (o != null
        && o instanceof OIdentifiable
        && ((OIdentifiable) o).getIdentity().isPersistent()) {
      if (o instanceof ORecord && !((ORecord) o).isDirty()) {
        try {
          super.setInternal(iIndex, ((ORecord) o).getIdentity());
          // CONVERTED
          return true;
        } catch (ORecordNotFoundException ignore) {
          // IGNORE THIS
        }
      } else if (o instanceof ORID)
        // ALREADY CONVERTED
        return true;
    }
    return false;
  }

  public boolean clearDeletedRecords() {
    boolean removed = false;
    Iterator<OIdentifiable> it = super.iterator();
    while (it.hasNext()) {
      OIdentifiable rec = it.next();
      if (!(rec instanceof ORecord) && rec.getRecord() == null) {
        it.remove();
        removed = true;
      }
    }
    return removed;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }
}
