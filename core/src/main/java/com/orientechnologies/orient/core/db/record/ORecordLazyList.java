/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.db.record;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_STATUS;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordLazyList extends ORecordTrackedList implements ORecordLazyMultiValue {
	protected final byte																recordType;
	protected ODatabaseRecord														database;
	protected ORecordMultiValueHelper.MULTIVALUE_STATUS	status							= MULTIVALUE_STATUS.EMPTY;
	protected boolean																		loaded							= true;
	protected String																		stream;
	protected boolean																		autoConvertToRecord	= true;

	public ORecordLazyList(final ODatabaseRecord iDatabase) {
		super(null);
		this.database = iDatabase;
		this.recordType = ODocument.RECORD_TYPE;
	}

	public ORecordLazyList(final ORecordInternal<?> iSourceRecord) {
		super(iSourceRecord);
		this.database = iSourceRecord.getDatabase();
		this.recordType = iSourceRecord.getRecordType();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean addAll(Collection<? extends OIdentifiable> c) {
		final Iterator<OIdentifiable> it = (Iterator<OIdentifiable>) (c instanceof ORecordLazyMultiValue ? ((ORecordLazyMultiValue) c)
				.rawIterator() : c.iterator());

		while (it.hasNext())
			add(it.next());

		return true;
	}

	@Override
	public boolean isEmpty() {
		if (loaded)
			return super.isEmpty();
		else
			// AVOID TO LAZY LOAD IT, JUST CHECK IF STREAM IS EMPTY OR NULL
			return stream.length() == 0;
	}

	/**
	 * Returns a iterator that just returns the elements without convertion.
	 * 
	 * @return
	 */
	public Iterator<OIdentifiable> rawIterator() {
		lazyLoad(false);
		final Iterator<OIdentifiable> subIterator = new Iterator<OIdentifiable>() {
			private int	pos	= 0;

			public boolean hasNext() {
				return pos < size();
			}

			public OIdentifiable next() {
				return ORecordLazyList.this.rawGet(pos++);
			}

			public void remove() {
				ORecordLazyList.this.remove(pos);
			}
		};
		return new OLazyRecordIterator(sourceRecord, database, recordType, subIterator, false);
	}

	public OIdentifiable rawGet(final int index) {
		lazyLoad(false);
		return super.get(index);
	}

	@Override
	public Iterator<OIdentifiable> iterator() {
		lazyLoad(false);
		return new OLazyRecordIterator(sourceRecord, database, recordType, super.iterator(), autoConvertToRecord);
	}

	@Override
	public boolean contains(final Object o) {
		lazyLoad(false);
		return super.contains(o);
	}

	@Override
	public boolean add(OIdentifiable e) {
		lazyLoad(true);

		if (status == MULTIVALUE_STATUS.ALL_RIDS && e instanceof ORecord<?> && !((ORecord<?>) e).getIdentity().isNew())
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = ((ORecord<?>) e).getIdentity();
		else
			status = ORecordMultiValueHelper.getStatus(status, e);

		return super.add(e);
	}

	@Override
	public void add(int index, OIdentifiable e) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS && e instanceof ORecord<?>)
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = ((ORecord<?>) e).getIdentity();
		else
			status = ORecordMultiValueHelper.getStatus(status, e);

		lazyLoad(true);
		super.add(index, e);
	}

	@Override
	public OIdentifiable set(int index, OIdentifiable e) {
		lazyLoad(true);
		status = ORecordMultiValueHelper.getStatus(status, e);
		return super.set(index, e);
	}

	@Override
	public OIdentifiable get(final int index) {
		lazyLoad(false);
		if (autoConvertToRecord)
			convertLink2Record(index);
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
	public boolean remove(Object o) {
		lazyLoad(true);
		final boolean result = super.remove(o);
		if (size() == 0)
			status = MULTIVALUE_STATUS.EMPTY;
		return result;
	}

	@Override
	public void clear() {
		super.clear();
		status = MULTIVALUE_STATUS.EMPTY;
		stream = null;
		loaded = true;
	}

	@Override
	public int size() {
		lazyLoad(false);
		return super.size();
	}

	@Override
	public Object[] toArray() {
		lazyLoad(false);
		convertLinks2Records();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		lazyLoad(false);
		convertLinks2Records();
		return super.toArray(a);
	}

	public boolean isLoaded() {
		return loaded;
	}

	public void convertLinks2Records() {
		lazyLoad(false);
		if (status == MULTIVALUE_STATUS.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		for (int i = 0; i < size(); ++i) {
			try {
				convertLink2Record(i);
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		status = MULTIVALUE_STATUS.ALL_RECORDS;
	}

	public boolean convertRecords2Links() {
		if (status == MULTIVALUE_STATUS.ALL_RIDS || sourceRecord == null || database == null)
			// PRECONDITIONS
			return true;

		boolean allConverted = true;
		for (int i = 0; i < size(); ++i) {
			try {
				if (!convertRecord2Link(i))
					allConverted = false;
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		if (allConverted)
			status = MULTIVALUE_STATUS.ALL_RIDS;

		return allConverted;
	}

	/**
	 * Convert the item requested from link to record.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convertLink2Record(final int iIndex) {
		lazyLoad(false);
		if (status == MULTIVALUE_STATUS.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		final OIdentifiable o = super.get(iIndex);

		if (o != null && o instanceof ORecordId) {
			final ORecordId rid = (ORecordId) o;

			try {
				final ORecordInternal<?> record = database.load(rid);
				set(iIndex, (OIdentifiable) record);
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
	}

	/**
	 * Convert the item requested from record to link.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private boolean convertRecord2Link(final int iIndex) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS || database == null)
			// PRECONDITIONS
			return true;

		final Object o = super.get(iIndex);

		if (o != null) {
			if (o instanceof ORecord<?> && !((ORecord<?>) o).isDirty())
				try {
					super.set(iIndex, ((ORecord<?>) o).getIdentity());
					// CONVERTED
					return true;
				} catch (ORecordNotFoundException e) {
					// IGNORE THIS
				}
			else if (o instanceof ORID)
				// ALREADY CONVERTED
				return true;
		}
		return false;
	}

	public boolean isAutoConvertToRecord() {
		return autoConvertToRecord;
	}

	public void setAutoConvertToRecord(boolean convertToDocument) {
		this.autoConvertToRecord = convertToDocument;
	}

	@Override
	public String toString() {
		if (loaded)
			return ORecordMultiValueHelper.toString(this);
		else {
			return "NOT LOADED: " + stream;
		}
	}

	public byte getRecordType() {
		return recordType;
	}

	@Override
	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		if (database != iDatabase) {
			database = iDatabase;
			super.setDatabase(iDatabase);
			return true;
		}
		return false;
	}

	public ORecordLazyList setStreamedContent(final String iStream) {
		// if (iStream != null && iStream.length() == 0)
		// stream = null;
		// else {
		// // CREATE A COPY TO FREE ORIGINAL BUFFER
		// stream = new String(iStream);
		// loaded = false;
		// }

		return this;
	}

	public String getStreamedContent() {
		return stream;
	}

	protected boolean lazyLoad(final boolean iInvalidateStream) {
		if (loaded)
			return false;

		if (super.isEmpty()) {
			final List<String> items = OStringSerializerHelper.smartSplit(stream, OStringSerializerHelper.RECORD_SEPARATOR);

			for (String item : items) {
				if (item != null && item.length() > 0)
					item = item.substring(1);

				if (item.length() == 0)
					continue;

				super.add(new ORecordId(item));
			}
		}

		if (iInvalidateStream)
			stream = null;

		loaded = true;

		return true;
	}
}
