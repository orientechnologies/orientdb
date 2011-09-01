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
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the list is changed. It handles an internal contentType to speed up some operations like conversion
 * to/from record/links.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordLazyList extends ORecordTrackedList implements ORecordLazyMultiValue {
	protected ORecordLazyListener															listener;
	protected final byte																			recordType;
	protected ODatabaseRecord																	database;
	protected ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE	contentType					= MULTIVALUE_CONTENT_TYPE.EMPTY;
	protected StringBuilder																		stream;
	protected boolean																					autoConvertToRecord	= true;
	protected boolean																					marshalling					= false;
	protected boolean																					ridOnly							= false;

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
		if (stream == null)
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
		return new OLazyRecordIterator(sourceRecord, ODatabaseRecordThreadLocal.INSTANCE.get(), recordType, super.iterator(),
				autoConvertToRecord);
	}

	@Override
	public boolean contains(final Object o) {
		if (OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean() && getStreamedContent() != null)
			return getStreamedContent().indexOf(((OIdentifiable) o).getIdentity().toString()) > -1;

		lazyLoad(false);
		return super.contains(o);
	}

	@Override
	public boolean add(OIdentifiable e) {
		if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || OGlobalConfiguration.LAZYSET_WORK_ON_STREAM
				.getValueAsBoolean()) && !e.getIdentity().isNew() && (e instanceof ODocument && !((ODocument) e).isDirty()))
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = e.getIdentity();
		else
			contentType = ORecordMultiValueHelper.updateContentType(contentType, e);

		lazyLoad(true);
		return super.add(e);
	}

	@Override
	public void add(int index, OIdentifiable e) {
		if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || OGlobalConfiguration.LAZYSET_WORK_ON_STREAM
				.getValueAsBoolean()) && !e.getIdentity().isNew() && (e instanceof ODocument && !((ODocument) e).isDirty()))
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = e.getIdentity();
		else
			contentType = ORecordMultiValueHelper.updateContentType(contentType, e);

		lazyLoad(true);
		super.add(index, e);
	}

	@Override
	public OIdentifiable set(int index, OIdentifiable e) {
		lazyLoad(true);

		if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || OGlobalConfiguration.LAZYSET_WORK_ON_STREAM
				.getValueAsBoolean()) && !e.getIdentity().isNew() && (e instanceof ODocument && !((ODocument) e).isDirty()))
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = e.getIdentity();
		else
			contentType = ORecordMultiValueHelper.updateContentType(contentType, e);

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
	public OIdentifiable remove(final int iIndex) {
		lazyLoad(true);
		return super.remove(iIndex);
	}

	@Override
	public boolean remove(final Object iElement) {
		final boolean result;
		if (OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean() && getStreamedContent() != null) {
			// WORK ON STREAM
			final StringBuilder stream = getStreamedContent();
			final String rid = ((OIdentifiable) iElement).getIdentity().toString();
			int pos = stream.indexOf(rid);
			if (pos > -1) {
				setDirty();
				// FOUND: REMOVE IT DIRECTLY FROM STREAM
				if (pos > 0)
					pos--;
				stream.delete(pos, pos + rid.length() + 1);
				if (stream.length() == 0)
					setStreamedContent(null);
				result = true;
			} else
				result = false;
		} else {
			lazyLoad(true);
			result = super.remove(iElement);
		}

		if (isEmpty())
			contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;

		return result;
	}

	@Override
	public void clear() {
		super.clear();
		contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;
		stream = null;
	}

	@Override
	public int size() {
		lazyLoad(false);
		return super.size();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <RET> RET setDirty() {
		if (!marshalling)
			return (RET) super.setDirty();
		return (RET) this;
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
		if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		for (int i = 0; i < size(); ++i) {
			try {
				convertLink2Record(i);
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		contentType = MULTIVALUE_CONTENT_TYPE.ALL_RECORDS;
	}

	public boolean convertRecords2Links() {
		if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || sourceRecord == null || database == null)
			// PRECONDITIONS
			return true;

		boolean allConverted = true;
		for (int i = 0; i < super.size(); ++i) {
			try {
				if (!convertRecord2Link(i))
					allConverted = false;
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		if (allConverted)
			contentType = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;

		return allConverted;
	}

	/**
	 * Convert the item requested from link to record.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convertLink2Record(final int iIndex) {
		if (ridOnly || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		final OIdentifiable o = super.get(iIndex);

		if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS && !o.getIdentity().isNew())
			// ALL RECORDS AND THE OBJECT IS NOT NEW, DO NOTHING
			return;

		if (o != null && o instanceof ORecordId) {
			final ORecordId rid = (ORecordId) o;

			marshalling = true;
			try {
				final ORecordInternal<?> record = database.load(rid);

				super.set(iIndex, (OIdentifiable) record);

			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			} finally {
				marshalling = false;
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
		if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || database == null)
			// PRECONDITIONS
			return true;

		final Object o = super.get(iIndex);

		if (o != null) {
			if (o instanceof ORecord<?> && !((ORecord<?>) o).isDirty()) {
				marshalling = true;
				try {
					super.set(iIndex, ((ORecord<?>) o).getIdentity());
					// CONVERTED
					return true;
				} catch (ORecordNotFoundException e) {
					// IGNORE THIS
				} finally {
					marshalling = false;
				}
			} else if (o instanceof ORID)
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
		if (stream == null)
			return ORecordMultiValueHelper.toString(this);
		else {
			return "[NOT LOADED: " + stream + ']';
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

	public ORecordLazyList copy(final ODocument iSourceRecord) {
		final ORecordLazyList copy = new ORecordLazyList(iSourceRecord);
		copy.database = database;
		copy.contentType = contentType;
		copy.stream = stream;
		copy.autoConvertToRecord = autoConvertToRecord;

		final int tot = super.size();
		for (int i = 0; i < tot; ++i)
			copy.rawAdd(rawGet(i));

		return copy;
	}

	public Iterator<OIdentifiable> newItemsIterator() {
		return null;
	}

	public ORecordLazyList setStreamedContent(final StringBuilder iStream) {
		if (iStream == null || iStream.length() == 0)
			stream = null;
		else {
			// CREATE A COPY TO FREE ORIGINAL BUFFER
			stream = iStream;
			final int prevModCount = modCount;
			reset();
			modCount = prevModCount;
		}

		contentType = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;
		return this;
	}

	public StringBuilder getStreamedContent() {
		return stream;
	}

	public ORecordLazyListener getListener() {
		return listener;
	}

	public ORecordLazyList setListener(final ORecordLazyListener listener) {
		this.listener = listener;
		return this;
	}

	public boolean lazyLoad(final boolean iInvalidateStream) {
		if (stream == null)
			return false;

		marshalling = true;
		int currentModCount = modCount;
		final List<String> items = OStringSerializerHelper.smartSplit(stream.toString(), OStringSerializerHelper.RECORD_SEPARATOR);

		for (String item : items) {
			if (item.length() == 0)
				continue;

			super.rawAdd(new ORecordId(item));
		}

		modCount = currentModCount;
		marshalling = false;

		// if (iInvalidateStream)
		stream = null;
		contentType = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;

		if (listener != null)
			listener.onLazyLoad();

		return true;
	}

	public boolean isRidOnly() {
		return ridOnly;
	}

	public ORecordLazyList setRidOnly(boolean ridOnly) {
		this.ridOnly = ridOnly;
		return this;
	}
}
