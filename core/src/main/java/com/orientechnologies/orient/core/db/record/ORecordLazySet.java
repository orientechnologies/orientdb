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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lazy implementation of Set. Can be bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty()
 * by hand when the set is changed.
 * <p>
 * <b>Internals</b>:
 * <ul>
 * <li>stores new records in a separate IdentityHashMap to keep underlying list (delegate) always ordered and minimizing sort
 * operations</li>
 * <li></li>
 * </ul>
 * 
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordLazySet implements Set<OIdentifiable>, ORecordLazyMultiValue, ORecordElement, ORecordLazyListener {
	public static final ORecordLazySet						EMPTY_SET			= new ORecordLazySet();
	private static final Object										NEWMAP_VALUE	= new Object();
	protected final ORecordLazyList								delegate;
	protected IdentityHashMap<ORecord<?>, Object>	newItems;
	protected boolean															sorted				= true;

	public ORecordLazySet() {
		delegate = new ORecordLazyList().setListener(this);
	}

	public ORecordLazySet(final ORecordInternal<?> iSourceRecord) {
		delegate = new ORecordLazyList(iSourceRecord).setListener(this);
	}

	public ORecordLazySet(final ODocument iSourceRecord, final ORecordLazySet iSource) {
		delegate = iSource.delegate.copy(iSourceRecord).setListener(this);
		sorted = iSource.sorted;
		if (iSource.newItems != null)
			newItems = new IdentityHashMap<ORecord<?>, Object>(iSource.newItems);
	}

	public void onBeforeIdentityChanged(final ORID iRID) {
		delegate.onBeforeIdentityChanged(iRID);
	}

	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
		delegate.onAfterIdentityChanged(iRecord);
	}

	@SuppressWarnings("unchecked")
	public <RET> RET setDirty() {
		delegate.setDirty();
		return (RET) this;
	}

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		return delegate.setDatabase(iDatabase);
	}

	public Iterator<OIdentifiable> iterator() {
		if (hasNewItems()) {
			lazyLoad(false);
			return new OLazyRecordMultiIterator(delegate.sourceRecord,
					new Object[] { delegate.iterator(), newItems.keySet().iterator() }, delegate.autoConvertToRecord);
		}
		return delegate.iterator();
	}

	public Iterator<OIdentifiable> rawIterator() {
		if (hasNewItems()) {
			lazyLoad(false);
			return new OLazyRecordMultiIterator(delegate.sourceRecord, new Object[] { delegate.rawIterator(),
					newItems.keySet().iterator() }, false);
		}
		return delegate.rawIterator();
	}

	public Iterator<OIdentifiable> newItemsIterator() {
		if (hasNewItems())
			return new OLazyRecordIterator(delegate.sourceRecord, newItems.keySet().iterator(), false);

		return null;
	}

	public boolean convertRecords2Links() {
		savedAllNewItems();
		return delegate.convertRecords2Links();
	}

	public boolean isAutoConvertToRecord() {
		return delegate.isAutoConvertToRecord();
	}

	public void setAutoConvertToRecord(final boolean convertToRecord) {
		delegate.setAutoConvertToRecord(convertToRecord);
	}

	public int size() {
		int tot = delegate.size();
		if (newItems != null)
			tot += newItems.size();
		return tot;
	}

	public boolean isEmpty() {
		boolean empty = delegate.isEmpty();

		if (empty && newItems != null)
			empty = newItems.isEmpty();

		return empty;
	}

	public boolean contains(final Object o) {
		boolean found;

		final OIdentifiable obj = (OIdentifiable) o;

		if (OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean() && getStreamedContent() != null) {
			found = getStreamedContent().indexOf(obj.getIdentity().toString()) > -1;
		} else {
			lazyLoad(false);
			found = indexOf((OIdentifiable) o) > -1;
		}

		if (!found && hasNewItems())
			// SEARCH INSIDE NEW ITEMS MAP
			found = newItems.containsKey(o);

		return found;
	}

	public Object[] toArray() {
		Object[] result = delegate.toArray();

		if (newItems != null && !newItems.isEmpty()) {
			int start = result.length;
			result = Arrays.copyOf(result, start + newItems.size());

			for (ORecord<?> r : newItems.keySet()) {
				result[start++] = r;
			}
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public <T> T[] toArray(final T[] a) {
		T[] result = delegate.toArray(a);

		if (newItems != null && !newItems.isEmpty()) {
			int start = result.length;
			result = Arrays.copyOf(result, start + newItems.size());

			for (ORecord<?> r : newItems.keySet()) {
				result[start++] = (T) r;
			}
		}

		return result;
	}

	/**
	 * Adds the item in the underlying List preserving the order of the collection.
	 */
	public boolean add(final OIdentifiable e) {
		if (e.getIdentity().isNew()) {
			final ORecord<?> record = e.getRecord();

			// ADD IN TEMP LIST
			if (newItems == null)
				newItems = new IdentityHashMap<ORecord<?>, Object>();
			else if (newItems.containsKey(record))
				return false;
			newItems.put(record, NEWMAP_VALUE);
			setDirty();
			return true;
		} else if (OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean() && getStreamedContent() != null) {
			// FAST INSERT
			final String ridString = e.getIdentity().toString();
			final StringBuilder buffer = getStreamedContent();
			if (buffer.indexOf(ridString) < 0) {
				if (buffer.length() > 0)
					buffer.append(',');
				e.getIdentity().toString(buffer);
				setDirty();
				return true;
			}
			return false;
		} else {
			final int pos = indexOf(e);

			if (pos < 0) {
				// FOUND
				delegate.add(pos * -1 - 1, e);
				return true;
			}
			return false;
		}
	}

	/**
	 * Returns the position of the element in the set. Execute a binary search since the elements are always ordered.
	 * 
	 * @param iElement
	 *          element to find
	 * @return The position of the element if found, otherwise false
	 */
	public int indexOf(final OIdentifiable iElement) {
		if (delegate.isEmpty())
			return -1;

		final boolean prevConvert = delegate.isAutoConvertToRecord();
		if (prevConvert)
			delegate.setAutoConvertToRecord(false);

		final int pos = Collections.binarySearch(delegate, iElement);

		if (prevConvert)
			// RESET PREVIOUS SETTINGS
			delegate.setAutoConvertToRecord(true);

		return pos;
	}

	public boolean remove(final Object o) {
		if (OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.getValueAsBoolean() && getStreamedContent() != null) {
			// WORK ON STREAM
			if (delegate.remove(o))
				return true;
		} else {
			lazyLoad(true);
			final int pos = indexOf((OIdentifiable) o);
			if (pos > -1) {
				delegate.remove(pos);
				return true;
			}
		}

		if (hasNewItems()) {
			// SEARCH INSIDE NEW ITEMS MAP
			final boolean removed = newItems.remove(o) != null;
			if (newItems.size() == 0)
				// EARLY REMOVE THE MAP TO SAVE MEMORY
				newItems = null;

			if (removed)
				setDirty();

			return removed;
		}

		return false;
	}

	public boolean containsAll(final Collection<?> c) {
		lazyLoad(false);
		return delegate.containsAll(c);
	}

	@SuppressWarnings("unchecked")
	public boolean addAll(Collection<? extends OIdentifiable> c) {
		final Iterator<OIdentifiable> it = (Iterator<OIdentifiable>) (c instanceof ORecordLazyMultiValue ? ((ORecordLazyMultiValue) c)
				.rawIterator() : c.iterator());

		while (it.hasNext())
			add(it.next());

		return true;
	}

	public boolean retainAll(final Collection<?> c) {
		if (hasNewItems()) {
			final Collection<Object> v = newItems.values();
			v.retainAll(c);
			if (newItems.size() == 0)
				newItems = null;
		}
		return delegate.retainAll(c);
	}

	public boolean removeAll(final Collection<?> c) {
		if (hasNewItems()) {
			final Collection<Object> v = newItems.values();
			v.removeAll(c);
			if (newItems.size() == 0)
				newItems = null;
		}
		return delegate.removeAll(c);
	}

	public void clear() {
		delegate.clear();
		if (newItems != null) {
			newItems.clear();
			newItems = null;
		}
	}

	public byte getRecordType() {
		return delegate.getRecordType();
	}

	@Override
	public String toString() {
		final StringBuilder buffer = new StringBuilder(delegate.toString());
		if (hasNewItems()) {
			for (ORecord<?> item : newItems.keySet()) {
				if (buffer.length() > 2)
					buffer.insert(buffer.length() - 1, ", ");

				buffer.insert(buffer.length() - 1, item.toString());
			}
			return buffer.toString();
		}
		return buffer.toString();
	}

	public void sort() {
		if (!sorted && !delegate.isEmpty()) {
			final boolean prevConvert = delegate.isAutoConvertToRecord();
			if (prevConvert)
				delegate.setAutoConvertToRecord(false);

			delegate.marshalling = true;
			Collections.sort(delegate);
			delegate.marshalling = false;

			if (prevConvert)
				// RESET PREVIOUS SETTINGS
				delegate.setAutoConvertToRecord(true);

			sorted = true;
		}
	}

	public ORecordLazySet setStreamedContent(final StringBuilder iStream) {
		delegate.setStreamedContent(iStream);
		return this;
	}

	public StringBuilder getStreamedContent() {
		return delegate.getStreamedContent();
	}

	public boolean lazyLoad(final boolean iNotIdempotent) {
		if (delegate.lazyLoad(iNotIdempotent)) {
			sort();
			return true;
		}
		return false;
	}

	public void convertLinks2Records() {
		delegate.convertLinks2Records();
	}

	private boolean hasNewItems() {
		return newItems != null && !newItems.isEmpty();
	}

	public void savedAllNewItems() {
		if (hasNewItems()) {
			for (ORecord<?> record : newItems.keySet()) {
				if (record.getIdentity().isNew() || getStreamedContent() == null
						|| getStreamedContent().indexOf(record.getIdentity().toString()) == -1)
					// NEW ITEM OR NOT CONTENT IN STREAMED BUFFER
					add(record.getIdentity());
			}

			newItems.clear();
			newItems = null;
		}
	}

	public ORecordLazySet copy(final ODocument iSourceRecord) {
		return new ORecordLazySet(iSourceRecord, this);
	}

	public void onLazyLoad() {
		sorted = false;
		sort();
	}

	public STATUS getInternalStatus() {
		return delegate.getInternalStatus();
	}

	public void setInternalStatus(final STATUS iStatus) {
		delegate.setInternalStatus(iStatus);
	}

	public boolean isRidOnly() {
		return delegate.isRidOnly();
	}

	public ORecordLazySet setRidOnly(final boolean ridOnly) {
		delegate.setRidOnly(ridOnly);
		return this;
	}

	public boolean detach() {
		return convertRecords2Links();
	}
}
