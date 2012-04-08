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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.WeakHashMap;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class OTrackedSet<T> extends HashSet<T> implements ORecordElement, OTrackedMultiValue<T, T>, Serializable {
	protected final ORecord<?>										sourceRecord;
	private STATUS																status					= STATUS.NOT_LOADED;
	private transient Set<OMultiValueChangeListener<T, T>>	changeListeners	= Collections
																																		.newSetFromMap(new WeakHashMap<OMultiValueChangeListener<T, T>, Boolean>());
	protected Class<?>														genericClass;

	public OTrackedSet(final ORecord<?> iRecord, final Collection<? extends T> iOrigin, final Class<?> cls) {
		this(iRecord);
		genericClass = cls;
		if (iOrigin != null && !iOrigin.isEmpty())
			addAll(iOrigin);
	}

	public OTrackedSet(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
	}

	public boolean add(final T e) {
		if (super.add(e)) {
			fireCollectionChangedEvent(new OMultiValueChangeEvent<T, T>(OMultiValueChangeEvent.OChangeType.ADD, e, e));
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(final Object o) {
		if (super.remove(o)) {
			fireCollectionChangedEvent(new OMultiValueChangeEvent<T, T>(OMultiValueChangeEvent.OChangeType.REMOVE, (T) o, null, (T) o));
			return true;
		}
		return false;
	}

	@Override
	public void clear() {
		final Set<T> origValues;
		if (changeListeners.isEmpty())
			origValues = null;
		else
			origValues = new HashSet<T>(this);

		super.clear();

		if (origValues != null) {
			for (final T item : origValues)
				fireCollectionChangedEvent(new OMultiValueChangeEvent<T, T>(OMultiValueChangeEvent.OChangeType.REMOVE, item, null, item));
		} else
			setDirty();
	}

	@SuppressWarnings("unchecked")
	public OTrackedSet<T> setDirty() {
		if (status != STATUS.UNMARSHALLING && sourceRecord != null && !sourceRecord.isDirty())
			sourceRecord.setDirty();
		return this;
	}

	public void onBeforeIdentityChanged(ORID iRID) {
	}

	public void onAfterIdentityChanged(ORecord<?> iRecord) {
	}

	public STATUS getInternalStatus() {
		return status;
	}

	public void setInternalStatus(final STATUS iStatus) {
		status = iStatus;
	}

	public void addChangeListener(final OMultiValueChangeListener<T, T> changeListener) {
		changeListeners.add(changeListener);
	}

	public void removeRecordChangeListener(final OMultiValueChangeListener<T, T> changeListener) {
		changeListeners.remove(changeListener);
	}

	public Set<T> returnOriginalState(final List<OMultiValueChangeEvent<T, T>> multiValueChangeEvents) {
		final Set<T> reverted = new HashSet<T>(this);

		final ListIterator<OMultiValueChangeEvent<T, T>> listIterator = multiValueChangeEvents.listIterator(multiValueChangeEvents
				.size());

		while (listIterator.hasPrevious()) {
			final OMultiValueChangeEvent<T, T> event = listIterator.previous();
			switch (event.getChangeType()) {
			case ADD:
				reverted.remove(event.getKey());
				break;
			case REMOVE:
				reverted.add(event.getOldValue());
				break;
			default:
				throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
			}
		}

		return reverted;
	}

	protected void fireCollectionChangedEvent(final OMultiValueChangeEvent<T, T> event) {
		if (status == STATUS.UNMARSHALLING)
			return;

		setDirty();
		for (final OMultiValueChangeListener<T, T> changeListener : changeListeners) {
			if (changeListener != null)
				changeListener.onAfterRecordChanged(event);
		}
	}

	public Class<?> getGenericClass() {
		return genericClass;
	}

	public void setGenericClass(Class<?> genericClass) {
		this.genericClass = genericClass;
	}
}
