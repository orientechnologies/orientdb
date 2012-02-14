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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.WeakHashMap;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes for literal types. This avoid to call the
 * makeDirty() by hand when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class OTrackedList<T> extends ArrayList<T> implements ORecordElement, OTrackedMultiValue<Integer, T>, Serializable {
	protected final ORecord<?>														sourceRecord;
	private STATUS																				status					= STATUS.NOT_LOADED;
	protected Set<OMultiValueChangeListener<Integer, T>>	changeListeners	= Collections
																																						.newSetFromMap(new WeakHashMap<OMultiValueChangeListener<Integer, T>, Boolean>());
	protected Class<?>																		genericClass;

	public OTrackedList(final ORecord<?> iRecord, final Collection<? extends T> iOrigin, final Class<?> iGenericClass) {
		this(iRecord);
		genericClass = iGenericClass;
		if (iOrigin != null && !iOrigin.isEmpty())
			addAll(iOrigin);
	}

	public OTrackedList(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
	}

	@Override
	public boolean add(T element) {
		final boolean result = super.add(element);

		if (result)
			fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.ADD, super.size() - 1,
					element));

		return result;
	}

	@Override
	public boolean addAll(final Collection<? extends T> c) {
		for (T o : c) {
			add(o);
		}
		return true;
	}

	@Override
	public void add(int index, T element) {
		super.add(index, element);
		fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.ADD, index, element));
	}

	@Override
	public T set(int index, T element) {
		final T oldValue = super.set(index, element);

		fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.UPDATE, index, element,
				oldValue));
		return oldValue;
	}

	@Override
	public T remove(int index) {
		final T oldValue = super.remove(index);
		fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.REMOVE, index, null,
				oldValue));
		return oldValue;
	}

	@Override
	public boolean remove(Object o) {
		final int index = indexOf(o);
		if (index >= 0) {
			remove(index);
			return true;
		}
		return false;
	}

	@Override
	public void clear() {
		final List<T> origValues;
		if (changeListeners.isEmpty())
			origValues = null;
		else
			origValues = new ArrayList<T>(this);

		super.clear();
		if (origValues != null)
			for (int i = origValues.size() - 1; i >= 0; i--) {
				fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.REMOVE, i, null,
						origValues.get(i)));
			}
		else
			setDirty();
	}

	public void reset() {
		super.clear();
	}

	@SuppressWarnings("unchecked")
	public <RET> RET setDirty() {
		if (status != STATUS.UNMARSHALLING && sourceRecord != null && !sourceRecord.isDirty())
			sourceRecord.setDirty();
		return (RET) this;
	}

	public void onBeforeIdentityChanged(ORID iRID) {
	}

	public void onAfterIdentityChanged(ORecord<?> iRecord) {
	}

	public void addChangeListener(final OMultiValueChangeListener<Integer, T> changeListener) {
		changeListeners.add(changeListener);
	}

	public void removeRecordChangeListener(final OMultiValueChangeListener<Integer, T> changeListener) {
		changeListeners.remove(changeListener);
	}

	public List<T> returnOriginalState(final List<OMultiValueChangeEvent<Integer, T>> multiValueChangeEvents) {
		final List<T> reverted = new ArrayList<T>(this);

		final ListIterator<OMultiValueChangeEvent<Integer, T>> listIterator = multiValueChangeEvents
				.listIterator(multiValueChangeEvents.size());

		while (listIterator.hasPrevious()) {
			final OMultiValueChangeEvent<Integer, T> event = listIterator.previous();
			switch (event.getChangeType()) {
			case ADD:
				reverted.remove(event.getKey().intValue());
				break;
			case REMOVE:
				reverted.add(event.getKey(), event.getOldValue());
				break;
			case UPDATE:
				reverted.set(event.getKey(), event.getOldValue());
				break;
			default:
				throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
			}
		}

		return reverted;
	}

	protected void fireCollectionChangedEvent(final OMultiValueChangeEvent<Integer, T> event) {
		if (status == STATUS.UNMARSHALLING)
			return;

		setDirty();
		for (final OMultiValueChangeListener<Integer, T> changeListener : changeListeners) {
			if (changeListener != null)
				changeListener.onAfterRecordChanged(event);
		}
	}

	public STATUS getInternalStatus() {
		return status;
	}

	public void setInternalStatus(final STATUS iStatus) {
		status = iStatus;
	}

	public Class<?> getGenericClass() {
		return genericClass;
	}

	public void setGenericClass(Class<?> genericClass) {
		this.genericClass = genericClass;
	}
}
