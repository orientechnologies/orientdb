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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes for literal types. This avoid to call the
 * makeDirty() by hand when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class OTrackedList<T> extends ArrayList<T> implements ORecordElement, Serializable {
	protected final ORecordInternal<?>	sourceRecord;
	private STATUS											status	= STATUS.NOT_LOADED;

	public OTrackedList(final ORecordInternal<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
	}

	@Override
	public boolean add(T element) {
		setDirty();
		return super.add(element);
	}

	@Override
	public boolean addAll(final Collection<? extends T> c) {
		setDirty();
		for (T o : c)
			rawAdd(o);
		return true;
	}

	@Override
	public void add(int index, T element) {
		setDirty();
		super.add(index, element);
	}

	@Override
	public T set(int index, T element) {
		setDirty();
		return super.set(index, element);
	}

	@Override
	public T remove(int index) {
		setDirty();
		return super.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		setDirty();
		return super.remove(o);
	}

	@Override
	public void clear() {
		setDirty();
		super.clear();
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

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		if (sourceRecord != null)
			return sourceRecord.setDatabase(iDatabase);
		return false;
	}

	protected boolean rawAdd(T element) {
		return super.add(element);
	}

	public STATUS getInternalStatus() {
		return status;
	}

	public void setInternalStatus(final STATUS iStatus) {
		status = iStatus;
	}
}
