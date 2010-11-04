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
import java.util.HashSet;
import java.util.Iterator;

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordTrackedSet extends HashSet<Object> {
	protected final ORecord<?>	sourceRecord;

	public ORecordTrackedSet(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
	}

	@Override
	public Iterator<Object> iterator() {
		return new ORecordTrackedIterator(sourceRecord, super.iterator());
	}

	@Override
	public boolean add(final Object e) {
		setDirty();
		return super.add(e);
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

	@Override
	public boolean removeAll(Collection<?> c) {
		setDirty();
		return super.removeAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends Object> c) {
		setDirty();
		return super.addAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		setDirty();
		return super.retainAll(c);
	}

	public void setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}
}
