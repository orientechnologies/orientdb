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
package com.orientechnologies.orient.core.record;

import java.util.Arrays;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

@SuppressWarnings("unchecked")
public abstract class ORecordAbstract<T> implements ORecord<T>, ORecordInternal<T> {
	@SuppressWarnings("rawtypes")
	protected ODatabaseRecord		_database;
	protected ORecordId					_recordId;
	protected int								_version;
	protected byte[]						_source;
	protected ORecordSerializer	_recordFormat;
	protected boolean						_pinned	= false;
	protected boolean						_dirty	= true;
	protected STATUS						_status	= STATUS.NEW;

	public ORecordAbstract() {
	}

	public ORecordAbstract(final ODatabaseRecord<?> iDatabase) {
		_database = iDatabase;
	}

	public ORecordAbstract(final ODatabaseRecord<?> iDatabase, final byte[] iSource) {
		this(iDatabase);
		_source = iSource;
		unsetDirty();
	}

	public ORecordAbstract<?> fill(final ODatabaseRecord<?> iDatabase, final int iClusterId, final long iPosition, final int iVersion) {
		_database = iDatabase;
		setIdentity(iClusterId, iPosition);
		_version = iVersion;
		return this;
	}

	public ORID getIdentity() {
		return _recordId;
	}

	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iPosition) {
		if (_recordId == null)
			_recordId = new ORecordId(iClusterId, iPosition);
		else {
			_recordId.clusterId = iClusterId;
			_recordId.clusterPosition = iPosition;
		}
		return this;
	}

	public ORecordAbstract<?> setIdentity(final ORecordId iIdentity) {
		_recordId = iIdentity;
		return this;
	}

	public ORecordAbstract<T> reset() {
		_status = STATUS.NEW;

		setDirty();
		if (_recordId != null)
			_recordId.reset();
		return this;
	}

	public byte[] toStream() {
		if (_source == null)
			_source = _recordFormat.toStream(_database, this);

		return _source;
	}

	public ORecordAbstract<T> fromStream(final byte[] iRecordBuffer) {
		_dirty = false;
		_source = iRecordBuffer;
		_status = STATUS.LOADED;
		return this;
	}

	public void unsetDirty() {
		if (_dirty)
			_dirty = false;
	}

	public ORecordAbstract<T> setDirty() {
		if (!_dirty)
			_dirty = true;
		_source = null;
		return this;
	}

	public boolean isDirty() {
		return _dirty;
	}

	public boolean isPinned() {
		return _pinned;
	}

	public ORecordAbstract<T> pin() {
		if (!_pinned)
			_pinned = true;
		return this;
	}

	public ORecordAbstract<T> unpin() {
		if (_pinned)
			_pinned = false;
		return this;
	}

	public ODatabaseRecord<?> getDatabase() {
		return _database;
	}

	public ORecordAbstract<?> setDatabase(final ODatabaseRecord<?> iDatabase) {
		this._database = iDatabase;
		return this;
	}

	public <RET extends ORecord<T>> RET fromJSON(final String iSource) {
		setDirty();
		ORecordSerializerJSON.INSTANCE.fromString(_database, iSource, this);
		return (RET) this;
	}

	public String toJSON() {
		return toJSON("rid,version,class,attribSameRow");
	}

	public String toJSON(final String iFormat) {
		return ORecordSerializerJSON.INSTANCE.toString(this, iFormat);
	}

	@Override
	public String toString() {
		return "@" + (_recordId.isValid() ? _recordId : "") + "[" + Arrays.toString(_source) + "]";
	}

	public int getVersion() {
		return _version;
	}

	public void setVersion(final int iVersion) {
		_version = iVersion;
	}

	public ORecordAbstract<T> load() {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		Object result = null;
		try {
			result = _database.load(this);
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}

		if (result == null)
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");

		return this;
	}

	public ORecordAbstract<T> save() {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record. Create it using the <DB>.newInstance()");

		_database.save(this);

		return this;
	}

	public ORecordAbstract<T> save(final String iClusterName) {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record. Create it using the <DB>.newInstance()");

		_database.save(this, iClusterName);

		return this;
	}

	public ORecordAbstract<T> delete() {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		_database.delete(this);
		return this;
	}

	protected void setup() {
		if (_recordId == null)
			_recordId = new ORecordId();
	}

	@Override
	public int hashCode() {
		return _recordId != null ? _recordId.hashCode() : 0;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() == obj.getClass()) {
			// DOCUMENT <-> DOCUMENT
			final ORecordAbstract<?> other = (ORecordAbstract<?>) obj;
			if (_recordId == null) {
				if (other._recordId != null)
					return false;
			} else if (!_recordId.equals(other._recordId))
				return false;
		} else if (obj instanceof ORID) {
			// DOCUMENT <-> ORID
			final ORID other = (ORID) obj;

			if (_recordId == null) {
				if (other != null)
					return false;
			} else if (!_recordId.equals(other))
				return false;
		} else
			return false;

		return true;
	}

	public STATUS getInternalStatus() {
		return _status;
	}

	public void setStatus(final STATUS iStatus) {
		this._status = iStatus;
	}
}
