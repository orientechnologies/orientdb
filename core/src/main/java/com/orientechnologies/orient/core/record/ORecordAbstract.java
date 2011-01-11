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
public abstract class ORecordAbstract<T> implements ORecord<T>, ORecordInternal<T>, Comparable<ORecordAbstract<T>> {
	protected ODatabaseRecord		_database;
	protected ORecordId					_recordId;
	protected int								_version;
	protected byte[]						_source;
	protected ORecordSerializer	_recordFormat;
	protected boolean						_pinned		= false;
	protected boolean						_dirty		= true;
	protected STATUS						_status		= STATUS.NEW;
	protected ORecordListener		_listener	= null;

	public ORecordAbstract() {
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase) {
		_database = iDatabase;
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase, final byte[] iSource) {
		this(iDatabase);
		_source = iSource;
		unsetDirty();
	}

	public ORecordAbstract<?> fill(final ODatabaseRecord iDatabase, final int iClusterId, final long iPosition, final int iVersion) {
		_database = iDatabase;
		setIdentity(iClusterId, iPosition);
		_version = iVersion;
		return this;
	}

	public ORID getIdentity() {
		return _recordId;
	}

	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iPosition) {
		if (_recordId == null || _recordId == ORecordId.EMPTY_RECORD_ID)
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

	public ORecordAbstract<T> clear() {
		setDirty();
		invokeListenerEvent(ORecordListener.EVENT.CLEAR);
		return this;
	}

	public ORecordAbstract<T> reset() {
		_status = STATUS.NEW;

		setDirty();
		if (_recordId != null)
			_recordId.reset();

		invokeListenerEvent(ORecordListener.EVENT.RESET);

		return this;
	}

	public byte[] toStream() {
		if (_source == null)
			_source = _recordFormat.toStream(_database, this);

		invokeListenerEvent(ORecordListener.EVENT.MARSHALL);

		return _source;
	}

	public ORecordAbstract<T> fromStream(final byte[] iRecordBuffer) {
		_dirty = false;
		_source = iRecordBuffer;
		_status = STATUS.LOADED;

		invokeListenerEvent(ORecordListener.EVENT.UNMARSHALL);

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

	public ODatabaseRecord getDatabase() {
		return _database;
	}

	public ORecordAbstract<?> setDatabase(final ODatabaseRecord iDatabase) {
		this._database = iDatabase;
		return this;
	}

	public <RET extends ORecord<T>> RET fromJSON(final String iSource) {
		ORecordSerializerJSON.INSTANCE.fromString(_database, iSource, this);
		return (RET) this;
	}

	public String toJSON() {
		return toJSON("rid,version,class,type,attribSameRow");
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

	public ORecordAbstract<T> unload() {
		_status = STATUS.NOT_LOADED;
		unsetDirty();
		invokeListenerEvent(ORecordListener.EVENT.UNLOAD);
		return this;
	}

	public ORecordAbstract<T> load() {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		if (!getIdentity().isValid())
			throw new ORecordNotFoundException("The record has no id, probably it's new or transient yet ");

		try {
			final ORecordInternal<?> result = _database.load(this);

			if (result == null)
				throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");

			if (result != this)
				// GET CONTENT
				fromStream(result.toStream());
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}

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
		setDirty();
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

	public int compareTo(final ORecordAbstract<T> iOther) {
		if (iOther == null)
			return 1;

		if (_recordId == null && iOther._recordId == null)
			return 0;

		return _recordId.compareTo(iOther._recordId);
	}

	public STATUS getInternalStatus() {
		return _status;
	}

	public void setStatus(final STATUS iStatus) {
		this._status = iStatus;
	}

	public ORecordAbstract<T> copyTo(final ORecordAbstract<T> cloned) {
		cloned._database = _database;
		cloned._recordId = _recordId.copy();
		cloned._version = _version;
		cloned._source = null;
		cloned._recordFormat = _recordFormat;
		cloned._pinned = _pinned;
		cloned._dirty = _dirty;
		cloned._status = _status;
		cloned._listener = _listener;
		return cloned;
	}

	/**
	 * Add a listener to the current document to catch all the supported events.
	 * 
	 * @see ORecordListener
	 * 
	 * @param iListener
	 *          ODocumentListener implementation
	 */
	public void setListener(final ORecordListener iListener) {
		_listener = iListener;
	}

	/**
	 * Remove the current event listener.
	 * 
	 * @see ORecordListener
	 */
	public void removeListener() {
		_listener = null;
	}

	protected void invokeListenerEvent(final ORecordListener.EVENT iEvent) {
		if (_listener != null)
			_listener.onEvent(this, iEvent);
	}
}
