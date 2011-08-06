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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

@SuppressWarnings({ "unchecked", "serial" })
public abstract class ORecordAbstract<T> implements ORecord<T>, ORecordInternal<T> {
	protected ODatabaseRecord				_database;
	protected ORecordId							_recordId;
	protected int										_version;
	protected byte[]								_source;
	protected int										_size;
	protected ORecordSerializer			_recordFormat;
	protected boolean								_pinned		= true;
	protected boolean								_dirty		= true;
	protected ORecordElement.STATUS	_status		= ORecordElement.STATUS.LOADED;
	protected ORecordListener				_listener	= null;

	public ORecordAbstract() {
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase) {
		_database = iDatabase;
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase, final byte[] iSource) {
		this(iDatabase);
		_source = iSource;
		_size = iSource.length;
		unsetDirty();
	}

	public ORecordAbstract<?> fill(final ODatabaseRecord iDatabase, final ORecordId iRid, final int iVersion, final byte[] iBuffer,
			boolean iDirty) {
		_database = iDatabase;
		_recordId.clusterId = iRid.clusterId;
		_recordId.clusterPosition = iRid.clusterPosition;
		_version = iVersion;
		_status = ORecordElement.STATUS.LOADED;
		_source = iBuffer;
		_size = iBuffer != null ? iBuffer.length : 0;
		if (_source != null && _source.length > 0)
			_dirty = iDirty;

		return this;
	}

	public ORID getIdentity() {
		return _recordId;
	}

	public ORecord<?> getRecord() {
		return this;
	}

	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iClusterPosition) {
		if (_recordId == null || _recordId == ORecordId.EMPTY_RECORD_ID)
			_recordId = new ORecordId(iClusterId, iClusterPosition);
		else {
			_recordId.clusterId = iClusterId;
			_recordId.clusterPosition = iClusterPosition;
		}
		return this;
	}

	public ORecordAbstract<?> setIdentity(final ORecordId iIdentity) {
		_recordId = iIdentity;
		return this;
	}

	public boolean detach() {
		_database = null;
		return true;
	}

	public ORecordAbstract<T> clear() {
		setDirty();
		invokeListenerEvent(ORecordListener.EVENT.CLEAR);
		return this;
	}

	public ORecordAbstract<T> reset() {
		_status = ORecordElement.STATUS.LOADED;
		_version = 0;

		setDirty();
		if (_recordId != null)
			_recordId.reset();

		invokeListenerEvent(ORecordListener.EVENT.RESET);

		return this;
	}

	public byte[] toStream() {
		if (_source == null)
			_source = _recordFormat.toStream(_database, this, false);

		invokeListenerEvent(ORecordListener.EVENT.MARSHALL);

		return _source;
	}

	public ORecordAbstract<T> fromStream(final byte[] iRecordBuffer) {
		_dirty = false;
		_source = iRecordBuffer;
		_size = iRecordBuffer != null ? iRecordBuffer.length : 0;
		_status = ORecordElement.STATUS.LOADED;

		invokeListenerEvent(ORecordListener.EVENT.UNMARSHALL);

		return this;
	}

	public void unsetDirty() {
		if (_dirty)
			_dirty = false;
	}

	public ORecordAbstract<T> setDirty() {
		if (!_dirty && _status != STATUS.UNMARSHALLING)
			_dirty = true;
		_source = null;
		return this;
	}

	public void onBeforeIdentityChanged(final ORID iRID) {
	}

	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
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

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		if (_database != iDatabase) {
			this._database = iDatabase;
			return true;
		}
		return false;
	}

	public <RET extends ORecord<T>> RET fromJSON(final String iSource) {
		ORecordSerializerJSON.INSTANCE.fromString(_database, iSource, this);
		return (RET) this;
	}

	public String toJSON() {
		return toJSON("rid,version,class,type,attribSameRow");
	}

	public String toJSON(final String iFormat) {
		return ORecordSerializerJSON.INSTANCE.toString(this, new StringBuilder(), iFormat).toString();
	}

	@Override
	public String toString() {
		return (_recordId.isValid() ? _recordId : "") + (_source != null ? Arrays.toString(_source) : "[]") + " v" + _version;
	}

	public int getVersion() {
		return _version;
	}

	public void setVersion(final int iVersion) {
		_version = iVersion;
	}

	public ORecordAbstract<T> unload() {
		_status = ORecordElement.STATUS.NOT_LOADED;
		_source = null;
		unsetDirty();
		invokeListenerEvent(ORecordListener.EVENT.UNLOAD);
		return this;
	}

	public ORecordInternal<T> load() {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		if (!getIdentity().isValid())
			throw new ORecordNotFoundException("The record has no id, probably it's new or transient yet ");

		try {
			final ORecordInternal<?> result = _database.load(this);

			if (result == null)
				throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");

			return (ORecordInternal<T>) result;
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}
	}

	public ORecordInternal<T> reload() {
		return reload(null);
	}

	public ORecordInternal<T> reload(final String iFetchPlan) {
		return reload(null, true);
	}

	public ORecordInternal<T> reload(final String iFetchPlan, final boolean iIgnoreCache) {
		if (_database == null)
			throw new ODatabaseException("No database assigned to current record");

		if (!getIdentity().isValid())
			throw new ORecordNotFoundException("The record has no id, probably it's new or transient yet ");

		try {
			_database.reload(this, iFetchPlan, iIgnoreCache);

			// GET CONTENT
			// fromStream(toStream());

			return this;
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}
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

	public int getSize() {
		return _size;
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

		if (obj instanceof OIdentifiable)
			return _recordId.equals(((OIdentifiable) obj).getIdentity());

		return false;
	}

	public int compare(final OIdentifiable iFirst, final OIdentifiable iSecond) {
		if (iFirst == null || iSecond == null)
			return -1;
		return iFirst.compareTo(iSecond);
	}

	public int compareTo(final OIdentifiable iOther) {
		if (iOther == null)
			return 1;

		if (_recordId == null && iOther.getIdentity() == null)
			return 0;

		return _recordId.compareTo(iOther.getIdentity());
	}

	public ORecordElement.STATUS getInternalStatus() {
		return _status;
	}

	public void setInternalStatus(final ORecordElement.STATUS iStatus) {
		this._status = iStatus;
	}

	public ORecordAbstract<T> copyTo(final ORecordAbstract<T> cloned) {
		cloned._source = _source;
		cloned._size = _size;
		cloned._database = _database;
		cloned._recordId = _recordId.copy();
		cloned._version = _version;
		cloned._pinned = _pinned;
		cloned._status = _status;
		cloned._recordFormat = _recordFormat;
		cloned._listener = null;
		cloned._dirty = false;
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

	public <RET extends ORecord<T>> RET flatCopy() {
		return (RET) copy();
	}
}
