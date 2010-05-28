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
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

@SuppressWarnings("unchecked")
public abstract class ORecordAbstract<T> implements ORecord<T>, ORecordInternal<T> {
	protected ODatabaseRecord		database;
	protected ORecordId					recordId;
	protected int								version;
	protected byte[]						source;
	protected ORecordSerializer	recordFormat;
	protected boolean						pinned	= false;
	protected boolean						dirty		= true;
	protected STATUS						status	= STATUS.NEW;

	public ORecordAbstract() {
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase) {
		database = iDatabase;
	}

	public ORecordAbstract(final ODatabaseRecord iDatabase, final byte[] iSource) {
		this(iDatabase);
		source = iSource;
		unsetDirty();
	}

	public ORecordAbstract fill(final ODatabaseRecord<?> iDatabase, final int iClusterId, final long iPosition, final int iVersion) {
		database = iDatabase;
		setIdentity(iClusterId, iPosition);
		version = iVersion;
		return this;
	}

	public ORID getIdentity() {
		return recordId;
	}

	public ORecordAbstract setIdentity(final int iClusterId, final long iPosition) {
		status = STATUS.NOT_LOADED;

		if (recordId == null)
			recordId = new ORecordId(iClusterId, iPosition);
		else {
			recordId.clusterId = iClusterId;
			recordId.clusterPosition = iPosition;
		}
		return this;
	}

	public ORecordAbstract<T> reset() {
		status = STATUS.NEW;

		setDirty();
		source = null;
		if (recordId != null)
			recordId.reset();
		return this;
	}

	public byte[] toStream() {
		if (source == null)
			source = recordFormat.toStream(database, this);

		return source;
	}

	public ORecordAbstract<T> fromStream(final byte[] iRecordBuffer) {
		source = iRecordBuffer;
		status = STATUS.LOADED;
		return this;
	}

	public void unsetDirty() {
		if (dirty)
			dirty = false;
	}

	public ORecordAbstract<T> setDirty() {
		if (!dirty)
			dirty = true;
		source = null;
		return this;
	}

	public boolean isDirty() {
		return dirty;
	}

	public boolean isPinned() {
		return pinned;
	}

	public ORecordAbstract<T> pin() {
		if (!pinned)
			pinned = true;
		return this;
	}

	public ORecordAbstract<T> unpin() {
		if (pinned)
			pinned = false;
		return this;
	}

	public ODatabaseRecord getDatabase() {
		return database;
	}

	public ORecordAbstract setDatabase(final ODatabaseRecord iDatabase) {
		this.database = iDatabase;
		return this;
	}

	public <RET extends ORecord<T>> RET fromJSON(final String iSource) {
		return (RET) ORecordSerializerJSON.INSTANCE.fromString(database, iSource, this);
	}

	public String toJSON() {
		return toJSON("id,ver,class");
	}

	public String toJSON(final String iFormat) {
		return ORecordSerializerJSON.INSTANCE.toString(this, iFormat);
	}

	@Override
	public String toString() {
		return "@" + (recordId.isValid() ? recordId : "") + "[" + Arrays.toString(source) + "]";
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(final int iVersion) {
		version = iVersion;
	}

	public ORecordAbstract<T> load() {
		if (database == null)
			throw new ODatabaseException("No database assigned to current record");

		try {
			if (database.load(this) == null)
				throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found");
		} catch (Exception e) {
			throw new ORecordNotFoundException("The record with id '" + getIdentity() + "' was not found", e);
		}

		return this;
	}

	public ORecordAbstract<T> save() {
		if (database == null)
			throw new ODatabaseException("No database assigned to current record. Create it using the <DB>.newInstance()");

		OSerializationThreadLocal.INSTANCE.get().clear();

		database.save(this);
		return this;
	}

	public ORecordAbstract<T> save(final String iClusterName) {
		if (database == null)
			throw new ODatabaseException("No database assigned to current record. Create it using the <DB>.newInstance()");

		OSerializationThreadLocal.INSTANCE.get().clear();

		database.save(this, iClusterName);

		return this;
	}

	public ORecordAbstract<T> save(ODatabaseRecord<?> iDatabase) {
		if (database != null)
			throw new IllegalArgumentException("Can't change database to a live record");

		database = iDatabase;
		database.save(this);
		return this;
	}

	public ORecordAbstract<T> save(ODatabaseRecord<?> iDatabase, final String iClusterName) {
		if (database != null)
			throw new IllegalArgumentException("Can't change database to a live record");

		database = iDatabase;
		database.save(this, iClusterName);
		return this;
	}

	public ORecordAbstract<T> delete() {
		if (database == null)
			throw new ODatabaseException("No database assigned to current record");

		database.delete(this);
		return this;
	}

	protected void setup() {
		if (recordId == null)
			recordId = new ORecordId();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((database == null) ? 0 : database.hashCode());
		result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ORecordAbstract other = (ORecordAbstract) obj;
		if (database == null) {
			if (other.database != null)
				return false;
		} else if (!database.equals(other.database))
			return false;
		if (recordId == null) {
			if (other.recordId != null)
				return false;
		} else if (!recordId.equals(other.recordId))
			return false;
		return true;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}
}
