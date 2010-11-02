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
package com.orientechnologies.orient.server.tx;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

@SuppressWarnings("unchecked")
public class OTransactionRecordProxy implements ORecordInternal<byte[]> {
	protected int			version;
	protected byte[]	stream;
	protected byte		recordType;
	protected ORID		recordId	= new ORecordId();

	public OTransactionRecordProxy() {
	}

	public OTransactionRecordProxy copy() {
		return null;
	}

	public ORID getIdentity() {
		return recordId;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int iVersion) {
		version = iVersion;
	}

	public boolean isDirty() {
		return true;
	}

	public boolean isPinned() {
		return false;
	}

	public OTransactionRecordProxy pin() {
		throw new UnsupportedOperationException();
	}

	public OTransactionRecordProxy setDirty() {
		throw new UnsupportedOperationException();
	}

	public OTransactionRecordProxy unpin() {
		throw new UnsupportedOperationException();
	}

	public OTransactionRecordProxy save() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	public OTransactionRecordProxy save(final String iCluster) {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	public OTransactionRecordProxy load() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	public OTransactionRecordProxy delete() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	public ORecord<byte[]> reset() {
		return null;
	}

	public ORecordAbstract<?> setDatabase(final ODatabaseRecord<?> iDatabase) {
		return null;
	}

	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iClusterPosition) {
		return null;
	}

	public void unsetDirty() {
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		return null;
	}

	public byte[] toStream() throws OSerializationException {
		return stream;
	}

	public ODatabaseRecord<ORecordInternal<byte[]>> getDatabase() {
		return null;
	}

	public com.orientechnologies.orient.core.record.ORecord.STATUS getInternalStatus() {
		return null;
	}

	public void setStatus(com.orientechnologies.orient.core.record.ORecord.STATUS iStatus) {
	}

	public ORecordAbstract<?> fill(final ODatabaseRecord<?> iDatabase, final int iClusterId, final long iPosition, final int iVersion) {
		return null;
	}

	public byte getRecordType() {
		return recordType;
	}

	public String toJSON(final String iFormat) {
		return null;
	}

	public String toJSON() {
		return null;
	}

	public ORecord<byte[]> fromJSON(String iJson) {
		return null;
	}

	public ORecordAbstract<?> setIdentity(final ORecordId iIdentity) {
		return null;
	}

	public <RET extends ORecord<byte[]>> RET unload() {
		return null;
	}

	public <RET extends ORecord<byte[]>> RET clear() {
		return null;
	}
}
