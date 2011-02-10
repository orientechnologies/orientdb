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

	@Override
	public OTransactionRecordProxy copy() {
		return null;
	}

	@Override
	public ORID getIdentity() {
		return recordId;
	}

	@Override
	public int getVersion() {
		return version;
	}

	@Override
	public void setVersion(int iVersion) {
		version = iVersion;
	}

	@Override
	public boolean isDirty() {
		return true;
	}

	@Override
	public boolean isPinned() {
		return false;
	}

	@Override
	public OTransactionRecordProxy pin() {
		throw new UnsupportedOperationException();
	}

	@Override
	public OTransactionRecordProxy setDirty() {
		throw new UnsupportedOperationException();
	}

	@Override
	public OTransactionRecordProxy unpin() {
		throw new UnsupportedOperationException();
	}

	@Override
	public OTransactionRecordProxy save() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	@Override
	public OTransactionRecordProxy save(final String iCluster) {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	@Override
	public OTransactionRecordProxy load() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	@Override
	public OTransactionRecordProxy delete() {
		throw new UnsupportedOperationException("Can't assign a user-defined cluster to this class");
	}

	@Override
	public ORecord<byte[]> reset() {
		return null;
	}

	@Override
	public ORecordAbstract<?> setDatabase(final ODatabaseRecord iDatabase) {
		return null;
	}

	@Override
	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iClusterPosition) {
		return null;
	}

	@Override
	public void unsetDirty() {
	}

	@Override
	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		stream = iStream;
		return this;
	}

	@Override
	public byte[] toStream() throws OSerializationException {
		return stream;
	}

	@Override
	public ODatabaseRecord getDatabase() {
		return null;
	}

	@Override
	public com.orientechnologies.orient.core.record.ORecord.STATUS getInternalStatus() {
		return null;
	}

	@Override
	public void setStatus(com.orientechnologies.orient.core.record.ORecord.STATUS iStatus) {
	}

	@Override
	public ORecordAbstract<?> fill(final ODatabaseRecord iDatabase, final int iClusterId, final long iPosition, final int iVersion) {
		return null;
	}

	@Override
	public byte getRecordType() {
		return recordType;
	}

	public void setRecordType(final byte iType) {
		recordType = iType;
	}

	@Override
	public String toJSON() {
		return null;
	}

	@Override
	public String toJSON(String iFormat) {
		return null;
	}

	@Override
	public <RET extends ORecord<byte[]>> RET unload() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <RET extends ORecord<byte[]>> RET clear() {
		return null;
	}

	@Override
	public <RET extends ORecord<byte[]>> RET fromJSON(String iJson) {
		return null;
	}

	@Override
	public ORecordAbstract<?> setIdentity(ORecordId iIdentity) {
		return null;
	}

	@Override
	public long getSize() {
		return 0;
	}
}
