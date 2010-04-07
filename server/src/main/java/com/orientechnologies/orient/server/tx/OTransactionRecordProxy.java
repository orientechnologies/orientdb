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

import java.io.IOException;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

public class OTransactionRecordProxy implements ORecordInternal<byte[]> {
	protected int							version;
	protected byte[]					stream;
	protected ORID						recordId		= new ORecordId();

	private static final char	RECORD_TYPE	= 'p';

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

	public boolean isDirty() {
		return true;
	}

	public boolean isPinned() {
		return false;
	}

	public void pin() {
		throw new UnsupportedOperationException();
	}

	public void setDirty() {
		throw new UnsupportedOperationException();
	}

	public void unpin() {
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

	public ORecordInternal<byte[]> setDatabase(ODatabaseRecord<?> iDatabase) {
		return null;
	}

	public ORecordInternal<byte[]> setIdentity(int iClusterId, long iClusterPosition) {
		return null;
	}

	public void unsetDirty() {
	}

	public OSerializableStream fromStream(byte[] iStream) throws IOException {
		return null;
	}

	public byte[] toStream() throws IOException {
		return stream;
	}

	public ODatabaseRecord<ORecordInternal<byte[]>> getDatabase() {
		return null;
	}

	public com.orientechnologies.orient.core.record.ORecord.STATUS getStatus() {
		return null;
	}

	public void setStatus(com.orientechnologies.orient.core.record.ORecord.STATUS iStatus) {
	}

	public ORecordInternal<byte[]> fill(ODatabaseRecord<?> iDatabase, int iClusterId, long iPosition, int iVersion) {
		return this;
	}

	public byte getRecordType() {
		return RECORD_TYPE;
	}
}
