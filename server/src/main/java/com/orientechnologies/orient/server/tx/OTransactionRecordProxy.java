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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

@SuppressWarnings({ "unchecked", "serial" })
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

	public ORecord<?> getRecord() {
		return this;
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
		throw new UnsupportedOperationException("pin()");

	}

	public OTransactionRecordProxy setDirty() {
		throw new UnsupportedOperationException("setDirty()");

	}

	/**
	 * The item's identity doesn't affect nothing.
	 */
	public void onBeforeIdentityChanged(final ORID iRID) {
	}

	/**
	 * The item's identity doesn't affect nothing.
	 */
	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
	}

	public OTransactionRecordProxy unpin() {
		throw new UnsupportedOperationException("unpin()");
	}

	public OTransactionRecordProxy save() {
		throw new UnsupportedOperationException("save()");
	}

	public boolean detach() {
		return true;
	}

	public OTransactionRecordProxy save(final String iCluster) {
		throw new UnsupportedOperationException("save()");
	}

	public OTransactionRecordProxy load() {
		throw new UnsupportedOperationException("load()");
	}

	public OTransactionRecordProxy reload() {
		throw new UnsupportedOperationException("reload()");
	}

	public OTransactionRecordProxy delete() {
		throw new UnsupportedOperationException("delete()");
	}

	public ORecord<byte[]> reset() {
		return null;
	}

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		return false;
	}

	public ORecordAbstract<?> setIdentity(final int iClusterId, final long iClusterPosition) {
		return null;
	}

	public void unsetDirty() {
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		stream = iStream;
		return this;
	}

	public byte[] toStream() throws OSerializationException {
		return stream;
	}

	public ODatabaseRecord getDatabase() {
		return null;
	}

	public com.orientechnologies.orient.core.db.record.ORecordElement.STATUS getInternalStatus() {
		return null;
	}

	public void setStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS iStatus) {
	}

	public ORecordAbstract<?> fill(final ODatabaseRecord iDatabase, final ORecordId iRid, final int iVersion, final byte[] iBuffer,
			boolean iDirty) {
		return null;
	}

	public byte getRecordType() {
		return recordType;
	}

	public void setRecordType(final byte iType) {
		recordType = iType;
	}

	public String toJSON() {
		return null;
	}

	public String toJSON(String iFormat) {
		return null;
	}

	public <RET extends ORecord<byte[]>> RET unload() {
		// TODO Auto-generated method stub
		return null;
	}

	public <RET extends ORecord<byte[]>> RET clear() {
		return null;
	}

	public <RET extends ORecord<byte[]>> RET fromJSON(String iJson) {
		return null;
	}

	public ORecordAbstract<?> setIdentity(ORecordId iIdentity) {
		return null;
	}

	public int getSize() {
		return 0;
	}

	public int compare(OIdentifiable o1, OIdentifiable o2) {
		return -1;
	}

	public int compareTo(OIdentifiable o) {
		return -1;
	}
}
