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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Interface for internal use only. Don't use this methods unless you're writing an internal component.
 */
public interface ORecordInternal<T> extends ORecord<T>, OSerializableStream {
	/**
	 * Internal only. Fills in one shot the record.
	 */
	public ORecordAbstract<?> fill(ODatabaseRecord iDatabase, ORecordId iRid, int iVersion, byte[] iBuffer, boolean iDirty);

	/**
	 * Internal only. Changes the identity of the record.
	 */
	public ORecordAbstract<?> setIdentity(int iClusterId, long iClusterPosition);

	/**
	 * Internal only. Changes the identity of the record.
	 */
	public ORecordAbstract<?> setIdentity(ORecordId iIdentity);

	/**
	 * Internal only. Unsets the dirty status of the record.
	 */
	public void unsetDirty();

	/**
	 * Internal only. Sets the version.
	 */
	public void setVersion(int iVersion);

	/**
	 * Internal only. Return the record type.
	 */
	public byte getRecordType();

	/**
	 * Internal only. Executes a flat copy of the record.
	 * 
	 * @see #copy()
	 */
	public <RET extends ORecord<T>> RET flatCopy();
}
