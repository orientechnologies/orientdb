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
import com.orientechnologies.orient.core.id.ORID;

/**
 * Generic record representation. The object can be reused across call to the database.
 */
public interface ORecord<T> {
	public enum STATUS {
		NOT_LOADED, LOADED, NEW, MARSHALLING, UNMARSHALLING
	}

	public ORecord<T> reset();

	public ORecord<T> copy();

	public ORID getIdentity();

	public int getVersion();

	public ODatabaseRecord<ORecordInternal<T>> getDatabase();

	public boolean isDirty();

	public void setDirty();

	public boolean isPinned();

	public void pin();

	public void unpin();

	public STATUS getStatus();

	public ORecord<T> load();

	public ORecord<T> save();

	public ORecord<T> save(String iCluster);

	public ORecord<T> delete();

	public String toJSON();

	public String toJSON(String iFormat);
}
