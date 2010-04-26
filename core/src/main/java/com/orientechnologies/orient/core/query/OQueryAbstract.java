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
package com.orientechnologies.orient.core.query;

import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

public abstract class OQueryAbstract<REC extends ORecordInternal<?>> implements OQueryInternal<REC> {
	protected String								requesterId;
	protected ODatabaseRecord<REC>	database;
	protected REC										record;

	protected int										limit	= -1;

	public List<REC> execute() {
		return execute(-1);
	}

	public ODatabaseRecord<REC> getDatabase() {
		return database;
	}

	public void setDatabase(ODatabaseRecord<REC> iDatabase) {
		database = iDatabase;
		if (record != null)
			record.setDatabase(iDatabase);
	}

	public void setRecord(REC record) {
		this.record = record;
	}

	@SuppressWarnings("unchecked")
	public Class<REC> getRecordClass() {
		return (Class<REC>) (record == null ? null : record.getClass());
	}

	public String getRequester() {
		return requesterId;
	}

	public void setRequester(String iRequester) {
		requesterId = iRequester;
	}

	public REC getRecord() {
		return record;
	}
}
