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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

public abstract class OMetadataRecord extends ODocument {

	public OMetadataRecord() {
		super();
	}

	public OMetadataRecord(byte[] iSource) {
		super(iSource);
	}

	public OMetadataRecord(OClass iClass) {
		super(iClass);
	}

	public OMetadataRecord(ODatabaseRecord<?> iDatabase, ORID iRID) {
		super(iDatabase, iRID);
	}

	public OMetadataRecord(ODatabaseRecord<?> iDatabase, String iClassName, ORID iRID) {
		super(iDatabase, iClassName, iRID);
	}

	public OMetadataRecord(ODatabaseRecord<?> iDatabase, String iClassName) {
		super(iDatabase, iClassName);
	}

	public OMetadataRecord(ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
	}

	public OMetadataRecord(String iClassName) {
		super(iClassName);
	}

	@Override
	public ORecordAbstract<Object> save() {
		return super.save("metadata");
	}
}
