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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.util.Set;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;

public class ORecordSerializerPositional2CSV extends ORecordSerializerCSVAbstract {
	public static final String	NAME	= "ORecordCSV2csv";

	@Override
	public String toString() {
		return NAME;
	}

	public ORecordInternal<?> fromStream(final ODatabaseRecord<?> iDatabase, final byte[] iSource) {
		return fromStream(iDatabase, iSource, new ORecordColumn());
	}

	@Override
	protected ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iContent,
			final ORecordInternal<?> iRecord) {
		return fromString(iDatabase, iContent, new ORecordColumn());
	}

	@Override
	protected ORecordSchemaAware<?> newObject(final ODatabaseRecord<?> iDatabase, final String iClassName) {
		return null;
	}

	@Override
	protected String toString(final ORecordInternal<?> iRecord, final String iFormat, final OUserObject2RecordHandler iObjHandler,
			Set<Integer> iMarshalledRecords) {
		if (!iRecord.getIdentity().isValid())
			iRecord.save();

		return iRecord.getIdentity().toString();
	}
}
