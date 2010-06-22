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
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

@SuppressWarnings("unchecked")
public class ORecordFactory {
	private static final ORecordFactory	instance	= new ORecordFactory();

	public <T extends ORecord<?>> T newInstance(final ODatabaseRecord iDatabase, final Class<?> iClass) {
		if (iClass == null)
			return null;

		try {
			if (iClass.equals(ODocument.class))
				return (T) new ODocument(iDatabase);

			else if (iClass.equals(ORecordFlat.class))
				return (T) new ORecordFlat(iDatabase);

			else if (iClass.equals(ORecordBytes.class))
				return (T) new ORecordBytes(iDatabase);

			else if (iClass.equals(ORecordColumn.class))
				return (T) new ORecordColumn(iDatabase);

			return (T) iClass.newInstance();

		} catch (Exception e) {

			throw new OConfigurationException("Error on the creation of the record of class " + iClass, e);
		}
	}

	public static ORecordFactory instance() {
		return instance;
	}

	public static ORecordInternal<?> newInstance(final byte iRecordType) {
		if (iRecordType == ODocument.RECORD_TYPE)
			return new ODocument();
		else if (iRecordType == ORecordFlat.RECORD_TYPE)
			return new ORecordFlat();
		else if (iRecordType == ORecordBytes.RECORD_TYPE)
			return new ORecordBytes();
		else if (iRecordType == ORecordColumn.RECORD_TYPE)
			return new ORecordColumn();

		return null;
	}
}
