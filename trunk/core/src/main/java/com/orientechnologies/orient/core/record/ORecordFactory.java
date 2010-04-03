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
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordCSV;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

@SuppressWarnings("unchecked")
public class ORecordFactory {
	private static final ORecordFactory	instance	= new ORecordFactory();

	public <T extends ORecord<?>> T newInstance(final ODatabaseRecord iDatabase, final Class<?> iClass) {
		if (iClass == null)
			return null;

		try {
			if (iClass.equals(ORecordVObject.class))
				return (T) new ORecordVObject((ODatabaseVObject) iDatabase);

			else if (iClass.equals(ORecordFlat.class))
				return (T) new ORecordFlat(iDatabase);

			else if (iClass.equals(ORecordBytes.class))
				return (T) new ORecordBytes(iDatabase);

			else if (iClass.equals(ORecordCSV.class))
				return (T) new ORecordCSV(iDatabase);

			return (T) iClass.newInstance();

		} catch (Exception e) {

			throw new OConfigurationException("Error on the creation of the record of class " + iClass, e);
		}
	}

	public static ORecordFactory instance() {
		return instance;
	}

	public static ORecord<?> getRecord(final byte iRecordType) {
		if (iRecordType == ORecordVObject.RECORD_TYPE)
			return new ORecordVObject();
		if (iRecordType == ORecordFlat.RECORD_TYPE)
			return new ORecordFlat();
		if (iRecordType == ORecordBytes.RECORD_TYPE)
			return new ORecordBytes();
		if (iRecordType == ORecordCSV.RECORD_TYPE)
			return new ORecordCSV();
		return null;
	}
}
