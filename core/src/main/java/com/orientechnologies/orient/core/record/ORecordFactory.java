/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

/**
 * Record factory. To use your own record implementation use the declareRecordType() method. Example of registration of the record
 * MyRecord:
 * <p>
 * <code>
 * declareRecordType('m', "myrecord", MyRecord.class);
 * </code>
 * </p>
 * 
 * @author Sylvain Spinelli
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class ORecordFactory {
	private static final ORecordFactory							instance				= new ORecordFactory();
	protected static String[]												recordTypeNames	= new String[Byte.MAX_VALUE];
	protected static Class<? extends ORecord<?>>[]	recordTypes			= new Class[Byte.MAX_VALUE];

	static {
		declareRecordType(ODocument.RECORD_TYPE, "document", ODocument.class);
		declareRecordType(ORecordFlat.RECORD_TYPE, "flat", ORecordFlat.class);
		declareRecordType(ORecordBytes.RECORD_TYPE, "bytes", ORecordBytes.class);
	}

	public <T extends ORecord<?>> T newInstance(final ODatabaseRecord iDatabase, final Class<?> iClass) {
		if (iClass == null)
			return null;
		try {
			T result = (T) iClass.newInstance();
			result.setDatabase(iDatabase);
			return result;
		} catch (Exception e) {
			throw new OConfigurationException("Error on the creation of the record of class " + iClass, e);
		}
	}

	public static ORecordFactory instance() {
		return instance;
	}

	public static String getRecordTypeName(final byte iRecordType) {
		String name = recordTypeNames[iRecordType];
		if (name == null)
			throw new IllegalArgumentException("Unsupported record type: " + iRecordType);
		return name;
	}

	public static ORecordInternal<?> newInstance(final byte iRecordType) {
		try {
			return (ORecordInternal<?>) recordTypes[iRecordType].newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException("Unsupported record type: " + iRecordType, e);
		}
	}

	public static void declareRecordType(byte iByte, String iName, Class<? extends ORecordInternal<?>> iClass) {
		if (recordTypes[iByte] != null)
			throw new OException("Record type byte '" + iByte + "' already in used : " + recordTypes[iByte].getName());
		recordTypeNames[iByte] = iName;
		recordTypes[iByte] = iClass;
	}
}
