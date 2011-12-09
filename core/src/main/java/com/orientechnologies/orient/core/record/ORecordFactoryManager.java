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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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
public class ORecordFactoryManager {
	protected final String[]											recordTypeNames	= new String[Byte.MAX_VALUE];
	protected final Class<? extends ORecord<?>>[]	recordTypes			= new Class[Byte.MAX_VALUE];
	protected final ORecordFactory[]							recordFactories	= new ORecordFactory[Byte.MAX_VALUE];

	public interface ORecordFactory {
		public ORecord<?> newRecord();
	}

	public ORecordFactoryManager() {
		declareRecordType(ODocument.RECORD_TYPE, "document", ODocument.class, new ORecordFactory() {
			public ORecord<?> newRecord() {
				return new ODocument();
			}
		});
		declareRecordType(ORecordFlat.RECORD_TYPE, "flat", ORecordFlat.class, new ORecordFactory() {
			public ORecord<?> newRecord() {
				return new ORecordFlat();
			}
		});
		declareRecordType(ORecordBytes.RECORD_TYPE, "bytes", ORecordBytes.class, new ORecordFactory() {
			public ORecord<?> newRecord() {
				return new ORecordBytes();
			}
		});
	}

	public String getRecordTypeName(final byte iRecordType) {
		String name = recordTypeNames[iRecordType];
		if (name == null)
			throw new IllegalArgumentException("Unsupported record type: " + iRecordType);
		return name;
	}

	public Class<? extends ORecord<?>> getRecordTypeClass(final byte iRecordType) {
		Class<? extends ORecord<?>> cls = recordTypes[iRecordType];
		if (cls == null)
			throw new IllegalArgumentException("Unsupported record type: " + iRecordType);
		return cls;
	}

	public ORecordInternal<?> newInstance() {
		final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
		try {
			return (ORecordInternal<?>) recordFactories[database.getRecordType()].newRecord();
		} catch (Exception e) {
			throw new IllegalArgumentException("Unsupported record type: " + database.getRecordType(), e);
		}
	}

	public ORecordInternal<?> newInstance(final byte iRecordType) {
		try {
			return (ORecordInternal<?>) recordFactories[iRecordType].newRecord();
		} catch (Exception e) {
			throw new IllegalArgumentException("Unsupported record type: " + iRecordType, e);
		}
	}

	public void declareRecordType(byte iByte, String iName, Class<? extends ORecordInternal<?>> iClass, final ORecordFactory iFactory) {
		if (recordTypes[iByte] != null)
			throw new OException("Record type byte '" + iByte + "' already in use : " + recordTypes[iByte].getName());
		recordTypeNames[iByte] = iName;
		recordTypes[iByte] = iClass;
		recordFactories[iByte] = iFactory;
	}
}
