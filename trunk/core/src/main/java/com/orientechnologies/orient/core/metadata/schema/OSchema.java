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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;

public class OSchema extends ORecordBytes {
	protected Map<String, OClass>	classesMap							= new LinkedHashMap<String, OClass>();
	protected List<OClass>				classes									= new ArrayList<OClass>();
	public static final int				CLASSES_RECORD_NUM			= 1;
	private static final int			CURRENT_VERSION_NUMBER	= 1;

	public OSchema(final ODatabaseRecord<?> iDatabaseOwner, final int schemaClusterId) {
		super(iDatabaseOwner);
		registerStandardClasses();
	}

	public Collection<OClass> classes() {
		return Collections.unmodifiableCollection(classes);
	}

	public OClass createClass(final String iClassName) {
		int clusterId = database.getClusterIdByName(iClassName);
		if (clusterId == -1)
			// CREATE A NEW CLUSTER
			clusterId = database.addLogicalCluster(iClassName, database.getDefaultClusterId());

		return createClass(iClassName, clusterId);
	}

	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		return createClass(iClassName, new int[] { iDefaultClusterId }, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final int[] iClusterIds, final int iDefaultClusterId) {
		String key = iClassName.toLowerCase();

		if (classesMap.containsKey(key))
			throw new OSchemaException("Class " + iClassName + " already exists in current database");

		OClass cls = new OClass(this, classesMap.size(), iClassName, iClusterIds, iDefaultClusterId);
		classesMap.put(key, cls);
		classes.add(cls);

		setDirty();

		return cls;
	}

	public boolean existsClass(final String iClassName) {
		return classesMap.containsKey(iClassName.toLowerCase());
	}

	public OClass getClassById(final int iClassId) {
		if (iClassId == -1)
			return null;

		OClass cls = classes.get(iClassId);

		if (cls == null)
			throw new OSchemaException("Class #" + iClassId + " was not found in current database");

		return cls;
	}

	public OClass getClass(final String iClassName) {
		return classesMap.get(iClassName.toLowerCase());
	}

	public OSchema fromStream(final byte[] iStream) {
		ORecordColumn record = new ORecordColumn().fromStream(iStream);

		// READ CURRENT SCHEMA VERSION
		int schemaVersion = Integer.parseInt(record.next());
		if (schemaVersion != CURRENT_VERSION_NUMBER) {
			// HANDLE SCHEMA UPGRADE
		}

		// REGISTER ALL THE CLASSES
		OClass cls;
		int classesNum = Integer.parseInt(record.next());
		for (int c = 0; c < classesNum; ++c) {
			cls = new OClass(this, c, record.next(), null, -1);
			classesMap.put(cls.getName().toLowerCase(), cls);
			classes.add(cls);
			cls.fromStream(record);
		}
		return this;
	}

	public byte[] toStream() {
		ORecordColumn record = new ORecordColumn();

		// WRITE CURRENT SCHEMA VERSION
		record.add(String.valueOf(CURRENT_VERSION_NUMBER));

		// WRITE CLASSES
		record.add(String.valueOf(classesMap.size()));
		for (OClass cls : classesMap.values()) {
			cls.toStream(record);
		}

		return record.toStream();
	}

	private void registerStandardClasses() {
	}

	public Collection<OClass> getClasses() {
		return Collections.unmodifiableCollection(classesMap.values());
	}

	public ORecordAbstract<byte[]> load(final int schemaClusterId) {
		setIdentity(schemaClusterId, CLASSES_RECORD_NUM);
		return super.load();
	}
}
