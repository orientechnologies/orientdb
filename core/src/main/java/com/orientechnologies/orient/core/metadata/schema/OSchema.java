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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OSchema extends ODocument {
	protected Map<String, OClass>	classes									= new LinkedHashMap<String, OClass>();
	private static final int			CURRENT_VERSION_NUMBER	= 2;

	public OSchema(final ODatabaseRecord<?> iDatabaseOwner, final int schemaClusterId) {
		super(iDatabaseOwner);
		registerStandardClasses();
	}

	public Collection<OClass> classes() {
		return Collections.unmodifiableCollection(classes.values());
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

		if (classes.containsKey(key))
			throw new OSchemaException("Class " + iClassName + " already exists in current database");

		OClass cls = new OClass(this, classes.size(), iClassName, iClusterIds, iDefaultClusterId);
		classes.put(key, cls);

		setDirty();

		return cls;
	}

	public boolean existsClass(final String iClassName) {
		return classes.containsKey(iClassName.toLowerCase());
	}

	public OClass getClassById(final int iClassId) {
		if (iClassId == -1)
			return null;

		for (OClass c : classes.values())
			if (c.getId() == iClassId)
				return c;

		throw new OSchemaException("Class #" + iClassId + " was not found in current database");
	}

	public OClass getClass(final String iClassName) {
		if (iClassName == null)
			return null;

		return classes.get(iClassName.toLowerCase());
	}

	/**
	 * Binds ODocument to POJO.
	 */
	@Override
	public OSchema fromStream(final byte[] iBuffer) {
		super.fromStream(iBuffer);

		// READ CURRENT SCHEMA VERSION
		int schemaVersion = field("schemaVersion");
		if (schemaVersion != CURRENT_VERSION_NUMBER) {
			// HANDLE SCHEMA UPGRADE
			throw new OConfigurationException(
					"Database schema is different. Please export your old database with the previous verison of Orient and reimport it using the current one.");
		}

		// REGISTER ALL THE CLASSES
		OClass cls;
		List<ODocument> storedClasses = field("classes");
		for (ODocument c : storedClasses) {
			cls = new OClass(this).fromDocument(c);
			classes.put(cls.getName().toLowerCase(), cls);
		}
		return this;
	}

	/**
	 * Binds POJO to ODocument.
	 */
	@Override
	public byte[] toStream() {
		field("schemaVersion", CURRENT_VERSION_NUMBER);
		field("classes", classes.values(), OType.EMBEDDEDSET);

		return super.toStream();
	}

	private void registerStandardClasses() {
	}

	public Collection<OClass> getClasses() {
		return Collections.unmodifiableCollection(classes.values());
	}

	public ODocument load() {
		recordId.fromString(database.getStorage().getConfiguration().schemaRecordId);
		return (ODocument) super.load();
	}
}
