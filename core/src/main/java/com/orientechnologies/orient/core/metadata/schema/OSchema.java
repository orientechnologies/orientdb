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

import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

public class OSchema extends ODocumentWrapperNoClass {
	protected Map<String, OClass>	classes									= new LinkedHashMap<String, OClass>();
	private static final int			CURRENT_VERSION_NUMBER	= 4;

	public OSchema(final ODatabaseRecord<?> iDatabaseOwner, final int schemaClusterId) {
		super(new ODocument(iDatabaseOwner));
		registerStandardClasses();
	}

	public Collection<OClass> classes() {
		return Collections.unmodifiableCollection(classes.values());
	}

	public OClass createClass(final String iClassName) {
		int clusterId = document.getDatabase().getClusterIdByName(iClassName);
		if (clusterId == -1)
			// CREATE A NEW CLUSTER
			clusterId = document.getDatabase().addLogicalCluster(iClassName,
					document.getDatabase().getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME));

		return createClass(iClassName, clusterId);
	}

	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		return createClass(iClassName, new int[] { iDefaultClusterId }, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final int[] iClusterIds) {
		return createClass(iClassName, iClusterIds, -1);
	}

	public OClass createClass(final String iClassName, final int[] iClusterIds, final int iDefaultClusterId) {
		String key = iClassName.toLowerCase();

		if (classes.containsKey(key))
			throw new OSchemaException("Class " + iClassName + " already exists in current database");

		OClass cls = new OClass(this, classes.size(), iClassName, iClusterIds, iDefaultClusterId);
		classes.put(key, cls);
		document.setDirty();

		return cls;
	}

	public void removeClass(final String iClassName) {
		String key = iClassName.toLowerCase();

		if (!classes.containsKey(key))
			throw new OSchemaException("Class " + iClassName + " was not found in current database");

		classes.remove(key);
		document.setDirty();
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
	public void fromStream() {
		// READ CURRENT SCHEMA VERSION
		int schemaVersion = ((Long) document.field("schemaVersion")).intValue();
		if (schemaVersion != CURRENT_VERSION_NUMBER) {
			// HANDLE SCHEMA UPGRADE
			throw new OConfigurationException(
					"Database schema is different. Please export your old database with the previous verison of OrientDB and reimport it using the current one.");
		}

		// REGISTER ALL THE CLASSES
		classes.clear();
		OClass cls;
		List<ODocument> storedClasses = document.field("classes");
		for (ODocument c : storedClasses) {
			c.setDatabase(document.getDatabase());
			cls = new OClass(this, c);
			cls.fromStream();
			classes.put(cls.getName().toLowerCase(), cls);
		}

		// REBUILD THE INHERITANCE TREE
		String superClassName;
		OClass superClass;
		for (ODocument c : storedClasses) {
			superClassName = c.field("superClass");

			if (superClassName != null) {
				// HAS A SUPER CLASS
				cls = classes.get(((String) c.field("name")).toLowerCase());

				superClass = classes.get(superClassName.toLowerCase());

				if (superClass == null)
					throw new OConfigurationException("Super class '" + superClassName + "' was declared in class '" + cls.getName()
							+ "' but was not found in schema. Remove the dependency or create the class to continue.");

				cls.setSuperClass(superClass);
			}
		}
	}

	/**
	 * Binds POJO to ODocument.
	 */
	@OBeforeSerialization
	public ODocument toStream() {
		document.field("schemaVersion", CURRENT_VERSION_NUMBER);
		document.field("classes", classes.values(), OType.EMBEDDEDSET);
		return document;
	}

	private void registerStandardClasses() {
	}

	public Collection<OClass> getClasses() {
		return Collections.unmodifiableCollection(classes.values());
	}

	@SuppressWarnings("unchecked")
	public OSchema load() {
		document.reset();
		((ORecordId) document.getIdentity()).fromString(document.getDatabase().getStorage().getConfiguration().schemaRecordId);
		return super.load("*:-1 index:0");
	}

	public void create() {
		save(OStorageLocal.CLUSTER_INTERNAL_NAME);
	}

	public OSchema setDirty() {
		document.setDirty();
		return this;
	}
}
