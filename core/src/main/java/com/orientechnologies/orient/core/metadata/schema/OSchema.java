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
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

public class OSchema extends ODocumentWrapperNoClass {
	protected Map<String, OClass>	classes									= new LinkedHashMap<String, OClass>();
	private static final int			CURRENT_VERSION_NUMBER	= 4;

	public OSchema(final ODatabaseRecord iDatabaseOwner, final int schemaClusterId) {
		super(new ODocument(iDatabaseOwner));
		registerStandardClasses();
	}

	public Collection<OClass> classes() {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);
		return Collections.unmodifiableCollection(classes.values());
	}

	public OClass createClass(final Class<?> iClass) {
		return createClass(iClass.getSimpleName(), OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
		return createClass(iClass.getSimpleName(), iDefaultClusterId);
	}

	public OClass createClass(final String iClassName) {
		return createClass(iClassName, OStorage.CLUSTER_TYPE.PHYSICAL);
	}

	public OClass createClass(final String iClassName, final OStorage.CLUSTER_TYPE iType) {
		int clusterId = document.getDatabase().getClusterIdByName(iClassName);
		if (clusterId == -1) {
			// CREATE A NEW CLUSTER
			clusterId = document.getDatabase().getStorage().addCluster(iClassName, iType);
		}

		return createClass(iClassName, clusterId);
	}

	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		return createClass(iClassName, new int[] { iDefaultClusterId }, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final int[] iClusterIds) {
		return createClass(iClassName, iClusterIds, -1);
	}

	public OClass createClass(final String iClassName, final int[] iClusterIds, final int iDefaultClusterId) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_CREATE);

		final String key = iClassName.toLowerCase();

		if (classes.containsKey(key))
			throw new OSchemaException("Class " + iClassName + " already exists in current database");

		final OClass cls = new OClass(this, classes.size(), iClassName, iClusterIds, iDefaultClusterId);
		classes.put(key, cls);
		document.setDirty();

		return cls;
	}

	public void removeClass(final String iClassName) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

		final String key = iClassName.toLowerCase();

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

	public OClass getClass(final Class<?> iClass) {
		return getClass(iClass.getSimpleName());
	}

	/**
	 * Returns the OClass instance by class name. If the class is not configured and the database has an entity manager with the
	 * requested class as registered, then creates a schema class for it at the fly.
	 * 
	 * @param iClassName
	 *          Name of the class to retrieve
	 * @return
	 */
	public OClass getClass(final String iClassName) {
		if (iClassName == null)
			return null;

		OClass cls = classes.get(iClassName.toLowerCase());

		if (cls == null) {
			// CHECK IF CAN AUTO-CREATE IT
			final ODatabase ownerDb = document.getDatabase().getDatabaseOwner();
			if (ownerDb instanceof ODatabaseObjectTx) {
				final Class<?> javaClass = ((ODatabaseObjectTx) ownerDb).getEntityManager().getEntityClass(iClassName);

				if (javaClass != null) {
					// AUTO REGISTER THE CLASS AT FIRST USE
					cls = createClass(iClassName);
					save();
				}
			}
		}

		return cls;
	}

	/**
	 * Binds ODocument to POJO.
	 */
	@Override
	public void fromStream() {
		// READ CURRENT SCHEMA VERSION
		int schemaVersion = (Integer) document.field("schemaVersion");
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
	@Override
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

	@Override
	@SuppressWarnings("unchecked")
	public OSchema load() {
		document.reset();
		((ORecordId) document.getIdentity()).fromString(document.getDatabase().getStorage().getConfiguration().schemaRecordId);
		return super.load("*:-1 index:0");
	}

	public void create() {
		save(OStorage.CLUSTER_INTERNAL_NAME);
		document.getDatabase().getStorage().getConfiguration().schemaRecordId = document.getIdentity().toString();
		document.getDatabase().getStorage().getConfiguration().update();
	}

	public OSchema setDirty() {
		document.setDirty();
		return this;
	}
}
