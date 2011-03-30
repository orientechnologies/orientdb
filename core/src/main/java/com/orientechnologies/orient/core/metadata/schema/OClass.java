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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

@SuppressWarnings("unchecked")
public class OClass extends ODocumentWrapperNoClass implements Comparable<OClass> {

	protected int											id;
	protected OSchema									owner;
	protected String									name;
	protected Class<?>								javaClass;
	protected int											fixedSize		= 0;
	protected Map<String, OProperty>	properties	= new HashMap<String, OProperty>();
	protected int[]										clusterIds;
	protected int											defaultClusterId;
	protected OClass									superClass;
	protected int[]										polymorphicClusterIds;
	protected List<OClass>						baseClasses;

	/**
	 * Constructor used in unmarshalling.
	 */
	public OClass() {
	}

	/**
	 * Constructor used in unmarshalling.
	 */
	public OClass(final OSchema iOwner) {
		document = new ODocument(iOwner.getDocument().getDatabase());
		owner = iOwner;
	}

	/**
	 * Constructor used in unmarshalling.
	 */
	public OClass(final OSchema iOwner, final ODocument iDocument) {
		document = iDocument;
		owner = iOwner;
	}

	public OClass(final OSchema iOwner, final int iId, final String iName, final String iJavaClassName, final int[] iClusterIds,
			final int iDefaultClusterId) throws ClassNotFoundException {
		this(iOwner, iId, iName, iClusterIds, iDefaultClusterId);
		javaClass = Class.forName(iJavaClassName);
	}

	public OClass(final OSchema iOwner, final int iId, final String iName, final int[] iClusterIds, final int iDefaultClusterId) {
		this(iOwner);
		id = iId;
		name = iName;
		clusterIds = iClusterIds;
		polymorphicClusterIds = iClusterIds;
		defaultClusterId = iDefaultClusterId;
	}

	/**
	 * Constructor called for inline OClass instances.
	 * 
	 * @param iDatabase
	 * 
	 * @param iClass
	 */
	public OClass(final Class<?> iClass) {
		name = iClass.getSimpleName();
	}

	public <T> T newInstance() throws InstantiationException, IllegalAccessException {
		if (javaClass == null)
			throw new IllegalArgumentException("Can't create an instance of class '" + name + "' since no Java class was specified");

		return (T) javaClass.newInstance();
	}

	public OClass getSuperClass() {
		return superClass;
	}

	/**
	 * Set the super class.
	 * 
	 * @param iSuperClass
	 *          Super class as OClass instance
	 * @return the object itself.
	 */
	public OClass setSuperClass(final OClass iSuperClass) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		this.superClass = iSuperClass;
		iSuperClass.addBaseClasses(this);
		return this;
	}

	public String getName() {
		return this.name;
	}

	public Collection<OProperty> declaredProperties() {
		return Collections.unmodifiableCollection(properties.values());
	}

	public Collection<OProperty> properties() {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

		Collection<OProperty> props = new ArrayList<OProperty>();

		OClass currentClass = this;

		do {
			if (currentClass.properties != null)
				props.addAll(currentClass.properties.values());

			currentClass = currentClass.getSuperClass();

		} while (currentClass != null);

		return props;
	}

	public OProperty getProperty(final String iPropertyName) {
		OClass currentClass = this;
		OProperty p = null;

		do {
			if (currentClass.properties != null)
				p = currentClass.properties.get(iPropertyName.toLowerCase());

			if (p != null)
				return p;

			currentClass = currentClass.getSuperClass();

		} while (currentClass != null);

		return p;
	}

	public OProperty getProperty(final int iIndex) {
		OClass currentClass = this;

		do {
			if (currentClass.properties != null)
				for (OProperty prop : currentClass.properties.values())
					if (prop.getId() == iIndex)
						return prop;

			currentClass = currentClass.getSuperClass();

		} while (currentClass != null);

		return null;
	}

	public OProperty createProperty(final String iPropertyName, final OType iType) {
		return addProperty(iPropertyName, iType, fixedSize);
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		if (iLinkedClass == null)
			throw new OSchemaException("Missed linked class");

		final OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedClass(iLinkedClass);
		return prop;
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		final OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedType(iLinkedType);
		return prop;
	}

	public void removeProperty(final String iPropertyName) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		final OProperty prop = properties.remove(iPropertyName.toLowerCase());

		if (prop == null)
			throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");

		prop.removeIndex();

		fixedSize -= prop.getType().size;
		setDirty();
	}

	public int fixedSize() {
		return fixedSize;
	}

	public boolean existsProperty(final String iPropertyName) {
		return properties.containsKey(iPropertyName.toLowerCase());
	}

	protected OProperty addProperty(String iName, final OType iType, final int iOffset) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		final String lowerName = iName.toLowerCase();

		if (properties.containsKey(lowerName))
			throw new OSchemaException("Class " + name + " already has the property '" + iName + "'");

		final OProperty prop = new OProperty(this, iName, iType, iOffset);

		properties.put(lowerName, prop);
		fixedSize += iType.size;

		setDirty();

		return prop;
	}

	@Override
	public void fromStream() {
		name = document.field("name");
		id = (Integer) document.field("id");
		defaultClusterId = (Integer) document.field("defaultClusterId");

		final Object cc = document.field("clusterIds");
		if (cc instanceof Collection<?>) {
			Collection<Integer> coll = document.field("clusterIds");
			clusterIds = new int[coll.size()];
			int i = 0;
			for (Integer item : coll)
				clusterIds[i++] = item.intValue();
		} else
			clusterIds = (int[]) cc;

		polymorphicClusterIds = clusterIds;

		// READ PROPERTIES
		OProperty prop;
		Collection<ODocument> storedProperties = document.field("properties");
		for (ODocument p : storedProperties) {
			p.setDatabase(document.getDatabase());
			prop = new OProperty(this, p);
			prop.fromStream();
			properties.put(prop.getName().toLowerCase(), prop);
		}
	}

	@Override
	@OBeforeSerialization
	public ODocument toStream() {
		document.setStatus(STATUS.UNMARSHALLING);

		try {
			document.field("name", name);
			document.field("id", id);
			document.field("defaultClusterId", defaultClusterId);
			document.field("clusterIds", clusterIds);

			Set<ODocument> props = new HashSet<ODocument>();
			for (OProperty p : properties.values()) {
				props.add(p.toStream());
			}
			document.field("properties", props, OType.EMBEDDEDSET);

			if (superClass != null)
				document.field("superClass", superClass.getName());

		} finally {
			document.setStatus(STATUS.LOADED);
		}

		return document;
	}

	public Class<?> getJavaClass() {
		return javaClass;
	}

	public int getId() {
		return id;
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public void setDefaultClusterId(final int iDefaultClusterId) {
		this.defaultClusterId = iDefaultClusterId;
		setDirty();
	}

	public int[] getClusterIds() {
		return clusterIds;
	}

	public int[] getPolymorphicClusterIds() {
		return polymorphicClusterIds;
	}

	public OClass addClusterIds(final int iId) {
		for (int currId : clusterIds)
			if (currId == iId)
				return this;

		clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
		clusterIds[clusterIds.length - 1] = iId;
		setDirty();
		return this;
	}

	public OClass setDirty() {
		document.setDirty();
		if (owner != null)
			owner.setDirty();
		return this;
	}

	public Iterator<OClass> getBaseClasses() {
		if (baseClasses == null)
			return null;

		return baseClasses.iterator();
	}

	/**
	 * Adds a base class to the current one. It adds also the base class cluster ids to the polymorphic cluster ids array.
	 * 
	 * @param iBaseClass
	 *          The base class to add.
	 */
	private OClass addBaseClasses(final OClass iBaseClass) {
		document.getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

		if (baseClasses == null)
			baseClasses = new ArrayList<OClass>();

		baseClasses.add(iBaseClass);

		// ADD CLUSTER IDS OF BASE CLASS TO THIS CLASS AND ALL SUPER-CLASSES
		OClass currentClass = this;
		while (currentClass != null) {
			currentClass.addPolymorphicClusterIds(iBaseClass);
			currentClass = currentClass.getSuperClass();
		}

		return this;
	}

	@Override
	public String toString() {
		return name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((owner == null) ? 0 : owner.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OClass other = (OClass) obj;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		return true;
	}

	public int compareTo(OClass o) {
		return id - o.getId();
	}

	/**
	 * Returns the number of the records of this class.
	 */
	public long count() {
		return owner.getDocument().getDatabase().countClusterElements(clusterIds);
	}

	/**
	 * Truncates all the clusters the class uses.
	 * 
	 * @throws IOException
	 */
	public void truncate() throws IOException {
		for (int id : clusterIds) {
			owner.getDocument().getDatabase().getStorage().getClusterById(id).truncate();
		}
	}

	/**
	 * Returns true if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClass
	 * @return
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final String iClassName) {
		return isSubClassOf(owner.getClass(iClassName));
	}

	/**
	 * Returns true if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClass
	 * @return
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final OClass iClass) {
		if (iClass == null)
			return false;

		OClass cls = this;
		while (cls != null) {
			if (cls.getName().equals(iClass.getName()))
				return true;
			cls = cls.getSuperClass();
		}
		return false;
	}

	/**
	 * Returns true if the passed schema class (iClass) extends the current instance.
	 * 
	 * @param iClass
	 * @return Returns true if the passed schema class extends the current instance
	 * @see #isSubClassOf(OClass)
	 */
	public boolean isSuperClassOf(final OClass iClass) {
		OClass cls = iClass;
		while (cls != null) {
			if (cls.equals(this))
				return true;
			cls = cls.getSuperClass();
		}
		return false;
	}

	/**
	 * Add different cluster ids to the "polymorphic cluster ids" array.
	 */
	private void addPolymorphicClusterIds(final OClass iBaseClass) {
		boolean found;
		for (int i : iBaseClass.polymorphicClusterIds) {
			found = false;
			for (int k : clusterIds) {
				if (i == k) {
					found = true;
					break;
				}
			}

			if (!found) {
				// ADD IT
				polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
				polymorphicClusterIds[polymorphicClusterIds.length - 1] = i;
			}
		}
	}
}
