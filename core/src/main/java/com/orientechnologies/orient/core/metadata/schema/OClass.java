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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OClass extends OSchemaRecord {

	protected int											id;
	protected OSchema									owner;
	protected String									name;
	protected Class<?>								javaClass;
	protected int											fixedSize		= 0;
	protected Map<String, OProperty>	properties	= new LinkedHashMap<String, OProperty>();
	protected int[]										clusterIds;
	protected int											defaultClusterId;
	protected OClass									superClass;
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
		owner = iOwner;
		database = iOwner.getDatabase();
	}

	public OClass(final OSchema iOwner, final int iId, final String iName, final String iJavaClassName, final int[] iClusterIds,
			final int iDefaultClusterId) throws ClassNotFoundException {
		this(iOwner, iId, iName, iClusterIds, iDefaultClusterId);
		javaClass = Class.forName(iJavaClassName);
	}

	public OClass(final OSchema iOwner, final int iId, final String iName, final int[] iClusterIds, final int iDefaultClusterId) {
		super(iOwner.getDatabase());
		id = iId;
		owner = iOwner;
		name = iName;
		clusterIds = iClusterIds;
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

	@SuppressWarnings("unchecked")
	public <T> T newInstance() throws InstantiationException, IllegalAccessException {
		if (javaClass == null)
			throw new IllegalArgumentException("Can't create an instance of class '" + name + "' since no Java class was specified");

		return (T) javaClass.newInstance();
	}

	public OClass getSuperClass() {
		return superClass;
	}

	public OClass setSuperClass(final OClass iSuperClass) {
		this.superClass = iSuperClass;
		iSuperClass.addBaseClasses(this);
		return this;
	}

	public String getName() {
		return this.name;
	}

	public Collection<OProperty> properties() {
		return Collections.unmodifiableCollection(properties.values());
	}

	public OProperty getProperty(final String iPropertyName) {
		return properties.get(iPropertyName.toLowerCase());
	}

	public OProperty getProperty(final int iIndex) {
		for (OProperty prop : properties.values())
			if (prop.getId() == iIndex)
				return prop;

		throw new OSchemaException("Property with index " + iIndex + " was not found in class: " + name);
	}

	public int getPropertyIndex(final String iPropertyName) {
		OProperty prop = properties.get(iPropertyName.toLowerCase());
		if (prop == null)
			return -1;
		return prop.getId();
	}

	public OProperty createProperty(final String iPropertyName, final OType iType) {
		if (iType == OType.LINK || iType == OType.LINKLIST || iType == OType.LINKSET)
			throw new OSchemaException("Can't add property '" + iPropertyName
					+ "' since it contains a relationship but no linked class was received");

		return addProperty(iPropertyName, iType, fixedSize);
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
		if (iLinkedClass == null)
			throw new OSchemaException("Missed linked class");

		OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedClass(iLinkedClass);
		return prop;
	}

	public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
		OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedType(iLinkedType);
		return prop;
	}

	public void removeProperty(final String iPropertyName) {
		OProperty prop = properties.remove(iPropertyName.toLowerCase());

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
		return properties.containsKey(iPropertyName);
	}

	protected OProperty addProperty(String iName, final OType iType, final int iOffset) {
		final String lowerName = iName.toLowerCase();

		if (properties.containsKey(lowerName))
			throw new OSchemaException("Class " + name + " already has the property '" + iName + "'");

		OProperty prop = new OProperty(this, iName, iType, iOffset);

		properties.put(lowerName, prop);
		fixedSize += iType.size;

		setDirty();

		return prop;
	}

	public OClass fromDocument(final ODocument iSource) {
		name = iSource.field("name");
		id = ((Long) iSource.field("id")).intValue();
		defaultClusterId = ((Long) iSource.field("defaultClusterId")).intValue();

		Collection<Long> coll = iSource.field("clusterIds");
		clusterIds = new int[coll.size()];
		int i = 0;
		for (Long item : coll)
			clusterIds[i++] = item.intValue();

		// READ PROPERTIES
		OProperty prop;
		List<ODocument> storedProperties = iSource.field("properties");
		for (ODocument p : storedProperties) {
			p.setDatabase(database);
			prop = new OProperty(this).fromDocument(p);
			properties.put(prop.getName().toLowerCase(), prop);
		}

		return this;
	}

	@Override
	public byte[] toStream() {
		field("name", name);
		field("id", id);
		field("defaultClusterId", defaultClusterId);
		field("clusterIds", clusterIds);
		field("properties", properties.values(), OType.EMBEDDEDSET);
		if (superClass != null)
			field("superClass", superClass.getName());
		return super.toStream();
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

	public OClass addClusterIds(final int iId) {
		for (int currId : clusterIds)
			if (currId == iId)
				return this;

		clusterIds = Arrays.copyOf(clusterIds, clusterIds.length + 1);
		setDirty();
		return this;
	}

	@Override
	public OClass setDirty() {
		super.setDirty();
		if (owner != null)
			owner.setDirty();
		return this;
	}

	public Iterator<OClass> getBaseClasses() {
		return baseClasses.iterator();
	}

	public void addBaseClasses(final OClass iBaseClass) {
		if (baseClasses == null)
			baseClasses = new ArrayList<OClass>();
		baseClasses.add(iBaseClass);
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
}
