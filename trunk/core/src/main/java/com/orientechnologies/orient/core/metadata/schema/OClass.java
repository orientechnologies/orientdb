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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.ORecordPositional;
import com.orientechnologies.orient.core.serialization.OSerializableRecordPositional;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerCSVAbstract;

public class OClass implements OSerializableRecordPositional {
	protected int											id;
	protected OSchema									owner;
	protected String									name;
	protected Class<?>								javaClass;
	protected int											fixedSize		= 0;
	protected Map<String, OProperty>	properties	= new LinkedHashMap<String, OProperty>();
	protected int[]										clusterIds;
	protected int											defaultClusterId;

	public OClass(OSchema iOwner, int iId, String iName, String iJavaClassName, int[] iClusterIds, int iDefaultClusterId)
			throws ClassNotFoundException {
		this(iOwner, iId, iName, iClusterIds, iDefaultClusterId);
		javaClass = Class.forName(iJavaClassName);
	}

	public OClass(OSchema iOwner, int iId, String iName, int[] iClusterIds, int iDefaultClusterId) {
		id = iId;
		owner = iOwner;
		name = iName;
		clusterIds = iClusterIds;
		defaultClusterId = iDefaultClusterId;
	}

	/**
	 * Constructor called for inline OClass instances.
	 * 
	 * @param iClass
	 */
	public OClass(Class<?> iClass) {
		name = iClass.getSimpleName();
	}

	@SuppressWarnings("unchecked")
	public <T> T newInstance() throws InstantiationException, IllegalAccessException {
		if (javaClass == null)
			throw new IllegalArgumentException("Can't create an instance of class '" + name + "' since no Java class was specified");

		return (T) javaClass.newInstance();
	}

	public String getName() {
		return this.name;
	}

	public Collection<OProperty> properties() {
		return Collections.unmodifiableCollection(properties.values());
	}

	public OProperty getProperty(String iPropertyName) {
		return properties.get(iPropertyName.toLowerCase());
	}

	public OProperty getProperty(int iIndex) {
		for (OProperty prop : properties.values())
			if (prop.getId() == iIndex)
				return prop;

		throw new OSchemaException("Property with index " + iIndex + " was not found in class: " + name);
	}

	public int getPropertyIndex(String iPropertyName) {
		OProperty prop = properties.get(iPropertyName.toLowerCase());
		if (prop == null)
			return -1;
		return prop.getId();
	}

	public OProperty createProperty(String iPropertyName, OType iType) {
		if (iType == OType.LINK || iType == OType.LINKLIST || iType == OType.LINKSET)
			throw new OSchemaException("Can't add property '" + iPropertyName
					+ "' since it contains a relationship but no linked class was received");

		return addProperty(iPropertyName, iType, fixedSize);
	}

	public OProperty createProperty(String iPropertyName, OType iType, OClass iLinkedClass) {
		OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedClass(iLinkedClass);
		return prop;
	}

	public OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType) {
		OProperty prop = addProperty(iPropertyName, iType, fixedSize);
		prop.setLinkedType(iLinkedType);
		return prop;
	}

	public int fixedSize() {
		return fixedSize;
	}

	public boolean existsProperty(String iPropertyName) {
		return properties.containsKey(iPropertyName);
	}

	protected OProperty addProperty(String iName, OType iType, int iOffset) {
		OProperty prop = new OProperty(this, iName, iType, iOffset);

		properties.put(iName.toLowerCase(), prop);
		fixedSize += iType.size;

		owner.setDirty();

		return prop;
	}

	public void fromStream(ORecordPositional<String> iRecord) {

		defaultClusterId = Integer.parseInt(iRecord.next());

		// READ CLUSTER IDS
		clusterIds = ORecordSerializerCSVAbstract.splitIntArray(iRecord.next());

		// READ PROPERTIES
		int propsNum = Integer.parseInt(iRecord.next());
		OProperty prop;
		for (int k = 0; k < propsNum; ++k) {
			prop = new OProperty(this);
			prop.fromStream(iRecord);
			properties.put(prop.getName().toLowerCase(), prop);
		}
	}

	public void toStream(ORecordPositional<String> iRecord) {
		iRecord.add(name);

		iRecord.add(String.valueOf(defaultClusterId));

		// WRITE CLUSTER IDS
		iRecord.add(ORecordSerializerCSVAbstract.joinIntArray(clusterIds));

		// WRITE PROPERTIES
		iRecord.add(String.valueOf(properties.size()));
		for (OProperty prop : properties.values()) {
			prop.toStream(iRecord);
		}
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

	public void setDefaultClusterId(int iDefaultClusterId) {
		this.defaultClusterId = iDefaultClusterId;
		owner.setDirty();
	}

	public int[] getClusterIds() {
		return clusterIds;
	}

	public OClass addClusterIds(int iId) {
		for (int currId : clusterIds)
			if (currId == iId)
				return this;

		clusterIds = Arrays.copyOf(clusterIds, clusterIds.length + 1);
		owner.setDirty();
		return this;
	}

	@Override
	public String toString() {
		return name + " (id=" + id + ", cluster=" + defaultClusterId + ")";
	}
}
