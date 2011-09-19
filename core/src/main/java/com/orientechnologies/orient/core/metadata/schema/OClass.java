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
import java.util.Collection;
import java.util.Iterator;

/**
 * Schema class
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OClass extends Comparable<OClass> {
	public static enum ATTRIBUTES {
		NAME, SHORTNAME, SUPERCLASS, OVERSIZE
	}

	public <T> T newInstance() throws InstantiationException, IllegalAccessException;

	public OClass getSuperClass();

	public OClass setSuperClass(OClass iSuperClass);

	public String getName();

	public String getStreamableName();

	public Collection<OProperty> declaredProperties();

	public Collection<OProperty> properties();

	public Collection<OProperty> getIndexedProperties();

	public OProperty getProperty(final String iPropertyName);

	public OProperty createProperty(final String iPropertyName, final OType iType);

	public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass);

	public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType);

	public void dropProperty(final String iPropertyName);

	public boolean existsProperty(final String iPropertyName);

	public Class<?> getJavaClass();

	public int getDefaultClusterId();

	public int[] getClusterIds();

	public int[] getPolymorphicClusterIds();

	public Iterator<OClass> getBaseClasses();

	/**
	 * Returns the oversize factor. Oversize is used to extend the record size by a factor to avoid defragmentation upon updates. 0 or
	 * 1.0 means no oversize.
	 * 
	 * @return Oversize factor
	 * @see #setOverSize(float)
	 */
	public float getOverSize();

	/**
	 * Sets the oversize factor. Oversize is used to extend the record size by a factor to avoid defragmentation upon updates. 0 or
	 * 1.0 means no oversize. Default is 0.
	 * 
	 * @return Oversize factor
	 * @see #getOverSize()
	 */
	public OClass setOverSize(final float overSize);

	/**
	 * Returns the number of the records of this class considering also subclasses (polymorphic).
	 */
	public long count();

	/**
	 * Returns the number of the records of this class and based on polymorphic parameter it consider or not the subclasses.
	 */
	public long count(final boolean iPolymorphic);

	/**
	 * Truncates all the clusters the class uses.
	 * 
	 * @throws IOException
	 */
	public void truncate() throws IOException;

	/**
	 * Tells if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClass
	 * @return true if the current instance extends the passed schema class (iClass).
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final String iClassName);

	/**
	 * Returns true if the current instance extends the passed schema class (iClass).
	 * 
	 * @param iClass
	 * @return
	 * @see #isSuperClassOf(OClass)
	 */
	public boolean isSubClassOf(final OClass iClass);

	/**
	 * Returns true if the passed schema class (iClass) extends the current instance.
	 * 
	 * @param iClass
	 * @return Returns true if the passed schema class extends the current instance
	 * @see #isSubClassOf(OClass)
	 */
	public boolean isSuperClassOf(final OClass iClass);

	public String getShortName();

	public OClass setShortName(final String shortName);

	public Object get(ATTRIBUTES iAttribute);

	public OClass set(ATTRIBUTES attribute, Object iValue);

	public abstract void setDefaultClusterId(final int iDefaultClusterId);
}
