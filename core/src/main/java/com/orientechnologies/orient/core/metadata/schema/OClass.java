/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OIndex;

/**
 * Schema class
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OClass extends Comparable<OClass> {
  public static enum ATTRIBUTES {
    NAME, SHORTNAME, SUPERCLASS, OVERSIZE, STRICTMODE, ADDCLUSTER, REMOVECLUSTER, CUSTOM, ABSTRACT
  }

  public static enum INDEX_TYPE {
    UNIQUE(true), NOTUNIQUE(true), FULLTEXT(true), DICTIONARY(false), PROXY(true), UNIQUE_HASH_INDEX(true), NOTUNIQUE_HASH_INDEX(
        true), FULLTEXT_HASH_INDEX(true), DICTIONARY_HASH_INDEX(false);

    private final boolean automaticIndexable;

    INDEX_TYPE(boolean iValue) {
      automaticIndexable = iValue;
    }

    public boolean isAutomaticIndexable() {
      return automaticIndexable;
    }
  }

  public <T> T newInstance() throws InstantiationException, IllegalAccessException;

  public boolean isAbstract();

  public OClass setAbstract(boolean iAbstract);

  public boolean isStrictMode();

  public OClass setStrictMode(boolean iMode);

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

  public OClass addClusterId(final int iId);

  public OClass removeClusterId(final int iId);

  public int[] getPolymorphicClusterIds();

  public Iterator<OClass> getBaseClasses();

  public long getSize();

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
   * @param iClassName
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

  /**
   * Creates database index that is based on passed in field names. Given index will be added into class instance and associated
   * with database index.
   * 
   * @param fields
   *          Field names from which index will be created.
   * @param iName
   *          Database index name
   * @param iType
   *          Index type.
   * 
   * @return Class index registered inside of given class ans associated with database index.
   */
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into class instance and associated
   * with database index.
   * 
   * @param fields
   *          Field names from which index will be created.
   * @param iName
   *          Database index name
   * @param iType
   *          Index type.
   * 
   * @return Class index registered inside of given class ans associated with database index.
   */
  public OIndex<?> createIndex(String iName, String iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into class instance.
   * 
   * @param fields
   *          Field names from which index will be created.
   * @param iName
   *          Database index name.
   * @param iType
   *          Index type.
   * @param iProgressListener
   *          Progress listener.
   * 
   * @return Class index registered inside of given class ans associated with database index.
   */
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, OProgressListener iProgressListener, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into class instance.
   * 
   * @param fields
   *          Field names from which index will be created.
   * @param iName
   *          Database index name.
   * @param iType
   *          Index type.
   * @param iProgressListener
   *          Progress listener.
   * 
   * @return Class index registered inside of given class ans associated with database index.
   */
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of fields does not matter.
   * 
   * All indexes sorted by their count of parameters in ascending order. If there are indexes for the given set of fields in super
   * class they will be taken into account.
   * 
   * 
   * 
   * @param fields
   *          Field names.
   * 
   * @return list of indexes that contain passed in fields names as their first keys.
   * 
   * @see com.orientechnologies.orient.core.index.OIndexDefinition#getParamCount()
   */
  public Set<OIndex<?>> getInvolvedIndexes(Collection<String> fields);

  /**
   * 
   * 
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * 
   * @see #getInvolvedIndexes(java.util.Collection)
   */
  public Set<OIndex<?>> getInvolvedIndexes(String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of fields does not matter.
   * 
   * Indexes that related only to the given class will be returned.
   * 
   * 
   * 
   * @param fields
   *          Field names.
   * 
   * @return list of indexes that contain passed in fields names as their first keys.
   * 
   * @see com.orientechnologies.orient.core.index.OIndexDefinition#getParamCount()
   */
  public Set<OIndex<?>> getClassInvolvedIndexes(Collection<String> fields);

  /**
   * 
   * 
   * @param fields
   *          Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * 
   * @see #getClassInvolvedIndexes(java.util.Collection)
   */
  public Set<OIndex<?>> getClassInvolvedIndexes(String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of fields does not matter. If there
   * are indexes for the given set of fields in super class they will be taken into account.
   * 
   * @param fields
   *          Field names.
   * 
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  public boolean areIndexed(Collection<String> fields);

  /**
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(java.util.Collection)
   */
  public boolean areIndexed(String... fields);

  /**
   * Returns index instance by database index name.
   * 
   * @param iName
   *          Database index name.
   * @return Index instance.
   */
  public OIndex<?> getClassIndex(String iName);

  /**
   * @return All indexes for given class.
   */
  public Set<OIndex<?>> getClassIndexes();

  /**
   * @return All indexes for given class and its super classes.
   */
  public Set<OIndex<?>> getIndexes();

  public abstract void setDefaultClusterId(final int iDefaultClusterId);

  public String getCustom(final String iName);

  public OClassImpl setCustom(final String iName, final String iValue);

  public void removeCustom(final String iName);

  public void clearCustom();

  public Set<String> getCustomKeys();
}
