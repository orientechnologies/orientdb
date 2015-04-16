/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema class
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OClass extends Comparable<OClass> {
  public static enum ATTRIBUTES {
    NAME, SHORTNAME, @Deprecated SUPERCLASS, SUPERCLASSES, OVERSIZE, STRICTMODE, ADDCLUSTER, REMOVECLUSTER, CUSTOM, ABSTRACT, CLUSTERSELECTION
  }

  public static enum INDEX_TYPE {
    UNIQUE(true), NOTUNIQUE(true), FULLTEXT(true), DICTIONARY(false), PROXY(true), UNIQUE_HASH_INDEX(true), NOTUNIQUE_HASH_INDEX(
        true), FULLTEXT_HASH_INDEX(true), DICTIONARY_HASH_INDEX(false), SPATIAL(true);

    private boolean automaticIndexable;

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

  @Deprecated
  public OClass getSuperClass();

  @Deprecated
  public OClass setSuperClass(OClass iSuperClass);
  
  public boolean hasSuperClasses();
  
  public List<String> getSuperClassesNames();
  
  public List<OClass> getSuperClasses();
  
  public OClass setSuperClasses(List<? extends OClass> classes);
  
  public OClass addSuperClass(OClass superClass);
  
  public OClass removeSuperClass(OClass superClass);

  public String getName();

  public OClass setName(String iName);

  public String getStreamableName();

  public Collection<OProperty> declaredProperties();

  public Collection<OProperty> properties();

  public Map<String, OProperty> propertiesMap();

  public Collection<OProperty> getIndexedProperties();

  public OProperty getProperty(String iPropertyName);

  public OProperty createProperty(String iPropertyName, OType iType);

  public OProperty createProperty(String iPropertyName, OType iType, OClass iLinkedClass);

  public OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType);

  public void dropProperty(String iPropertyName);

  public boolean existsProperty(String iPropertyName);

  public Class<?> getJavaClass();

  int getClusterForNewInstance(final ODocument doc);

  public int getDefaultClusterId();

  public abstract void setDefaultClusterId(int iDefaultClusterId);

  public int[] getClusterIds();

  public OClass addClusterId(int iId);

  public OClusterSelectionStrategy getClusterSelection();

  public OClass setClusterSelection(OClusterSelectionStrategy clusterSelection);

  public OClass setClusterSelection(String iStrategyName);

  public OClass addCluster(String iClusterName);

  public OClass removeClusterId(int iId);

  public int[] getPolymorphicClusterIds();

  @Deprecated
  public Collection<OClass> getBaseClasses();

  @Deprecated
  public Collection<OClass> getAllBaseClasses();

  /**
   *
   * @return all the subclasses (one level hierarchy only)
   */
  public Collection<OClass> getSubclasses();

  /**
   *
   * @return all the subclass hierarchy
   */
  public Collection<OClass> getAllSubclasses();


  public long getSize();

	public float getClassOverSize();

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
  public OClass setOverSize(float overSize);

  /**
   * Returns the number of the records of this class considering also subclasses (polymorphic).
   */
  public long count();

  /**
   * Returns the number of the records of this class and based on polymorphic parameter it consider or not the subclasses.
   */
  public long count(boolean iPolymorphic);

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
  public boolean isSubClassOf(String iClassName);

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   * 
   * @param iClass
   * @return
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(OClass iClass);

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   * 
   * @param iClass
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(OClass)
   */
  public boolean isSuperClassOf(OClass iClass);

  public String getShortName();

  public OClass setShortName(String shortName);

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
   * 
   * @param iName
   *          Database index name.
   * @param iType
   *          Index type.
   * @param iProgressListener
   *          Progress listener.
   * 
   * @param metadata
   *          Additional parameters which will be added in index configuration document as "metadata" field.
   * 
   * @param algorithm
   *          Algorithm to use for indexing.
   * 
   * @param fields
   *          Field names from which index will be created. @return Class index registered inside of given class ans associated with
   *          database index.
   */
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String algorithm, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into class instance.
   * 
   * 
   * @param iName
   *          Database index name.
   * @param iType
   *          Index type.
   * @param iProgressListener
   *          Progress listener.
   * 
   * @param metadata
   *          Additional parameters which will be added in index configuration document as "metadata" field.
   * 
   * @param fields
   *          Field names from which index will be created. @return Class index registered inside of given class ans associated with
   *          database index.
   */
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String... fields);

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
   * @return All indexes for given class, not the inherited ones.
   */
  public Set<OIndex<?>> getClassIndexes();

  /**
   * Internal.
   * 
   * @return Copy all the indexes for given class, not the inherited ones, in the collection received as argument.
   */
  void getClassIndexes(Collection<OIndex<?>> indexes);
  
  /**
   * Internal.
   * 
   * @return All indexes for given class and its super classes.
   */
  void getIndexes(Collection<OIndex<?>> indexes);

  /**
   * @return All indexes for given class and its super classes.
   */
  public Set<OIndex<?>> getIndexes();

  public String getCustom(String iName);

  public OClass setCustom(String iName, String iValue);

  public void removeCustom(String iName);

  public void clearCustom();

  public Set<String> getCustomKeys();

  boolean hasClusterId(int clusterId);
}
