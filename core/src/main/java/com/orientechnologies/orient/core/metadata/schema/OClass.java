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
  enum ATTRIBUTES {
    NAME, SHORTNAME, SUPERCLASS, SUPERCLASSES, OVERSIZE, STRICTMODE, ADDCLUSTER, REMOVECLUSTER, CUSTOM, ABSTRACT, CLUSTERSELECTION
  }

  enum INDEX_TYPE {
    UNIQUE(true), NOTUNIQUE(true), FULLTEXT(true), DICTIONARY(false), PROXY(true), UNIQUE_HASH_INDEX(true), NOTUNIQUE_HASH_INDEX(
        true), FULLTEXT_HASH_INDEX(true), DICTIONARY_HASH_INDEX(false), SPATIAL(true);

    private boolean automaticIndexable;

    INDEX_TYPE(boolean iValue) {
      automaticIndexable = iValue;
    }

    boolean isAutomaticIndexable() {
      return automaticIndexable;
    }
  }

  <T> T newInstance() throws InstantiationException, IllegalAccessException;

  boolean isAbstract();

  OClass setAbstract(boolean iAbstract);

  boolean isStrictMode();

  OClass setStrictMode(boolean iMode);

  @Deprecated
  OClass getSuperClass();

  @Deprecated
  OClass setSuperClass(OClass iSuperClass);

  boolean hasSuperClasses();

  List<String> getSuperClassesNames();

  List<OClass> getSuperClasses();

  OClass setSuperClasses(List<? extends OClass> classes);

  OClass addSuperClass(OClass superClass);

  OClass removeSuperClass(OClass superClass);

  String getName();

  OClass setName(String iName);

  String getStreamableName();

  Collection<OProperty> declaredProperties();

  Collection<OProperty> properties();

  Map<String, OProperty> propertiesMap();

  Collection<OProperty> getIndexedProperties();

  OProperty getProperty(String iPropertyName);

  OProperty createProperty(String iPropertyName, OType iType);

  OProperty createProperty(String iPropertyName, OType iType, OClass iLinkedClass);

  OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType);

  void dropProperty(String iPropertyName);

  boolean existsProperty(String iPropertyName);

  Class<?> getJavaClass();

  int getClusterForNewInstance(ODocument doc);

  int getDefaultClusterId();

  abstract void setDefaultClusterId(int iDefaultClusterId);

  int[] getClusterIds();

  OClass addClusterId(int iId);

  OClusterSelectionStrategy getClusterSelection();

  OClass setClusterSelection(OClusterSelectionStrategy clusterSelection);

  OClass setClusterSelection(String iStrategyName);

  OClass addCluster(String iClusterName);

  OClass removeClusterId(int iId);

  int[] getPolymorphicClusterIds();

  @Deprecated
  Collection<OClass> getBaseClasses();

  @Deprecated
  Collection<OClass> getAllBaseClasses();

  /**
   *
   * @return all the subclasses (one level hierarchy only)
   */
  Collection<OClass> getSubclasses();

  /**
   *
   * @return all the subclass hierarchy
   */
  Collection<OClass> getAllSubclasses();

  /**
   * @return all recursively collected super classes
   */
  Collection<OClass> getAllSuperClasses();

  long getSize();

  float getClassOverSize();

  /**
   * Returns the oversize factor. Oversize is used to extend the record size by a factor to avoid defragmentation upon updates. 0 or
   * 1.0 means no oversize.
   * 
   * @return Oversize factor
   * @see #setOverSize(float)
   */
  float getOverSize();

  /**
   * Sets the oversize factor. Oversize is used to extend the record size by a factor to avoid defragmentation upon updates. 0 or
   * 1.0 means no oversize. Default is 0.
   * 
   * @return Oversize factor
   * @see #getOverSize()
   */
  OClass setOverSize(float overSize);

  /**
   * Returns the number of the records of this class considering also subclasses (polymorphic).
   */
  long count();

  /**
   * Returns the number of the records of this class and based on polymorphic parameter it consider or not the subclasses.
   */
  long count(boolean iPolymorphic);

  /**
   * Truncates all the clusters the class uses.
   * 
   * @throws IOException
   */
  void truncate() throws IOException;

  /**
   * Tells if the current instance extends the passed schema class (iClass).
   * 
   * @param iClassName
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(OClass)
   */
  boolean isSubClassOf(String iClassName);

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   * 
   * @param iClass
   * @return
   * @see #isSuperClassOf(OClass)
   */
  boolean isSubClassOf(OClass iClass);

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   * 
   * @param iClass
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(OClass)
   */
  boolean isSuperClassOf(OClass iClass);

  String getShortName();

  OClass setShortName(String shortName);

  Object get(ATTRIBUTES iAttribute);

  OClass set(ATTRIBUTES attribute, Object iValue);

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
  OIndex<?> createIndex(String iName, INDEX_TYPE iType, String... fields);

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
  OIndex<?> createIndex(String iName, String iType, String... fields);

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
  OIndex<?> createIndex(String iName, INDEX_TYPE iType, OProgressListener iProgressListener, String... fields);

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
  OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata, String algorithm,
      String... fields);

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
  OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata, String... fields);

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
  Set<OIndex<?>> getInvolvedIndexes(Collection<String> fields);

  /**
   * 
   * 
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * 
   * @see #getInvolvedIndexes(java.util.Collection)
   */
  Set<OIndex<?>> getInvolvedIndexes(String... fields);

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
  Set<OIndex<?>> getClassInvolvedIndexes(Collection<String> fields);

  /**
   * 
   * 
   * @param fields
   *          Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * 
   * @see #getClassInvolvedIndexes(java.util.Collection)
   */
  Set<OIndex<?>> getClassInvolvedIndexes(String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of fields does not matter. If there
   * are indexes for the given set of fields in super class they will be taken into account.
   * 
   * @param fields
   *          Field names.
   * 
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  boolean areIndexed(Collection<String> fields);

  /**
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(java.util.Collection)
   */
  boolean areIndexed(String... fields);

  /**
   * Returns index instance by database index name.
   * 
   * @param iName
   *          Database index name.
   * @return Index instance.
   */
  OIndex<?> getClassIndex(String iName);

  /**
   * @return All indexes for given class, not the inherited ones.
   */
  Set<OIndex<?>> getClassIndexes();

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
  Set<OIndex<?>> getIndexes();

  String getCustom(String iName);

  OClass setCustom(String iName, String iValue);

  void removeCustom(String iName);

  void clearCustom();

  Set<String> getCustomKeys();

  boolean hasClusterId(int clusterId);

  boolean hasPolymorphicClusterId(int clusterId);
}
