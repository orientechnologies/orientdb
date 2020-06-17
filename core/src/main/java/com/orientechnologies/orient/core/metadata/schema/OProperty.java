/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;

/**
 * Contains the description of a persistent class property.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OProperty extends Comparable<OProperty> {

  public static enum ATTRIBUTES {
    LINKEDTYPE,
    LINKEDCLASS,
    MIN,
    MAX,
    MANDATORY,
    NAME,
    NOTNULL,
    REGEXP,
    TYPE,
    CUSTOM,
    READONLY,
    COLLATE,
    DEFAULT,
    DESCRIPTION
  }

  public String getName();

  /** Returns the full name as <class>.<property> */
  public String getFullName();

  public OProperty setName(String iName);

  public void set(ATTRIBUTES attribute, Object iValue);

  public OType getType();

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded
   * yet.
   *
   * @return
   */
  public OClass getLinkedClass();

  public OProperty setLinkedClass(OClass oClass);

  public OType getLinkedType();

  public OProperty setLinkedType(OType type);

  public boolean isNotNull();

  public OProperty setNotNull(boolean iNotNull);

  public OCollate getCollate();

  public OProperty setCollate(String iCollateName);

  public OProperty setCollate(OCollate collate);

  public boolean isMandatory();

  public OProperty setMandatory(boolean mandatory);

  boolean isReadonly();

  OProperty setReadonly(boolean iReadonly);

  /**
   * Min behavior depends on the Property OType.
   *
   * <p>
   *
   * <ul>
   *   <li>String : minimum length
   *   <li>Number : minimum value
   *   <li>date and time : minimum time in millisecond, date must be written in the storage date
   *       format
   *   <li>binary : minimum size of the byte array
   *   <li>List,Set,Collection : minimum size of the collection
   * </ul>
   *
   * @return String, can be null
   */
  public String getMin();

  /**
   * @see OProperty#getMin()
   * @param min can be null
   * @return this property
   */
  public OProperty setMin(String min);

  /**
   * Max behavior depends on the Property OType.
   *
   * <p>
   *
   * <ul>
   *   <li>String : maximum length
   *   <li>Number : maximum value
   *   <li>date and time : maximum time in millisecond, date must be written in the storage date
   *       format
   *   <li>binary : maximum size of the byte array
   *   <li>List,Set,Collection : maximum size of the collection
   * </ul>
   *
   * @return String, can be null
   */
  public String getMax();

  /**
   * @see OProperty#getMax()
   * @param max can be null
   * @return this property
   */
  public OProperty setMax(String max);

  /**
   * Default value for the property; can be function
   *
   * @return String, can be null
   */
  public String getDefaultValue();

  /**
   * @see OProperty#getDefaultValue()
   * @param defaultValue can be null
   * @return this property
   */
  public OProperty setDefaultValue(String defaultValue);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType One of types supported.
   *     <ul>
   *       <li>UNIQUE: Doesn't allow duplicates
   *       <li>NOTUNIQUE: Allow duplicates
   *       <li>FULLTEXT: Indexes single word for full text search
   *     </ul>
   *
   * @return see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)}.
   */
  public OIndex createIndex(final OClass.INDEX_TYPE iType);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType
   * @return see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)}.
   */
  public OIndex createIndex(final String iType);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType One of types supported.
   *     <ul>
   *       <li>UNIQUE: Doesn't allow duplicates
   *       <li>NOTUNIQUE: Allow duplicates
   *       <li>FULLTEXT: Indexes single word for full text search
   *     </ul>
   *
   * @param metadata the index metadata
   * @return see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)}.
   */
  public OIndex createIndex(String iType, ODocument metadata);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param iType One of types supported.
   *     <ul>
   *       <li>UNIQUE: Doesn't allow duplicates
   *       <li>NOTUNIQUE: Allow duplicates
   *       <li>FULLTEXT: Indexes single word for full text search
   *     </ul>
   *
   * @param metadata the index metadata
   * @return see {@link OClass#createIndex(String, OClass.INDEX_TYPE, String...)}.
   */
  public OIndex createIndex(OClass.INDEX_TYPE iType, ODocument metadata);

  /**
   * Remove the index on property
   *
   * @return
   * @deprecated Use SQL command instead.
   */
  @Deprecated
  public OProperty dropIndexes();

  /**
   * @return All indexes in which this property participates as first key item.
   * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
   */
  @Deprecated
  public Set<OIndex> getIndexes();

  /**
   * @return The first index in which this property participates as first key item.
   * @deprecated Use {@link OClass#getInvolvedIndexes(String...)} instead.
   */
  @Deprecated
  public OIndex getIndex();

  /** @return All indexes in which this property participates. */
  public Collection<OIndex> getAllIndexes();

  /**
   * Indicates whether property is contained in indexes as its first key item. If you would like to
   * fetch all indexes or check property presence in other indexes use {@link #getAllIndexes()}
   * instead.
   *
   * @return <code>true</code> if and only if this property is contained in indexes as its first key
   *     item.
   * @deprecated Use {@link OClass#areIndexed(String...)} instead.
   */
  @Deprecated
  public boolean isIndexed();

  public String getRegexp();

  public OProperty setRegexp(String regexp);

  /**
   * Change the type. It checks for compatibility between the change of type.
   *
   * @param iType
   */
  public OProperty setType(final OType iType);

  public String getCustom(final String iName);

  public OProperty setCustom(final String iName, final String iValue);

  public void removeCustom(final String iName);

  public void clearCustom();

  public Set<String> getCustomKeys();

  public OClass getOwnerClass();

  public Object get(ATTRIBUTES iAttribute);

  public Integer getId();

  public String getDescription();

  public OProperty setDescription(String iDescription);
}
