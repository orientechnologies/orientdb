/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Optional;
import java.util.Set;

/** @author Luigi Dell'Aquila */
public interface OElement extends ORecord {

  /**
   * Returns all the names of defined properties
   *
   * @return all the names of defined properties
   */
  Set<String> getPropertyNames();

  /**
   * Gets a property given its name
   *
   * @param name the property name
   * @param <RET>
   * @return Returns the property value
   */
  <RET> RET getProperty(String name);

  /**
   * Check if a property exists in the Element
   *
   * @param propertyName
   * @return true if exists otherwise false.
   */
  boolean hasProperty(final String propertyName);

  /**
   * Sets a property value
   *
   * @param name the property name
   * @param value the property value
   */
  void setProperty(String name, Object value);

  /**
   * Sets a property value
   *
   * @param name the property name
   * @param value the property value
   * @param fieldType Forced type (not auto-determined)
   */
  void setProperty(String name, Object value, OType... fieldType);

  /**
   * Remove a property
   *
   * @param name the property name
   */
  <RET> RET removeProperty(String name);

  /**
   * Returns an instance of OVertex representing current element
   *
   * @return An OVertex that represents the current element. An empty optional if the current
   *     element is not a vertex
   */
  Optional<OVertex> asVertex();

  /**
   * Returns an instance of OEdge representing current element
   *
   * @return An OEdge that represents the current element. An empty optional if the current element
   *     is not an edge
   */
  Optional<OEdge> asEdge();

  /**
   * return true if the current element is a vertex
   *
   * @return true if the current element is a vertex
   */
  boolean isVertex();

  /**
   * return true if the current element is an edge
   *
   * @return true if the current element is an edge
   */
  boolean isEdge();

  /**
   * Returns the type of current element, ie the class in the schema (if any)
   *
   * @return the type of current element. An empty optional is returned if current element does not
   *     have a schema
   */
  Optional<OClass> getSchemaType();
}
