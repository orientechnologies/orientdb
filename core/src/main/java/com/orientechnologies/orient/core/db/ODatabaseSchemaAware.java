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
package com.orientechnologies.orient.core.db;

/**
 * Generic interface for record based Database implementations with schema concept.
 * 
 * @author Luca Garulli
 * 
 */
public interface ODatabaseSchemaAware<T extends Object> extends ODatabase<T> {
  /**
   * Creates a new entity instance. Each database implementation will return the right type.
   * 
   * @return The new instance.
   */
  public <RET extends Object> RET newInstance(String iClassName);

  /**
   * Counts the entities contained in the specified class and sub classes (polymorphic).
   * 
   * @param iClassName
   *          Class name
   * @return Total entities
   */
  public long countClass(String iClassName);

  /**
   * Counts the entities contained in the specified class.
   * 
   * @param iClassName
   *          Class name
   * @param iPolymorphic
   *          True if consider also the sub classes, otherwise false
   * @return Total entities
   */
  public long countClass(String iClassName, final boolean iPolymorphic);
}
