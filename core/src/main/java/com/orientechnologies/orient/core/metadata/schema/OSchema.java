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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface OSchema {

  int countClasses();

  OClass createClass(String iClassName);

  OClass createClass(String iClassName, OClass iSuperClass);

  OClass createClass(String className, int clusters, OClass... superClasses);

  OClass createClass(String iClassName, OClass... superClasses);

  OClass createClass(String iClassName, OClass iSuperClass, int[] iClusterIds);

  OClass createClass(String className, int[] clusterIds, OClass... superClasses);

  OClass createAbstractClass(Class<?> iClass);

  OClass createAbstractClass(String iClassName);

  OClass createAbstractClass(String iClassName, OClass iSuperClass);

  OClass createAbstractClass(String iClassName, OClass... superClasses);

  void dropClass(String iClassName);

  <RET extends ODocumentWrapper> RET reload();

  boolean existsClass(String iClassName);

  OClass getClass(Class<?> iClass);

  /**
   * Returns the OClass instance by class name.
   * <p>
   * If the class is not configured and the database has an entity manager with the requested class as registered, then creates a
   * schema class for it at the fly.
   * <p>
   * If the database nor the entity manager have not registered class with specified name, returns null.
   *
   * @param iClassName Name of the class to retrieve
   *
   * @return class instance or null if class with given name is not configured.
   */
  OClass getClass(String iClassName);

  OClass getOrCreateClass(String iClassName);

  OClass getOrCreateClass(String iClassName, OClass iSuperClass);

  OClass getOrCreateClass(String iClassName, OClass... superClasses);

  Collection<OClass> getClasses();

  @Deprecated
  void create();

  @Deprecated
  int getVersion();

  ORID getIdentity();

  /**
   * Do nothing. Starting from 1.0rc2 the schema is auto saved!
   *
   * @COMPATIBILITY 1.0rc1
   */
  @Deprecated
  <RET extends ODocumentWrapper> RET save();

  /**
   * Returns all the classes that rely on a cluster
   *
   * @param iClusterName Cluster name
   */
  Set<OClass> getClassesRelyOnCluster(String iClusterName);

  OClass getClassByClusterId(int clusterId);

  OGlobalProperty getGlobalPropertyById(int id);

  List<OGlobalProperty> getGlobalProperties();

  OGlobalProperty createGlobalProperty(String name, OType type, Integer id);

  OClusterSelectionFactory getClusterSelectionFactory();

  OImmutableSchema makeSnapshot();

}
