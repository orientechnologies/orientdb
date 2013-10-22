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

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

public interface OSchema {

  public int countClasses();

  public OClass createClass(final Class<?> iClass);

  public OClass createClass(final Class<?> iClass, final int iDefaultClusterId);

  public OClass createClass(final String iClassName);

  public OClass createClass(final String iClassName, final OClass iSuperClass);

  public OClass createClass(final String iClassName, final OClass iSuperClass, final OStorage.CLUSTER_TYPE iType);

  public OClass createClass(final String iClassName, final int iDefaultClusterId);

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId);

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds);

  public OClass createAbstractClass(final Class<?> iClass);

  public OClass createAbstractClass(final String iClassName);

  public OClass createAbstractClass(final String iClassName, final OClass iSuperClass);

  public void dropClass(final String iClassName);

  public <RET extends ODocumentWrapper> RET reload();

  public boolean existsClass(final String iClassName);

  public OClass getClass(final Class<?> iClass);

  /**
   * Returns the OClass instance by class name.
   * 
   * If the class is not configured and the database has an entity manager with the requested class as registered, then creates a
   * schema class for it at the fly.
   * 
   * If the database nor the entity manager have not registered class with specified name, returns null.
   * 
   * @param iClassName
   *          Name of the class to retrieve
   * @return class instance or null if class with given name is not configured.
   */
  public OClass getClass(final String iClassName);

  public OClass getOrCreateClass(final String iClassName);

  public OClass getOrCreateClass(final String iClassName, final OClass iSuperClass);

  public Collection<OClass> getClasses();

  public void create();

  @Deprecated
  public int getVersion();

  public ORID getIdentity();

  /**
   * Do nothing. Starting from 1.0rc2 the schema is auto saved!
   * 
   * @COMPATIBILITY 1.0rc1
   */
  public <RET extends ODocumentWrapper> RET save();

  /**
   * Returns all the classes that rely on a cluster
   * 
   * @param name
   *          Cluster name
   */
  public Set<OClass> getClassesRelyOnCluster(String iClusterName);
}
