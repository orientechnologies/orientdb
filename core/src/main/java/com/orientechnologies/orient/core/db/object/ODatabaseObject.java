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
package com.orientechnologies.orient.core.db.object;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseSchemaAware;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.iterator.object.OObjectIteratorClassInterface;
import com.orientechnologies.orient.core.iterator.object.OObjectIteratorClusterInterface;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic interface for object based Database implementations. Binds to/from Document and POJOs.
 * 
 * @author Luca Garulli
 */
public interface ODatabaseObject extends ODatabaseSchemaAware<Object>, OUserObject2RecordHandler {

  /**
   * Sets as dirty a POJO. This is useful when you change the object and need to tell to the engine to treat as dirty.
   * 
   * @param iPojo
   *          User object
   */
  void setDirty(final Object iPojo);

  /**
   * Sets as not dirty a POJO. This is useful when you change some other object and need to tell to the engine to treat this one as
   * not dirty.
   * 
   * @param iPojo
   *          User object
   */
  void unsetDirty(final Object iPojo);

  /**
   * Browses all the records of the specified cluster.
   * 
   * @param iClusterName
   *          Cluster name to iterate
   * @return Iterator of Object instances
   */
  <RET> OObjectIteratorClusterInterface<RET> browseCluster(String iClusterName);

  /**
   * Browses all the records of the specified class.
   * 
   * @param iClusterClass
   *          Class name to iterate
   * @return Iterator of Object instances
   */
  <RET> OObjectIteratorClassInterface<RET> browseClass(Class<RET> iClusterClass);

  /**
   * Creates a new entity of the specified class.
   * 
   * @param iType
   *          Class name where to originate the instance
   * @return New instance
   */
  <T> T newInstance(Class<T> iType);

  /**
   * Returns the entity manager that handle the binding from ODocuments and POJOs.
   * 
   * @return
   */
  OEntityManager getEntityManager();

  boolean isRetainObjects();

  ODatabase setRetainObjects(boolean iRetainObjects);

  Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan);

  ODocument pojo2Stream(final Object iPojo, final ODocument iRecord);

  boolean isLazyLoading();

  void setLazyLoading(final boolean lazyLoading);
}
