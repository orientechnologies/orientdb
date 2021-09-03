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
package com.orientechnologies.orient.core.db.object;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.iterator.object.OObjectIteratorClassInterface;
import com.orientechnologies.orient.core.iterator.object.OObjectIteratorClusterInterface;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.metadata.OMetadataObject;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Generic interface for object based Database implementations. Binds to/from Document and POJOs.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabaseObject extends ODatabase<Object>, OUserObject2RecordHandler {

  /**
   * Sets as dirty a POJO. This is useful when you change the object and need to tell to the engine
   * to treat as dirty.
   *
   * @param iPojo User object
   */
  void setDirty(final Object iPojo);

  /**
   * Sets as not dirty a POJO. This is useful when you change some other object and need to tell to
   * the engine to treat this one as not dirty.
   *
   * @param iPojo User object
   */
  void unsetDirty(final Object iPojo);

  /**
   * Browses all the records of the specified cluster.
   *
   * @param iClusterName Cluster name to iterate
   * @return Iterator of Object instances
   */
  <RET> OObjectIteratorClusterInterface<RET> browseCluster(String iClusterName);

  /**
   * Browses all the records of the specified class.
   *
   * @param iClusterClass Class name to iterate
   * @return Iterator of Object instances
   */
  <RET> OObjectIteratorClassInterface<RET> browseClass(Class<RET> iClusterClass);

  /**
   * Creates a new entity instance. Each database implementation will return the right type.
   *
   * @return The new instance.
   */
  <RET extends Object> RET newInstance(String iClassName);

  /**
   * Counts the entities contained in the specified class and sub classes (polymorphic).
   *
   * @param iClassName Class name
   * @return Total entities
   */
  long countClass(String iClassName);

  /**
   * Counts the entities contained in the specified class.
   *
   * @param iClassName Class name
   * @param iPolymorphic True if consider also the sub classes, otherwise false
   * @return Total entities
   */
  long countClass(String iClassName, final boolean iPolymorphic);

  /**
   * Creates a new entity of the specified class.
   *
   * @param iType Class name where to originate the instance
   * @return New instance
   */
  <T> T newInstance(Class<T> iType);

  /**
   * Returns the entity manager that handle the binding from ODocuments and POJOs.
   *
   * @return
   */
  OEntityManager getEntityManager();

  /**
   * Method that detaches all fields contained in the document to the given object. It returns by
   * default a proxied instance. To get a detached non proxied instance @see {@link
   * OObjectEntitySerializer.detach(T, ODatabaseObject)}
   *
   * @param iPojo :- the object to detach
   * @return the detached object
   */
  <RET> RET detach(final Object iPojo);

  /**
   * Method that detaches all fields contained in the document to the given object.
   *
   * @param <RET>
   * @param iPojo :- the object to detach
   * @param returnNonProxiedInstance :- defines if the return object will be a proxied instance or
   *     not. If set to TRUE and the object does not contains @Id and @Version fields it could
   *     procude data replication
   * @return the object serialized or with detached data
   */
  <RET> RET detach(final Object iPojo, boolean returnNonProxiedInstance);

  /**
   * Method that detaches all fields contained in the document to the given object and recursively
   * all object tree. This may throw a {@link StackOverflowError} with big objects tree. To avoid it
   * set the stack size with -Xss java option
   *
   * @param <RET>
   * @param iPojo :- the objects to detach
   * @return the object serialized or with detached data
   */
  default <RET> RET detachAll(final Object iPojo) {
    return detachAll(iPojo, false);
  }

  /**
   * Method that detaches all fields contained in the document to the given object and recursively
   * all object tree. This may throw a {@link StackOverflowError} with big objects tree. To avoid it
   * set the stack size with -Xss java option
   *
   * @param <RET>
   * @param iPojo :- the objects to detach
   * @param returnNonProxiedInstance :- defines if the return object will be a proxied instance or
   *     not. If set to TRUE and the object does not contains @Id and @Version fields it could
   *     procude data replication
   * @return the object serialized or with detached data
   */
  <RET> RET detachAll(final Object iPojo, boolean returnNonProxiedInstance);

  boolean isRetainObjects();

  ODatabase setRetainObjects(boolean iRetainObjects);

  Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan);

  ODocument pojo2Stream(final Object iPojo, final ODocument iRecord);

  boolean isLazyLoading();

  void setLazyLoading(final boolean lazyLoading);

  @Override
  OMetadataObject getMetadata();

  <RET extends List<?>> RET objectQuery(String iCommand, Object... iArgs);

  <RET extends List<?>> RET objectQuery(String iCommand, Map<String, Object> iArgs);

  <RET extends List<?>> RET objectCommand(String iCommand, Object... iArgs);

  <RET extends List<?>> RET objectCommand(String iCommand, Map<String, Object> iArgs);

  @Override
  default <T> T executeWithRetry(int nRetries, Function<ODatabaseSession, T> function)
      throws IllegalStateException, IllegalArgumentException, ONeedRetryException,
          UnsupportedOperationException {
    throw new UnsupportedOperationException();
    // TODO test it before enabling it!
  }
}
