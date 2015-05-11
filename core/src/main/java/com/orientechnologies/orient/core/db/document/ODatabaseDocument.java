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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseSchemaAware;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic interface for document based Database implementations.
 * 
 * @author Luca Garulli
 */
public interface ODatabaseDocument extends ODatabase<ORecord>, ODatabaseSchemaAware<ORecord> {

  final static String TYPE = "document";

  /**
   * Browses all the records of the specified class and also all the subclasses. If you've a class Vehicle and Car that extends
   * Vehicle then a db.browseClass("Vehicle", true) will return all the instances of Vehicle and Car. The order of the returned
   * instance starts from record id with position 0 until the end. Base classes are worked at first.
   * 
   * @param iClassName
   *          Class name to iterate
   * @return Iterator of ODocument instances
   */
  public ORecordIteratorClass<ODocument> browseClass(String iClassName);

  /**
   * Browses all the records of the specified class and if iPolymorphic is true also all the subclasses. If you've a class Vehicle
   * and Car that extends Vehicle then a db.browseClass("Vehicle", true) will return all the instances of Vehicle and Car. The order
   * of the returned instance starts from record id with position 0 until the end. Base classes are worked at first.
   * 
   * @param iClassName
   *          Class name to iterate
   * @param iPolymorphic
   *          Consider also the instances of the subclasses or not
   * @return Iterator of ODocument instances
   */
  public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic);

  /**
   * Flush all indexes and cached storage content to the disk.
   * 
   * After this call users can perform only select queries. All write-related commands will queued till {@link #release()} command
   * will be called.
   * 
   * Given command waits till all on going modifications in indexes or DB will be finished.
   * 
   * IMPORTANT: This command is not reentrant.
   */
  public void freeze();

  /**
   * Allows to execute write-related commands on DB. Called after {@link #freeze()} command.
   */
  public void release();

  /**
   * Flush all indexes and cached storage content to the disk.
   * 
   * After this call users can perform only select queries. All write-related commands will queued till {@link #release()} command
   * will be called or exception will be thrown on attempt to modify DB data. Concrete behaviour depends on
   * <code>throwException</code> parameter.
   * 
   * IMPORTANT: This command is not reentrant.
   * 
   * @param throwException
   *          If <code>true</code> {@link com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *          exception will be thrown in case of write command will be performed.
   */
  public void freeze(boolean throwException);

  /**
   * Browses all the records of the specified cluster.
   *
   * @param iClusterName
   *          Cluster name to iterate
   * @return Iterator of ODocument instances
   */
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName);

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, long startClusterPosition,
      long endClusterPosition, boolean loadTombstones);

  /**
   * Browses all the records of the specified cluster of the passed record type.
   *
   * @param iClusterName
   *          Cluster name to iterate
   * @param iRecordClass
   *          The record class expected
   * @return Iterator of ODocument instances
   */
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass);

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition);

  @Deprecated
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition, boolean loadTombstones);

  /**
   * Returns the record for a OIdentifiable instance. If the argument received already is a ORecord instance, then it's returned as
   * is, otherwise a new ORecord is created with the identity received and returned.
   *
   * @param iIdentifiable
   * @return A ORecord instance
   */
  public <RET extends ORecord> RET getRecord(OIdentifiable iIdentifiable);

  /**
   * Returns the default record type for this kind of database.
   */
  public byte getRecordType();

  /**
   * Returns true if current configuration retains objects, otherwise false
   *
   * @see #setRetainRecords(boolean)
   */
  public boolean isRetainRecords();

  /**
   * Specifies if retain handled objects in memory or not. Setting it to false can improve performance on large inserts. Default is
   * enabled.
   *
   * @param iValue
   *          True to enable, false to disable it.
   * @see #isRetainRecords()
   */
  public ODatabaseDocument setRetainRecords(boolean iValue);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param iResource
   *          Resource where to execute the operation
   * @param iOperation
   *          Operation to execute against the resource
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric resourceGeneric, String resourceSpecific,
      int iOperation);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two steps:
   * <ol>
   * <li>
   * Access to all the resource as *</li>
   * <li>
   * Access to the specific target resource</li>
   * </ol>
   *
   * @param iResourceGeneric
   *          Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation
   *          Operation to execute against the resource
   * @param iResourceSpecific
   *          Target resource, i.e.: "employee" to specify the cluster name.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric iResourceGeneric, int iOperation,
      Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check is made in two steps:
   * <ol>
   * <li>
   * Access to all the resource as *</li>
   * <li>
   * Access to the specific target resources</li>
   * </ol>
   *
   * @param iResourceGeneric
   *          Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation
   *          Operation to execute against the resource
   * @param iResourcesSpecific
   *          Target resources as an array of Objects, i.e.: ["employee", 2] to specify cluster name and id.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric iResourceGeneric, int iOperation,
      Object... iResourcesSpecific);

  /**
   * Tells if validation of record is active. Default is true.
   *
   * @return true if it's active, otherwise false.
   */
  public boolean isValidationEnabled();

  /**
   * Enables or disables the record validation.
   *
   * @param iEnabled
   *          True to enable, false to disable
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseDocument> DB setValidationEnabled(boolean iEnabled);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param iResource
   *          Resource where to execute the operation
   * @param iOperation
   *          Operation to execute against the resource
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResource, int iOperation);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two steps:
   * <ol>
   * <li>
   * Access to all the resource as *</li>
   * <li>
   * Access to the specific target resource</li>
   * </ol>
   *
   * @param iResourceGeneric
   *          Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation
   *          Operation to execute against the resource
   * @param iResourceSpecific
   *          Target resource, i.e.: "employee" to specify the cluster name.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResourceGeneric, int iOperation, Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check is made in two steps:
   * <ol>
   * <li>
   * Access to all the resource as *</li>
   * <li>
   * Access to the specific target resources</li>
   * </ol>
   *
   * @param iResourceGeneric
   *          Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation
   *          Operation to execute against the resource
   * @param iResourcesSpecific
   *          Target resources as an array of Objects, i.e.: ["employee", 2] to specify cluster name and id.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific);
}
