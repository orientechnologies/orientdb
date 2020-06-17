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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic interface for document based Database implementations.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabaseDocument extends ODatabase<ORecord> {

  String TYPE = "document";

  /**
   * Browses all the records of the specified class and also all the subclasses. If you've a class
   * Vehicle and Car that extends Vehicle then a db.browseClass("Vehicle", true) will return all the
   * instances of Vehicle and Car. The order of the returned instance starts from record id with
   * position 0 until the end. Base classes are worked at first.
   *
   * @param iClassName Class name to iterate
   * @return Iterator of ODocument instances
   */
  ORecordIteratorClass<ODocument> browseClass(String iClassName);

  /**
   * Browses all the records of the specified class and if iPolymorphic is true also all the
   * subclasses. If you've a class Vehicle and Car that extends Vehicle then a
   * db.browseClass("Vehicle", true) will return all the instances of Vehicle and Car. The order of
   * the returned instance starts from record id with position 0 until the end. Base classes are
   * worked at first.
   *
   * @param iClassName Class name to iterate
   * @param iPolymorphic Consider also the instances of the subclasses or not
   * @return Iterator of ODocument instances
   */
  ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic);

  /**
   * Creates a new entity instance. Each database implementation will return the right type.
   *
   * @return The new instance.
   */
  <RET extends Object> RET newInstance(String iClassName);

  /**
   * Create a new instance of a blob containing the given bytes.
   *
   * @param bytes content of the OBlob
   * @return the OBlob instance.
   */
  OBlob newBlob(byte[] bytes);

  /**
   * Create a new empty instance of a blob.
   *
   * @return the OBlob instance.
   */
  OBlob newBlob();

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

  long countView(String iClassName);

  /**
   * Flush all indexes and cached storage content to the disk.
   *
   * <p>After this call users can perform only select queries. All write-related commands will
   * queued till {@link #release()} command will be called.
   *
   * <p>Given command waits till all on going modifications in indexes or DB will be finished.
   *
   * <p>IMPORTANT: This command is not reentrant.
   */
  void freeze();

  /** Allows to execute write-related commands on DB. Called after {@link #freeze()} command. */
  void release();

  /**
   * Flush all indexes and cached storage content to the disk.
   *
   * <p>After this call users can perform only select queries. All write-related commands will
   * queued till {@link #release()} command will be called or exception will be thrown on attempt to
   * modify DB data. Concrete behaviour depends on <code>throwException</code> parameter.
   *
   * <p>IMPORTANT: This command is not reentrant.
   *
   * @param throwException If <code>true</code> {@link
   *     com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *     exception will be thrown in case of write command will be performed.
   */
  void freeze(boolean throwException);

  /**
   * Browses all the records of the specified cluster.
   *
   * @param iClusterName Cluster name to iterate
   * @return Iterator of ODocument instances
   */
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName);

  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones);

  /**
   * Browses all the records of the specified cluster of the passed record type.
   *
   * @param iClusterName Cluster name to iterate
   * @param iRecordClass The record class expected
   * @return Iterator of ODocument instances
   */
  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName, Class<REC> iRecordClass);

  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition);

  @Deprecated
  <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(
      String iClusterName,
      Class<REC> iRecordClass,
      long startClusterPosition,
      long endClusterPosition,
      boolean loadTombstones);

  /**
   * Returns the record for a OIdentifiable instance. If the argument received already is a ORecord
   * instance, then it's returned as is, otherwise a new ORecord is created with the identity
   * received and returned.
   *
   * @param iIdentifiable
   * @return A ORecord instance
   */
  <RET extends ORecord> RET getRecord(OIdentifiable iIdentifiable);

  /** Returns the default record type for this kind of database. */
  byte getRecordType();

  /**
   * Returns true if current configuration retains objects, otherwise false
   *
   * @see #setRetainRecords(boolean)
   */
  boolean isRetainRecords();

  /**
   * Specifies if retain handled objects in memory or not. Setting it to false can improve
   * performance on large inserts. Default is enabled.
   *
   * @param iValue True to enable, false to disable it.
   * @see #isRetainRecords()
   */
  ODatabaseDocument setRetainRecords(boolean iValue);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param resourceGeneric Generic Resource where to execute the operation
   * @param resourceGeneric Specific resource name where to execute the operation
   * @param iOperation Operation to execute against the resource
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  <DB extends ODatabaseDocument> DB checkSecurity(
      ORule.ResourceGeneric resourceGeneric, String resourceSpecific, int iOperation);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two
   * steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resource
   * </ol>
   *
   * @param iResourceGeneric Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation Operation to execute against the resource
   * @param iResourceSpecific Target resource, i.e.: "employee" to specify the cluster name.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  <DB extends ODatabaseDocument> DB checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check
   * is made in two steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resources
   * </ol>
   *
   * @param iResourceGeneric Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation Operation to execute against the resource
   * @param iResourcesSpecific Target resources as an array of Objects, i.e.: ["employee", 2] to
   *     specify cluster name and id.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  <DB extends ODatabaseDocument> DB checkSecurity(
      ORule.ResourceGeneric iResourceGeneric, int iOperation, Object... iResourcesSpecific);

  /**
   * Tells if validation of record is active. Default is true.
   *
   * @return true if it's active, otherwise false.
   */
  boolean isValidationEnabled();

  /**
   * Enables or disables the record validation.
   *
   * <p>Since 2.2 this setting is persistent.
   *
   * @param iEnabled True to enable, false to disable
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  <DB extends ODatabaseDocument> DB setValidationEnabled(boolean iEnabled);

  /**
   * Checks if the operation on a resource is allowed for the current user.
   *
   * @param iResource Resource where to execute the operation
   * @param iOperation Operation to execute against the resource
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(String iResource, int iOperation);

  /**
   * Checks if the operation on a resource is allowed for the current user. The check is made in two
   * steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resource
   * </ol>
   *
   * @param iResourceGeneric Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation Operation to execute against the resource
   * @param iResourceSpecific Target resource, i.e.: "employee" to specify the cluster name.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(
      String iResourceGeneric, int iOperation, Object iResourceSpecific);

  /**
   * Checks if the operation against multiple resources is allowed for the current user. The check
   * is made in two steps:
   *
   * <ol>
   *   <li>Access to all the resource as *
   *   <li>Access to the specific target resources
   * </ol>
   *
   * @param iResourceGeneric Resource where to execute the operation, i.e.: database.clusters
   * @param iOperation Operation to execute against the resource
   * @param iResourcesSpecific Target resources as an array of Objects, i.e.: ["employee", 2] to
   *     specify cluster name and id.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabaseDocument> DB checkSecurity(
      String iResourceGeneric, int iOperation, Object... iResourcesSpecific);

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code>
   *     otherwise.
   */
  boolean isPooled();

  /**
   * Add a cluster for blob records.
   *
   * @param iClusterName Cluster name
   * @param iParameters Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addBlobCluster(String iClusterName, Object... iParameters);

  OElement newElement();

  OElement newElement(final String className);

  OElement newEmbeddedElement();

  OElement newEmbeddedElement(final String className);

  /**
   * Creates a new Edge of type E
   *
   * @param from the starting point vertex
   * @param to the endpoint vertex
   * @return the edge
   */
  default OEdge newEdge(OVertex from, OVertex to) {
    return newEdge(from, to, "E");
  }

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  OEdge newEdge(OVertex from, OVertex to, OClass type);

  /**
   * Creates a new Edge
   *
   * @param from the starting point vertex
   * @param to the endpoint vertex
   * @param type the edge type
   * @return the edge
   */
  OEdge newEdge(OVertex from, OVertex to, String type);

  /**
   * Creates a new Vertex of type V
   *
   * @return
   */
  default OVertex newVertex() {
    return newVertex("V");
  }

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type
   * @return
   */
  OVertex newVertex(OClass type);

  /**
   * Creates a new Vertex
   *
   * @param type the vertex type (class name)
   * @return
   */
  OVertex newVertex(String type);

  /**
   * creates a new vertex class (a class that extends V)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws OSchemaException if the class already exists or if V class is not defined (Eg. if it
   *     was deleted from the schema)
   */
  default OClass createVertexClass(String className) throws OSchemaException {
    return createClass(className, "V");
  }

  /**
   * creates a new edge class (a class that extends E)
   *
   * @param className the class name
   * @return The object representing the class in the schema
   * @throws OSchemaException if the class already exists or if E class is not defined (Eg. if it
   *     was deleted from the schema)
   */
  default OClass createEdgeClass(String className) {
    return createClass(className, "E");
  }

  /**
   * If a class with given name already exists, it's just returned, otherwise the method creates a
   * new class and returns it.
   *
   * @param className the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws OSchemaException if one of the superclasses does not exist in the schema
   */
  default OClass createClassIfNotExist(String className, String... superclasses)
      throws OSchemaException {
    OSchema schema = getMetadata().getSchema();
    schema.reload();

    OClass result = schema.getClass(className);
    if (result == null) {
      result = createClass(className, superclasses);
    }
    return result;
  }
}
