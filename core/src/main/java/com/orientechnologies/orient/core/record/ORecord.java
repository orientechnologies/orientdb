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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.io.Serializable;

/**
 * Generic record representation. The object can be reused across multiple calls to the database by
 * using the {@link #reset()} method.
 */
public interface ORecord extends ORecordElement, OIdentifiable, Serializable, OSerializableStream {
  /**
   * Removes all the dependencies with other records. All the relationships remain in form of
   * RecordID. If some links contain dirty records, the detach cannot be complete and this method
   * returns false.
   *
   * @return True if the document has been successfully detached, otherwise false.
   */
  boolean detach();

  /**
   * Resets the record to be reused. The record is fresh like just created. Use this method to
   * recycle records avoiding the creation of them stressing the JVM Garbage Collector.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET reset();

  /**
   * Unloads current record. All information are lost but the record identity. At the next access
   * the record will be auto-reloaded. Useful to free memory or to avoid to keep an old version of
   * it.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET unload();

  /**
   * All the fields are deleted but the record identity is maintained. Use this to remove all the
   * document's fields.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET clear();

  /**
   * Creates a copy of the record. All the record contents are copied.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET copy();

  /** Returns the record identity as &lt;cluster-id&gt;:&lt;cluster-position&gt; */
  ORID getIdentity();

  /**
   * Returns the current version number of the record. When the record is created has version = 0.
   * At every change the storage increment the version number. Version number is used by Optimistic
   * transactions to check if the record is changed in the meanwhile of the transaction.
   *
   * @see OTransactionOptimistic
   * @return The version number. 0 if it's a brand new record.
   */
  int getVersion();

  /**
   * Returns the database where the record belongs.
   *
   * @return
   */
  ODatabaseDocument getDatabase();

  /**
   * Checks if the record is dirty, namely if it was changed in memory.
   *
   * @return True if dirty, otherwise false
   */
  boolean isDirty();

  /**
   * Loads the record content in memory. If the record is in cache will be returned a new instance,
   * so pay attention to use the returned. If the record is dirty, then it returns to the original
   * content. If the record does not exist a ORecordNotFoundException exception is thrown.
   *
   * @return The record loaded or itself if the record has been reloaded from the storage. Useful to
   *     call methods in chain.
   */
  <RET extends ORecord> RET load() throws ORecordNotFoundException;

  /**
   * Loads the record content in memory. No cache is used. If the record is dirty, then it returns
   * to the original content. If the record does not exist a ORecordNotFoundException exception is
   * thrown.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET reload() throws ORecordNotFoundException;

  <RET extends ORecord> RET reload(final String fetchPlan, final boolean ignoreCache, boolean force)
      throws ORecordNotFoundException;

  /**
   * Saves in-memory changes to the database. Behavior depends by the current running transaction if
   * any. If no transaction is running then changes apply immediately. If an Optimistic transaction
   * is running then the record will be changed at commit time. The current transaction will
   * continue to see the record as modified, while others not. If a Pessimistic transaction is
   * running, then an exclusive lock is acquired against the record. Current transaction will
   * continue to see the record as modified, while others cannot access to it since it's locked.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET save();

  /**
   * Saves in-memory changes to the database defining a specific cluster where to save it. Behavior
   * depends by the current running transaction if any. If no transaction is running then changes
   * apply immediately. If an Optimistic transaction is running then the record will be changed at
   * commit time. The current transaction will continue to see the record as modified, while others
   * not. If a Pessimistic transaction is running, then an exclusive lock is acquired against the
   * record. Current transaction will continue to see the record as modified, while others cannot
   * access to it since it's locked.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET save(String iCluster);

  <RET extends ORecord> RET save(boolean forceCreate);

  <RET extends ORecord> RET save(String iCluster, boolean forceCreate);

  /**
   * Deletes the record from the database. Behavior depends by the current running transaction if
   * any. If no transaction is running then the record is deleted immediately. If an Optimistic
   * transaction is running then the record will be deleted at commit time. The current transaction
   * will continue to see the record as deleted, while others not. If a Pessimistic transaction is
   * running, then an exclusive lock is acquired against the record. Current transaction will
   * continue to see the record as deleted, while others cannot access to it since it's locked.
   *
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET delete();

  /**
   * Fills the record parsing the content in JSON format.
   *
   * @param iJson Object content in JSON format
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods
   *     in chain.
   */
  <RET extends ORecord> RET fromJSON(String iJson);

  /**
   * Exports the record in JSON format.
   *
   * @return Object content in JSON format
   */
  String toJSON();

  /**
   * Exports the record in JSON format specifying additional formatting settings.
   *
   * @param iFormat Format settings separated by comma. Available settings are:
   *     <ul>
   *       <li><b>rid</b>: exports the record's id as property "@rid"
   *       <li><b>version</b>: exports the record's version as property "@version"
   *       <li><b>class</b>: exports the record's class as property "@class"
   *       <li><b>attribSameRow</b>: exports all the record attributes in the same row
   *       <li><b>indent:&lt;level&gt;</b>: Indents the output if the &lt;level&gt; specified.
   *           Default is 0
   *     </ul>
   *     Example: "rid,version,class,indent:6" exports record id, version and class properties along
   *     with record properties using an indenting level equals of 6.
   * @return Object content in JSON format
   */
  String toJSON(String iFormat);

  /**
   * Returns the size in bytes of the record. The size can be computed only for not new records.
   *
   * @return the size in bytes
   */
  int getSize();

  /**
   * Returns the current status of the record.
   *
   * @return Current status as value between the defined values in the enum {@link STATUS}
   */
  STATUS getInternalStatus();

  /**
   * Changes the current internal status.
   *
   * @param iStatus status between the values defined in the enum {@link STATUS}
   */
  void setInternalStatus(STATUS iStatus);
}
