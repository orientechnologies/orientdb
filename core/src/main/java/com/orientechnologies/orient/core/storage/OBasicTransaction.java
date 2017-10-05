/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;

/**
 * Captures the basic transaction traits.
 *
 * @author Sergey Sitnikov
 */
public interface OBasicTransaction {

  /**
   * Indicates the record deleted in a transaction.
   *
   * @see #getRecord(ORID)
   */
  ORecord DELETED_RECORD = new ORecordBytes();

  /**
   * @return {@code true} if this transaction is active, {@code false} otherwise.
   */
  boolean isActive();

  /**
   * Saves the given record in this transaction.
   *
   * @param record          the record to save.
   * @param clusterName     record's cluster name.
   * @param operationMode   the operation mode.
   * @param forceCreate     the force creation flag, {@code true} to force the creation of the record, {@code false} to allow
   *                        updates.
   * @param createdCallback the callback to invoke when the record save operation triggered the creation of the record.
   * @param updatedCallback the callback to invoke when the record save operation triggered the update of the record.
   *
   * @return the record saved.
   */
  ORecord saveRecord(ORecord record, String clusterName, ODatabase.OPERATION_MODE operationMode, boolean forceCreate,
      ORecordCallback<? extends Number> createdCallback, ORecordCallback<Integer> updatedCallback);

  /**
   * Deletes the given record in this transaction.
   *
   * @param record the record to delete.
   * @param mode   the operation mode.
   */
  void deleteRecord(ORecord record, ODatabase.OPERATION_MODE mode);

  /**
   * Resolves a record with the given RID in the context of this transaction.
   *
   * @param rid the record RID.
   *
   * @return the resolved record, or {@code null} if no record is found, or {@link #DELETED_RECORD} if the record was deleted in
   * this transaction.
   */
  ORecord getRecord(ORID rid);

  /**
   * Adds the transactional index entry in this transaction.
   *
   * @param index     the index.
   * @param indexName the index name.
   * @param operation the index operation to register.
   * @param key       the index key.
   * @param value     the index key value.
   */
  void addIndexEntry(OIndex<?> index, String indexName, OTransactionIndexChanges.OPERATION operation, Object key,
      OIdentifiable value);

  /**
   * Adds the given document to a set of changed documents known to this transaction.
   *
   * @param document the document to add.
   */
  void addChangedDocument(ODocument document);

  /**
   * Obtains the index changes done in the context of this transaction.
   *
   * @param indexName the index name.
   *
   * @return the index changes in question or {@code null} if index is not found.
   */
  OTransactionIndexChanges getIndexChanges(String indexName);

  /**
   * Does the same thing as {@link #getIndexChanges(String)}, but handles remote storages in a special way.
   *
   * @param indexName the index name.
   *
   * @return the index changes in question or {@code null} if index is not found or storage is remote.
   */
  OTransactionIndexChanges getIndexChangesInternal(String indexName);

  /**
   * Obtains the custom value by its name stored in the context of this transaction.
   *
   * @param name the value name.
   *
   * @return the obtained value or {@code null} if no value found.
   */
  Object getCustomData(String name);

  /**
   * Sets the custom value by its name stored in the context of this transaction.
   *
   * @param name  the value name.
   * @param value the value to store.
   */
  void setCustomData(String name, Object value);

  /**
   * @return this transaction ID as seen by the client of this transaction.
   */
  default int getClientTransactionId() {
    return getId();
  }

  int getId();

}
