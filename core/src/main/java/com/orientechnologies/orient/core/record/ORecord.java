/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

/**
 * Generic record representation. The object can be reused across call to the database.
 */
public interface ORecord<T> extends ORecordElement, OIdentifiable {
	/**
	 * Remove all the dependencies by other records. All the relationships remain in form of RecordID. If some links contain dirty
	 * records, the detach can't be complete and this method returns false.
	 * 
	 * @return True if the document has been successfully detached, otherwise false.
	 */
	public boolean detach();

	/**
	 * Resets the record to be reused.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET reset();

	/**
	 * Unloads current record. All information are lost but the record identity.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET unload();

	/**
	 * All the fields are deleted but the record identity is maintained.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET clear();

	/**
	 * Creates a copy of the record. All the record contents are copied.
	 * 
	 * @return A new record instance copied by the current.
	 */
	public <RET extends ORecord<T>> RET copy();

	/**
	 * Returns the record identity as &lt;cluster-id&gt;:&lt;cluster-position&gt;
	 */
	public ORID getIdentity();

	/**
	 * Returns the current version number of the record. When the record is created has version = 0. At every change the storage
	 * increment the version number. Version number is used by Optimistic transactions to check if the record is changed in the
	 * meanwhile of the transaction.
	 * 
	 * @see OTransactionOptimistic
	 * @return The version number. 0 if it's a brand new record.
	 */
	public int getVersion();

	/**
	 * Returns the database where the record belongs.
	 * 
	 * @return
	 */
	public ODatabaseRecord getDatabase();

	/**
	 * Checks if the record is dirty, namely if it was changed in memory.
	 * 
	 * @return True if dirty, otherwise false
	 */
	public boolean isDirty();

	/**
	 * Checks if the record is pinned.
	 * 
	 * @return True if pinned, otherwise false
	 */
	public boolean isPinned();

	/**
	 * Suggests to the engine to keep the record in cache. Use it for the most read records.
	 * 
	 * @see ORecord#unpin()
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET pin();

	/**
	 * Suggests to the engine to not keep the record in cache.
	 * 
	 * @see ORecord#pin()
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET unpin();

	/**
	 * Loads the record content in memory. If the record is in cache will be returned a new instance, so pay attention to use the
	 * returned. If the record is dirty, then it returns to the original content. If the record doesn't exist a
	 * ORecordNotFoundException exception is thrown.
	 * 
	 * @return The record loaded or itself if the record has been reloaded from the storage. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET load() throws ORecordNotFoundException;

	/**
	 * Loads the record content in memory. No cache is used. If the record is dirty, then it returns to the original content. If the
	 * record doesn't exist a ORecordNotFoundException exception is thrown.
	 * 
	 * @return Always the object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET reload() throws ORecordNotFoundException;

	/**
	 * Saves in-memory changes to the storage. Behavior depends by the current running transaction if any. If no transaction is
	 * running then changes apply immediately. If an Optimistic transaction is running then the record will be changed on commit time.
	 * The current transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running,
	 * then an exclusive lock is acquired onto the record. Current transaction will continue to see the record as modified, while
	 * others can't access to it since it's locked.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET save();

	public <RET extends ORecord<T>> RET save(String iCluster);

	/**
	 * Deletes the record from the storage. Behavior depends by the current running transaction if any. If no transaction is running
	 * then the record is deleted immediately. If an Optimistic transaction is running then the record will be deleted on commit time.
	 * The current transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running,
	 * then an exclusive lock is acquired onto the record. Current transaction will continue to see the record as deleted, while
	 * others can't access to it since it's locked.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET delete();

	/**
	 * Fills the record parsing the content in JSON format.
	 * 
	 * @param iJson
	 *          Object content in JSON format
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET extends ORecord<T>> RET fromJSON(String iJson);

	/**
	 * Exports the record in JSON format.
	 * 
	 * @return Object content in JSON format
	 */
	public String toJSON();

	/**
	 * Exports the record in JSON format specifying additional formatting settings.
	 * 
	 * @param iFormat
	 *          Format settings separated by comma. Available settings are:
	 *          <ul>
	 *          <li><b>rid</b>: exports the record's id as property "@rid"</li>
	 *          <li><b>version</b>: exports the record's version as property "@version"</li>
	 *          <li><b>class</b>: exports the record's class as property "@class"</li>
	 *          <li><b>attribSameRow</b>: exports all the record attributes in the same row</li>
	 *          <li><b>indent:&lt;level&gt;</b>: Indents the output if the &lt;level&gt; specified. Default is 0</li>
	 *          </ul>
	 *          Example: "rid,version,class,indent:6" exports record id, version and class properties along with record properties
	 *          using an indenting level equals of 6.
	 * @return Object content in JSON format
	 */
	public String toJSON(String iFormat);

	/**
	 * Returns the size in bytes of the record.
	 * 
	 * @return
	 */
	public int getSize();
}
