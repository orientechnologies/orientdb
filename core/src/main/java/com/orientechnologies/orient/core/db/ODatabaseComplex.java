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
package com.orientechnologies.orient.core.db;

import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.HOOK_POSITION;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Database interface that represents a complex database. It extends the base ODatabase interface adding all the higher-level APIs
 * to treats records. Entities can be implementations of ORecord class for ODatabaseRecord or any POJO for ODatabaseObject. The
 * behaviour of the datastore depends by the OStorage implementation used.
 * 
 * @author Luca Garulli
 * 
 * @see ODatabaseRecord
 * @see ODatabaseObject
 * @see OStorage
 * @param <T>
 */
public interface ODatabaseComplex<T extends Object> extends ODatabase, OUserObject2RecordHandler {
  public enum OPERATION_MODE {
    SYNCHRONOUS, ASYNCHRONOUS, ASYNCHRONOUS_NOANSWER
  }

  /**
   * Creates a new entity instance.
   * 
   * @return The new instance.
   */
  public <RET extends Object> RET newInstance();

  /**
   * Returns the Dictionary manual index.
   * 
   * @return ODictionary instance
   */
  public ODictionary<T> getDictionary();

  /**
   * Returns the current user logged into the database.
   * 
   * @see OSecurity
   */
  public OUser getUser();

  /**
   * Set user for current database instance
   */
  public void setUser(OUser user);

  /**
   * Loads the entity and return it.
   * 
   * @param iObject
   *          The entity to load. If the entity was already loaded it will be reloaded and all the changes will be lost.
   * @return
   */
  public <RET extends T> RET load(T iObject);

  /**
   * Loads a record using a fetch plan.
   * 
   * @param iObject
   *          Record to load
   * @param iFetchPlan
   *          Fetch plan used
   * @return The record received
   */
  public <RET extends T> RET load(T iObject, String iFetchPlan);

  /**
   * Loads a record using a fetch plan.
   * 
   * 
   * @param iObject
   *          Record to load
   * @param iFetchPlan
   *          Fetch plan used
   * @param iLockingStrategy
   * @return The record received
   */
  public <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  /**
   * Loads a record using a fetch plan.
   * 
   * @param iObject
   *          Record to load
   * @param iFetchPlan
   *          Fetch plan used
   * @param iIgnoreCache
   *          Ignore cache or use it
   * @return The record received
   */
  public <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Force the reloading of the entity.
   * 
   * @param iObject
   *          The entity to load. If the entity was already loaded it will be reloaded and all the changes will be lost.
   * @param iFetchPlan
   *          Fetch plan used
   * @param iIgnoreCache
   *          Ignore cache or use it
   * @return The loaded entity
   */
  public <RET extends T> RET reload(final T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Loads the entity by the Record ID.
   * 
   * @param iRecordId
   *          The unique record id of the entity to load.
   * @return The loaded entity
   */
  public <RET extends T> RET load(ORID iRecordId);

  /**
   * Loads the entity by the Record ID using a fetch plan.
   * 
   * @param iRecordId
   *          The unique record id of the entity to load.
   * @param iFetchPlan
   *          Fetch plan used
   * @return The loaded entity
   */
  public <RET extends T> RET load(ORID iRecordId, String iFetchPlan);

  /**
   * Loads the entity by the Record ID using a fetch plan and specifying if the cache must be ignored.
   * 
   * @param iRecordId
   *          The unique record id of the entity to load.
   * @param iFetchPlan
   *          Fetch plan used
   * @param iIgnoreCache
   *          Ignore cache or use it
   * @return The loaded entity
   */
  public <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache);

  public <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  /**
   * Saves an entity in synchronous mode. If the entity is not dirty, then the operation will be ignored. For custom entity
   * implementations assure to set the entity as dirty.
   * 
   * @param iObject
   *          The entity to save
   * @return The saved entity.
   */
  public <RET extends T> RET save(T iObject);

  /**
   * Saves an entity specifying the mode. If the entity is not dirty, then the operation will be ignored. For custom entity
   * implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   * 
   * 
   * @param iObject
   *          The entity to save
   * @param iMode
   *          Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  public <RET extends T> RET save(T iObject, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  /**
   * Saves an entity in the specified cluster in synchronous mode. If the entity is not dirty, then the operation will be ignored.
   * For custom entity implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   * 
   * @param iObject
   *          The entity to save
   * @param iClusterName
   *          Name of the cluster where to save
   * @return The saved entity.
   */
  public <RET extends T> RET save(T iObject, String iClusterName);

  public boolean updatedReplica(T iObject);

  /**
   * Saves an entity in the specified cluster specifying the mode. If the entity is not dirty, then the operation will be ignored.
   * For custom entity implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   * 
   * 
   * @param iObject
   *          The entity to save
   * @param iClusterName
   *          Name of the cluster where to save
   * @param iMode
   *          Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate
   *          Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  public <RET extends T> RET save(T iObject, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  /**
   * Deletes an entity from the database in synchronous mode.
   * 
   * @param iObject
   *          The entity to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> delete(T iObject);

  /**
   * Deletes the entity with the received RID from the database.
   * 
   * @param iRID
   *          The RecordID to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> delete(ORID iRID);

  /**
   * Deletes the entity with the received RID from the database.
   * 
   * @param iRID
   *          The RecordID to delete.
   * @param iVersion
   *          for MVCC
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> delete(ORID iRID, ORecordVersion iVersion);

  /**
   * Hides records content by putting tombstone on the records position but does not delete record itself.
   * 
   * This method is used in case of record content itself is broken and can not be read or deleted. So it is emergence method. This
   * method can be used only if there is no active transaction in database.
   * 
   * 
   * 
   * @param rid
   *          record id.
   * @throws java.lang.UnsupportedOperationException
   *           In case current version of cluster does not support given operation.
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException
   *           if record is already deleted/hidden.
   * 
   * @return <code>true</code> if record was hidden and <code>false</code> if record does not exits in database.
   */

  public boolean hide(ORID rid);

  public ODatabaseComplex<T> cleanOutRecord(ORID rid, ORecordVersion version);

  /**
   * Return active transaction. Cannot be null. If no transaction is active, then a OTransactionNoTx instance is returned.
   * 
   * @return OTransaction implementation
   */
  public OTransaction getTransaction();

  /**
   * Begins a new transaction. By default the type is OPTIMISTIC. If a previous transaction was started it will be rollbacked and
   * closed before to start a new one. A transaction once begun has to be closed by calling the {@link #commit()} or
   * {@link #rollback()}.
   * 
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> begin();

  /**
   * Begins a new transaction specifying the transaction type. If a previous transaction was started it will be rollbacked and
   * closed before to start a new one. A transaction once begun has to be closed by calling the {@link #commit()} or
   * {@link #rollback()}.
   * 
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> begin(TXTYPE iStatus);

  /**
   * Attaches a transaction as current.
   * 
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODatabaseComplex<T> begin(OTransaction iTx) throws OTransactionException;

  /**
   * Commits the current transaction. The approach is all or nothing. All changes will be permanent following the storage type. If
   * the operation succeed all the entities changed inside the transaction context will be effectives. If the operation fails, all
   * the changed entities will be restored in the datastore. Memory instances are not guaranteed to being restored as well.
   * 
   * @return
   */
  public ODatabaseComplex<T> commit() throws OTransactionException;

  public ODatabaseComplex<T> commit(boolean force) throws OTransactionException;

  /**
   * Aborts the current running transaction. All the pending changed entities will be restored in the datastore. Memory instances
   * are not guaranteed to being restored as well.
   * 
   * @return
   */
  public ODatabaseComplex<T> rollback() throws OTransactionException;

  public ODatabaseComplex<T> rollback(boolean force) throws OTransactionException;

  /**
   * Execute a query against the database.
   * 
   * @param iCommand
   *          Query command
   * @param iArgs
   *          Optional parameters to bind to the query
   * @return List of POJOs
   */
  public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs);

  /**
   * Execute a command against the database. A command can be a SQL statement or a Procedure. If the OStorage used is remote
   * (OStorageRemote) then the command will be executed remotely and the result returned back to the calling client.
   * 
   * @param iCommand
   *          Command request to execute.
   * @return The same Command request received as parameter.
   */
  public <RET extends OCommandRequest> RET command(OCommandRequest iCommand);

  /**
   * Return the OMetadata instance. Cannot be null.
   * 
   * @return The OMetadata instance.
   */
  public OMetadata getMetadata();

  /**
   * Returns the database owner. Used in wrapped instances to know the up level ODatabase instance.
   * 
   * @return Returns the database owner.
   */
  public ODatabaseComplex<?> getDatabaseOwner();

  /**
   * Internal. Sets the database owner.
   */
  public ODatabaseComplex<?> setDatabaseOwner(ODatabaseComplex<?> iOwner);

  /**
   * Return the underlying database. Used in wrapper instances to know the down level ODatabase instance.
   * 
   * @return The underlying ODatabase implementation.
   */
  public <DB extends ODatabase> DB getUnderlying();

  /**
   * Internal method. Don't call it directly unless you're building an internal component.
   */
  public void setInternal(ATTRIBUTES attribute, Object iValue);

  /**
   * Registers a hook to listen all events for Records.
   * 
   * @param iHookImpl
   *          ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseComplex<?>> DB registerHook(ORecordHook iHookImpl);

  public <DB extends ODatabaseComplex<?>> DB registerHook(final ORecordHook iHookImpl, HOOK_POSITION iPosition);

  /**
   * Retrieves all the registered hooks.
   * 
   * @return A not-null unmodifiable map of ORecordHook and position instances. If there are no hooks registered, the Map is empty.
   */
  public Map<ORecordHook, HOOK_POSITION> getHooks();

  /**
   * Unregisters a previously registered hook.
   * 
   * @param iHookImpl
   *          ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseComplex<?>> DB unregisterHook(ORecordHook iHookImpl);

  /**
   * Invokes the callback on all the configured hooks.
   * 
   * @param iObject
   *          The object passed change based on the Database implementation: records for {@link ODatabaseRecord} implementations and
   *          POJO for {@link ODatabaseObject} implementations.
   * @return True if the input record is changed, otherwise false
   */
  public RESULT callbackHooks(TYPE iType, OIdentifiable iObject);

  /**
   * Returns if the Multi Version Concurrency Control is enabled or not. If enabled the version of the record is checked before each
   * update and delete against the records.
   * 
   * @return true if enabled, otherwise false
   * @see ODatabaseRecord#setMVCC(boolean)
   */
  public boolean isMVCC();

  /**
   * Enables or disables the Multi-Version Concurrency Control. If enabled the version of the record is checked before each update
   * and delete against the records.
   * 
   * @param iValue
   * @see ODatabaseRecord#isMVCC()
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabaseComplex<?>> DB setMVCC(boolean iValue);

  public String getType();
}
