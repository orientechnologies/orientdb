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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.util.OBackupable;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Generic Database interface. Represents the lower level of the Database providing raw API to access to the raw records.<br>
 * Limits:
 * <ul>
 * <li>Maximum records per cluster/class = <b>9.223.372.036 Billions</b>: 2^63 = 9.223.372.036.854.775.808 records</li>
 * <li>Maximum records per database = <b>302.231.454.903.657 Billions</b>: 2^15 clusters x 2^63 records = (2^78) 32.768 *
 * 9,223.372.036.854.775.808 = 302.231,454.903.657.293.676.544 records</li>
 * <li>Maximum storage per database = <b>19.807.040.628.566.084 Terabytes</b>: 2^31 data-segments x 2^63 bytes = (2^94)
 * 2.147.483.648 x 9,223.372.036.854.775.808 Exabytes = 19.807,040.628.566.084.398.385.987.584 Yottabytes</li>
 * </ul>
 *
 * @author Luca Garulli
 */
public interface ODatabase<T> extends OBackupable, Closeable, OUserObject2RecordHandler {

  enum OPTIONS {
    SECURITY
  }

  enum STATUS {
    OPEN, CLOSED, IMPORTING
  }

  enum ATTRIBUTES {
    TYPE, STATUS, DEFAULTCLUSTERID, DATEFORMAT, DATETIMEFORMAT, TIMEZONE, LOCALECOUNTRY, LOCALELANGUAGE, CHARSET, CUSTOM, CLUSTERSELECTION, MINIMUMCLUSTERS, CONFLICTSTRATEGY, VALIDATION
  }

  /**
   * Opens a database using the user and password received as arguments.
   *
   * @param iUserName     Username to login
   * @param iUserPassword Password associated to the user
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword);

  /**
   * Creates a new database.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase> DB create();

  /**
   * Creates a new database passing initial settings.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase> DB create(Map<OGlobalConfiguration, Object> iInitialSettings);

  /**
   * Activate current database instance on current thread. Call this method before using the database if you switch between multiple
   * databas instances on the same thread or if you pass them across threads.
   */
  ODatabase activateOnCurrentThread();

  /**
   * Returns true if the current database instance is active on current thread, otherwise false.
   */
  boolean isActiveOnCurrentThread();

  /**
   * Reloads the database information like the cluster list.
   */
  void reload();

  /**
   * Drops a database.
   *
   * @throws ODatabaseException if database is closed.
   */
  void drop();

  /**
   * Returns the database configuration settings. If defined, any database configuration overwrites the global one.
   *
   * @return OContextConfiguration
   */
  OContextConfiguration getConfiguration();

  /**
   * Declares an intent to the database. Intents aim to optimize common use cases.
   *
   * @param iIntent The intent
   */
  boolean declareIntent(final OIntent iIntent);

  /**
   * Checks if the database exists.
   *
   * @return True if already exists, otherwise false.
   */
  boolean exists();

  /**
   * Closes an opened database.
   */
  void close();

  /**
   * Returns the current status of database.
   */
  STATUS getStatus();

  /**
   * Returns the current status of database.
   */
  <DB extends ODatabase> DB setStatus(STATUS iStatus);

  /**
   * Returns the total size of database as the real used space.
   */
  long getSize();

  /**
   * Returns the database name.
   *
   * @return Name of the database
   */
  String getName();

  /**
   * Returns the database URL.
   *
   * @return URL of the database
   */
  String getURL();

  /**
   * Returns the level1 cache. Cannot be null.
   *
   * @return Current cache.
   */
  OLocalRecordCache getLocalCache();

  /**
   * Returns the default cluster id. If not specified all the new entities will be stored in the default cluster.
   *
   * @return The default cluster id
   */
  int getDefaultClusterId();

  /**
   * Returns the number of clusters.
   *
   * @return Number of the clusters
   */
  int getClusters();

  /**
   * Returns true if the cluster exists, otherwise false.
   *
   * @param iClusterName Cluster name
   * @return true if the cluster exists, otherwise false
   */
  boolean existsCluster(String iClusterName);

  /**
   * Returns all the names of the clusters.
   *
   * @return Collection of cluster names.
   */
  Collection<String> getClusterNames();

  /**
   * Returns the cluster id by name.
   *
   * @param iClusterName Cluster name
   * @return The id of searched cluster.
   */
  int getClusterIdByName(String iClusterName);

  /**
   * Returns the cluster name by id.
   *
   * @param iClusterId Cluster id
   * @return The name of searched cluster.
   */
  String getClusterNameById(int iClusterId);

  /**
   * Returns the total size of records contained in the cluster defined by its name.
   *
   * @param iClusterName Cluster name
   * @return Total size of records contained.
   */
  long getClusterRecordSizeByName(String iClusterName);

  /**
   * Returns the total size of records contained in the cluster defined by its id.
   *
   * @param iClusterId Cluster id
   * @return The name of searched cluster.
   */
  long getClusterRecordSizeById(int iClusterId);

  /**
   * Checks if the database is closed.
   *
   * @return true if is closed, otherwise false.
   */
  boolean isClosed();

  /**
   * Counts all the entities in the specified cluster id.
   *
   * @param iCurrentClusterId Cluster id
   * @return Total number of entities contained in the specified cluster
   */
  long countClusterElements(int iCurrentClusterId);

  long countClusterElements(int iCurrentClusterId, boolean countTombstones);

  /**
   * Removes all data in the cluster with given name.
   * As result indexes for this class will be rebuilt.
   *
   * @param clusterName Name of cluster to be truncated.
   */
  void truncateCluster(String clusterName);

  /**
   * Counts all the entities in the specified cluster ids.
   *
   * @param iClusterIds Array of cluster ids Cluster id
   * @return Total number of entities contained in the specified clusters
   */
  long countClusterElements(int[] iClusterIds);

  long countClusterElements(int[] iClusterIds, boolean countTombstones);

  /**
   * Counts all the entities in the specified cluster name.
   *
   * @param iClusterName Cluster name
   * @return Total number of entities contained in the specified cluster
   */
  long countClusterElements(String iClusterName);

  /**
   * Adds a new cluster.
   *
   * @param iClusterName Cluster name
   * @param iParameters  Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addCluster(String iClusterName, Object... iParameters);

  /**
   * Adds a new cluster.
   *
   * @param iClusterName Cluster name
   * @param iRequestedId requested id of the cluster
   * @param iParameters  Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addCluster(String iClusterName, int iRequestedId, Object... iParameters);

  /**
   * Drops a cluster by its name. Physical clusters will be completely deleted
   *
   * @param iClusterName the name of the cluster
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(String iClusterName, final boolean iTruncate);

  /**
   * Drops a cluster by its id. Physical clusters will be completely deleted.
   *
   * @param iClusterId id of cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(int iClusterId, final boolean iTruncate);

  /**
   * Sets a property value
   *
   * @param iName  Property name
   * @param iValue new value to set
   * @return The previous value if any, otherwise null
   */
  Object setProperty(String iName, Object iValue);

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   */
  Object getProperty(String iName);

  /**
   * Returns an iterator of the property entries
   */
  Iterator<Map.Entry<String, Object>> getProperties();

  /**
   * Returns a database attribute value
   *
   * @param iAttribute Attributes between #ATTRIBUTES enum
   * @return The attribute value
   */
  Object get(ATTRIBUTES iAttribute);

  /**
   * Sets a database attribute value
   *
   * @param iAttribute Attributes between #ATTRIBUTES enum
   * @param iValue     Value to set
   * @return underlying
   */
  <DB extends ODatabase> DB set(ATTRIBUTES iAttribute, Object iValue);

  /**
   * Registers a listener to the database events.
   *
   * @param iListener the listener to register
   */
  void registerListener(ODatabaseListener iListener);

  /**
   * Unregisters a listener to the database events.
   *
   * @param iListener the listener to unregister
   */
  void unregisterListener(ODatabaseListener iListener);

  ORecordMetadata getRecordMetadata(final ORID rid);

  /**
   * Flush cached storage content to the disk.
   * <p>
   * After this call users can perform only idempotent calls like read records and select/traverse queries. All write-related
   * operations will queued till {@link #release()} command will be called.
   * <p>
   * Given command waits till all on going modifications in indexes or DB will be finished.
   * <p>
   * IMPORTANT: This command is not reentrant.
   *
   * @see #release()
   */
  void freeze();

  /**
   * Allows to execute write-related commands on DB. Called after {@link #freeze()} command.
   *
   * @see #freeze()
   */
  void release();

  /**
   * Flush cached storage content to the disk.
   * <p>
   * After this call users can perform only select queries. All write-related commands will queued till {@link #release()} command
   * will be called or exception will be thrown on attempt to modify DB data. Concrete behaviour depends on
   * <code>throwException</code> parameter.
   * <p>
   * IMPORTANT: This command is not reentrant.
   *
   * @param throwException If <code>true</code> {@link com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *                       exception will be thrown in case of write command will be performed.
   */
  void freeze(boolean throwException);

  /**
   * Flush cached cluster content to the disk.
   * <p>
   * After this call users can perform only select queries. All write-related commands will queued till {@link #releaseCluster(int)}
   * command will be called.
   * <p>
   * Given command waits till all on going modifications in indexes or DB will be finished.
   * <p>
   * IMPORTANT: This command is not reentrant.
   *
   * @param iClusterId that must be released
   */
  void freezeCluster(int iClusterId);

  /**
   * Allows to execute write-related commands on the cluster
   *
   * @param iClusterId that must be released
   */
  void releaseCluster(int iClusterId);

  /**
   * Flush cached cluster content to the disk.
   * <p>
   * After this call users can perform only select queries. All write-related commands will queued till {@link #releaseCluster(int)}
   * command will be called.
   * <p>
   * Given command waits till all on going modifications in indexes or DB will be finished.
   * <p>
   * IMPORTANT: This command is not reentrant.
   *
   * @param iClusterId     that must be released
   * @param throwException If <code>true</code> {@link com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *                       exception will be thrown in case of write command will be performed.
   */
  void freezeCluster(int iClusterId, boolean throwException);

  enum OPERATION_MODE {
    SYNCHRONOUS, ASYNCHRONOUS, ASYNCHRONOUS_NOANSWER
  }

  /**
   * Creates a new entity instance.
   *
   * @return The new instance.
   */
  <RET extends Object> RET newInstance();

  /**
   * Returns the Dictionary manual index.
   *
   * @return ODictionary instance
   */
  ODictionary<T> getDictionary();

  /**
   * Returns the current user logged into the database.
   *
   * @see com.orientechnologies.orient.core.metadata.security.OSecurity
   */
  OSecurityUser getUser();

  /**
   * Set user for current database instance
   */
  void setUser(OSecurityUser user);

  /**
   * Loads the entity and return it.
   *
   * @param iObject The entity to load. If the entity was already loaded it will be reloaded and all the changes will be lost.
   * @return
   */
  <RET extends T> RET load(T iObject);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject    Record to load
   * @param iFetchPlan Fetch plan used
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject          Record to load
   * @param iFetchPlan       Fetch plan used
   * @param iLockingStrategy
   * @return The record received
   * @deprecated Usage of this method may lead to deadlocks.
   */
  @Deprecated
  <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject          Record to load
   * @param iFetchPlan       Fetch plan used
   * @param iLockingStrategy
   * @return The record received
   * @deprecated Usage of this method may lead to deadlocks.
   */
  @Deprecated
  <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache, boolean iUpdateCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject      Record to load
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Force the reloading of the entity.
   *
   * @param iObject      The entity to load. If the entity was already loaded it will be reloaded and all the changes will be lost.
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The loaded entity
   */
  <RET extends T> RET reload(final T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Force the reloading of the entity.
   *
   * @param iObject      The entity to load. If the entity was already loaded it will be reloaded and all the changes will be lost.
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @param force        Force to reload record even if storage has the same record as reloaded record, it is useful if fetch plan is not null
   *                     and alongside with root record linked records will be reloaded.
   * @return The loaded entity
   */
  <RET extends T> RET reload(final T iObject, String iFetchPlan, boolean iIgnoreCache, boolean force);

  /**
   * Loads the entity by the Record ID.
   *
   * @param recordId The unique record id of the entity to load.
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID recordId);

  /**
   * Loads the entity by the Record ID using a fetch plan.
   *
   * @param iRecordId  The unique record id of the entity to load.
   * @param iFetchPlan Fetch plan used
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan);

  /**
   * Loads the entity by the Record ID using a fetch plan and specifying if the cache must be ignored.
   *
   * @param iRecordId    The unique record id of the entity to load.
   * @param iFetchPlan   Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache);

  @Deprecated
  /**
   * @deprecated Usage of this method may lead to deadlocks.
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  @Deprecated
  /**
   * @deprecated Usage of this method may lead to deadlocks.
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean iUpdateCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy);

  /**
   * Saves an entity in synchronous mode. If the entity is not dirty, then the operation will be ignored. For custom entity
   * implementations assure to set the entity as dirty.
   *
   * @param iObject The entity to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject);

  /**
   * Saves an entity specifying the mode. If the entity is not dirty, then the operation will be ignored. For custom entity
   * implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject                The entity to save
   * @param iMode                  Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  <RET extends T> RET save(T iObject, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  /**
   * Saves an entity in the specified cluster in synchronous mode. If the entity is not dirty, then the operation will be ignored.
   * For custom entity implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject      The entity to save
   * @param iClusterName Name of the cluster where to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject, String iClusterName);

  /**
   * Saves an entity in the specified cluster specifying the mode. If the entity is not dirty, then the operation will be ignored.
   * For custom entity implementations assure to set the entity as dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject                The entity to save
   * @param iClusterName           Name of the cluster where to save
   * @param iMode                  Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  <RET extends T> RET save(T iObject, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  /**
   * Deletes an entity from the database in synchronous mode.
   *
   * @param iObject The entity to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> delete(T iObject);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID The RecordID to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> delete(ORID iRID);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID     The RecordID to delete.
   * @param iVersion for MVCC
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> delete(ORID iRID, ORecordVersion iVersion);

  /**
   * Hides records content by putting tombstone on the records position but does not delete record itself.
   * <p>
   * This method is used in case of record content itself is broken and cannot be read or deleted. So it is emergence method. This
   * method can be used only if there is no active transaction in database.
   *
   * @param rid record id.
   * @return <code>true</code> if record was hidden and <code>false</code> if record does not exits in database.
   * @throws java.lang.UnsupportedOperationException                              In case current version of cluster does not support given operation.
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if record is already deleted/hidden.
   */

  boolean hide(ORID rid);

  ODatabase<T> cleanOutRecord(ORID rid, ORecordVersion version);

  /**
   * Return active transaction. Cannot be null. If no transaction is active, then a OTransactionNoTx instance is returned.
   *
   * @return OTransaction implementation
   */
  OTransaction getTransaction();

  /**
   * Begins a new transaction. By default the type is OPTIMISTIC. If a previous transaction was started it will be rollbacked and
   * closed before to start a new one. A transaction once begun has to be closed by calling the {@link #commit()} or
   * {@link #rollback()}.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> begin();

  /**
   * Begins a new transaction specifying the transaction type. If a previous transaction was started it will be rollbacked and
   * closed before to start a new one. A transaction once begun has to be closed by calling the {@link #commit()} or
   * {@link #rollback()}.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> begin(OTransaction.TXTYPE iStatus);

  /**
   * Attaches a transaction as current.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  ODatabase<T> begin(OTransaction iTx) throws OTransactionException;

  /**
   * Commits the current transaction. The approach is all or nothing. All changes will be permanent following the storage type. If
   * the operation succeed all the entities changed inside the transaction context will be effectives. If the operation fails, all
   * the changed entities will be restored in the datastore. Memory instances are not guaranteed to being restored as well.
   *
   * @return
   */
  ODatabase<T> commit() throws OTransactionException;

  ODatabase<T> commit(boolean force) throws OTransactionException;

  /**
   * Aborts the current running transaction. All the pending changed entities will be restored in the datastore. Memory instances
   * are not guaranteed to being restored as well.
   *
   * @return
   */
  ODatabase<T> rollback() throws OTransactionException;

  ODatabase<T> rollback(boolean force) throws OTransactionException;

  /**
   * Execute a query against the database. If the OStorage used is remote (OStorageRemote) then the command will be executed
   * remotely and the result returned back to the calling client.
   *
   * @param iCommand Query command
   * @param iArgs    Optional parameters to bind to the query
   * @return List of POJOs
   */
  <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs);

  /**
   * Execute a command against the database. A command can be a SQL statement or a Procedure. If the OStorage used is remote
   * (OStorageRemote) then the command will be executed remotely and the result returned back to the calling client.
   *
   * @param iCommand Command request to execute.
   * @return The same Command request received as parameter.
   */
  <RET extends OCommandRequest> RET command(OCommandRequest iCommand);

  /**
   * Return the OMetadata instance. Cannot be null.
   *
   * @return The OMetadata instance.
   */
  OMetadata getMetadata();

  /**
   * Registers a hook to listen all events for Records.
   *
   * @param iHookImpl ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase<?>> DB registerHook(ORecordHook iHookImpl);

  <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition);

  /**
   * Retrieves all the registered hooks.
   *
   * @return A not-null unmodifiable map of ORecordHook and position instances. If there are no hooks registered, the Map is empty.
   */
  Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks();

  /**
   * Unregisters a previously registered hook.
   *
   * @param iHookImpl ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase<?>> DB unregisterHook(ORecordHook iHookImpl);

  /**
   * Invokes the callback on all the configured hooks.
   *
   * @param iObject The object passed change based on the Database implementation: records for
   *                {@link com.orientechnologies.orient.core.db.document.ODatabaseDocument} implementations and POJO for
   *                {@link com.orientechnologies.orient.core.db.object.ODatabaseObject} implementations.
   * @return True if the input record is changed, otherwise false
   */
  ORecordHook.RESULT callbackHooks(ORecordHook.TYPE iType, OIdentifiable iObject);

  /**
   * Returns if the Multi Version Concurrency Control is enabled or not. If enabled the version of the record is checked before each
   * update and delete against the records.
   *
   * @return true if enabled, otherwise false
   * @see com.orientechnologies.orient.core.db.document.ODatabaseDocument#setMVCC(boolean)
   */
  boolean isMVCC();

  /**
   * Retrieves all the registered listeners.
   *
   * @return An iterable of ODatabaseListener instances.
   */
  Iterable<ODatabaseListener> getListeners();

  /**
   * Enables or disables the Multi-Version Concurrency Control. If enabled the version of the record is checked before each update
   * and delete against the records.
   *
   * @param iValue
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @see com.orientechnologies.orient.core.db.document.ODatabaseDocument#isMVCC()
   */
  <DB extends ODatabase<?>> DB setMVCC(boolean iValue);

  String getType();

  /**
   * Returns the current record conflict strategy.
   */
  ORecordConflictStrategy getConflictStrategy();

  /**
   * Overrides record conflict strategy selecting the strategy by name.
   *
   * @param iStrategyName ORecordConflictStrategy strategy name
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase<?>> DB setConflictStrategy(String iStrategyName);

  /**
   * Overrides record conflict strategy.
   *
   * @param iResolver ORecordConflictStrategy implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  <DB extends ODatabase<?>> DB setConflictStrategy(ORecordConflictStrategy iResolver);
}
