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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.util.OBackupable;
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Generic Database interface. Represents the lower level of the Database providing raw API to
 * access to the raw records.<br>
 * Limits:
 *
 * <ul>
 *   <li>Maximum records per cluster/class = <b>9.223.372.036 Billions</b>: 2^63 =
 *       9.223.372.036.854.775.808 records
 *   <li>Maximum records per database = <b>302.231.454.903.657 Billions</b>: 2^15 clusters x 2^63
 *       records = (2^78) 32.768 * 9,223.372.036.854.775.808 = 302.231,454.903.657.293.676.544
 *       records
 *   <li>Maximum storage per database = <b>19.807.040.628.566.084 Terabytes</b>: 2^31 data-segments
 *       x 2^63 bytes = (2^94) 2.147.483.648 x 9,223.372.036.854.775.808 Exabytes =
 *       19.807,040.628.566.084.398.385.987.584 Yottabytes
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface ODatabase<T> extends OBackupable, Closeable {

  enum STATUS {
    OPEN,
    CLOSED,
    IMPORTING
  }

  enum ATTRIBUTES {
    TYPE,
    STATUS,
    DEFAULTCLUSTERID,
    DATEFORMAT,
    DATETIMEFORMAT,
    TIMEZONE,
    LOCALECOUNTRY,
    LOCALELANGUAGE,
    CHARSET,
    CUSTOM,
    CLUSTERSELECTION,
    MINIMUMCLUSTERS,
    CONFLICTSTRATEGY,
    VALIDATION
  }

  /**
   * Opens a database using the user and password received as arguments.
   *
   * @param iUserName Username to login
   * @param iUserPassword Password associated to the user
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword);

  /**
   * Creates a new database.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create();

  /**
   * Creates new database from database backup. Only incremental backups are supported.
   *
   * @param incrementalBackupPath Path to incremental backup
   * @param <DB> Concrete database instance type.
   * @return he Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create(String incrementalBackupPath);

  /**
   * Creates a new database passing initial settings.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase> DB create(Map<OGlobalConfiguration, Object> iInitialSettings);

  /**
   * Activate current database instance on current thread. Call this method before using the
   * database if you switch between multiple databases instances on the same thread or if you pass
   * them across threads.
   */
  ODatabase activateOnCurrentThread();

  /** Returns true if the current database instance is active on current thread, otherwise false. */
  boolean isActiveOnCurrentThread();

  /** Reloads the database information like the cluster list. */
  void reload();

  /**
   * Drops a database.
   *
   * @throws ODatabaseException if database is closed. @Deprecated use instead {@link OrientDB#drop}
   */
  @Deprecated
  void drop();

  /**
   * Returns the database configuration settings. If defined, any database configuration overwrites
   * the global one.
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
   * Get the active intent in the current session.
   *
   * @return
   */
  OIntent getActiveIntent();

  /**
   * Checks if the database exists.
   *
   * @return True if already exists, otherwise false.
   */
  @Deprecated
  boolean exists();

  /**
   * Closes an opened database, if the database is already closed does nothing, if a transaction is
   * active will be rollback.
   */
  void close();

  /** Returns the current status of database. */
  STATUS getStatus();

  /** Set the current status of database. deprecated since 2.2 */
  @Deprecated
  <DB extends ODatabase> DB setStatus(STATUS iStatus);

  /** Returns the total size of the records in the database. */
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
   * Returns the default cluster id. If not specified all the new entities will be stored in the
   * default cluster.
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
  @Deprecated
  long getClusterRecordSizeByName(String iClusterName);

  /**
   * Returns the total size of records contained in the cluster defined by its id.
   *
   * @param iClusterId Cluster id
   * @return The name of searched cluster.
   */
  @Deprecated
  long getClusterRecordSizeById(int iClusterId);

  /**
   * Checks if the database is closed.
   *
   * @return true if is closed, otherwise false.
   */
  boolean isClosed();

  /**
   * Removes all data in the cluster with given name. As result indexes for this class will be
   * rebuilt.
   *
   * @param clusterName Name of cluster to be truncated.
   */
  void truncateCluster(String clusterName);

  /**
   * Counts all the entities in the specified cluster id.
   *
   * @param iCurrentClusterId Cluster id
   * @return Total number of entities contained in the specified cluster
   */
  long countClusterElements(int iCurrentClusterId);

  @Deprecated
  long countClusterElements(int iCurrentClusterId, boolean countTombstones);

  /**
   * Counts all the entities in the specified cluster ids.
   *
   * @param iClusterIds Array of cluster ids Cluster id
   * @return Total number of entities contained in the specified clusters
   */
  long countClusterElements(int[] iClusterIds);

  @Deprecated
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
   * @param iParameters Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addCluster(String iClusterName, Object... iParameters);

  /**
   * Adds a new cluster for store blobs.
   *
   * @param iClusterName Cluster name
   * @param iParameters Additional parameters to pass to the factories
   * @return Cluster id
   */
  int addBlobCluster(String iClusterName, Object... iParameters);

  /**
   * Retrieve the set of defined blob cluster.
   *
   * @return the set of defined blob cluster ids.
   */
  Set<Integer> getBlobClusterIds();

  /**
   * Adds a new cluster.
   *
   * @param iClusterName Cluster name
   * @param iRequestedId requested id of the cluster
   * @return Cluster id
   */
  int addCluster(String iClusterName, int iRequestedId);

  /**
   * Drops a cluster by its name. Physical clusters will be completely deleted
   *
   * @param iClusterName the name of the cluster
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(String iClusterName);

  /**
   * Drops a cluster by its id. Physical clusters will be completely deleted.
   *
   * @param iClusterId id of cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(int iClusterId);

  /**
   * Sets a property value
   *
   * @param iName Property name
   * @param iValue new value to set
   * @return The previous value if any, otherwise null
   * @deprecated use <code>OrientDBConfig.builder().setConfig(propertyName, propertyValue).build();
   *     </code> instead if you use >=3.0 API.
   */
  @Deprecated
  Object setProperty(String iName, Object iValue);

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   * @deprecated use {@link ODatabase#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
  Object getProperty(String iName);

  /**
   * Returns an iterator of the property entries
   *
   * @deprecated use {@link ODatabase#getConfiguration()} instead if you use >=3.0 API.
   */
  @Deprecated
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
   * @param iValue Value to set
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

  @Deprecated
  ORecordMetadata getRecordMetadata(final ORID rid);

  /**
   * Flush cached storage content to the disk.
   *
   * <p>After this call users can perform only idempotent calls like read records and
   * select/traverse queries. All write-related operations will queued till {@link #release()}
   * command will be called.
   *
   * <p>Given command waits till all on going modifications in indexes or DB will be finished.
   *
   * <p>IMPORTANT: This command is not reentrant.
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

  enum OPERATION_MODE {
    SYNCHRONOUS,
    ASYNCHRONOUS,
    ASYNCHRONOUS_NOANSWER
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
   * @deprecated Manual indexes are prohibited and will be removed
   */
  @Deprecated
  ODictionary<T> getDictionary();

  /**
   * Returns the current user logged into the database.
   *
   * @see com.orientechnologies.orient.core.metadata.security.OSecurity
   */
  OSecurityUser getUser();

  /**
   * retrieves a class from the schema
   *
   * @param className The class name
   * @return The object representing the class in the schema. Null if the class does not exist.
   */
  default OClass getClass(String className) {
    OSchema schema = getMetadata().getSchema();
    return schema.getClass(className);
  }

  /**
   * Creates a new class in the schema
   *
   * @param className the class name
   * @param superclasses a list of superclasses for the class (can be empty)
   * @return the class with the given name
   * @throws OSchemaException if a class with this name already exists or if one of the superclasses
   *     does not exist.
   */
  default OClass createClass(String className, String... superclasses) throws OSchemaException {
    OSchema schema = getMetadata().getSchema();
    schema.reload();
    OClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new OClass[superclasses.length];
      for (int i = 0; i < superclasses.length; i++) {
        String superclass = superclasses[i];
        OClass superclazz = schema.getClass(superclass);
        if (superclazz == null) {
          throw new OSchemaException("Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superclazz;
      }
    }
    OClass result = schema.getClass(className);
    if (result != null) {
      throw new OSchemaException("Class " + className + " already exists");
    }
    if (superclassInstances == null) {
      return schema.createClass(className);
    } else {
      return schema.createClass(className, superclassInstances);
    }
  }

  /**
   * Loads the entity and return it.
   *
   * @param iObject The entity to load. If the entity was already loaded it will be reloaded and all
   *     the changes will be lost.
   * @return
   */
  <RET extends T> RET load(T iObject);

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject Record to load
   * @param iFetchPlan Fetch plan used
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan);

  /**
   * Pessimistic lock a record.
   *
   * <p>In case of lock inside the transaction the lock will be release by the commit operation, In
   * case of lock outside a transaction unlock need to be call manually.
   *
   * @param recordId the id of the record that need to be locked
   * @return the record updated to the last state after the lock.
   * @throws OLockException In case of deadlock detected
   */
  <RET extends T> RET lock(ORID recordId) throws OLockException;

  /**
   * Pessimistic lock a record.
   *
   * @param recordId the id of the record that need to be locked
   * @param timeout for the record locking
   * @param timeoutUnit relative for the timeout
   * @return the record updated to the last state after the lock.
   * @throws OLockException In case of deadlock detected
   */
  <RET extends T> RET lock(ORID recordId, long timeout, TimeUnit timeoutUnit) throws OLockException;

  /**
   * Pessimistic unlock
   *
   * @param recordId the id of the record to unlock
   * @throws OLockException if the record is not locked.
   */
  void unlock(ORID recordId) throws OLockException;

  /**
   * Loads a record using a fetch plan.
   *
   * @param iObject Record to load
   * @param iFetchPlan Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The record received
   */
  <RET extends T> RET load(T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Force the reloading of the entity.
   *
   * @param iObject The entity to load. If the entity was already loaded it will be reloaded and all
   *     the changes will be lost.
   * @param iFetchPlan Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The loaded entity
   */
  <RET extends T> RET reload(final T iObject, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Force the reloading of the entity.
   *
   * @param iObject The entity to load. If the entity was already loaded it will be reloaded and all
   *     the changes will be lost.
   * @param iFetchPlan Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @param force Force to reload record even if storage has the same record as reloaded record, it
   *     is useful if fetch plan is not null and alongside with root record linked records will be
   *     reloaded.
   * @return The loaded entity
   */
  <RET extends T> RET reload(
      final T iObject, String iFetchPlan, boolean iIgnoreCache, boolean force);

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
   * @param iRecordId The unique record id of the entity to load.
   * @param iFetchPlan Fetch plan used
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan);

  /**
   * Loads the entity by the Record ID using a fetch plan and specifying if the cache must be
   * ignored.
   *
   * @param iRecordId The unique record id of the entity to load.
   * @param iFetchPlan Fetch plan used
   * @param iIgnoreCache Ignore cache or use it
   * @return The loaded entity
   */
  <RET extends T> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache);

  /**
   * Saves an entity in synchronous mode. If the entity is not dirty, then the operation will be
   * ignored. For custom entity implementations assure to set the entity as dirty.
   *
   * @param iObject The entity to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject);

  /**
   * Saves an entity specifying the mode. If the entity is not dirty, then the operation will be
   * ignored. For custom entity implementations assure to set the entity as dirty. If the cluster
   * does not exist, an error will be thrown.
   *
   * @param iObject The entity to save
   * @param iMode Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate Flag that indicates that record should be created. If record with current
   *     rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  <RET extends T> RET save(
      T iObject,
      OPERATION_MODE iMode,
      boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback);

  /**
   * Saves an entity in the specified cluster in synchronous mode. If the entity is not dirty, then
   * the operation will be ignored. For custom entity implementations assure to set the entity as
   * dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject The entity to save
   * @param iClusterName Name of the cluster where to save
   * @return The saved entity.
   */
  <RET extends T> RET save(T iObject, String iClusterName);

  /**
   * Saves an entity in the specified cluster specifying the mode. If the entity is not dirty, then
   * the operation will be ignored. For custom entity implementations assure to set the entity as
   * dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject The entity to save
   * @param iClusterName Name of the cluster where to save
   * @param iMode Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate Flag that indicates that record should be created. If record with current
   *     rid already exists, exception is thrown
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  <RET extends T> RET save(
      T iObject,
      String iClusterName,
      OPERATION_MODE iMode,
      boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback);

  /**
   * Deletes an entity from the database in synchronous mode.
   *
   * @param iObject The entity to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  ODatabase<T> delete(T iObject);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID The RecordID to delete.
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  ODatabase<T> delete(ORID iRID);

  /**
   * Deletes the entity with the received RID from the database.
   *
   * @param iRID The RecordID to delete.
   * @param iVersion for MVCC
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  ODatabase<T> delete(ORID iRID, int iVersion);

  /**
   * Return active transaction. Cannot be null. If no transaction is active, then a OTransactionNoTx
   * instance is returned.
   *
   * @return OTransaction implementation
   */
  OTransaction getTransaction();

  /**
   * Begins a new transaction. By default the type is OPTIMISTIC. If a previous transaction is
   * running a nested call counter is incremented. A transaction once begun has to be closed by
   * calling the {@link #commit()} or {@link #rollback()}.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  ODatabase<T> begin();

  /**
   * Begins a new transaction specifying the transaction type. If a previous transaction is running
   * a nested call counter is incremented. A transaction once begun has to be closed by calling the
   * {@link #commit()} or {@link #rollback()}.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  ODatabase<T> begin(OTransaction.TXTYPE iStatus);

  /**
   * Attaches a transaction as current.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  ODatabase<T> begin(OTransaction iTx) throws OTransactionException;

  /**
   * Commits the current transaction. The approach is all or nothing. All changes will be permanent
   * following the storage type. If the operation succeed all the entities changed inside the
   * transaction context will be effective. If the operation fails, all the changed entities will be
   * restored in the data store.
   *
   * @return
   */
  ODatabase<T> commit() throws OTransactionException;

  ODatabase<T> commit(boolean force) throws OTransactionException;

  /**
   * Aborts the current running transaction. All the pending changed entities will be restored in
   * the data store.
   *
   * @return
   */
  ODatabase<T> rollback() throws OTransactionException;

  ODatabase<T> rollback(boolean force) throws OTransactionException;

  /**
   * Execute a query against the database. If the OStorage used is remote (OStorageRemote) then the
   * command will be executed remotely and the result returned back to the calling client.
   *
   * @param iCommand Query command
   * @param iArgs Optional parameters to bind to the query
   * @return List of POJOs
   * @deprecated use {@link #query(String, Map)} or {@link #query(String, Object...)} instead
   */
  @Deprecated
  <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs);

  /**
   * Creates a command request to run a command against the database (you have to invoke
   * .execute(parameters) to actually execute it). A command can be a SQL statement or a Procedure.
   * If the OStorage used is remote (OStorageRemote) then the command will be executed remotely and
   * the result returned back to the calling client.
   *
   * @param iCommand Command request to execute.
   * @return The same Command request received as parameter.
   * @deprecated use {@link #command(String, Map)}, {@link #command(String, Object...)}, {@link
   *     #execute(String, String, Map)}, {@link #execute(String, String, Object...)} instead
   */
  @Deprecated
  <RET extends OCommandRequest> RET command(OCommandRequest iCommand);

  /**
   * Executes an SQL query. The result set has to be closed after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  OResultSet rs = db.query("SELECT FROM V where name = ?", "John"); while(rs.hasNext()){ OResult item = rs.next(); ... }
   * rs.close(); </code>
   *
   * @param query the query string
   * @param args query parameters (positional)
   * @return the query result set
   */
  default OResultSet query(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes an SQL query (idempotent). The result set has to be closed after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("name", "John"); OResultSet rs = db.query("SELECT
   * FROM V where name = :name", params); while(rs.hasNext()){ OResult item = rs.next(); ... } rs.close();
   * </code>
   *
   * @param query the query string
   * @param args query parameters (named)
   * @return
   */
  default OResultSet query(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes a generic (idempotent or non idempotent) command. The result set has to be closed
   * after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  OResultSet rs = db.command("INSERT INTO Person SET name = ?", "John"); ... rs.close(); </code>
   *
   * @param query
   * @param args query arguments
   * @return
   */
  default OResultSet command(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Executes a generic (idempotent or non idempotent) command. The result set has to be closed
   * after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("name", "John"); OResultSet rs = db.query("INSERT
   * INTO Person SET name = :name", params); ... rs.close(); </code>
   *
   * @param query
   * @param args
   * @return
   */
  default OResultSet command(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Execute a script in a specified query language. The result set has to be closed after usage
   * <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  String script = "INSERT INTO Person SET name = 'foo', surname = ?;"+ "INSERT INTO Person SET name = 'bar', surname =
   * ?;"+ "INSERT INTO Person SET name = 'baz', surname = ?;";
   * <p>
   * OResultSet rs = db.execute("sql", script, "Surname1", "Surname2", "Surname3"); ... rs.close();
   * </code>
   *
   * @param language
   * @param script
   * @param args
   * @return
   */
  default OResultSet execute(String language, String script, Object... args)
      throws OCommandExecutionException, OCommandScriptException {
    throw new UnsupportedOperationException();
  }

  /**
   * Execute a script of a specified query language The result set has to be closed after usage <br>
   * <br>
   * Sample usage:
   *
   * <p><code>
   *  Map&lt;String, Object&gt params = new HashMapMap&lt;&gt(); params.put("surname1", "Jones"); params.put("surname2",
   * "May"); params.put("surname3", "Ali");
   * <p>
   * String script = "INSERT INTO Person SET name = 'foo', surname = :surname1;"+ "INSERT INTO Person SET name = 'bar', surname =
   * :surname2;"+ "INSERT INTO Person SET name = 'baz', surname = :surname3;";
   * <p>
   * OResultSet rs = db.execute("sql", script, params); ... rs.close(); </code>
   *
   * @param language
   * @param script
   * @param args
   * @return
   */
  default OResultSet execute(String language, String script, Map<String, ?> args)
      throws OCommandExecutionException, OCommandScriptException {
    throw new UnsupportedOperationException();
  }

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
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  <DB extends ODatabase<?>> DB registerHook(ORecordHook iHookImpl);

  <DB extends ODatabase<?>> DB registerHook(
      final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition);

  /**
   * Retrieves all the registered hooks.
   *
   * @return A not-null unmodifiable map of ORecordHook and position instances. If there are no
   *     hooks registered, the Map is empty.
   */
  Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks();

  /**
   * Unregisters a previously registered hook.
   *
   * @param iHookImpl ORecordHook implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain. deprecated since 2.2
   */
  <DB extends ODatabase<?>> DB unregisterHook(ORecordHook iHookImpl);

  /**
   * Returns if the Multi Version Concurrency Control is enabled or not. If enabled the version of
   * the record is checked before each update and delete against the records.
   *
   * @return true if enabled, otherwise false
   * @see com.orientechnologies.orient.core.db.document.ODatabaseDocument#setMVCC(boolean)
   *     deprecated since 2.2
   */
  @Deprecated
  boolean isMVCC();

  /**
   * Retrieves all the registered listeners.
   *
   * @return An iterable of ODatabaseListener instances.
   */
  Iterable<ODatabaseListener> getListeners();

  /**
   * Enables or disables the Multi-Version Concurrency Control. If enabled the version of the record
   * is checked before each update and delete against the records.
   *
   * @param iValue
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain. deprecated since 2.2
   * @see com.orientechnologies.orient.core.db.document.ODatabaseDocument#isMVCC()
   */
  @Deprecated
  <DB extends ODatabase<?>> DB setMVCC(boolean iValue);

  String getType();

  /** Returns the current record conflict strategy. */
  @Deprecated
  ORecordConflictStrategy getConflictStrategy();

  /**
   * Overrides record conflict strategy selecting the strategy by name.
   *
   * @param iStrategyName ORecordConflictStrategy strategy name
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase<?>> DB setConflictStrategy(String iStrategyName);

  /**
   * Overrides record conflict strategy.
   *
   * @param iResolver ORecordConflictStrategy implementation
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple
   *     methods in chain.
   */
  @Deprecated
  <DB extends ODatabase<?>> DB setConflictStrategy(ORecordConflictStrategy iResolver);

  /**
   * Performs incremental backup of database content to the selected folder. This is thread safe
   * operation and can be done in normal operational mode.
   *
   * <p>If it will be first backup of data full content of database will be copied into folder
   * otherwise only changes after last backup in the same folder will be copied.
   *
   * @param path Path to backup folder.
   * @return File name of the backup
   * @since 2.2
   */
  String incrementalBackup(String path) throws UnsupportedOperationException;

  /**
   * Subscribe a query as a live query for future create/update event with the referred conditions
   *
   * @param query live query
   * @param listener the listener that receive the query results
   * @param args the live query args
   */
  OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Map<String, ?> args);

  /**
   * Subscribe a query as a live query for future create/update event with the referred conditions
   *
   * @param query live query
   * @param listener the listener that receive the query results
   * @param args the live query args
   */
  OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args);

  /**
   * Tries to execute a lambda in a transaction, retrying it if an ONeedRetryException is thrown.
   *
   * <p>If the DB does not have an active transaction, after the execution you will still be out of
   * tx.
   *
   * <p>If the DB has an active transaction, then the transaction has to be empty (no operations
   * executed yet) and after the execution you will be in a new transaction.
   *
   * @param nRetries the maximum number of retries (> 0)
   * @param function a lambda containing application code to execute in a commit/retry loop
   * @param <T> the return type of the lambda
   * @return The result of the execution of the lambda
   * @throws IllegalStateException if there are operations in the current transaction
   * @throws ONeedRetryException if the maximum number of retries is executed and all failed with an
   *     ONeedRetryException
   * @throws IllegalArgumentException if nRetries is <= 0
   * @throws UnsupportedOperationException if this type of database does not support automatic
   *     commit/retry
   */
  default <T> T executeWithRetry(int nRetries, Function<ODatabaseSession, T> function)
      throws IllegalStateException, IllegalArgumentException, ONeedRetryException,
          UnsupportedOperationException {
    if (nRetries < 1) {
      throw new IllegalArgumentException("invalid number of retries: " + nRetries);
    }
    OTransaction tx = getTransaction();
    boolean txActive = tx.isActive();
    if (txActive) {
      if (tx.getEntryCount() > 0) {
        throw new IllegalStateException(
            "executeWithRetry() cannot be used within a pending (dirty) transaction. Please commit or rollback before invoking it");
      }
    }
    if (!txActive) {
      begin();
    }

    T result = null;

    for (int i = 0; i < nRetries; i++) {
      try {
        result = function.apply((ODatabaseSession) this);
        commit();
        break;
      } catch (ONeedRetryException e) {
        if (i == nRetries - 1) {
          throw e;
        }
        rollback();
        begin();
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Error during tx retry"), e);
      }
    }

    if (txActive) {
      begin();
    }

    return result;
  }
}
