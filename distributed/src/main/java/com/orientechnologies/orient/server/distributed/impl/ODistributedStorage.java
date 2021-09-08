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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.impl.local.OSyncSource;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.OWriteOperationNotPermittedException;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;

/**
 * Distributed storage implementation that routes to the owner node the request.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedStorage
    implements OStorage, OFreezableStorageComponent, OAutoshardedStorage {
  private final String name;
  private final OServer serverInstance;
  private final ODistributedServerManager dManager;
  private volatile OAbstractPaginatedStorage wrapped;

  private ODistributedServerManager.DB_STATUS prevStatus;
  private ODistributedDatabase localDistributedDatabase;
  private ODistributedStorageEventListener eventListener;

  private volatile ODistributedConfiguration distributedConfiguration;
  private volatile OSyncSource lastValidBackup = null;

  public ODistributedStorage(final OServer iServer, final String dbName) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.name = dbName;
  }

  public synchronized void replaceIfNeeded(final OAbstractPaginatedStorage wrapped) {
    if (this.wrapped != wrapped) this.wrapped = wrapped;
  }

  public synchronized void wrap(final OAbstractPaginatedStorage wrapped) {
    if (this.wrapped != null)
      // ALREADY WRAPPED
      return;

    this.wrapped = wrapped;
    this.localDistributedDatabase = dManager.getMessageService().getDatabase(getName());
    ((ODistributedDatabaseImpl) this.localDistributedDatabase).fillStatus();
    ODistributedServerLog.debug(
        this,
        dManager != null ? dManager.getLocalNodeName() : "?",
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Installing distributed storage on database '%s'",
        wrapped.getName());

    final int queueSize =
        getServer()
            .getContextConfiguration()
            .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_ASYNCH_QUEUE_SIZE);
  }

  /** Supported only in embedded storage. Use <code>SELECT FROM metadata:storage</code> instead. */
  @Override
  public String getCreatedAtVersion() {
    throw new UnsupportedOperationException(
        "Supported only in embedded storage. Use 'SELECT FROM metadata:storage' instead.");
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return true;
  }

  public Object command(final OCommandRequestText iCommand) {
    return wrapped.command(iCommand);
  }

  public void acquireDistributedExclusiveLock(final long timeout) {
    dManager
        .getLockManagerRequester()
        .acquireExclusiveLock(getName(), dManager.getLocalNodeName(), timeout);
  }

  public void releaseDistributedExclusiveLock() {
    dManager.getLockManagerRequester().releaseExclusiveLock(getName(), dManager.getLocalNodeName());
  }

  public boolean isLocalEnv() {
    return localDistributedDatabase == null
        || dManager == null
        || distributedConfiguration == null
        || OScenarioThreadLocal.INSTANCE.isRunModeDistributed();
  }

  public OStorageOperationResult<ORawBuffer> readRecord(
      final ORecordId iRecordId,
      final String iFetchPlan,
      final boolean iIgnoreCache,
      final boolean prefetchRecords,
      final ORecordCallback<ORawBuffer> iCallback) {
    // ALREADY DISTRIBUTED
    return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(
      final ORecordId rid,
      final String fetchPlan,
      final boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    return wrapped.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(
      final ORecordId iRecordId,
      final int iVersion,
      final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    // IF is a real delete should be with a tx
    return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return wrapped.getSBtreeCollectionManager();
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return wrapped.getRecordMetadata(rid);
  }

  @Override
  public boolean cleanOutRecord(
      ORecordId recordId, final int recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return wrapped.cleanOutRecord(recordId, recordVersion, iMode, callback);
  }

  @Override
  public boolean existsResource(final String iName) {
    return wrapped.existsResource(iName);
  }

  @Override
  public String getClusterName(final int clusterId) {
    return wrapped.getClusterName(clusterId);
  }

  @Override
  public boolean setClusterAttribute(
      final int id, final OCluster.ATTRIBUTES attribute, final Object value) {
    return wrapped.setClusterAttribute(id, attribute, value);
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return getUnderlying().getRecordConflictStrategy();
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy iResolver) {
    getUnderlying().setConflictStrategy(iResolver);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T removeResource(final String iName) {
    return (T) wrapped.removeResource(iName);
  }

  @Override
  public <T> T getResource(final String iName, final Callable<T> iCallback) {
    return wrapped.getResource(iName, iCallback);
  }

  @Override
  public void open(
      final String iUserName, final String iUserPassword, final OContextConfiguration iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  @Override
  public void create(final OContextConfiguration iProperties) throws IOException {
    wrapped.create(iProperties);
  }

  @Override
  public boolean exists() {
    return wrapped.exists();
  }

  @Override
  public void reload() {
    wrapped.reload();
  }

  @Override
  public void delete() {
    if (wrapped instanceof OLocalPaginatedStorage) dropStorageFiles();

    wrapped.delete();
  }

  @Override
  public String incrementalBackup(final String backupDirectory, OCallable<Void, Void> started) {
    return wrapped.incrementalBackup(backupDirectory, started);
  }

  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    wrapped.restoreFromIncrementalBackup(filePath);
  }

  @Override
  public boolean supportIncremental() {
    return wrapped.supportIncremental();
  }

  @Override
  public void fullIncrementalBackup(final OutputStream stream)
      throws UnsupportedOperationException {
    wrapped.fullIncrementalBackup(stream);
  }

  @Override
  public void restoreFullIncrementalBackup(final InputStream stream)
      throws UnsupportedOperationException {
    wrapped.restoreFullIncrementalBackup(stream);
  }

  @Override
  public void close() {
    close(false, false);
  }

  @Override
  public void close(final boolean iForce, final boolean onDelete) {
    if (wrapped == null) return;

    if (onDelete && wrapped instanceof OLocalPaginatedStorage) {
      // REMOVE distributed-config.json and distributed-sync.json files to allow removal of
      // directory
      dropStorageFiles();
    }

    wrapped.close(iForce, onDelete);
  }

  public void closeOnDrop() {
    if (wrapped == null) return;
    if (wrapped instanceof OLocalPaginatedStorage) {
      // REMOVE distributed-config.json and distributed-sync.json files to allow removal of
      // directory
      dropStorageFiles();
    }
  }

  @Override
  public boolean isClosed() {
    if (wrapped == null) return true;

    return wrapped.isClosed();
  }

  @Override
  public List<ORecordOperation> commit(final OTransactionInternal iTx) {
    return wrapped.commit(iTx);
  }

  @Override
  public OStorageConfiguration getConfiguration() {
    return wrapped.getConfiguration();
  }

  @Override
  public int getClusters() {
    return wrapped.getClusters();
  }

  @Override
  public Set<String> getClusterNames() {
    return wrapped.getClusterNames();
  }

  @Override
  public Collection<? extends OCluster> getClusterInstances() {
    return wrapped.getClusterInstances();
  }

  @Override
  public int addCluster(final String iClusterName, final Object... iParameters) {
    return wrapped.addCluster(iClusterName, iParameters);
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId) {
    return wrapped.addCluster(iClusterName, iRequestedId);
  }

  public boolean dropCluster(final String iClusterName) {
    resetLastValidBackup();
    return wrapped.dropCluster(iClusterName);
  }

  @Override
  public boolean dropCluster(final int iId) {
    resetLastValidBackup();
    return wrapped.dropCluster(iId);
  }

  @Override
  public String getClusterNameById(int clusterId) {
    return wrapped.getClusterNameById(clusterId);
  }

  @Override
  public long getClusterRecordsSizeById(int clusterId) {
    return wrapped.getClusterRecordsSizeById(clusterId);
  }

  @Override
  public long getClusterRecordsSizeByName(String clusterName) {
    return wrapped.getClusterRecordsSizeByName(clusterName);
  }

  @Override
  public boolean setClusterAttribute(
      String clusterName, OCluster.ATTRIBUTES attribute, Object value) {
    return wrapped.setClusterAttribute(clusterName, attribute, value);
  }

  @Override
  public String getClusterRecordConflictStrategy(int clusterId) {
    return wrapped.getClusterRecordConflictStrategy(clusterId);
  }

  @Override
  public String getClusterEncryption(int clusterId) {
    return wrapped.getClusterEncryption(clusterId);
  }

  @Override
  public boolean isSystemCluster(int clusterId) {
    return wrapped.isSystemCluster(clusterId);
  }

  @Override
  public long getLastClusterPosition(int clusterId) {
    return wrapped.getLastClusterPosition(clusterId);
  }

  @Override
  public long getClusterNextPosition(int clusterId) {
    return wrapped.getClusterNextPosition(clusterId);
  }

  @Override
  public OPaginatedCluster.RECORD_STATUS getRecordStatus(ORID rid) {
    return wrapped.getRecordStatus(rid);
  }

  @Override
  public long count(final int iClusterId) {
    return wrapped.count(iClusterId);
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return wrapped.count(iClusterId, countTombstones);
  }

  public long count(final int[] iClusterIds) {
    return wrapped.count(iClusterIds);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    // TODO: SUPPORT SHARDING HERE
    return wrapped.count(iClusterIds, countTombstones);
  }

  @Override
  public long getSize() {
    return wrapped.getSize();
  }

  @Override
  public long countRecords() {
    return wrapped.countRecords();
  }

  @Override
  public int getDefaultClusterId() {
    return wrapped.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(final int defaultClusterId) {
    wrapped.setDefaultClusterId(defaultClusterId);
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    return wrapped.getClusterIdByName(iClusterName);
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    return wrapped.getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return wrapped.checkForRecordValidity(ppos);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getURL() {
    return wrapped.getURL();
  }

  @Override
  public long getVersion() {
    return wrapped.getVersion();
  }

  @Override
  public void synch() {
    wrapped.synch();
  }

  @Override
  public long[] getClusterDataRange(final int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  public STATUS getStatus() {
    return wrapped.getStatus();
  }

  public ODistributedStorageEventListener getEventListener() {
    return eventListener;
  }

  public void setEventListener(final ODistributedStorageEventListener eventListener) {
    this.eventListener = eventListener;
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(
      int currentClusterId, OPhysicalPosition entry) {
    return wrapped.higherPhysicalPositions(currentClusterId, entry);
  }

  public OServer getServer() {
    return serverInstance;
  }

  public ODistributedServerManager getDistributedManager() {
    return dManager;
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    if (distributedConfiguration == null) {
      final Map<String, Object> map = dManager.getConfigurationMap();
      if (map == null) return null;

      ODocument doc = (ODocument) map.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + getName());
      if (doc != null) {
        // DISTRIBUTED CFG AVAILABLE: COPY IT TO THE LOCAL DIRECTORY
        ODistributedServerLog.info(
            this,
            dManager.getLocalNodeName(),
            null,
            ODistributedServerLog.DIRECTION.NONE,
            "Downloaded configuration for database '%s' from the cluster",
            getName());
        setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
      } else {
        doc = loadDatabaseConfiguration(getDistributedConfigFile());
        if (doc == null) {
          // LOOK FOR THE STD FILE
          doc = loadDatabaseConfiguration(dManager.getDefaultDatabaseConfigFile());
          if (doc == null)
            throw new OConfigurationException(
                "Cannot load default distributed for database '"
                    + getName()
                    + "' config file: "
                    + dManager.getDefaultDatabaseConfigFile());

          // SAVE THE GENERIC FILE AS DATABASE FILE
          setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
        } else
          // JUST LOAD THE FILE IN MEMORY
          distributedConfiguration = new ODistributedConfiguration(doc);

        // LOADED FILE, PUBLISH IT IN THE CLUSTER
        dManager.updateCachedDatabaseConfiguration(
            getName(), new OModifiableDistributedConfiguration(doc), true);
      }
    }
    return distributedConfiguration;
  }

  public void setDistributedConfiguration(
      final OModifiableDistributedConfiguration distributedConfiguration) {
    if (this.distributedConfiguration == null
        || distributedConfiguration.getVersion() > this.distributedConfiguration.getVersion()) {
      this.distributedConfiguration =
          new ODistributedConfiguration(distributedConfiguration.getDocument().copy());

      // PRINT THE NEW CONFIGURATION
      final String cfgOutput =
          ODistributedOutput.formatClusterTable(
              dManager, getName(), distributedConfiguration, dManager.getTotalNodes(getName()));

      ODistributedServerLog.info(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Setting new distributed configuration for database: %s (version=%d)%s\n",
          getName(),
          distributedConfiguration.getVersion(),
          cfgOutput);

      saveDatabaseConfiguration();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(
      int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.ceilingPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(
      int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.floorPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition entry) {
    return wrapped.lowerPhysicalPositions(currentClusterId, entry);
  }

  public OStorage getUnderlying() {
    return wrapped;
  }

  @Override
  public boolean isRemote() {
    return false;
  }

  @Override
  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return wrapped.getComponentsFactory();
  }

  @Override
  public String getType() {
    return "distributed";
  }

  @Override
  public void freeze(final boolean throwException) {
    final String localNode = dManager.getLocalNodeName();

    prevStatus = dManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    getFreezableStorage().freeze(throwException);
  }

  @Override
  public void release() {
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE) {
      // RESTORE PREVIOUS STATUS
      getLocalDistributedDatabase().setOnline();
    }

    getFreezableStorage().release();
  }

  @Override
  public List<String> backup(
      final OutputStream out,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener,
      final int compressionLevel,
      final int bufferSize)
      throws IOException {
    // THIS CAUSES DEADLOCK
    // try {
    // return (List<String>) executeOperationInLock(new OCallable<Object, Void>() {
    // @Override
    // public Object call(Void iArgument) {
    final String localNode = dManager.getLocalNodeName();

    final ODistributedServerManager.DB_STATUS prevStatus =
        dManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    try {

      return wrapped.backup(out, options, callable, iListener, compressionLevel, bufferSize);

    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error on executing backup"), e);
    } finally {
      if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE) {
        // RESTORE PREVIOUS STATUS
        dManager.getMessageService().getDatabase(getName()).setOnline();
      }
    }
    // }
    // });
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // throw OException.wrapException(new OIOException("Backup interrupted"), e);
    // }
  }

  @Override
  public void restore(
      final InputStream in,
      final Map<String, Object> options,
      final Callable<Object> callable,
      final OCommandOutputListener iListener)
      throws IOException {
    wrapped.restore(in, options, callable, iListener);
  }

  public String getClusterNameByRID(final ORecordId iRid) {
    return wrapped.getClusterNameById(iRid.getClusterId());
  }

  @Override
  public String getStorageId() {
    return dManager.getLocalNodeName() + "." + getName();
  }

  @Override
  public String getNodeId() {
    return dManager != null ? dManager.getLocalNodeName() : "?";
  }

  @Override
  public void shutdown() {
    close(true, false);
  }

  protected void checkNodeIsMaster(
      final String localNodeName, final ODistributedConfiguration dbCfg, final String operation) {
    final ODistributedConfiguration.ROLES nodeRole = dbCfg.getServerRole(localNodeName);
    if (nodeRole != ODistributedConfiguration.ROLES.MASTER)
      throw new OWriteOperationNotPermittedException(
          "Cannot execute write operation ("
              + operation
              + ") on node '"
              + localNodeName
              + "' because is non a master");
  }

  public OSyncSource getLastValidBackup() {
    return lastValidBackup;
  }

  public void setLastValidBackup(final OSyncSource lastValidBackup) {
    this.lastValidBackup = lastValidBackup;
  }

  protected void handleDistributedException(
      final String iMessage, final Exception e, final Object... iParams) {
    if (e != null) {
      if (e instanceof OException) throw (OException) e;
      else if (e.getCause() instanceof OException) throw (OException) e.getCause();
      else if (e.getCause() != null && e.getCause().getCause() instanceof OException)
        throw (OException) e.getCause().getCause();
    }

    OLogManager.instance().error(this, iMessage, e, iParams);
    throw OException.wrapException(new OStorageException(String.format(iMessage, iParams)), e);
  }

  private OFreezableStorageComponent getFreezableStorage() {
    if (wrapped instanceof OFreezableStorageComponent) return wrapped;
    else
      throw new UnsupportedOperationException(
          "Storage engine " + wrapped.getType() + " does not support freeze operation");
  }

  public void resetLastValidBackup() {
    if (lastValidBackup != null) {
      lastValidBackup.invalidate();
    }
  }

  public void clearLastValidBackup() {
    if (lastValidBackup != null) {
      lastValidBackup = null;
    }
  }

  protected void dropStorageFiles() {
    dropStorageFiles((OLocalPaginatedStorage) wrapped);
  }

  public static void dropStorageFiles(OLocalPaginatedStorage storage) {
    // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
    final File dCfg =
        new File(
            storage.getStoragePath() + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);

    try {
      if (dCfg.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg.delete()) break;
          Thread.sleep(100);
        }
      }

      final File dCfg2 =
          new File(
              storage.getStoragePath()
                  + "/"
                  + ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME);
      if (dCfg2.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg2.delete()) break;
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  public ODocument loadDatabaseConfiguration(final File file) {
    if (!file.exists() || file.length() == 0) return null;

    ODistributedServerLog.info(
        this,
        dManager.getLocalNodeName(),
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "Loaded configuration for database '%s' from disk: %s",
        getName(),
        file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 1);
      return doc;

    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on loading distributed configuration file in: %s",
          e,
          file.getAbsolutePath());
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  protected void saveDatabaseConfiguration() {
    // SAVE THE CONFIGURATION TO DISK
    FileOutputStream f = null;
    try {
      File file = getDistributedConfigFile();

      ODistributedServerLog.debug(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Saving distributed configuration file for database '%s' to: %s",
          getName(),
          file);

      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }

      f = new FileOutputStream(file);
      f.write(distributedConfiguration.getDocument().toJSON().getBytes());
      f.flush();
    } catch (Exception e) {
      ODistributedServerLog.error(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Error on saving distributed configuration file",
          e);

    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
  }

  protected File getDistributedConfigFile() {
    return new File(
        serverInstance.getDatabaseDirectory()
            + getName()
            + "/"
            + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);
  }

  public ODistributedDatabase getLocalDistributedDatabase() {
    return localDistributedDatabase;
  }

  @Override
  public void setSchemaRecordId(String schemaRecordId) {
    wrapped.setSchemaRecordId(schemaRecordId);
  }

  @Override
  public void setDateFormat(String dateFormat) {
    wrapped.setDateFormat(dateFormat);
  }

  @Override
  public void setTimeZone(TimeZone timeZoneValue) {
    wrapped.setTimeZone(timeZoneValue);
  }

  @Override
  public void setLocaleLanguage(String locale) {
    wrapped.setLocaleLanguage(locale);
  }

  @Override
  public void setCharset(String charset) {
    wrapped.setCharset(charset);
  }

  @Override
  public void setIndexMgrRecordId(String indexMgrRecordId) {
    wrapped.setIndexMgrRecordId(indexMgrRecordId);
  }

  @Override
  public void setDateTimeFormat(String dateTimeFormat) {
    wrapped.setDateTimeFormat(dateTimeFormat);
  }

  @Override
  public void setLocaleCountry(String localeCountry) {
    wrapped.setLocaleCountry(localeCountry);
  }

  @Override
  public void setClusterSelection(String clusterSelection) {
    wrapped.setClusterSelection(clusterSelection);
  }

  @Override
  public void setMinimumClusters(int minimumClusters) {
    wrapped.setMinimumClusters(minimumClusters);
  }

  @Override
  public void setValidation(boolean validation) {
    wrapped.setValidation(validation);
  }

  @Override
  public void removeProperty(String property) {
    wrapped.removeProperty(property);
  }

  @Override
  public void setProperty(String property, String value) {
    wrapped.setProperty(property, value);
  }

  @Override
  public void setRecordSerializer(String recordSerializer, int version) {
    wrapped.setRecordSerializer(recordSerializer, version);
  }

  @Override
  public void clearProperties() {
    wrapped.clearProperties();
  }
}
