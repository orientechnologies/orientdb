package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;
import com.orientechnologies.orient.server.distributed.impl.task.ORunQueryExecutionPlanTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncClusterTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 30/03/17.
 */
public class ODatabaseDocumentDistributed extends ODatabaseDocumentEmbedded {

  private final OHazelcastPlugin hazelcastPlugin;

  public ODatabaseDocumentDistributed(OStorage storage, OHazelcastPlugin hazelcastPlugin) {
    super(storage);
    this.hazelcastPlugin = hazelcastPlugin;
  }

  public ODistributedStorage getStorageDistributed() {
    return (ODistributedStorage) super.getStorage();
  }

  /**
   * return the name of local node in the cluster
   *
   * @return the name of local node in the cluster
   */
  public String getLocalNodeName() {
    return getStorageDistributed().getNodeId();
  }

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values contain names of clusters (data
   * files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  public Map<String, Set<String>> getActiveClusterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getStorageDistributed().getDistributedConfiguration();
    for (String server : cfg.getRegisteredServers()) {
      if (server.equals("*") && cfg.getClustersOnServer(getLocalNodeName()).size() == 1 && cfg
          .getClustersOnServer(getLocalNodeName()).iterator().next().equals("*")) {
        //TODO check this!!!
        result.put(getLocalNodeName(), getStorage().getClusterNames());
      } else {
        result.put(server, getClustersOnServer(cfg, server));
      }
    }
    return result;
  }

  public Set<String> getClustersOnServer(ODistributedConfiguration cfg, String server) {
    Set<String> result = cfg.getClustersOnServer(server);
    if (result.contains("*")) {
      result.remove("*");
      HashSet<String> more = new HashSet<>();
      more.addAll(getStorage().getClusterNames());
      for (String s : cfg.getClusterNames()) {
        if (!cfg.getServers(s, null).contains(s)) {
          more.remove(s);
        }
      }
      result.addAll(more);
    }
    return result;
  }

  @Override
  protected void loadMetadata() {
    metadata = new OMetadataDefault(this);
    sharedContext = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextDistributed(getStorage());
        return shared;
      }
    });
    metadata.init(sharedContext);
    sharedContext.load(this);
  }

  /**
   * returns the data center map for current deploy. The keys are data center names, the values are node names per data center
   *
   * @return data center map for current deploy
   */
  public Map<String, Set<String>> getActiveDataCenterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getStorageDistributed().getDistributedConfiguration();
    Set<String> servers = cfg.getRegisteredServers();
    for (String server : servers) {
      String dc = cfg.getDataCenterOfServer(server);
      Set<String> dcConfig = result.get(dc);
      if (dcConfig == null) {
        dcConfig = new HashSet<>();
        result.put(dc, dcConfig);
      }
      dcConfig.add(server);
    }
    return result;
  }

  @Override
  public boolean isSharded() {
    Map<String, Set<String>> clusterMap = getActiveClusterMap();
    Iterator<Set<String>> iter = clusterMap.values().iterator();
    Set<String> firstClusterSet = null;
    if (iter.hasNext()) {
      firstClusterSet = iter.next();
    }
    while (iter.hasNext()) {
      if (!firstClusterSet.equals(iter.next())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentDistributed database = new ODatabaseDocumentDistributed(getStorage(), hazelcastPlugin);
    database.init(getConfig());
    database.internalOpen(getUser().getName(), null, false);
    database.callOnOpenListeners();
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean sync(boolean forceDeployment, boolean tryWithDelta) {
    checkSecurity(ORule.ResourceGeneric.DATABASE, "sync", ORole.PERMISSION_UPDATE);
    final OStorage stg = getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("SYNC DATABASE command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return dManager.installDatabase(true, databaseName, forceDeployment, tryWithDelta);
  }

  @Override
  public Map<String, Object> getHaStatus(boolean servers, boolean db, boolean latency, boolean messages) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    final String dbUrl = getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);

    Map<String, Object> row = new HashMap<>();
    final StringBuilder output = new StringBuilder();
    if (servers)
      row.put("servers", dManager.getClusterConfiguration());
    if (db)
      row.put("database", cfg.getDocument());
    if (latency)
      row.put("latency", ODistributedOutput.formatLatency(dManager, dManager.getClusterConfiguration()));
    if (messages)
      row.put("messages", ODistributedOutput.formatMessages(dManager, dManager.getClusterConfiguration()));

    return row;
  }

  @Override
  public boolean removeHaServer(String serverName) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE);

    final String dbUrl = getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    // The last parameter (true) indicates to set the node's database status to OFFLINE.
    // If this is changed to false, the node will be set to NOT_AVAILABLE, and then the auto-repairer will
    // re-synchronize the database on the node, and then set it to ONLINE.
    return dManager.removeNodeFromConfiguration(serverName, databaseName, false, true);
  }

  @Override
  public Map<String, Object> syncCluster(String clusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, "sync", ORole.PERMISSION_UPDATE);

    final String dbUrl = getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    final String nodeName = dManager.getLocalNodeName();

    final List<String> nodesWhereClusterIsCfg = cfg.getServers(clusterName, null);
    nodesWhereClusterIsCfg.remove(nodeName);

    if (nodesWhereClusterIsCfg.isEmpty())
      throw new OCommandExecutionException(
          "Cannot synchronize cluster '" + clusterName + "' because is not configured on any running nodes");

    final OSyncClusterTask task = new OSyncClusterTask(clusterName);
    final ODistributedResponse response = dManager
        .sendRequest(databaseName, null, nodesWhereClusterIsCfg, task, dManager.getNextMessageIdCounter(),
            ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

    final Map<String, Object> results = (Map<String, Object>) response.getPayload();

    File tempFile = null;
    FileOutputStream out = null;
    try {
      tempFile = new File(Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + "_toInstall.zip");
      if (tempFile.exists())
        tempFile.delete();
      else
        tempFile.getParentFile().mkdirs();
      tempFile.createNewFile();

      long fileSize = 0;
      out = new FileOutputStream(tempFile, false);
      for (Map.Entry<String, Object> r : results.entrySet()) {
        final Object value = r.getValue();

        if (value instanceof Boolean) {
          continue;
        } else if (value instanceof Throwable) {
          ODistributedServerLog
              .error(null, nodeName, r.getKey(), ODistributedServerLog.DIRECTION.IN, "error on installing cluster %s in %s",
                  (Exception) value, databaseName, dbPath);
        } else if (value instanceof ODistributedDatabaseChunk) {
          ODistributedDatabaseChunk chunk = (ODistributedDatabaseChunk) value;

          // DELETE ANY PREVIOUS .COMPLETED FILE
          final File completedFile = new File(tempFile.getAbsolutePath() + ".completed");
          if (completedFile.exists())
            completedFile.delete();

          fileSize = writeDatabaseChunk(nodeName, 1, chunk, out);
          for (int chunkNum = 2; !chunk.last; chunkNum++) {
            final Object result = dManager.sendRequest(databaseName, null, OMultiValue.getSingletonList(r.getKey()),
                new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

            if (result instanceof Boolean)
              continue;
            else if (result instanceof Exception) {
              ODistributedServerLog.error(null, nodeName, r.getKey(), ODistributedServerLog.DIRECTION.IN,
                  "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
            } else if (result instanceof ODistributedDatabaseChunk) {
              chunk = (ODistributedDatabaseChunk) result;

              fileSize += writeDatabaseChunk(nodeName, chunkNum, chunk, out);
            }
          }

          out.flush();

          // CREATE THE .COMPLETED FILE TO SIGNAL EOF
          new File(tempFile.getAbsolutePath() + ".completed").createNewFile();
        }
      }

      final String tempDirectoryPath = Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + "_toInstall";
      final File tempDirectory = new File(tempDirectoryPath);
      tempDirectory.mkdirs();

      OZIPCompressionUtil.uncompressDirectory(new FileInputStream(tempFile), tempDirectory.getAbsolutePath(), null);

      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      final boolean openDatabaseHere = db == null;
      if (db == null)
        db = serverInstance.openDatabase("plocal:" + dbPath, "", "", null, true);

      try {

        final OAbstractPaginatedStorage stg = (OAbstractPaginatedStorage) db.getStorage().getUnderlying();

        // TODO: FREEZE COULD IT NOT NEEDED
        stg.freeze(false);
        try {

          final OPaginatedCluster cluster = (OPaginatedCluster) stg.getClusterByName(clusterName);

          final File tempClusterFile = new File(tempDirectoryPath + "/" + clusterName + OPaginatedCluster.DEF_EXTENSION);

          cluster.replaceFile(tempClusterFile);

        } finally {
          stg.release();
        }

        db.getLocalCache().invalidate();

      } finally {
        if (openDatabaseHere)
          db.close();
      }

      Map<String, Object> result = new HashMap<>();
      result.put("fileSize", fileSize);
      result.put("message", "Cluster correctly replaced");
      result.put("result", true);
      return result;

    } catch (Exception e) {
      ODistributedServerLog
          .error(null, nodeName, null, ODistributedServerLog.DIRECTION.NONE, "error on transferring database '%s' to '%s'", e,
              databaseName, tempFile);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    } finally {
      try {
        if (out != null) {
          out.flush();
          out.close();
        }
      } catch (IOException e) {
      }
    }

  }

  protected static long writeDatabaseChunk(final String iNodeName, final int iChunkId, final ODistributedDatabaseChunk chunk,
      final FileOutputStream out) throws IOException {

    ODistributedServerLog
        .warn(null, iNodeName, null, ODistributedServerLog.DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId,
            chunk.offset, OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }

  @Override
  public OResultSet queryOnNode(String nodeName, OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    ORunQueryExecutionPlanTask task = new ORunQueryExecutionPlanTask(executionPlan, inputParameters, nodeName);
    ODistributedResponse result = executeTaskOnNode(task, nodeName);
    return task.getResult(result, this);
  }

  public ODistributedResponse executeTaskOnNode(ORemoteTask task, String nodeName) {
    final String dbUrl = getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    ODistributedServerManager dManager = serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new ODistributedException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return dManager.sendRequest(databaseName, null, Collections.singletonList(nodeName), task, dManager.getNextMessageIdCounter(),
        ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);
  }

  @Override
  public void init(OrientDBConfig config) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      super.init(config);
      return null;
    });
  }

  protected void createMetadata() {
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextDistributed(getStorage());
        return shared;
      }
    });
    metadata.init(shared);
    ((OSharedContextDistributed) shared).create(this);
  }

  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= ORID.CLUSTER_POS_INVALID && iClusterName != null) {
      rid.setClusterId(getClusterIdByName(iClusterName));
      if (rid.getClusterId() == -1)
        throw new IllegalArgumentException("Cluster name '" + iClusterName + "' is not configured");

    }
    OClass schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= ORID.CLUSTER_ID_INVALID && getStorage().isAssigningClusterIds()) {
      if (record instanceof ODocument) {
        //Immutable Schema Class not support distributed yet.
        schemaClass = ((ODocument) record).getSchemaClass();
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException("Document belongs to abstract class " + schemaClass.getName() + " and cannot be saved");
          rid.setClusterId(schemaClass.getClusterForNewInstance((ODocument) record));
        } else
          rid.setClusterId(getDefaultClusterId());
      } else {
        rid.setClusterId(getDefaultClusterId());
        if (record instanceof OBlob && rid.getClusterId() != ORID.CLUSTER_ID_INVALID) {
          // Set<Integer> blobClusters = getMetadata().getSchema().getBlobClusters();
          // if (!blobClusters.contains(rid.clusterId) && rid.clusterId != getDefaultClusterId() && rid.clusterId != 0) {
          // if (iClusterName == null)
          // iClusterName = getClusterNameById(rid.clusterId);
          // throw new IllegalArgumentException(
          // "Cluster name '" + iClusterName + "' (id=" + rid.clusterId + ") is not configured to store blobs, valid are "
          // + blobClusters.toString());
          // }
        }
      }
    } else if (record instanceof ODocument)
      schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
    // If the cluster id was set check is validity
    if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '" + messageClusterName + "' (id=" + rid.getClusterId() + ") is not configured to store the class '"
                  + schemaClass.getName() + "', valid are " + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  protected OMicroTransaction beginMicroTransaction() {
    if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed())
      return null;
    return super.beginMicroTransaction();
  }

  @Override
  protected boolean supportsMicroTransactions(ORecord record) {
    return OScenarioThreadLocal.INSTANCE.isRunModeDistributed();
  }
}
