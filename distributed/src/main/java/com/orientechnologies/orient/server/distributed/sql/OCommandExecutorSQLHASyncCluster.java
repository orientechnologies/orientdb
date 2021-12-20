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
package com.orientechnologies.orient.server.distributed.sql;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OHaSyncClusterStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncClusterTask;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * SQL HA SYNC CLUSTER command: synchronizes a cluster from distributed servers.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHASyncCluster extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "HA SYNC CLUSTER";

  private OHaSyncClusterStatement parsedStatement;

  public OCommandExecutorSQLHASyncCluster parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);
    try {
      parsedStatement =
          (OHaSyncClusterStatement) OStatementCache.get(this.parserText, getDatabase());
      preParsedStatement = parsedStatement;
    } catch (OCommandSQLParsingException sqlx) {
      throw sqlx;
    } catch (Exception e) {
      throwParsingException("Error parsing query: \n" + this.parserText + "\n" + e.getMessage(), e);
    }
    return this;
  }

  /** Execute the SYNC CLUSTER. */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.CLUSTER, "sync", ORole.PERMISSION_UPDATE);

    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }

    final ODistributedPlugin dManager =
        (ODistributedPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    try {
      if (this.parsedStatement.modeFull) {
        return replaceCluster(
            dManager,
            database,
            dManager.getServerInstance(),
            databaseName,
            this.parsedStatement.clusterName.getStringValue());
      }
      // else {
      // int merged = 0;
      // return String.format("Merged %d records", merged);
      // }
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException("Cannot execute synchronization of cluster"), e);
    }

    return "Mode not supported";
  }

  public static Object replaceCluster(
      final ODistributedPlugin dManager,
      final ODatabaseDocumentInternal database,
      final OServer serverInstance,
      final String databaseName,
      final String clusterName)
      throws IOException {

    return replaceCluster(dManager, serverInstance, databaseName, clusterName);
  }

  public static Object replaceCluster(
      final ODistributedPlugin dManager,
      final OServer serverInstance,
      final String databaseName,
      final String clusterName) {
    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    final String nodeName = dManager.getLocalNodeName();

    final List<String> nodesWhereClusterIsCfg = cfg.getServers(clusterName, null);
    nodesWhereClusterIsCfg.remove(nodeName);

    if (nodesWhereClusterIsCfg.isEmpty())
      throw new OCommandExecutionException(
          "Cannot synchronize cluster '"
              + clusterName
              + "' because is not configured on any running nodes");

    final OSyncClusterTask task = new OSyncClusterTask(clusterName);
    final ODistributedResponse response =
        dManager.sendRequest(
            databaseName,
            null,
            nodesWhereClusterIsCfg,
            task,
            dManager.getNextMessageIdCounter(),
            ODistributedRequest.EXECUTION_MODE.RESPONSE,
            null);

    final Map<String, Object> results = (Map<String, Object>) response.getPayload();

    File tempFile = null;
    FileOutputStream out = null;
    try {
      tempFile =
          new File(
              Orient.getTempPath()
                  + "/backup_"
                  + databaseName
                  + "_"
                  + clusterName
                  + "_toInstall.zip");
      if (tempFile.exists()) tempFile.delete();
      else tempFile.getParentFile().mkdirs();
      tempFile.createNewFile();

      long fileSize = 0;
      out = new FileOutputStream(tempFile, false);
      for (Map.Entry<String, Object> r : results.entrySet()) {
        final Object value = r.getValue();

        if (value instanceof Boolean) {
          continue;
        } else if (value instanceof Throwable) {
          ODistributedServerLog.error(
              null,
              nodeName,
              r.getKey(),
              ODistributedServerLog.DIRECTION.IN,
              "error on installing cluster %s in %s",
              (Exception) value,
              databaseName,
              dbPath);
        } else if (value instanceof ODistributedDatabaseChunk) {
          ODistributedDatabaseChunk chunk = (ODistributedDatabaseChunk) value;

          // DELETE ANY PREVIOUS .COMPLETED FILE
          final File completedFile = new File(tempFile.getAbsolutePath() + ".completed");
          if (completedFile.exists()) completedFile.delete();

          fileSize = writeDatabaseChunk(nodeName, 1, chunk, out);
          for (int chunkNum = 2; !chunk.last; chunkNum++) {
            final Object result =
                dManager.sendRequest(
                    databaseName,
                    null,
                    OMultiValue.getSingletonList(r.getKey()),
                    new OCopyDatabaseChunkTask(
                        chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                    dManager.getNextMessageIdCounter(),
                    ODistributedRequest.EXECUTION_MODE.RESPONSE,
                    null);

            if (result instanceof Boolean) {
              continue;
            } else if (result instanceof Exception) {
              ODistributedServerLog.error(
                  null,
                  nodeName,
                  r.getKey(),
                  ODistributedServerLog.DIRECTION.IN,
                  "error on installing database %s in %s (chunk #%d)",
                  (Exception) result,
                  databaseName,
                  dbPath,
                  chunkNum);
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

      final String tempDirectoryPath =
          Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + "_toInstall";
      final File tempDirectory = new File(tempDirectoryPath);
      tempDirectory.mkdirs();

      OZIPCompressionUtil.uncompressDirectory(
          new FileInputStream(tempFile), tempDirectory.getAbsolutePath(), null);

      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      final boolean openDatabaseHere = db == null;
      if (db == null) db = serverInstance.openDatabase(databaseName);

      try {

        db.getLocalCache().invalidate();
        int clusterId = db.getClusterIdByName(clusterName);
        OClass klass = db.getMetadata().getSchema().getClassByClusterId(clusterId);
        for (OIndex index : klass.getIndexes()) {
          index.rebuild();
        }

      } finally {
        if (openDatabaseHere) db.close();
      }

      return String.format("Cluster correctly replaced, transferred %d bytes", fileSize);

    } catch (Exception e) {
      ODistributedServerLog.error(
          null,
          nodeName,
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "error on transferring database '%s' to '%s'",
          e,
          databaseName,
          tempFile);
      throw OException.wrapException(
          new ODistributedException("Error on transferring database"), e);
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

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "HA SYNC CLUSTER <cluster-name> [-full_replace|-merge]";
  }

  protected static long writeDatabaseChunk(
      final String iNodeName,
      final int iChunkId,
      final ODistributedDatabaseChunk chunk,
      final FileOutputStream out)
      throws IOException {

    ODistributedServerLog.warn(
        null,
        iNodeName,
        null,
        ODistributedServerLog.DIRECTION.NONE,
        "- writing chunk #%d offset=%d size=%s",
        iChunkId,
        chunk.offset,
        OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
  }
}
