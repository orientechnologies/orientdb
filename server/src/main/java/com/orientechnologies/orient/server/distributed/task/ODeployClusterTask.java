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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ask for deployment of single cluster from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODeployClusterTask extends OAbstractReplicatedTask implements OCommandOutputListener {
  public final static int    CHUNK_MAX_SIZE = 1048576;         // 1MB
  public static final String DEPLOYCLUSTER  = "deploycluster.";
  protected long             random;

  protected String           clusterName;

  public ODeployClusterTask() {
  }

  public ODeployClusterTask(final String iName) {
    random = UUID.randomUUID().getLeastSignificantBits();
    clusterName = iName;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final ODistributedConfiguration dCfg = iManager.getDatabaseConfiguration(databaseName);
      if (!clusterName.equalsIgnoreCase(dCfg.getMasterServer(clusterName)))
        // NOT MASTER SERVER FOR THIS CLUSTER, SKIP IT
        return Boolean.FALSE;

      final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYCLUSTER + databaseName + "." + clusterName);
      if (lastDeployment != null && lastDeployment.longValue() == random) {
        // SKIP IT
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "skip deploying cluster '%s' because already executed", clusterName);
        return Boolean.FALSE;
      }

      iManager.getConfigurationMap().put(DEPLOYCLUSTER + databaseName + "." + clusterName, random);

      ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying cluster %s...",
          databaseName);

      final File f = new File(Orient.getTempPath() + "/backup_" + database.getName() + "_" + clusterName + ".zip");
      if (f.exists())
        f.delete();
      else
        f.getParentFile().mkdirs();
      f.createNewFile();

      ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          "creating backup of cluster '%s' in directory: %s...", databaseName, f.getAbsolutePath());

      final AtomicLong lastOperationId = new AtomicLong(-1);

      FileOutputStream fileOutputStream = new FileOutputStream(f);
      try {
        final OCluster cluster = database.getStorage().getClusterByName(clusterName);

        // cluster.backup(fileOutputStream, null, null, this,
        // OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(), CHUNK_MAX_SIZE);

        final long fileSize = f.length();

        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "sending the compressed cluster '%s.%s' over the NETWORK to node '%s', size=%s, lastOperationId=%d...", databaseName,
            clusterName, getNodeSource(), OFileUtils.getSizeAsString(fileSize), lastOperationId.get());

        final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(lastOperationId.get(), f, 0, CHUNK_MAX_SIZE);

        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

        if (chunk.last)
          // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
          iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

        return chunk;

      } finally {
        fileOutputStream.close();
      }

    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying cluster %s.%s from the same node");

    return Boolean.FALSE;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public boolean isRequireNodeOnline() {
    return false;
  }

  @Override
  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getPayload() {
    return null;
  }

  @Override
  public String getName() {
    return "deploy_cluster";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(random);
    out.writeUTF(clusterName);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    random = in.readLong();
    clusterName = in.readUTF();
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isRequiredOpenDatabase() {
    return true;
  }

}
