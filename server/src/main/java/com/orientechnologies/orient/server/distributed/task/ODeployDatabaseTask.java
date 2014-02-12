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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.locks.Lock;

/**
 * Ask for deployment of database from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODeployDatabaseTask extends OAbstractReplicatedTask implements OCommandOutputListener {
  public final static int CHUNK_MAX_SIZE = 1048576; // 1MB

  public ODeployDatabaseTask() {
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {

      final String databaseName = database.getName();

      final Lock lock = iManager.getLock(databaseName);
      if (lock.tryLock()) {
        try {
          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying database %s...",
              databaseName);

          final File f = new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");
          if (f.exists())
            f.delete();
          else
            f.getParentFile().mkdirs();
          f.createNewFile();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "creating backup of database '%s' in directory: %s...", databaseName, f.getAbsolutePath());

          FileOutputStream fileOutputStream = new FileOutputStream(f);
          try {
            database.backup(fileOutputStream, null, null, this, 9, 1038336);

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "sending the compressed database '%s' over the NETWORK to node '%s', size=%s...", databaseName, getNodeSource(),
                OFileUtils.getSizeAsString(f.length()));

            final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(f, 0, CHUNK_MAX_SIZE);

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
                "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

            return chunk;

          } finally {
            fileOutputStream.close();
          }

        } finally {
          lock.unlock();
        }

      } else
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "skip deploying database %s because another node is doing it", databaseName);
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying database from the same node");

    return Boolean.FALSE;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }

  @Override
  public boolean isRequireNodeOnline() {
    return false;
  }

  @Override
  public long getTimeout() {
    return 60000;
  }

  @Override
  public String getPayload() {
    return null;
  }

  @Override
  public String getName() {
    return "deploy_db";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
  }

  @Override
  public OFixUpdateRecordTask getFixTask(ODistributedRequest iRequest, ODistributedResponse iBadResponse,
      ODistributedResponse iGoodResponse) {
    return null;
  }

  @Override
  public void onMessage(String iText) {
    OLogManager.instance().info(this, iText);
  }
}
