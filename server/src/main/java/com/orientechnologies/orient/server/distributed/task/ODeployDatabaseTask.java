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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.locks.Lock;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.type.OBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Ask for deployment of database from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODeployDatabaseTask extends OAbstractReplicatedTask {
  private static final long  serialVersionUID = 1L;
  // private static final String BACKUP_DIRECTORY = "tempBackups";

  protected final static int CHUNK_MAX_SIZE   = 1048576; // 1MB

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

          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          database.backup(out, null, null);

          final byte[] buffer = out.toByteArray();

          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "sending the compressed database %s over the network, total %s", databaseName,
              OFileUtils.getSizeAsString(buffer.length));

          return new OBuffer(buffer);

          // final File f = new File(BACKUP_DIRECTORY + "/" + database.getName());
          // database.backup(new FileOutputStream(f), null);
          //
          // final ByteArrayOutputStream out = new ByteArrayOutputStream(CHUNK_MAX_SIZE);
          // final FileInputStream in = new FileInputStream(f);
          // try {
          // final long fileSize = f.length();
          //
          // ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          // "copying %s to remote node...", OFileUtils.getSizeAsString(fileSize));
          //
          // for (int byteCopied = 0; byteCopied < fileSize;) {
          // byteCopied += OIOUtils.copyStream(in, out, CHUNK_MAX_SIZE);
          //
          // if ((Boolean) iManager.sendRequest(database.getName(), null, new OCopyDatabaseChunkTask(out.toByteArray()),
          // EXECUTION_MODE.RESPONSE)) {
          // out.reset();
          // }
          // }
          //
          // return "deployed";
          // } finally {
          // OFileUtils.deleteRecursively(new File(BACKUP_DIRECTORY));
          // }

        } finally {
          lock.unlock();
        }

      } else
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "skip deploying database %s because another node is doing it", databaseName);
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying database from the same node");

    return new OBuffer(new byte[0]);
  }

  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
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
}
