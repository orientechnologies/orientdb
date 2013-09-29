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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Copy database in chunks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCopyDatabaseChunkTask extends OAbstractReplicatedTask {
  private static final long serialVersionUID = 1L;

  private String            databaseName;
  private boolean           lastChunk        = false;
  private byte[]            chunkContent;

  public OCopyDatabaseChunkTask() {
  }

  public OCopyDatabaseChunkTask(final byte[] chunk) {
    chunkContent = chunk;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "writing database %s in chunk to disk size=%d...", database.getName(), chunkContent.length);

    final File f = new File("importDatabase/" + database.getName());

    final FileOutputStream out = new FileOutputStream(f, true);
    try {
      final ByteArrayInputStream in = new ByteArrayInputStream(chunkContent);
      try {
        OIOUtils.copyStream(in, out, chunkContent.length);
      } finally {
        in.close();
      }
    } finally {
      out.close();
    }

    if (lastChunk)
      try {
        ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "importing database %s...",
            database.getName());

        final ODatabaseImport importDb = new ODatabaseImport(database, f.getAbsolutePath(), null);
        try {
          importDb.importDatabase();
        } finally {
          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "database %s imported correctly", database.getName());

          importDb.close();
        }
      } finally {
        OFileUtils.deleteRecursively(new File("importDatabase"));
      }

    return Boolean.TRUE;
  }

  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }

  @Override
  public String getPayload() {
    return null;
  }

  @Override
  public OFixUpdateRecordTask getFixTask(ODistributedRequest iRequest, ODistributedResponse iGoodResponse) {
    return null;
  }

  @Override
  public String getName() {
    return "deploy_db";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.write(chunkContent);
    out.writeBoolean(lastChunk);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    databaseName = in.readUTF();
    in.read(chunkContent);
    lastChunk = in.readBoolean();
  }
}
