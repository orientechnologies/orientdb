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
package com.orientechnologies.orient.server.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedThreadLocal;
import com.orientechnologies.orient.server.distributed.OServerOfflineException;

/**
 * Distributed task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLCommandDistributedTask extends OAbstractDistributedTask<Object> {
  private static final long serialVersionUID = 1L;

  protected String          text;

  public OSQLCommandDistributedTask() {
  }

  public OSQLCommandDistributedTask(final String nodeSource, final String databaseName, final EXECUTION_MODE iMode,
      final String iCommand) {
    super(nodeSource, databaseName, iMode);
    text = iCommand;
  }

  public OSQLCommandDistributedTask(final long iRunId, final long iOperationId, final String iCommand) {
    text = iCommand;
  }

  @Override
  public Object call() throws Exception {
    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(this, "DISTRIBUTED <- command: %s", text.toString());

    final ODistributedServerManager dManager = getDistributedServerManager();
    if (status != STATUS.ALIGN && !dManager.checkStatus("online") && !nodeSource.equals(dManager.getLocalNodeId()))
      // NODE NOT ONLINE, REFUSE THE OPEPRATION
      throw new OServerOfflineException(dManager.getLocalNodeId(), dManager.getStatus(),
          "Cannot execute the operation because the server is offline: current status: " + dManager.getStatus());

    final ODatabaseDocumentTx db = openDatabase();
    ODistributedThreadLocal.INSTANCE.distributedExecution = true;
    try {

      Object result = openDatabase().command(new OCommandSQL(text)).execute();

      if (mode != EXECUTION_MODE.FIRE_AND_FORGET)
        return result;

      // FIRE AND FORGET MODE: AVOID THE PAYLOAD AS RESULT
      return null;

    } finally {
      ODistributedThreadLocal.INSTANCE.distributedExecution = false;
      closeDatabase(db);
    }
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeUTF(text);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    text = in.readUTF();
  }

  @Override
  public String getName() {
    return "command_sql";
  }

  @Override
  public String toString() {
    return getName() + "(" + text + ")";
  }
}
