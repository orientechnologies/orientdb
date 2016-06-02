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
package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractCommandTask;

/**
 * Executes a script on distributed servers.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptTask extends OAbstractCommandTask {
  private static final long     serialVersionUID = 1L;
  public static final int       FACTORYID        = 6;

  protected String              text;
  protected Map<Object, Object> params;
  protected RESULT_STRATEGY     resultStrategy;

  public OScriptTask() {
  }

  public OScriptTask(final OCommandRequestText iCommand) {
    text = iCommand.getText();
    params = iCommand.getParameters();
  }

  public Object execute(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "execute command=%s db=%s",
        text.toString(), database.getName());

    final OCommandRequest cmd = database.command(new OCommandScript(text));

    final Object res;
    if (params != null)
      // EXECUTE WITH PARAMETERS
      res = cmd.execute(params);
    else
      res = cmd.execute();

    return res;
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return resultStrategy;
  }

  @Override
  public void setResultStrategy(final RESULT_STRATEGY resultStrategy) {
    this.resultStrategy = resultStrategy;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(text);
    out.writeObject(params);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    text = in.readUTF();
    params = (Map<Object, Object>) in.readObject();
  }

  @Override
  public String getName() {
    return "script";
  }

  @Override
  public String toString() {
    return super.toString() + "(" + text + ")";
  }

  @Override
  public String getPayload() {
    return text;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
