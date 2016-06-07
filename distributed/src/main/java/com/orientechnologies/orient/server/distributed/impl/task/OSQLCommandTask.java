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

import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORetryQueryException;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractCommandTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * Distributed task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSQLCommandTask extends OAbstractCommandTask {
  private static final long                                 serialVersionUID = 1L;
  public static final int                                   FACTORYID        = 5;

  protected String                                          text;
  protected Map<Object, Object>                             params;
  protected RESULT_STRATEGY                                 resultStrategy;
  protected Collection<String>                              clusters;
  protected OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType;
  protected long                                            timeout;

  public OSQLCommandTask() {
    clusters = new HashSet<String>();
  }

  public OSQLCommandTask(final OCommandRequestText iCommand, final Collection<String> iClusterNames) {
    clusters = iClusterNames;

    text = iCommand.getText();
    params = iCommand.getParameters();

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
    executor.parse(iCommand);
    quorumType = ((OCommandDistributedReplicateRequest) executor).getQuorumType();
    timeout = ((OCommandDistributedReplicateRequest) executor).getDistributedTimeout();
  }

  public Object execute(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "Execute command=%s db=%s",
          text.toString(), database.getName());

    Object res;

    while (true) {
      try {
        final OCommandRequest cmd = database.command(new OCommandSQL(text));

        OCommandExecutor executor = OCommandManager.instance().getExecutor((OCommandRequestInternal) cmd);
        executor.parse(cmd);

        final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate
            ? ((OCommandExecutorSQLDelegate) executor).getDelegate() : executor;

        if (exec instanceof OCommandExecutorSQLSelect && clusters.size() > 0) {
          // REWRITE THE TARGET TO USE CLUSTERS
          final StringBuilder buffer = new StringBuilder("cluster:[");
          int i = 0;
          for (String c : clusters) {
            if (i++ > 0)
              buffer.append(',');
            buffer.append(c);
          }
          buffer.append("]");

          ((OCommandExecutorSQLSelect) exec).setParsedTarget(new OSQLTarget(buffer.toString(), exec.getContext()));
        }

        if (params != null)
          // EXECUTE WITH PARAMETERS
          res = executor.execute(params);
        else
          res = executor.execute(null);

        break;
      } catch (ORetryQueryException e) {
        continue;
      }
    }

    return res;
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return quorumType;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return resultStrategy;
  }

  public void setResultStrategy(final RESULT_STRATEGY resultStrategy) {
    this.resultStrategy = resultStrategy;
  }

  @Override
  public long getDistributedTimeout() {
    return timeout;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(text);
    out.writeObject(params);

    out.writeInt(clusters.size());
    for (String c : clusters)
      out.writeUTF(c);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    text = in.readUTF();
    params = (Map<Object, Object>) in.readObject();

    final int cSize = in.readInt();
    clusters = new HashSet<String>(cSize);
    for (int i = 0; i < cSize; ++i)
      clusters.add(in.readUTF());
  }

  @Override
  public String getName() {
    return "command_sql";
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
  public ORemoteTask getUndoTask(final ODistributedRequestId reqId) {
    final OCommandRequest cmd = ODatabaseRecordThreadLocal.INSTANCE.get().command(new OCommandSQL(text));
    OCommandExecutor executor = OCommandManager.instance().getExecutor((OCommandRequestInternal) cmd);
    executor.parse(cmd);

    if (executor instanceof OCommandExecutorSQLDelegate)
      executor = ((OCommandExecutorSQLDelegate) executor).getDelegate();

    if (executor instanceof OCommandDistributedReplicateRequest) {
      final String undoCommand = ((OCommandDistributedReplicateRequest) executor).getUndoCommand();
      if (undoCommand != null) {
        final OSQLCommandTask undoTask = new OSQLCommandTask((OCommandRequestText) cmd, clusters);
        undoTask.setResultStrategy(resultStrategy);
        return undoTask;
      }
    }

    return super.getUndoTask(reqId);
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
