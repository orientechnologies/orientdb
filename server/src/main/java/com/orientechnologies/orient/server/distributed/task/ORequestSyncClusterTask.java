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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Send the request to ask for deployment of single cluster from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORequestSyncClusterTask extends OAbstractRemoteTask {
  protected String clusterName;

  public ORequestSyncClusterTask() {
  }

  public ORequestSyncClusterTask(final String iClusterName) {
    clusterName = iClusterName;
  }

  @Override
  public Object execute(final OServer iServer, final ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    final ODistributedConfiguration dbCfg = iManager.getDatabaseConfiguration(database.getName());
    List<String> nodes = dbCfg.getServers(clusterName, null);
    nodes.remove(iManager.getLocalNodeName());

    iManager.sendRequest(database.getName(), null, nodes, new OSyncClusterTask(clusterName),
        ODistributedRequest.EXECUTION_MODE.NO_RESPONSE);

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
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getName() {
    return "req_deploy_cluster";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeUTF(clusterName);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterName = in.readUTF();
  }

  @Override
  public boolean isRequiredOpenDatabase() {
    return true;
  }

}
