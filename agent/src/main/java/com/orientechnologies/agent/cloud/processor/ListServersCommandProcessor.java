package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.ServerBasicInfo;
import com.orientechnologies.orientdb.cloud.protocol.ServerList;

import java.util.Collection;

public class ListServersCommandProcessor implements CloudCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    ServerList serverList = getClusterConfig(agent.server.getDistributedManager());
    response.setPayload(serverList);

    return response;
  }

  public ServerList getClusterConfig(final ODistributedServerManager manager) {
    ServerList result = new ServerList();
    if (manager == null) { //single node
      ServerBasicInfo server = new ServerBasicInfo();
      server.setName("orientdb");
      server.setId("orientdb");
      result.addInfo(server);
    } else { //distributed
      final ODocument doc = manager.getClusterConfiguration();

      final Collection<ODocument> documents = doc.field("members");

      for (ODocument document : documents) {
        ServerBasicInfo server = new ServerBasicInfo();
        server.setName((String) document.field("name"));
        server.setId((String) document.field("name"));
        result.addInfo(server);
      }
    }
    return result;
  }

}
