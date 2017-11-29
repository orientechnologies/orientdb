package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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

    CommandResponse response = new CommandResponse();
    response.setId(command.getId());
    response.setProjectId(OGlobalConfiguration.CLOUD_PROJECT_ID.getValueAsString());
    response.setResponseChannel(command.getResponseChannel());
    response.setSuccess(true);

    ServerList serverList = getClusterConfig(agent.server.getDistributedManager());
    response.setPayload(serverList);

    return response;
  }

  public ServerList getClusterConfig(final ODistributedServerManager manager) {
    final ODocument doc = manager.getClusterConfiguration();

    ServerList result = new ServerList();

    final Collection<ODocument> documents = doc.field("members");

    for (ODocument document : documents) {
      ServerBasicInfo server = new ServerBasicInfo();
      server.setName((String) document.field("name"));
      server.setId((String) document.field("name"));
      result.addInfo(server);
    }

    return result;
  }

}
