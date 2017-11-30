package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;

public interface CloudCommandProcessor {
  CommandResponse execute(Command command, OEnterpriseAgent agent);

  default CommandResponse fromRequest(Command command) {
    CommandResponse response = new CommandResponse();
    response.setId(command.getId());
    response.setProjectId(OGlobalConfiguration.CLOUD_PROJECT_ID.getValueAsString());
    response.setResponseChannel(command.getResponseChannel());
    response.setSuccess(true);
    return response;
  }
}
