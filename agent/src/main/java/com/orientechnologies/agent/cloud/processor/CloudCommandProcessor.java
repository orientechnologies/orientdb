package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;

public interface CloudCommandProcessor {
  CommandResponse execute(Command command, OEnterpriseAgent agent);
}
