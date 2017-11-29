package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.orientdb.cloud.protocol.CommandType;

import java.util.HashMap;
import java.util.Map;

public class CloudCommandProcessorFactory {

  public static final CloudCommandProcessorFactory INSTANCE = new CloudCommandProcessorFactory();

  private Map<String, CloudCommandProcessor> processors;

  private CloudCommandProcessorFactory() {
    processors = new HashMap<>();
    processors.put(CommandType.LIST_SERVERS.command, new ListServersCommandProcessor());
  }

  public CloudCommandProcessor getProcessorFor(String type) {
    return processors.get(type);
  }
}
