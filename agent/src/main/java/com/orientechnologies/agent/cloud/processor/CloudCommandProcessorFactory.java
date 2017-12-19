package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.cloud.processor.backup.RemoveBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.backup.ListBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.backup.AddBackupCommandProcessor;
import com.orientechnologies.orientdb.cloud.protocol.CommandType;

import java.util.HashMap;
import java.util.Map;

public class CloudCommandProcessorFactory {

  public static final CloudCommandProcessorFactory INSTANCE = new CloudCommandProcessorFactory();

  private Map<String, CloudCommandProcessor> processors;

  private CloudCommandProcessorFactory() {
    processors = new HashMap<>();
    processors.put(CommandType.LIST_SERVERS.command, new ListServersCommandProcessor());
    processors.put(CommandType.LIST_BACKUPS.command, new ListBackupCommandProcessor());
    processors.put(CommandType.ADD_BACKUP.command, new AddBackupCommandProcessor());
    processors.put(CommandType.REMOVE_BACKUP.command, new RemoveBackupCommandProcessor());
  }

  public CloudCommandProcessor getProcessorFor(String type) {
    return processors.get(type);
  }
}
