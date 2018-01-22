package com.orientechnologies.agent.cloud.processor;

import com.orientechnologies.agent.cloud.processor.backup.*;
import com.orientechnologies.agent.cloud.processor.server.ListConnectionsCommandProcessor;
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
    processors.put(CommandType.CHANGE_BACKUP.command, new ChangeBackupCommandProcessor());
    processors.put(CommandType.LIST_BACKUP_LOGS.command, new ListBackupLogsCommandProcessor());
    processors.put(CommandType.REMOVE_BACKUP.command, new RemoveBackupCommandProcessor());
    processors.put(CommandType.RESTORE_BACKUP.command, new RestoreBackupCommandProcessor());
    processors.put(CommandType.SERVER_CONNECTIONS.command, new ListConnectionsCommandProcessor());
  }

  public CloudCommandProcessor getProcessorFor(String type) {
    return processors.get(type);
  }
}
