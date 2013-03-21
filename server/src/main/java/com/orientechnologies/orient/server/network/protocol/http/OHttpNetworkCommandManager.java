package com.orientechnologies.orient.server.network.protocol.http;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;

public class OHttpNetworkCommandManager {
  private final Map<String, OServerCommand> exactCommands    = new HashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> wildcardCommands = new HashMap<String, OServerCommand>();
  private OHttpNetworkCommandManager        parent;

  public OHttpNetworkCommandManager(final OHttpNetworkCommandManager iParent) {
    parent = iParent;
  }

  public Object getCommand(String iName) {
    OServerCommand cmd = exactCommands.get(iName);

    if (cmd == null) {
      // TRY WITH WILDCARD COMMANDS
      // TODO: OPTIMIZE SEARCH!
      String partLeft, partRight;
      for (Entry<String, OServerCommand> entry : wildcardCommands.entrySet()) {
        final int wildcardPos = entry.getKey().indexOf('*');
        partLeft = entry.getKey().substring(0, wildcardPos);
        partRight = entry.getKey().substring(wildcardPos + 1);

        if (iName.startsWith(partLeft) && iName.endsWith(partRight)) {
          cmd = entry.getValue();
          break;
        }
      }
    }

    if (cmd == null && parent != null)
      cmd = (OServerCommand) parent.getCommand(iName);

    return cmd;
  }

  /**
   * Register all the names for the same instance.
   * 
   * @param iServerCommandInstance
   */
  public void registerCommand(OServerCommand iServerCommandInstance) {
    for (String name : iServerCommandInstance.getNames())
      if (OStringSerializerHelper.contains(name, '*'))
        wildcardCommands.put(name, iServerCommandInstance);
      else
        exactCommands.put(name, iServerCommandInstance);
  }
}
