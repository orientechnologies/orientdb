package com.orientechnologies.tinkerpop.command;

import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandPostCommandGremlinLoader implements OServerCommandLoader {

  @Override
  public String getCommandClass() {
    return OServerCommandPostCommandGremlin.class.getName();
  }

  @Override
  public String getName() {
    return "gremlin";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandPostCommandGremlin();
  }
}
