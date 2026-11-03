package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandGetStaticContentLoader implements OServerCommandLoader {

  @Override
  public String getCommandClass() {
    return OServerCommandGetStaticContent.class.getName();
  }

  @Override
  public String getName() {
    return "static-content";
  }

  @Override
  public OServerCommand newInstance(OServerCommandConfiguration config) {
    return new OServerCommandGetStaticContent(config);
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandGetStaticContent(pattern, configs);
  }
}
