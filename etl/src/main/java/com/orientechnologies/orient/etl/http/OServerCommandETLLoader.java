package com.orientechnologies.orient.etl.http;

import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandETLLoader implements OServerCommandLoader {

  @Override
  public String getCommandClass() {
    return OServerCommandETL.class.getName();
  }

  @Override
  public String getName() {
    return "etl-loader";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandETL();
  }
}
