package com.orientechnologies.agent.http.command;

import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandGetProfilerLoader implements OServerCommandLoader {

  @Override
  public String getCommandClass() {
    return OServerCommandGetProfiler.class.getName();
  }

  @Override
  public String getName() {
    return "enterprise-profiler";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OServerCommandGetProfiler();
  }
}
