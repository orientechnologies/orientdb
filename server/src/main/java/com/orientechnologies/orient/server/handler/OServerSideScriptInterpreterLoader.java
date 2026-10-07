package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OServerSideScriptInterpreterLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "script";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OServerSideScriptInterpreter();
  }
}
