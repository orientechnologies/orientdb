package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OServerSideScriptInterpreterLoader
    implements OServerPluginLoader<OServerSideScriptInterpreterConfig> {

  @Override
  public String getName() {
    return "script";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OServerSideScriptInterpreter();
  }

  @Override
  public String getPluginClassName() {
    return OServerSideScriptInterpreter.class.getName();
  }

  @Override
  public Class<OServerSideScriptInterpreterConfig> getConfigClass() {
    return OServerSideScriptInterpreterConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, OServerSideScriptInterpreterConfig config) {
    return new OServerSideScriptInterpreter(context, config);
  }
}
