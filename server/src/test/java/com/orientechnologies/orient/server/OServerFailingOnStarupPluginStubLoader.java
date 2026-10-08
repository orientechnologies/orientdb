package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OServerFailingOnStarupPluginStubLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "failStart";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OServerFailingOnStarupPluginStub();
  }

  @Override
  public String getPluginClassName() {
    return OServerFailingOnStarupPluginStub.class.getName();
  }
}
