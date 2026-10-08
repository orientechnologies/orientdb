package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OProxyServerLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "proxy";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OProxyServer();
  }

  @Override
  public String getPluginClassName() {
    return OProxyServer.class.getName();
  }
}
