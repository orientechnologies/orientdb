package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OProxyServerLoader implements OServerPluginLoader<OProxyServerConfig> {

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

  @Override
  public Class<OProxyServerConfig> getConfigClass() {
    return OProxyServerConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, OProxyServerConfig config) {
    return new OProxyServer(context, config);
  }
}
