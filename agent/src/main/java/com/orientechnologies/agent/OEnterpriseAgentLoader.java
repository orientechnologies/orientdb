package com.orientechnologies.agent;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OEnterpriseAgentLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "enterpriseAgent";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OEnterpriseAgent();
  }

  @Override
  public String getPluginClassName() {
    return OEnterpriseAgent.class.getName();
  }

  @Override
  public Class getConfigClass() {
    return null;
  }

  @Override
  public OServerPlugin newInstance(OServer context, Object config) {
    return new OEnterpriseAgent(context, config);
  }
}
