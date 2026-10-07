package com.orientechnologies.agent;

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
}
