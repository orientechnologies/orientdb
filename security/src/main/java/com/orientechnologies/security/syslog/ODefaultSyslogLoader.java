package com.orientechnologies.security.syslog;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class ODefaultSyslogLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "syslog";
  }

  @Override
  public OServerPlugin newInstance() {
    return new ODefaultSyslog();
  }

  @Override
  public String getPluginClassName() {
    return ODefaultSyslog.class.getName();
  }
}
