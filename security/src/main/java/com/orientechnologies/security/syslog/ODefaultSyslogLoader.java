package com.orientechnologies.security.syslog;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class ODefaultSyslogLoader implements OServerPluginLoader<ODefaultSyslogConfig> {

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

  @Override
  public Class<ODefaultSyslogConfig> getConfigClass() {
    return ODefaultSyslogConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, ODefaultSyslogConfig config) {
    return new ODefaultSyslog(context, config);
  }
}
