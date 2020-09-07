package com.orientechnologies.orient.server;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.security.OSecurityConfig;
import com.orientechnologies.orient.server.security.OSyslog;
import java.io.IOException;

public class OServerSecurityConfig implements OSecurityConfig {

  private OServer server;
  private OServerConfigurationManager serverCfg;
  private OSyslog sysLog;

  public OServerSecurityConfig(OServer server, OServerConfigurationManager serverCfg) {
    super();
    this.server = server;
    this.serverCfg = serverCfg;
  }

  @Override
  public boolean existsUser(String user) {
    return serverCfg.existsUser(user);
  }

  @Override
  public void setUser(String user, String password, String permissions) {
    serverCfg.setUser(user, password, permissions);
  }

  @Override
  public void dropUser(String iUserName) {
    serverCfg.dropUser(iUserName);
  }

  @Override
  public void saveConfiguration() {
    try {
      serverCfg.saveConfiguration();
    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error saving the server configuration"), e);
    }
  }

  @Override
  public void setEphemeralUser(String iName, String iPassword, String iPermissions) {
    serverCfg.setEphemeralUser(iName, iPassword, iPermissions);
  }

  @Override
  public OServerUserConfiguration getUser(String username) {
    return serverCfg.getUser(username);
  }

  @Override
  public OSyslog getSyslog() {
    if (sysLog == null && server != null) {
      OServerPluginInfo syslogPlugin = server.getPluginManager().getPluginByName("syslog");
      if (syslogPlugin != null) {
        sysLog = (OSyslog) syslogPlugin.getInstance();
      }
    }
    return sysLog;
  }

  @Override
  public String getConfigurationFile() {
    // Default
    String configFile =
        OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

    String ssf =
        server
            .getContextConfiguration()
            .getValueAsString(OGlobalConfiguration.SERVER_SECURITY_FILE);
    if (ssf != null) configFile = ssf;
    return configFile;
  }
}
