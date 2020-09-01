package com.orientechnologies.orient.server.security;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.OServerLifecycleListener;

public class OSecurityServerLifecycleListener implements OServerLifecycleListener {

  private ODefaultServerSecurity security;

  public OSecurityServerLifecycleListener(ODefaultServerSecurity security) {
    this.security = security;
  }

  /** * OServerLifecycleListener Interface * */
  public void onBeforeActivate() {
    security.createSuperUser();

    // Default
    String configFile =
        OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

    // The default "security.json" file can be overridden in the server config file.
    String securityFile = security.getConfigProperty("server.security.file");
    if (securityFile != null) configFile = securityFile;

    String ssf =
        security
            .getServer()
            .getContextConfiguration()
            .getValueAsString(OGlobalConfiguration.SERVER_SECURITY_FILE);
    if (ssf != null) configFile = ssf;

    security.load(configFile);
  }

  // OServerLifecycleListener Interface
  public void onAfterActivate() {
    // Does nothing now.
  }

  // OServerLifecycleListener Interface
  public void onBeforeDeactivate() {
    security.close();
  }

  // OServerLifecycleListener Interface
  public void onAfterDeactivate() {}
}
