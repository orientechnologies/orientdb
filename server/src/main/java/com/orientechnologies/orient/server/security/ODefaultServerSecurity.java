/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityExternal;
import com.orientechnologies.orient.core.metadata.security.OSystemUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OAuditingOperation;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OSecurityFactory;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.OSecuritySystemException;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides an implementation of OServerSecurity.
 * 
 * @author S. Colin Leister
 * 
 */
public class ODefaultServerSecurity implements OSecurityFactory, OServerLifecycleListener, OServerSecurity {
  private boolean                             enabled                = false;                                    // Defaults to not
                                                                                                                 // enabled at
                                                                                                                 // first.
  private boolean                             debug                  = false;

  private boolean                             storePasswords         = true;

  // OServerSecurity (via OSecurityAuthenticator)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  private boolean                             allowDefault           = true;

  private Object                              passwordValidatorSynch = new Object();
  private OPasswordValidator                  passwordValidator;

  private Object                              importLDAPSynch        = new Object();
  private OSecurityComponent                  importLDAP;

  private Object                              auditingSynch          = new Object();
  private OAuditingService                    auditingService;

  private ODocument                           configDoc;                                                         // Holds the
                                                                                                                 // current JSON
                                                                                                                 // configuration.
  private OServer                             server;
  private OServerConfigurationManager         serverConfig;

  private ODocument                           auditingDoc;
  private ODocument                           serverDoc;
  private ODocument                           authDoc;
  private ODocument                           passwdValDoc;
  private ODocument                           ldapImportDoc;

  // The SuperUser is now only used by the ODefaultServerSecurity for self-authentication.
  private final String                        superUser              = "OSecurityModuleSuperUser";
  private String                              superUserPassword;
  private OServerUserConfiguration            superUserCfg;

  // We use a list because the order indicates priority of method.
  private final List<OSecurityAuthenticator>  authenticatorsList     = new ArrayList<OSecurityAuthenticator>();

  private ConcurrentHashMap<String, Class<?>> securityClassMap       = new ConcurrentHashMap<String, Class<?>>();
  private OSyslog                             sysLog;

  public ODefaultServerSecurity(final OServer oServer, final OServerConfigurationManager serverCfg) {
    server = oServer;
    serverConfig = serverCfg;

    oServer.registerLifecycleListener(this);
    OSecurityManager.instance().setSecurityFactory(this);
  }

  private Class<?> getClass(final ODocument jsonConfig) {
    Class<?> cls = null;

    try {
      if (jsonConfig.containsField("class")) {
        final String clsName = jsonConfig.field("class");

        if (securityClassMap.containsKey(clsName)) {
          cls = securityClassMap.get(clsName);
        } else {
          cls = Class.forName(clsName);
        }
      }
    } catch (Throwable th) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getClass() Throwable: ", th);
    }

    return cls;
  }

  // OSecuritySystem (via OServerSecurity)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  public boolean isDefaultAllowed() {
    if (isEnabled())
      return allowDefault;
    else
      return true; // If the security system is disabled return the original system default.
  }

  // OSecuritySystem (via OServerSecurity)
  public String authenticate(final String username, final String password) {
    try {
      // It's possible for the username to be null or an empty string in the case of SPNEGO Kerberos tickets.
      if (username != null && !username.isEmpty()) {
        if (debug)
          OLogManager.instance().info(this, "ODefaultServerSecurity.authenticate() ** Authenticating username: %s", username);

        // This means it originates from us (used by openDatabase).
        if (username.equals(superUser) && password.equals(superUserPassword))
          return superUser;
      }

      synchronized (authenticatorsList) {
        // Walk through the list of OSecurityAuthenticators.
        for (OSecurityAuthenticator sa : authenticatorsList) {
          if (sa.isEnabled()) {
            String principal = sa.authenticate(username, password);

            if (principal != null)
              return principal;
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate() Exception: %s", ex.getMessage());
    }

    return null; // Indicates authentication failed.
  }

  protected OServer getServer() {
    return server;
  }

  // OSecuritySystem (via OServerSecurity)
  // Used for generating the appropriate HTTP authentication mechanism.
  public String getAuthenticationHeader(final String databaseName) {
    String header = null;

    // Default to Basic.
    if (databaseName != null)
      header = "WWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
    else
      header = "WWW-Authenticate: Basic realm=\"OrientDB Server\"";

    if (isEnabled()) {
      synchronized (authenticatorsList) {
        StringBuilder sb = new StringBuilder();

        // Walk through the list of OSecurityAuthenticators.        
        for (OSecurityAuthenticator sa : authenticatorsList) {
          if (sa.isEnabled()) {
            String sah = sa.getAuthenticationHeader(databaseName);

            if (sah != null && sah.trim().length() > 0) {
              // If we're not the first authenticator, then append "\n".
              if(sb.length() > 0){
                 sb.append("\n");
              }
              sb.append(sah);
            }
          }
        }

        if (sb.length() > 0) {
          header = sb.toString();
        }
      }
    }

    return header;
  }

  // OSecuritySystem (via OServerSecurity)
  public ODocument getConfig() {
    ODocument jsonConfig = new ODocument();

    try {
      jsonConfig.field("enabled", enabled);
      jsonConfig.field("debug", debug);

      if (serverDoc != null) {
        jsonConfig.field("server", serverDoc, OType.EMBEDDED);
      }

      if (authDoc != null) {
        jsonConfig.field("authentication", authDoc, OType.EMBEDDED);
      }

      if (passwdValDoc != null) {
        jsonConfig.field("passwordValidator", passwdValDoc, OType.EMBEDDED);
      }

      if (ldapImportDoc != null) {
        jsonConfig.field("ldapImporter", ldapImportDoc, OType.EMBEDDED);
      }

      if (auditingDoc != null) {
        jsonConfig.field("auditing", auditingDoc, OType.EMBEDDED);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getConfig() Exception: %s", ex);
    }

    return jsonConfig;
  }

  // OSecuritySystem (via OServerSecurity)
  // public ODocument getComponentConfig(final String name) { return getSection(name); }

  public ODocument getComponentConfig(final String name) {
    if (name != null) {
      if (name.equalsIgnoreCase("auditing")) {
        return auditingDoc;
      } else if (name.equalsIgnoreCase("authentication")) {
        return authDoc;
      } else if (name.equalsIgnoreCase("ldapImporter")) {
        return ldapImportDoc;
      } else if (name.equalsIgnoreCase("passwordValidator")) {
        return passwdValDoc;
      } else if (name.equalsIgnoreCase("server")) {
        return serverDoc;
      }
    }

    return null;
  }

  // OServerSecurity
  /**
   * Returns the "System User" associated with 'username' from the system database. If not found, returns null. dbName is used to
   * filter the assigned roles. It may be null.
   */
  public OUser getSystemUser(final String username, final String dbName) {
    if (isEnabled()) {
      return (OUser) server.getSystemDatabase().execute(new OCallable<Object, Object>() {
        @Override
        public Object call(Object iArgument) {
          final List<ODocument> result = (List<ODocument>) iArgument;
          if (result != null && !result.isEmpty())
            return new OSystemUser(result.get(0), dbName);
          return null;
        }
      }, "select from OUser where name = ? limit 1 fetchplan roles:1", username);
    }
    return null;
  }

  // OSecuritySystem (via OServerSecurity)
  // This will first look for a user in the security.json "users" array and then check if a resource matches.
  public boolean isAuthorized(final String username, final String resource) {
    if (isEnabled()) {
      if (username == null || resource == null)
        return false;

      if (username.equals(superUser))
        return true;

      synchronized (authenticatorsList) {
        // Walk through the list of OSecurityAuthenticators.
        for (OSecurityAuthenticator sa : authenticatorsList) {
          if (sa.isEnabled()) {
            if (sa.isAuthorized(username, resource))
              return true;
          }
        }
      }
    }

    return false;
  }

  // OSecuritySystem (via OServerSecurity)
  public boolean isEnabled() {
    return enabled;
  }

  // OSecuritySystem (via OServerSecurity)
  // Indicates if passwords should be stored for users.
  public boolean arePasswordsStored() {
    if (isEnabled())
      return storePasswords;
    else
      return true; // If the security system is disabled return the original system default.
  }

  // OSecuritySystem (via OServerSecurity)
  // Indicates if the primary security mechanism supports single sign-on.
  public boolean isSingleSignOnSupported() {
    if (isEnabled()) {
      OSecurityAuthenticator priAuth = getPrimaryAuthenticator();

      if (priAuth != null)
        return priAuth.isSingleSignOnSupported();
    }

    return false;
  }

  // OSecuritySystem (via OServerSecurity)
  public void validatePassword(final String password) throws OInvalidPasswordException {
    if (isEnabled()) {
      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.validatePassword(password);
        }
      }
    }
  }

  /***
   * OServerSecurity Interface
   ***/

  // OServerSecurity
  public OAuditingService getAuditing() {
    return auditingService;
  }

  // OServerSecurity
  public OSecurityAuthenticator getAuthenticator(final String authMethod) {
    if (isEnabled()) {
      synchronized (authenticatorsList) {
        for (OSecurityAuthenticator am : authenticatorsList) {
          // If authMethod is null or an empty string, then return the first OSecurityAuthenticator.
          if (authMethod == null || authMethod.isEmpty())
            return am;

          if (am.getName() != null && am.getName().equalsIgnoreCase(authMethod))
            return am;
        }
      }
    }

    return null;
  }

  // OServerSecurity
  // Returns the first OSecurityAuthenticator in the list.
  public OSecurityAuthenticator getPrimaryAuthenticator() {
    if (isEnabled()) {
      synchronized (authenticatorsList) {
        if (authenticatorsList.size() > 0)
          return authenticatorsList.get(0);
      }
    }

    return null;
  }

  // OServerSecurity
  public OServerUserConfiguration getUser(final String username) {
    OServerUserConfiguration userCfg = null;

    if (isEnabled()) {
      if (username.equals(superUser))
        return superUserCfg;

      synchronized (authenticatorsList) {
        // Walk through the list of OSecurityAuthenticators.
        for (OSecurityAuthenticator sa : authenticatorsList) {
          if (sa.isEnabled()) {
            userCfg = sa.getUser(username);
            if (userCfg != null)
              break;
          }
        }
      }
    }

    return userCfg;
  }

  // OServerSecurity
  public ODatabase<?> openDatabase(final String dbName) {
    ODatabase<?> db = null;

    if (isEnabled()) {
      db = server.openDatabase(dbName, superUser, "", null, true); // true indicates bypassing security.
    }

    return db;
  }

  @Override
  public OSyslog getSyslog() {
    if (sysLog == null) {
      OServerPluginInfo syslogPlugin = server.getPluginManager().getPluginByName("syslog");
      if (syslogPlugin != null) {
        sysLog = (OSyslog) syslogPlugin.getInstance();
      }
    }
    return sysLog;
  }

  // OSecuritySystem
  public void log(final OAuditingOperation operation, final String dbName, final String username, final String message) {
    synchronized (auditingSynch) {
      if (auditingService != null)
        auditingService.log(operation, dbName, username, message);
    }  	
  }


  // OSecuritySystem
  public void registerSecurityClass(final Class<?> cls) {
    String fullTypeName = getFullTypeName(cls);

    if (fullTypeName != null) {
      securityClassMap.put(fullTypeName, cls);
    }
  }

  // OSecuritySystem
  public void unregisterSecurityClass(final Class<?> cls) {
    String fullTypeName = getFullTypeName(cls);

    if (fullTypeName != null) {
      securityClassMap.remove(fullTypeName);
    }
  }

  // Returns the package plus type name of Class.
  private static String getFullTypeName(Class<?> type) {
    String typeName = null;

    typeName = type.getSimpleName();

    Package pack = type.getPackage();

    if (pack != null) {
      typeName = pack.getName() + "." + typeName;
    }

    return typeName;
  }

  // OSecuritySystem
  public void reload(final String cfgPath) {
    reload(loadConfig(cfgPath));
  }

  // OSecuritySystem
  public void reload(final ODocument configDoc) {
    if (configDoc != null) {
      onBeforeDeactivate();

      this.configDoc = configDoc;

      onAfterActivate();

      log(OAuditingOperation.RELOADEDSECURITY, null, null, "The security configuration file has been reloaded");
    } else {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
      throw new OSecuritySystemException("ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
    }
  }

  public void reloadComponent(final String name, final ODocument jsonConfig) {
    if (name == null || name.isEmpty())
      throw new OSecuritySystemException("ODefaultServerSecurity.reloadComponent() name is null or empty");
    if (jsonConfig == null)
      throw new OSecuritySystemException("ODefaultServerSecurity.reloadComponent() Configuration document is null");

    if (name.equalsIgnoreCase("auditing")) {
      auditingDoc = jsonConfig;
      reloadAuditingService();
    } else if (name.equalsIgnoreCase("authentication")) {
      authDoc = jsonConfig;
      reloadAuthMethods();
    } else if (name.equalsIgnoreCase("ldapImporter")) {
      ldapImportDoc = jsonConfig;
      reloadImportLDAP();
    } else if (name.equalsIgnoreCase("passwordValidator")) {
      passwdValDoc = jsonConfig;
      reloadPasswordValidator();
    } else if (name.equalsIgnoreCase("server")) {
      serverDoc = jsonConfig;
      reloadServer();
    }
    
    log(OAuditingOperation.RELOADEDSECURITY, null, null, String.format("The %s security component has been reloaded", name));
  }

  /**
   * Called each time one of the security classes (OUser, ORole, OServerRole) is modified.
   */
  public void securityRecordChange(final String dbURL, final ODocument record) {

    // The point of this is to notify any matching (via URL) active database that its user
    // needs to be reloaded to pick up any changes that may affect its security permissions.

    // We execute this in a new thread to avoid blocking the caller for too long.
    Orient.instance().submit(new Runnable() {
      @Override
      public void run() {
        try {
          OClientConnectionManager ccm = server.getClientConnectionManager();

          if (ccm != null) {
            for (OClientConnection cc : ccm.getConnections()) {
              try {
                ODatabaseDocumentInternal ccDB = cc.getDatabase();
                if (ccDB != null) {
                  ccDB.activateOnCurrentThread();
                  if (!ccDB.isClosed() && ccDB.getURL() != null) {
                    if (ccDB.getURL().equals(dbURL)) {
                      ccDB.reloadUser();
                    }
                  }
                }
              } catch (Exception ex) {
                OLogManager.instance().error(this, "securityRecordChange() Exception: ", ex);
              }
            }
          }
        } catch (Exception ex) {
          OLogManager.instance().error(this, "securityRecordChange() Exception: ", ex);
        }
        ODatabaseRecordThreadLocal.INSTANCE.remove();
      }
    });

  }

  private void createSuperUser() {
    if (superUser == null)
      throw new OSecuritySystemException("ODefaultServerSecurity.createSuperUser() SuperUser cannot be null");

    try {
      // Assign a temporary password so that we know if authentication requests coming from the SuperUser are from us.
      superUserPassword = OSecurityManager.instance().createSHA256(String.valueOf(new java.util.Random().nextLong()));

      superUserCfg = new OServerUserConfiguration(superUser, superUserPassword, "*");
    } catch (Exception ex) {
      OLogManager.instance().error(this, "createSuperUser() Exception: ", ex);
    }

    if (superUserPassword == null)
      throw new OSecuritySystemException("ODefaultServerSecurity Could not create SuperUser");
  }

  private void loadAuthenticators(final ODocument authDoc) {
    synchronized (authenticatorsList) {
      for (OSecurityAuthenticator sa : authenticatorsList) {
        sa.dispose();
      }

      authenticatorsList.clear();

      if (authDoc.containsField("authenticators")) {
        List<ODocument> authMethodsList = authDoc.field("authenticators");

        for (ODocument authMethodDoc : authMethodsList) {
          try {
            if (authMethodDoc.containsField("name")) {
              final String name = authMethodDoc.field("name");

              // defaults to enabled if "enabled" is missing
              boolean enabled = true;

              if (authMethodDoc.containsField("enabled"))
                enabled = authMethodDoc.field("enabled");

              if (enabled) {
                Class<?> authClass = getClass(authMethodDoc);

                if (authClass != null) {
                  if (OSecurityAuthenticator.class.isAssignableFrom(authClass)) {
                    OSecurityAuthenticator authPlugin = (OSecurityAuthenticator) authClass.newInstance();

                    authPlugin.config(server, serverConfig, authMethodDoc);
                    authPlugin.active();

                    authenticatorsList.add(authPlugin);
                  } else {
                    OLogManager.instance().error(this,
                        "ODefaultServerSecurity.loadAuthenticators() class is not an OSecurityAuthenticator");
                  }
                } else {
                  OLogManager.instance().error(this,
                      "ODefaultServerSecurity.loadAuthenticators() authentication class is null for %s", name);
                }
              }
            } else {
              OLogManager.instance().error(this,
                  "ODefaultServerSecurity.loadAuthenticators() authentication object is missing name");
            }
          } catch (Throwable ex) {
            OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() Exception: ", ex);
          }
        }
      }
    }

  }

  /***
   * OServerLifecycleListener Interface
   ***/
  public void onBeforeActivate() {
    createSuperUser();

    // Default
    String configFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

    // The default "security.json" file can be overridden in the server config file.
    String securityFile = getConfigProperty("server.security.file");
    if (securityFile != null)
      configFile = securityFile;

    String ssf = OGlobalConfiguration.SERVER_SECURITY_FILE.getValueAsString();
    if (ssf != null)
      configFile = ssf;

    configDoc = loadConfig(configFile);
  }

  // OServerLifecycleListener Interface
  public void onAfterActivate() {
    if (configDoc != null) {
      loadComponents();

      if (isEnabled()) {
        registerRESTCommands();
        
        log(OAuditingOperation.SECURITY, null, null, "The security module is now loaded");
      }
    } else {
      OLogManager.instance().error(this, "onAfterActivate() Configuration document is empty");
    }
  }

  // OServerLifecycleListener Interface
  public void onBeforeDeactivate() {
    if (enabled) {
      unregisterRESTCommands();

      synchronized (importLDAPSynch) {
        if (importLDAP != null) {
          importLDAP.dispose();
          importLDAP = null;
        }
      }

      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.dispose();
          passwordValidator = null;
        }
      }

      synchronized (auditingSynch) {
        if (auditingService != null) {
          auditingService.dispose();
          auditingService = null;
        }
      }

      synchronized (authenticatorsList) {
        // Notify all the security components that the server is active.
        for (OSecurityAuthenticator sa : authenticatorsList) {
          sa.dispose();
        }

        authenticatorsList.clear();
      }

      enabled = false;
    }
  }

  // OServerLifecycleListener Interface
  public void onAfterDeactivate() {
  }

  protected void loadComponents() {
    // Loads the top-level configuration properties ("enabled" and "debug").
    loadSecurity();

    if (isEnabled()) {
      // Loads the "auditing" configuration properties.
      auditingDoc = getSection("auditing");
      reloadAuditingService();

      // Loads the "server" configuration properties.
      serverDoc = getSection("server");
      reloadServer();

      // Loads the "authentication" configuration properties.
      authDoc = getSection("authentication");
      reloadAuthMethods();

      // Loads the "passwordValidator" configuration properties.
      passwdValDoc = getSection("passwordValidator");
      reloadPasswordValidator();

      // Loads the "ldapImporter" configuration properties.
      ldapImportDoc = getSection("ldapImporter");
      reloadImportLDAP();
    }
  }

  // Returns a section of the JSON document configuration as an ODocument if section is present.
  private ODocument getSection(final String section) {
    ODocument sectionDoc = null;

    try {
      if (configDoc != null) {
        if (configDoc.containsField(section)) {
          sectionDoc = configDoc.field(section);
        }
      } else {
        OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s) Configuration document is null", section);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s) Exception: %s", section, ex.getMessage());
    }

    return sectionDoc;
  }

  // "${ORIENTDB_HOME}/config/security.json"
  private ODocument loadConfig(final String cfgPath) {
    ODocument securityDoc = null;

    try {
      if (cfgPath != null) {
        // Default
        String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);

        File file = new File(jsonFile);

        if (file.exists() && file.canRead()) {
          FileInputStream fis = null;

          try {
            fis = new FileInputStream(file);

            final byte[] buffer = new byte[(int) file.length()];
            fis.read(buffer);

            securityDoc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
          } finally {
            if (fis != null)
              fis.close();
          }
        } else {
          OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Could not access the security JSON file: %s",
              jsonFile);
        }
      } else {
        OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Configuration file path is null");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Exception: %s", ex.getMessage());
    }

    return securityDoc;
  }

  protected String getConfigProperty(final String name) {
    String value = null;

    if (server.getConfiguration() != null && server.getConfiguration().properties != null) {
      for (OServerEntryConfiguration p : server.getConfiguration().properties) {
        if (p.name.equals(name)) {
          value = OSystemVariableResolver.resolveSystemVariables(p.value);
          break;
        }
      }
    }

    return value;
  }

  private boolean isEnabled(final ODocument sectionDoc) {
    boolean enabled = true;

    try {
      if (sectionDoc.containsField("enabled")) {
        enabled = sectionDoc.field("enabled");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.isEnabled() Exception: %s", ex.getMessage());
    }

    return enabled;
  }

  private void loadSecurity() {
    try {
      enabled = false;

      if (configDoc != null) {
        if (configDoc.containsField("enabled")) {
          enabled = configDoc.field("enabled");
        }

        if (configDoc.containsField("debug")) {
          debug = configDoc.field("debug");
        }
      } else {
        OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity() jsonConfig is null");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity() Exception: %s", ex.getMessage());
    }
  }

  private void reloadServer() {
    try {
      storePasswords = true;

      if (serverDoc != null) {
        if (serverDoc.containsField("createDefaultUsers")) {
          OGlobalConfiguration.CREATE_DEFAULT_USERS.setValue(serverDoc.field("createDefaultUsers"));
        }

        if (serverDoc.containsField("storePasswords")) {
          storePasswords = serverDoc.field("storePasswords");
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadServer() Exception: %s", ex.getMessage());
    }
  }

  private void reloadAuthMethods() {
    if (authDoc != null) {
      if (authDoc.containsField("allowDefault")) {
        allowDefault = authDoc.field("allowDefault");
      }

      loadAuthenticators(authDoc);
    }
  }

  private void reloadPasswordValidator() {
    try {
      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.dispose();
          passwordValidator = null;
        }

        if (passwdValDoc != null && isEnabled(passwdValDoc)) {
          Class<?> cls = getClass(passwdValDoc);

          if (cls != null) {
            if (OPasswordValidator.class.isAssignableFrom(cls)) {
              passwordValidator = (OPasswordValidator) cls.newInstance();
              passwordValidator.config(server, serverConfig, passwdValDoc);
              passwordValidator.active();
            } else {
              OLogManager.instance().error(this,
                  "ODefaultServerSecurity.reloadPasswordValidator() class is not an OPasswordValidator");
            }
          } else {
            OLogManager.instance().error(this,
                "ODefaultServerSecurity.reloadPasswordValidator() PasswordValidator class property is missing");
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator() Exception: %s", ex.getMessage());
    }
  }

  private void reloadImportLDAP() {
    try {
      synchronized (importLDAPSynch) {
        if (importLDAP != null) {
          importLDAP.dispose();
          importLDAP = null;
        }

        if (ldapImportDoc != null && isEnabled(ldapImportDoc)) {
          Class<?> cls = getClass(ldapImportDoc);

          if (cls != null) {
            if (OSecurityComponent.class.isAssignableFrom(cls)) {
              importLDAP = (OSecurityComponent) cls.newInstance();
              importLDAP.config(server, serverConfig, ldapImportDoc);
              importLDAP.active();
            } else {
              OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() class is not an OSecurityComponent");
            }
          } else {
            OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() ImportLDAP class property is missing");
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP() Exception: %s", ex.getMessage());
    }
  }

  private void reloadAuditingService() {
    try {
      synchronized (auditingSynch) {
        if (auditingService != null) {
          auditingService.dispose();
          auditingService = null;
        }

        if (auditingDoc != null && isEnabled(auditingDoc)) {
          Class<?> cls = getClass(auditingDoc);

          if (cls != null) {
            if (OAuditingService.class.isAssignableFrom(cls)) {
              auditingService = (OAuditingService) cls.newInstance();
              auditingService.config(server, serverConfig, auditingDoc);
              auditingService.active();
            } else {
              OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() class is not an OAuditingService");
            }
          } else {
            OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() Auditing class property is missing");
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService() Exception: %s", ex.getMessage());
    }
  }

  /***
   * OSecurityFactory Interface
   ***/
  public OSecurity newSecurity() {
    return new OSecurityExternal();
  }

  private void registerRESTCommands() {
    try {
      final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

      if (listener != null) {
        // Register the REST API Command.
        // listener.registerStatelessCommand(new OServerCommandPostSecurityReload(this));
      } else {
        OLogManager.instance().error(this,
            "ODefaultServerSecurity.registerRESTCommands() unable to retrieve Network Protocol listener.");
      }
    } catch (Throwable th) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.registerRESTCommands() Throwable: " + th.getMessage());
    }
  }

  private void unregisterRESTCommands() {
    try {
      final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

      if (listener != null) {
        // listener.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
      } else {
        OLogManager.instance().error(this,
            "ODefaultServerSecurity.unregisterRESTCommands() unable to retrieve Network Protocol listener.");
      }
    } catch (Throwable th) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.unregisterRESTCommands() Throwable: " + th.getMessage());
    }
  }
}
