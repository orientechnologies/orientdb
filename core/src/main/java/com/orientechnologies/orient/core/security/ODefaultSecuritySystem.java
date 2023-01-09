/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(-at-)orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.authenticator.ODatabaseUserAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OServerConfigAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OSystemUserAuthenticator;
import com.orientechnologies.orient.core.security.authenticator.OTemporaryGlobalUser;
import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides an implementation of OServerSecurity.
 *
 * @author S. Colin Leister
 */
public class ODefaultSecuritySystem implements OSecuritySystem {
  private boolean enabled = false; // Defaults to not
  // enabled at
  // first.
  private boolean debug = false;

  private boolean storePasswords = true;

  // OServerSecurity (via OSecurityAuthenticator)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  private boolean allowDefault = true;

  private final Object passwordValidatorSynch = new Object();
  private OPasswordValidator passwordValidator;

  private final Object importLDAPSynch = new Object();
  private OSecurityComponent importLDAP;

  private final Object auditingSynch = new Object();
  private OAuditingService auditingService;

  private ODocument configDoc; // Holds the
  // current JSON
  // configuration.
  private OSecurityConfig serverConfig;
  private OrientDBInternal context;

  private ODocument auditingDoc;
  private ODocument serverDoc;
  private ODocument authDoc;
  private ODocument passwdValDoc;
  private ODocument ldapImportDoc;

  // We use a list because the order indicates priority of method.
  private volatile List<OSecurityAuthenticator> authenticatorsList;
  private volatile List<OSecurityAuthenticator> enabledAuthenticators;

  private final ConcurrentHashMap<String, Class<?>> securityClassMap =
      new ConcurrentHashMap<String, Class<?>>();
  private OTokenSign tokenSign;
  private final Map<String, OTemporaryGlobalUser> ephemeralUsers =
      new ConcurrentHashMap<String, OTemporaryGlobalUser>();

  private final Map<String, OGlobalUser> configUsers = new HashMap<String, OGlobalUser>();

  public ODefaultSecuritySystem() {}

  public void activate(final OrientDBInternal context, final OSecurityConfig serverCfg) {
    this.context = context;
    this.serverConfig = serverCfg;
    if (serverConfig != null) {
      this.load(serverConfig.getConfigurationFile());
    }
    onAfterDynamicPlugins();
    tokenSign = new OTokenSignImpl(context.getConfigurations().getConfigurations());
    for (OGlobalUser user : context.getConfigurations().getUsers()) {
      configUsers.put(user.getName(), user);
    }
  }

  public void createSystemRoles(ODatabaseSession session) {
    OSecurity security = session.getMetadata().getSecurity();
    if (security.getRole("root") == null) {
      ORole root = security.createRole("root", ORole.ALLOW_MODES.DENY_ALL_BUT);
      for (ORule.ResourceGeneric resource : ORule.ResourceGeneric.values()) {
        root.addRule(resource, null, ORole.PERMISSION_ALL);
      }
      // Do not allow root to have access to audit log class by default.
      root.addRule(ORule.ResourceGeneric.CLASS, "OAuditingLog", ORole.PERMISSION_NONE);
      root.addRule(ORule.ResourceGeneric.CLUSTER, "oauditinglog", ORole.PERMISSION_NONE);
      root.save();
    }
    if (security.getRole("guest") == null) {
      ORole guest = security.createRole("guest", ORole.ALLOW_MODES.DENY_ALL_BUT);
      guest.addRule(ResourceGeneric.SERVER, "listDatabases", ORole.PERMISSION_ALL);
      guest.save();
    }
    // for monitoring/logging purposes, intended to connect from external monitoring systems
    if (security.getRole("monitor") == null) {
      ORole guest = security.createRole("monitor", ORole.ALLOW_MODES.DENY_ALL_BUT);
      guest.addRule(ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
      guest.addRule(ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
      guest.addRule(ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_READ);
      guest.addRule(ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
      guest.addRule(ResourceGeneric.FUNCTION, null, ORole.PERMISSION_ALL);
      guest.addRule(ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
      guest.addRule(ResourceGeneric.COMMAND_GREMLIN, null, ORole.PERMISSION_ALL);
      guest.addRule(ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
      guest.addRule(ResourceGeneric.SERVER, null, ORole.PERMISSION_READ);
      guest.save();
    }
    // a separate role for accessing the auditing logs
    if (security.getRole("auditor") == null) {
      ORole auditor = security.createRole("auditor", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);
      auditor.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_READ);
      auditor.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_READ);
      auditor.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_READ);
      auditor.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_READ);
      auditor.addRule(ORule.ResourceGeneric.CLUSTER, "orole", ORole.PERMISSION_NONE);
      auditor.addRule(ORule.ResourceGeneric.CLUSTER, "ouser", ORole.PERMISSION_NONE);
      auditor.addRule(ORule.ResourceGeneric.CLASS, "OUser", ORole.PERMISSION_NONE);
      auditor.addRule(ORule.ResourceGeneric.CLASS, "orole", ORole.PERMISSION_NONE);
      auditor.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_NONE);
      auditor.addRule(
          ORule.ResourceGeneric.CLASS,
          "OAuditingLog",
          ORole.PERMISSION_CREATE + ORole.PERMISSION_READ + ORole.PERMISSION_UPDATE);
      auditor.addRule(
          ORule.ResourceGeneric.CLUSTER,
          "oauditinglog",
          ORole.PERMISSION_CREATE + ORole.PERMISSION_READ + ORole.PERMISSION_UPDATE);
      auditor.save();
    }
  }

  private void initDefultAuthenticators() {
    OServerConfigAuthenticator serverAuth = new OServerConfigAuthenticator();
    serverAuth.config(null, this);

    ODatabaseUserAuthenticator databaseAuth = new ODatabaseUserAuthenticator();
    databaseAuth.config(null, this);

    OSystemUserAuthenticator systemAuth = new OSystemUserAuthenticator();
    systemAuth.config(null, this);

    List<OSecurityAuthenticator> authenticators = new ArrayList<OSecurityAuthenticator>();
    authenticators.add(serverAuth);
    authenticators.add(systemAuth);
    authenticators.add(databaseAuth);
    setAuthenticatorList(authenticators);
  }

  public void shutdown() {
    close();
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
    } catch (Exception th) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getClass() Throwable: ", th);
    }

    return cls;
  }

  // OSecuritySystem (via OServerSecurity)
  // Some external security implementations may permit falling back to a
  // default authentication mode if external authentication fails.
  public boolean isDefaultAllowed() {
    if (isEnabled()) return allowDefault;
    else return true; // If the security system is disabled return the original system default.
  }

  @Override
  public OSecurityUser authenticate(
      ODatabaseSession session, OAuthenticationInfo authenticationInfo) {
    try {
      for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
        OSecurityUser principal = sa.authenticate(session, authenticationInfo);

        if (principal != null) return principal;
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate()", ex);
    }

    return null; // Indicates authentication failed.
  }

  // OSecuritySystem (via OServerSecurity)
  public OSecurityUser authenticate(
      ODatabaseSession session, final String username, final String password) {
    try {
      // It's possible for the username to be null or an empty string in the case of SPNEGO
      // Kerberos
      // tickets.
      if (username != null && !username.isEmpty()) {
        if (debug)
          OLogManager.instance()
              .info(
                  this,
                  "ODefaultServerSecurity.authenticate() ** Authenticating username: %s",
                  username);
      }

      for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
        OSecurityUser principal = sa.authenticate(session, username, password);

        if (principal != null) return principal;
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate()", ex);
    }

    return null; // Indicates authentication failed.
  }

  public OSecurityUser authenticateServerUser(final String username, final String password) {
    OSecurityUser user = getServerUser(username);

    if (user != null && user.getPassword() != null) {
      if (OSecurityManager.checkPassword(password, user.getPassword().trim())) {
        return user;
      }
    }
    return null;
  }

  public OrientDBInternal getContext() {
    return context;
  }

  // OSecuritySystem (via OServerSecurity)
  // Used for generating the appropriate HTTP authentication mechanism.
  public String getAuthenticationHeader(final String databaseName) {
    String header = null;

    // Default to Basic.
    if (databaseName != null)
      header = "WWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
    else header = "WWW-Authenticate: Basic realm=\"OrientDB Server\"";

    if (isEnabled()) {
      StringBuilder sb = new StringBuilder();

      // Walk through the list of OSecurityAuthenticators.
      for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
        String sah = sa.getAuthenticationHeader(databaseName);

        if (sah != null && sah.trim().length() > 0) {
          // If we're not the first authenticator, then append "\n".
          if (sb.length() > 0) {
            sb.append("\r\n");
          }
          sb.append(sah);
        }
      }

      if (sb.length() > 0) {
        header = sb.toString();
      }
    }

    return header;
  }

  @Override
  public Map<String, String> getAuthenticationHeaders(String databaseName) {
    Map<String, String> headers = new HashMap<>();

    // Default to Basic.
    if (databaseName != null)
      headers.put("WWW-Authenticate", "Basic realm=\"OrientDB db-" + databaseName + "\"");
    else headers.put("WWW-Authenticate", "Basic realm=\"OrientDB Server\"");

    if (isEnabled()) {

      // Walk through the list of OSecurityAuthenticators.
      for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
        if (sa.isEnabled()) {
          Map<String, String> currentHeaders = sa.getAuthenticationHeaders(databaseName);
          currentHeaders.entrySet().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        }
      }
    }

    return headers;
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
   * Returns the "System User" associated with 'username' from the system database. If not found,
   * returns null. dbName is used to filter the assigned roles. It may be null.
   */
  public OSecurityUser getSystemUser(final String username, final String dbName) {
    // ** There are cases when we need to retrieve an OUser that is a system user.
    //  if (isEnabled() && !OSystemDatabase.SYSTEM_DB_NAME.equals(dbName)) {
    if (context.getSystemDatabase().exists()) {
      return (OImmutableUser)
          context
              .getSystemDatabase()
              .execute(
                  (resultset) -> {
                    if (resultset != null && resultset.hasNext())
                      return new OImmutableUser(
                          0,
                          new OSystemUser(
                              (ODocument) resultset.next().getElement().get().getRecord(), dbName));
                    return null;
                  },
                  "select from OUser where name = ? limit 1 fetchplan roles:1",
                  username);
    }
    return null;
  }

  // OSecuritySystem (via OServerSecurity)
  // This will first look for a user in the security.json "users" array and then check if a resource
  // matches.
  public boolean isAuthorized(final String username, final String resource) {
    if (username == null || resource == null) return false;

    // Walk through the list of OSecurityAuthenticators.
    for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
      if (sa.isAuthorized(username, resource)) return true;
    }
    return false;
  }

  public boolean isServerUserAuthorized(final String username, final String resource) {
    final OSecurityUser user = getServerUser(username);

    if (user != null) {
      // TODO: to verify if this logic match previous logic
      return user.checkIfAllowed(resource, ORole.PERMISSION_ALL) != null;
      /*
      if (user.getResources().equals("*"))
        // ACCESS TO ALL
        return true;

      String[] resourceParts = user.getResources().split(",");
      for (String r : resourceParts) if (r.equalsIgnoreCase(resource)) return true;
      */
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
    if (isEnabled()) return storePasswords;
    else return true; // If the security system is disabled return the original system default.
  }

  // OSecuritySystem (via OServerSecurity)
  // Indicates if the primary security mechanism supports single sign-on.
  public boolean isSingleSignOnSupported() {
    if (isEnabled()) {
      OSecurityAuthenticator priAuth = getPrimaryAuthenticator();

      if (priAuth != null) return priAuth.isSingleSignOnSupported();
    }

    return false;
  }

  // OSecuritySystem (via OServerSecurity)
  public void validatePassword(final String username, final String password)
      throws OInvalidPasswordException {
    if (isEnabled()) {
      synchronized (passwordValidatorSynch) {
        if (passwordValidator != null) {
          passwordValidator.validatePassword(username, password);
        }
      }
    }
  }

  public void replacePasswordValidator(OPasswordValidator validator) {
    synchronized (passwordValidatorSynch) {
      if (passwordValidator == null || !passwordValidator.isEnabled()) {
        passwordValidator = validator;
      }
    }
  }

  /** OServerSecurity Interface * */

  // OServerSecurity
  public OAuditingService getAuditing() {
    return auditingService;
  }

  // OServerSecurity
  public OSecurityAuthenticator getAuthenticator(final String authMethod) {
    for (OSecurityAuthenticator am : getAuthenticatorsList()) {
      // If authMethod is null or an empty string, then return the first OSecurityAuthenticator.
      if (authMethod == null || authMethod.isEmpty()) return am;

      if (am.getName() != null && am.getName().equalsIgnoreCase(authMethod)) return am;
    }

    return null;
  }

  // OServerSecurity
  // Returns the first OSecurityAuthenticator in the list.
  public OSecurityAuthenticator getPrimaryAuthenticator() {
    if (isEnabled()) {
      List<OSecurityAuthenticator> auth = getAuthenticatorsList();
      if (auth.size() > 0) return auth.get(0);
    }

    return null;
  }

  // OServerSecurity
  public OSecurityUser getUser(final String username) {
    OSecurityUser userCfg = null;

    // Walk through the list of OSecurityAuthenticators.
    for (OSecurityAuthenticator sa : getEnabledAuthenticators()) {
      userCfg = sa.getUser(username);
      if (userCfg != null) break;
    }

    return userCfg;
  }

  public OSecurityUser getServerUser(final String username) {
    OSecurityUser systemUser = null;
    // This will throw an IllegalArgumentException if iUserName is null or empty.
    // However, a null or empty iUserName is possible with some security implementations.
    if (username != null && !username.isEmpty()) {
      OGlobalUser userCfg = configUsers.get(username);
      if (userCfg == null) {
        for (OTemporaryGlobalUser user : ephemeralUsers.values()) {
          if (username.equalsIgnoreCase(user.getName())) {
            // FOUND
            userCfg = user;
          }
        }
      }
      if (userCfg != null) {
        OSecurityRole role = OSecurityShared.createRole(null, userCfg);
        systemUser =
            new OImmutableUser(
                username, userCfg.getPassword(), OSecurityUser.SERVER_USER_TYPE, role);
      }
    }

    return systemUser;
  }

  @Override
  public OSyslog getSyslog() {
    return serverConfig.getSyslog();
  }

  // OSecuritySystem
  public void log(
      final OAuditingOperation operation,
      final String dbName,
      OSecurityUser user,
      final String message) {
    synchronized (auditingSynch) {
      if (auditingService != null) auditingService.log(operation, dbName, user, message);
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

  public void load(final String cfgPath) {
    this.configDoc = loadConfig(cfgPath);
  }

  // OSecuritySystem
  public void reload(final String cfgPath) {
    reload(null, cfgPath);
  }

  @Override
  public void reload(OSecurityUser user, String cfgPath) {
    reload(user, loadConfig(cfgPath));
  }

  // OSecuritySystem
  public void reload(final ODocument configDoc) {
    reload(null, configDoc);
  }

  @Override
  public void reload(OSecurityUser user, ODocument configDoc) {
    if (configDoc != null) {
      close();

      this.configDoc = configDoc;

      onAfterDynamicPlugins(user);

      log(
          OAuditingOperation.RELOADEDSECURITY,
          null,
          user,
          "The security configuration file has been reloaded");
    } else {
      OLogManager.instance()
          .warn(
              this,
              "ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
      throw new OSecuritySystemException(
          "ODefaultServerSecurity.reload(ODocument) The provided configuration document is null");
    }
  }

  public void reloadComponent(OSecurityUser user, final String name, final ODocument jsonConfig) {
    if (name == null || name.isEmpty())
      throw new OSecuritySystemException(
          "ODefaultServerSecurity.reloadComponent() name is null or empty");
    if (jsonConfig == null)
      throw new OSecuritySystemException(
          "ODefaultServerSecurity.reloadComponent() Configuration document is null");

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
    setSection(name, jsonConfig);

    log(
        OAuditingOperation.RELOADEDSECURITY,
        null,
        user,
        String.format("The %s security component has been reloaded", name));
  }

  private void loadAuthenticators(final ODocument authDoc) {
    if (authDoc.containsField("authenticators")) {
      List<OSecurityAuthenticator> autheticators = new ArrayList<OSecurityAuthenticator>();
      List<ODocument> authMethodsList = authDoc.field("authenticators");

      for (ODocument authMethodDoc : authMethodsList) {
        try {
          if (authMethodDoc.containsField("name")) {
            final String name = authMethodDoc.field("name");

            // defaults to enabled if "enabled" is missing
            boolean enabled = true;

            if (authMethodDoc.containsField("enabled")) enabled = authMethodDoc.field("enabled");

            if (enabled) {
              Class<?> authClass = getClass(authMethodDoc);

              if (authClass != null) {
                if (OSecurityAuthenticator.class.isAssignableFrom(authClass)) {
                  OSecurityAuthenticator authPlugin =
                      (OSecurityAuthenticator) authClass.newInstance();

                  authPlugin.config(authMethodDoc, this);
                  authPlugin.active();

                  autheticators.add(authPlugin);
                } else {
                  OLogManager.instance()
                      .error(
                          this,
                          "ODefaultServerSecurity.loadAuthenticators() class is not an OSecurityAuthenticator",
                          null);
                }
              } else {
                OLogManager.instance()
                    .error(
                        this,
                        "ODefaultServerSecurity.loadAuthenticators() authentication class is null for %s",
                        null,
                        name);
              }
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.loadAuthenticators() authentication object is missing name",
                    null);
          }
        } catch (Exception ex) {
          OLogManager.instance()
              .error(this, "ODefaultServerSecurity.loadAuthenticators() Exception: ", ex);
        }
      }
      if (isDefaultAllowed()) {
        autheticators.add(new ODatabaseUserAuthenticator());
      }
      setAuthenticatorList(autheticators);
    } else {
      initDefultAuthenticators();
    }
  }

  // OServerSecurity
  public void onAfterDynamicPlugins() {
    onAfterDynamicPlugins(null);
  }

  @Override
  public void onAfterDynamicPlugins(OSecurityUser user) {
    if (configDoc != null) {
      loadComponents();

      if (isEnabled()) {
        log(OAuditingOperation.SECURITY, null, user, "The security module is now loaded");
      }
    } else {
      initDefultAuthenticators();
      OLogManager.instance().debug(this, "onAfterDynamicPlugins() Configuration document is empty");
    }
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
        OLogManager.instance()
            .error(
                this,
                "ODefaultServerSecurity.getSection(%s) Configuration document is null",
                null,
                section);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.getSection(%s)", ex, section);
    }

    return sectionDoc;
  }

  // Change the component section and save it to disk
  private void setSection(final String section, ODocument sectionDoc) {

    ODocument oldSection = getSection(section);
    try {
      if (configDoc != null) {

        configDoc.field(section, sectionDoc);
        String configFile =
            OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

        String ssf = OGlobalConfiguration.SERVER_SECURITY_FILE.getValueAsString();
        if (ssf != null) configFile = ssf;

        File f = new File(configFile);
        OIOUtils.writeFile(f, configDoc.toJSON("prettyPrint"));
      }
    } catch (Exception ex) {
      configDoc.field(section, oldSection);
      OLogManager.instance().error(this, "ODefaultServerSecurity.setSection(%s)", ex, section);
    }
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

            securityDoc = new ODocument().fromJSON(new String(buffer), "noMap");
          } finally {
            if (fis != null) fis.close();
          }
        } else {
          if (file.exists()) {
            OLogManager.instance()
                .warn(this, "Could not read the security JSON file: %s", null, jsonFile);
          } else {
            if (file.exists()) {
              OLogManager.instance()
                  .warn(this, "Security JSON file: %s do not exists", null, jsonFile);
            }
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "error loading security config", ex);
    }

    return securityDoc;
  }

  private boolean isEnabled(final ODocument sectionDoc) {
    boolean enabled = true;

    try {
      if (sectionDoc.containsField("enabled")) {
        enabled = sectionDoc.field("enabled");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.isEnabled()", ex);
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
        OLogManager.instance()
            .debug(this, "ODefaultServerSecurity.loadSecurity() jsonConfig is null");
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity()", ex);
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
      OLogManager.instance().error(this, "ODefaultServerSecurity.loadServer()", ex);
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
        if (passwdValDoc != null && isEnabled(passwdValDoc)) {

          if (passwordValidator != null) {
            passwordValidator.dispose();
            passwordValidator = null;
          }

          Class<?> cls = getClass(passwdValDoc);

          if (cls != null) {
            if (OPasswordValidator.class.isAssignableFrom(cls)) {
              passwordValidator = (OPasswordValidator) cls.newInstance();
              passwordValidator.config(passwdValDoc, this);
              passwordValidator.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadPasswordValidator() class is not an OPasswordValidator",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadPasswordValidator() PasswordValidator class property is missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadPasswordValidator()", ex);
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
              importLDAP.config(ldapImportDoc, this);
              importLDAP.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadImportLDAP() class is not an OSecurityComponent",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadImportLDAP() ImportLDAP class property is missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadImportLDAP()", ex);
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
              auditingService.config(auditingDoc, this);
              auditingService.active();
            } else {
              OLogManager.instance()
                  .error(
                      this,
                      "ODefaultServerSecurity.reloadAuditingService() class is not an OAuditingService",
                      null);
            }
          } else {
            OLogManager.instance()
                .error(
                    this,
                    "ODefaultServerSecurity.reloadAuditingService() Auditing class property is missing",
                    null);
          }
        }
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultServerSecurity.reloadAuditingService()", ex);
    }
  }

  public void close() {
    if (enabled) {

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

      setAuthenticatorList(Collections.emptyList());

      enabled = false;
    }
  }

  @Override
  public OSecurityUser authenticateAndAuthorize(
      String iUserName, String iPassword, String iResourceToCheck) {
    // Returns the authenticated username, if successful, otherwise null.
    OSecurityUser user = authenticate(null, iUserName, iPassword);

    // Authenticated, now see if the user is authorized.
    if (user != null) {
      if (isAuthorized(user.getName(), iResourceToCheck)) {
        return user;
      }
    }
    return null;
  }

  public boolean existsUser(String user) {
    return configUsers.containsKey(user);
  }

  public void addTemporaryUser(String iName, String iPassword, String iPermissions) {
    OTemporaryGlobalUser userCfg = new OTemporaryGlobalUser(iName, iPassword, iPermissions);
    ephemeralUsers.put(iName, userCfg);
  }

  @Override
  public OSecurityInternal newSecurity(String database) {
    return new OSecurityShared(this);
  }

  public synchronized void setAuthenticatorList(List<OSecurityAuthenticator> authenticators) {
    if (authenticatorsList != null) {
      for (OSecurityAuthenticator sa : authenticatorsList) {
        sa.dispose();
      }
    }
    this.authenticatorsList = Collections.unmodifiableList(authenticators);
    this.enabledAuthenticators =
        Collections.unmodifiableList(
            authenticators.stream().filter((x) -> x.isEnabled()).collect(Collectors.toList()));
  }

  public synchronized List<OSecurityAuthenticator> getEnabledAuthenticators() {
    return enabledAuthenticators;
  }

  public synchronized List<OSecurityAuthenticator> getAuthenticatorsList() {
    return authenticatorsList;
  }

  public OTokenSign getTokenSign() {
    return tokenSign;
  }
}
