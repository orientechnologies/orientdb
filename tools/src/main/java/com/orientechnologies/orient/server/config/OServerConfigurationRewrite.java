package com.orientechnologies.orient.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OConfig;
import com.orientechnologies.orient.core.config.OConfigAuthentication;
import com.orientechnologies.orient.core.config.OConfigAuthenticator;
import com.orientechnologies.orient.core.config.OConfigNetwork;
import com.orientechnologies.orient.core.config.OConfigNetworkTls;
import com.orientechnologies.orient.core.config.OConfigPasswordValidator;
import com.orientechnologies.orient.core.config.OConfigPlugins;
import com.orientechnologies.orient.core.config.OConfigProperty;
import com.orientechnologies.orient.core.config.OConfigSecurity;
import com.orientechnologies.orient.core.config.OConfigSecurityAuditing;
import com.orientechnologies.orient.core.config.OConfigUser;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OHandlerConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class OServerConfigurationRewrite {

  public void rewrite(String file) throws IOException {
    var loader = new OServerConfigurationLoaderXml(OServerConfiguration.class, new File(file));
    OServerConfiguration load = loader.load();
    migrateDistributed(load, false);
    loader.save(load);
  }

  public OConfig convertToOConfig(OServerConfiguration configuration) throws IOException {
    OConfig config = new OConfig();
    config.setDatabases(configuration.getLocation());
    config.setPlugins(migratePlugins(configuration));
    config.setHandlers(migrateHandlers(configuration.getHandlers()));
    config.setListeners(migrateListeners(configuration.getNetwork()));
    config.setProperties(migrateProperties(configuration.getProperties()));
    config.setSecurity(migrateSecurity(configuration));
    return config;
  }

  private OConfigSecurity migrateSecurity(OServerConfiguration configuration) throws IOException {
    String configFile =
        OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");
    OConfigSecurity config;
    var ssf = configuration.getProperty(OGlobalConfiguration.SERVER_SECURITY_FILE.getKey());
    if (ssf != null) configFile = ssf;
    var path = Path.of(configFile);
    if (path.toFile().exists()) {
      String content = Files.readString(path);
      var configDoc = new ODocument();
      configDoc.fromJSON(content, "noMap");
      var newConfig = migrateConfigSecurity(configDoc);
      config = newConfig;
    } else {
      config = new OConfigSecurity();
    }
    var users = configuration.getUsers();
    if (users != null) {
      for (var user : users) {
        var newUsers = config.getUsers();
        if (newUsers == null) {
          newUsers = new ArrayList<>();
        }
        newUsers.add(new OConfigUser(user.getName(), user.getPassword(), user.getResources()));
        config.setUsers(newUsers);
      }
    }
    var sec = configuration.getSecurity();
    if (sec != null) {
      if (sec.getUsers() != null) {
        for (var user : sec.getUsers()) {
          var newUsers = config.getUsers();
          if (newUsers == null) {
            newUsers = new ArrayList<>();
          }
          newUsers.add(new OConfigUser(user.getName(), user.getPassword(), user.getResources()));
          config.setUsers(newUsers);
        }
      }
    }
    return config;
  }

  protected void migrateUsers(OConfigSecurity config, OServerUserConfiguration[] users) {
    for (var user : users) {
      var newUsers = config.getUsers();
      if (newUsers == null) {
        newUsers = new ArrayList<>();
      }
      newUsers.add(new OConfigUser(user.getName(), user.getPassword(), user.getResources()));
      config.setUsers(newUsers);
    }
  }

  private OConfigSecurity migrateConfigSecurity(ODocument configDoc) {
    var newConfig = new OConfigSecurity();
    if (configDoc.hasProperty("enabled")) {
      newConfig.setEnabled(configDoc.getProperty("enabled"));
    }
    if (configDoc.hasProperty("debug")) {
      newConfig.setDebug(configDoc.getProperty("debug"));
    }
    if (configDoc.hasProperty("auditing")) {
      newConfig.setAuditing(migrateAditingConfig((ODocument) configDoc.getProperty("auditing")));
    }
    if (configDoc.hasProperty("server")) {
      var server = (ODocument) configDoc.getProperty("server");
      if (server.hasProperty("storePasswords")) {
        newConfig.setStorePasswords(server.getProperty("storePasswords"));
      }
    }
    List<OConfigUser> foundUsers = new ArrayList<>();
    if (configDoc.hasProperty("authentication")) {
      newConfig.setAuthentication(
          migrateAuthentication((ODocument) configDoc.getProperty("authentication"), foundUsers));
    }
    newConfig.setUsers(foundUsers);
    if (configDoc.hasProperty("passwordValidator")) {
      newConfig.setPasswordValidator(
          migratePasswordValidator((ODocument) configDoc.getProperty("passwordValidator")));
    }
    if (configDoc.hasProperty("ldapImporter")) {
      // TODO: migrate ldap importer config
    }

    return newConfig;
  }

  private OConfigPasswordValidator migratePasswordValidator(ODocument confDoc) {
    OConfigPasswordValidator newConfig = new OConfigPasswordValidator();
    if (confDoc.hasProperty("enabled")) {
      newConfig.setEnabled(confDoc.getProperty("enabled"));
    }

    if (confDoc.hasProperty("class")) {
      String cls = confDoc.getProperty("class");
      switch (cls) {
        case "com.orientechnologies.agent.EnterprisePasswordValidator" -> {}
        case "com.orientechnologies.security.password.ODefaultPasswordValidator" -> {
          if (confDoc.containsField("ignoreUUID")) {
            newConfig.setIgnoreUUID(confDoc.field("ignoreUUID"));
          }

          if (confDoc.containsField("minimumLength")) {
            newConfig.setMinimumLength(confDoc.field("minimumLength"));
          }

          if (confDoc.containsField("numberRegEx")) {
            newConfig.setNumberRegEx(confDoc.field("numberRegEx"));
          }

          if (confDoc.containsField("specialRegEx")) {
            newConfig.setSpecialRegEx(confDoc.field("specialRegEx"));
          }

          if (confDoc.containsField("uppercaseRegEx")) {
            newConfig.setUppercaseRegEx(confDoc.field("uppercaseRegEx"));
          }
        }
        default -> throw new UnsupportedOperationException("unsupported conversion of type:" + cls);
      }
      ;
    }

    return newConfig;
  }

  private OConfigAuthentication migrateAuthentication(
      ODocument confDoc, List<OConfigUser> foundUsers) {
    OConfigAuthentication config = new OConfigAuthentication();

    if (confDoc.hasProperty("allowDefault")) {
      config.setAllowDefault(confDoc.getProperty("allowDefault"));
    }
    if (confDoc.hasProperty("authenticators")) {
      List<ODocument> authMethodsList = confDoc.getProperty("authenticators");
      List<OConfigAuthenticator> auths = new ArrayList<>();
      for (var auth : authMethodsList) {
        auths.add(migrateAuthenticator(auth, foundUsers));
      }
      config.setAuthenticators(auths);
    }
    return config;
  }

  private OConfigAuthenticator migrateAuthenticator(ODocument auth, List<OConfigUser> foundUsers) {
    OConfigAuthenticator authenticator = new OConfigAuthenticator();
    if (auth.hasProperty("name")) {
      authenticator.setName(auth.getProperty("name"));
    }

    if (auth.hasProperty("enabled")) {
      authenticator.setEnabled(auth.getProperty("enabled"));
    }

    if (auth.hasProperty("caseSensitive")) {
      authenticator.setCaseSensitive(auth.getProperty("caseSensitive"));
    }

    if (auth.hasProperty("debug")) {
      authenticator.setDebug(auth.getProperty("debug"));
    }

    if (auth.hasProperty("class")) {
      String cls = auth.getProperty("class");
      var type =
          switch (cls) {
            case "com.orientechnologies.orient.core.security.authenticator.ODatabaseUserAuthenticator" -> {
              yield "database-users";
            }
            case "com.orientechnologies.orient.core.security.authenticator.ODefaultPasswordAuthenticator" -> {
              yield "default";
            }
            case "com.orientechnologies.orient.server.security.authenticator.ODefaultPasswordAuthenticator" -> {
              if (auth.hasProperty("users")) {
                fillUsers((List<ODocument>) auth.getProperty("users"), foundUsers);
              }
              yield "default";
            }
            case "com.orientechnologies.agent.security.authenticator.OSecuritySymmetricKeyAuth" -> {
              yield "symmetric-key";
            }
            case "com.orientechnologies.orient.core.security.authenticator.OSystemUserAuthenticator" -> {
              yield "system-users";
            }
            case "com.orientechnologies.orient.server.security.authenticator.OSystemUserAuthenticator" -> {
              yield "system-users";
            }
            case "com.orientechnologies.orient.server.security.authenticator.OServerConfigAuthenticator" -> {
              yield "server-config";
            }
            case "com.orientechnologies.orient.core.security.authenticator.OServerConfigAuthenticator" -> {
              yield "server-config";
            }
            case " com.orientechnologies.agent.security.authenticator.OSystemSymmetricKeyAuth" -> {
              yield "system-symmetric-key";
            }

            default ->
                throw new UnsupportedOperationException("unsupported conversion of type:" + cls);
          };
      authenticator.setType(type);
    }

    return authenticator;
  }

  private void fillUsers(List<ODocument> property, List<OConfigUser> foundUsers) {
    for (ODocument user : property) {
      foundUsers.add(
          new OConfigUser(
              user.getProperty("username"),
              user.getProperty("password"),
              user.getProperty("resources")));
    }
  }

  private OConfigSecurityAuditing migrateAditingConfig(ODocument configDoc) {
    OConfigSecurityAuditing auditing = new OConfigSecurityAuditing();
    if (configDoc.hasProperty("enabled")) {
      auditing.setEnabled(configDoc.getProperty("enabled"));
    }
    if (configDoc.hasProperty("class")
        && "com.orientechnologies.security.auditing.ODefaultAuditingLoader"
            .equals(configDoc.getProperty("class"))) {
      auditing.setEngine("default");

      ObjectMapper mapper = new ObjectMapper();
      var object = mapper.createObjectNode();
      if (configDoc.hasProperty("retentionDays")) {
        object.put("retentionDays", (int) configDoc.getProperty("retentionDays"));
      }
      if (configDoc.hasProperty("distributed")) {
        var distributed = mapper.createObjectNode();
        var configDocDistributed = (ODocument) configDoc.getProperty("distributed");
        if (configDocDistributed.hasProperty("onNodeJoinedEnabled")) {
          distributed.put(
              "onNodeJoinedEnabled",
              (Boolean) configDocDistributed.getProperty("onNodeJoinedEnabled"));
        }

        if (configDocDistributed.hasProperty("onNodeJoinedMessage")) {
          distributed.put(
              "onNodeJoinedMessage",
              (String) configDocDistributed.getProperty("onNodeJoinedMessage"));
        }

        if (configDocDistributed.hasProperty("onNodeLeftEnabled")) {
          distributed.put(
              "onNodeLeftEnabled", (Boolean) configDocDistributed.getProperty("onNodeLeftEnabled"));
        }

        if (configDocDistributed.hasProperty("onNodeLeftMessage")) {
          distributed.put(
              "onNodeLeftMessage", (String) configDocDistributed.getProperty("onNodeLeftMessage"));
        }
        object.set("distributed", distributed);
      }

      if (configDoc.hasProperty("systemImport")) {
        var systemImport = mapper.createObjectNode();
        var configDocSystemImport = (ODocument) configDoc.getProperty("systemImport");
        if (configDocSystemImport.hasProperty("enabled")) {
          systemImport.put("enabled", (Boolean) configDocSystemImport.getProperty("enabled"));
        }

        if (configDocSystemImport.hasProperty("databases")) {
          var values = mapper.createArrayNode();
          ((List<String>) configDocSystemImport.getProperty("databases")).forEach(values::add);
          systemImport.set("databases", values);
        }

        if (configDocSystemImport.hasProperty("limit")) {
          systemImport.put("limit", (int) configDocSystemImport.getProperty("limit"));
        }

        if (configDocSystemImport.hasProperty("sleepPeriod")) {
          systemImport.put("sleepPeriod", (int) configDocSystemImport.getProperty("sleepPeriod"));
        }
        object.set("systemImport", systemImport);
      }
    }
    return auditing;
  }

  private List<OHandlerConfig> migrateHandlers(List<OServerHandlerConfiguration> handlers) {
    List<OHandlerConfig> newHandlers = new ArrayList<>();
    for (var handler : handlers) {
      var config =
          switch (handler.getClazz()) {
            case "com.orientechnologies.orient.server.handler.OAutomaticBackup" -> {
              yield new OHandlerConfig(
                  "automaticBackup", createAtomaticBackup(handler.getParameters()));
            }
            case "com.orientechnologies.orient.server.handler.OCustomSQLFunctionPlugin" -> {
              yield new OHandlerConfig(
                  "custom-sql-functions-manager",
                  customSQLFunctionPluginConfig(handler.getParameters()));
            }
            case "com.orientechnologies.security.syslog.ODefaultSyslog" -> {
              yield new OHandlerConfig("syslog", createSyslogConfig(handler.getParameters()));
            }
            case "com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin" -> {
              yield new OHandlerConfig("cluster", createDistributedConfig(handler.getParameters()));
            }
            case "com.orientechnologies.agent.OEnterpriseAgent" -> {
              yield new OHandlerConfig("enterprise-agent", null);
            }
            case "com.orientechnologies.orient.etl.OETLPlugin" -> {
              yield new OHandlerConfig("etl", null);
            }
            case "com.orientechnologies.tinkerpop.handler.OGraphServerHandler" -> {
              yield new OHandlerConfig("graph", null);
            }
            case "com.orientechnologies.tinkerpop.server.OGremlinServerPlugin" -> {
              yield new OHandlerConfig("gremlin-server", null);
            }
            case "com.orientechnologies.orient.server.handler.OJMXPlugin" -> {
              yield new OHandlerConfig("jmx", createJmxConfig(handler.getParameters()));
            }
            case "com.orientechnologies.orient.server.distributed.impl.proxy.OProxyServer" -> {
              yield new OHandlerConfig("proxy", createProxyConfig(handler.getParameters()));
            }
            case "com.orientechnologies.security.OSecurityPlugin" -> {
              yield new OHandlerConfig("security-plugin", null);
            }

            case "com.orientechnologies.orient.server.OServerFailingOnStarupPluginStub" -> {
              yield new OHandlerConfig("failing on startup plugin", null);
            }
            case "com.orientechnologies.orient.server.handler.OServerSideScriptInterpreter" -> {
              yield new OHandlerConfig(
                  "script-interpreter", serverSideInterpreter(handler.getParameters()));
            }

            default ->
                throw new UnsupportedOperationException(
                    "unsupported conversion of type:" + handler.getClazz());
          };
      newHandlers.add(config);
    }

    return newHandlers;
  }

  private JsonNode createDistributedConfig(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }
    ObjectMapper mapper = new ObjectMapper();
    var object = mapper.createObjectNode();

    for (OServerParameterConfiguration param : parameters) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        object.put("enabled", Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("nodeName")) {
        object.put("nodeName", param.getValue());
      } else if (param.getName().startsWith("configuration.db.default")) {
        object.put("configuration.db.default", param.getValue());
      }
    }

    return object;
  }

  private JsonNode createJmxConfig(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }
    ObjectMapper mapper = new ObjectMapper();
    var object = mapper.createObjectNode();
    for (var param : parameters) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        object.put("enabled", Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("profilerManaged")) {
        object.put("profilerManaged", Boolean.parseBoolean(param.getValue()));
      }
    }
    return object;
  }

  private JsonNode createProxyConfig(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    var object = mapper.createObjectNode();
    for (OServerParameterConfiguration param : parameters) {
      if (param.getName().equalsIgnoreCase("enabled"))
        object.put("enabled", Boolean.parseBoolean(param.getValue()));
      else if (param.getName().equalsIgnoreCase("remoteHost")) {
        object.put("remoteHost", param.getValue());
      } else if (param.getName().equalsIgnoreCase("tracing")) {
        object.put("tracing", param.getValue());
      } else if (param.getName().equalsIgnoreCase("ports")) {
        String portsAsString = param.getValue();
        var array = mapper.createArrayNode();
        final String[] pairs = portsAsString.split(",");
        for (String pair : pairs) {
          array.add(pair);
        }
        object.set("ports", array);
      }
    }
    return object;
  }

  private JsonNode serverSideInterpreter(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }
    ObjectMapper mapper = new ObjectMapper();
    var object = mapper.createObjectNode();
    for (OServerParameterConfiguration param : parameters) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        object.put("allowedPackages", Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("allowedLanguages")) {
        var array = mapper.createArrayNode();
        for (var single : param.getValue().toLowerCase(Locale.ENGLISH).split(",")) {
          array.add(single);
        }
        object.set("allowedLanguages", array);
      } else if (param.getName().equalsIgnoreCase("allowedPackages")) {
        var array = mapper.createArrayNode();
        for (var single : param.getValue().split(",")) {
          array.add(single);
        }
        object.set("allowedPackages", array);
      }
    }
    return object;
  }

  private JsonNode createAtomaticBackup(OServerParameterConfiguration[] parameters) {
    for (var param : parameters) {
      if (param.getName().equalsIgnoreCase("config")) {
        ObjectMapper mapper = new ObjectMapper();
        var object = mapper.createObjectNode();
        object.put("config", param.getValue());
        return object;
      }
    }
    return null;
  }

  private JsonNode createSyslogConfig(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }
    ObjectMapper mapper = new ObjectMapper();
    var object = mapper.createObjectNode();
    for (var param : parameters) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        object.put("enabled", Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("debug")) {
        object.put("debug", Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("hostname")) {
        object.put("hostname", param.getValue());
      } else if (param.getName().equalsIgnoreCase("port")) {
        object.put("port", Integer.parseInt(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("appName")) {
        object.put("appName", param.getValue());
      }
    }
    return object;
  }

  private JsonNode customSQLFunctionPluginConfig(OServerParameterConfiguration[] parameters) {
    for (var param : parameters) {
      if (param.getName().equalsIgnoreCase("config")) {
        ObjectMapper mapper = new ObjectMapper();
        var object = mapper.createObjectNode();
        object.put("config", param.getValue());
        return object;
      }
    }
    return null;
  }

  private List<OConfigProperty> migrateProperties(OServerEntryConfiguration[] properties) {
    List<OConfigProperty> props = new ArrayList<>();
    for (var prop : properties) {
      props.add(new OConfigProperty(prop.getName(), prop.getValue()));
    }
    return props;
  }

  private List<OConfigNetwork> migrateListeners(OServerNetworkConfiguration network) {
    Map<String, OServerSocketFactoryConfiguration> sockets = new HashMap<>();
    for (var fact : network.getSockets()) {
      sockets.put(fact.getName(), fact);
    }

    List<OConfigNetwork> listeners = new ArrayList<>();
    for (var listener : network.getListeners()) {
      OConfigNetwork newListener = new OConfigNetwork();
      newListener.setPortRange(listener.getPortRange());
      newListener.setHost(listener.getIpAddress());
      newListener.setProtocol(listener.getProtocol());
      var sock = sockets.get(listener.getSocket());
      newListener.setTls(migrateTlsConfig(sock.getParameters()));
      listeners.add(newListener);
      if (listener.getProtocol().equals("http")) {
        var commands = listener.getCommands();
      }
    }
    return listeners;
  }

  private OConfigNetworkTls migrateTlsConfig(OServerParameterConfiguration[] parameters) {
    if (parameters.length == 0) {
      return null;
    }
    OConfigNetworkTls tls = new OConfigNetworkTls();

    for (OServerParameterConfiguration param : parameters) {
      if (param.getName().equalsIgnoreCase("network.ssl.clientAuth")) {
        tls.setClientAuth(Boolean.parseBoolean(param.getValue()));
      } else if (param.getName().equalsIgnoreCase("network.ssl.keyStore")) {
        tls.setKeyStorePath(param.getValue());
      } else if (param.getName().equalsIgnoreCase("network.ssl.keyStorePassword")) {
        tls.setKeyStorePassword(param.getValue());
      } else if (param.getName().equalsIgnoreCase("network.ssl.keyStoreType")) {
        tls.setKeyStoreType(param.getValue());
      } else if (param.getName().equalsIgnoreCase("network.ssl.trustStore")) {
        tls.setTrustStorePath(param.getValue());
      } else if (param.getName().equalsIgnoreCase("network.ssl.trustStorePassword")) {
        tls.setTrustStorePassword(param.getValue());
      } else if (param.getName().equalsIgnoreCase("network.ssl.trustStoreType")) {
        tls.setTrustStoreType(param.getValue());
      }
    }

    return tls;
  }

  protected OConfigPlugins migratePlugins(OServerConfiguration configuration) {
    OConfigPlugins plugins = new OConfigPlugins();
    for (OServerEntryConfiguration p : configuration.getProperties()) {
      if (p.getName().equals("plugin.hotReload")) {
        plugins.setHotReload(Boolean.parseBoolean(p.getValue()));
      } else if (p.getName().equals("plugin.dynamic")) {
        plugins.setDynamic(Boolean.parseBoolean(p.getValue()));
      } else if (p.getName().equals("plugin.directory")) plugins.setDirectory(p.getValue());
    }

    return plugins;
  }

  private void migrateDistributed(OServerConfiguration load, boolean removeOld) throws IOException {
    Iterator<OServerHandlerConfiguration> iter = load.getHandlers().iterator();
    while (iter.hasNext()) {
      OServerHandlerConfiguration handler = iter.next();
      if (handler
          .getClazz()
          .equals("com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin")) {
        if (removeOld) {
          iter.remove();
        }
        load.setDistributed(new OServerDistributedConfiguration());

        for (OServerParameterConfiguration par : handler.getParameters()) {
          if ("enabled".equalsIgnoreCase(par.getName())) {
            load.getDistributed().setEnabled(Boolean.valueOf(par.getValue()));
          }
          if ("nodeName".equalsIgnoreCase(par.getName())) {
            load.getDistributed().setNodeName(par.getValue());
          }
          if ("configuration.db.default".equalsIgnoreCase(par.getName())) {
            String config = Files.readString(Path.of(par.getValue()));
            var c = new ODocument();
            c.fromJSON(config);
            String quorumValue = c.getProperty("writeQuorum");
            if ("majority".equals(quorumValue)) {
              load.getDistributed().setQuorum(2);
            } else {
              try {
                load.getDistributed().setQuorum(Integer.valueOf(quorumValue));
              } catch (NumberFormatException e) {
              }
            }
            if (load.getDistributed().getQuorum() == 0) {
              load.getDistributed().setQuorum(2);
            }
          }
        }
      }
    }
  }
}
