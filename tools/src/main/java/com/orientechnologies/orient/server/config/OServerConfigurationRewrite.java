package com.orientechnologies.orient.server.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.config.OConfig;
import com.orientechnologies.orient.core.config.OConfigNetwork;
import com.orientechnologies.orient.core.config.OConfigNetworkTls;
import com.orientechnologies.orient.core.config.OConfigPlugins;
import com.orientechnologies.orient.core.config.OConfigProperty;
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

  public OConfig convertToOConfig(OServerConfiguration configuration) {
    OConfig config = new OConfig();
    config.setDatabases(configuration.getLocation());
    config.setPlugins(migratePlugins(configuration));
    config.setHandlers(migrateHandlers(configuration.getHandlers()));
    config.setListeners(migrateListeners(configuration.getNetwork()));
    config.setProperties(migrateProperties(configuration.getProperties()));
    return config;
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
              yield new OHandlerConfig("cluster", null);
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

            default -> throw new UnsupportedOperationException("shit");
          };
      newHandlers.add(config);
    }
    //       final Class<? extends OServerPlugin> classToLoad =
    //           (Class<? extends OServerPlugin>) pluginClassLoader.loadClass(cl);
    //       final OServerPlugin instance = classToLoad.newInstance();

    return newHandlers;
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
    return null;
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
