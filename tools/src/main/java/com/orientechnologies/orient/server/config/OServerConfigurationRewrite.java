package com.orientechnologies.orient.server.config;

import com.orientechnologies.orient.core.config.OConfig;
import com.orientechnologies.orient.core.config.OConfigNetwork;
import com.orientechnologies.orient.core.config.OConfigPlugins;
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
    config.setListeners(migrateListeners(configuration.getNetwork()));

    return config;
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
      // TODO: copy SSL settings ecc.
      listeners.add(newListener);
    }
    return listeners;
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

    var handlers = configuration.getHandlers();
    for (var handler : handlers) {
      String cl = handler.getClazz();
      //       final Class<? extends OServerPlugin> classToLoad =
      //           (Class<? extends OServerPlugin>) pluginClassLoader.loadClass(cl);
      //       final OServerPlugin instance = classToLoad.newInstance();
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
