/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.plugin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OService;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent.OStaticContent;

/**
 * Manages Server Extensions
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerPluginManager implements OService {
  private static final int                             CHECK_DELAY   = 5000;
  private OServer                                      server;
  private ConcurrentHashMap<String, OServerPluginInfo> activePlugins = new ConcurrentHashMap<String, OServerPluginInfo>();
  private ConcurrentHashMap<String, String>            loadedPlugins = new ConcurrentHashMap<String, String>();

  public void config(OServer iServer) {
    server = iServer;
  }

  public void startup() {
    boolean hotReload = true;
    boolean dynamic = true;

    for (OServerEntryConfiguration p : server.getConfiguration().properties) {
      if (p.name.equals("plugin.hotReload"))
        hotReload = Boolean.parseBoolean(p.value);
      else if (p.name.equals("plugin.dynamic"))
        dynamic = Boolean.parseBoolean(p.value);
    }

    if (!dynamic)
      return;

    updatePlugins();

    if (hotReload)
      // SCHEDULE A TIMER TASK FOR AUTO-RELOAD
      Orient.instance().getTimer().schedule(new TimerTask() {
        @Override
        public void run() {
          updatePlugins();
        }
      }, CHECK_DELAY, CHECK_DELAY);
  }

  public OServerPluginInfo getPluginByName(final String iName) {
    if (iName == null)
      return null;
    return activePlugins.get(iName);
  }

  public String getPluginNameByFile(final String iFileName) {
    return loadedPlugins.get(iFileName);
  }

  public OServerPluginInfo getPluginByFile(final String iFileName) {
    return getPluginByName(getPluginNameByFile(iFileName));
  }

  public String[] getPluginNames() {
    return activePlugins.keySet().toArray(new String[activePlugins.size()]);
  }

  public void registerPlugin(final OServerPluginInfo iPlugin) {
    final String pluginName = iPlugin.getName();

    if (activePlugins.containsKey(pluginName))
      throw new IllegalStateException("Plugin '" + pluginName + "' already registered");
    activePlugins.putIfAbsent(pluginName, iPlugin);
  }

  public Collection<OServerPluginInfo> getPlugins() {
    return activePlugins.values();
  }

  private void updatePlugins() {
    final File pluginsDirectory = new File(OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/plugins/"));
    if (!pluginsDirectory.exists())
      pluginsDirectory.mkdirs();

    final File[] plugins = pluginsDirectory.listFiles();

    final Set<String> currentDynamicPlugins = new HashSet<String>();
    for (Entry<String, String> entry : loadedPlugins.entrySet()) {
      currentDynamicPlugins.add(entry.getKey());
    }

    if (plugins != null)
      for (File plugin : plugins) {
        final String pluginName = updatePlugin(plugin);
        if (pluginName != null)
          currentDynamicPlugins.remove(pluginName);
      }

    // REMOVE MISSING PLUGIN
    for (String pluginName : currentDynamicPlugins)
      uninstallPluginByFile(pluginName);
  }

  public void uninstallPluginByFile(final String iFileName) {
    final String pluginName = loadedPlugins.remove(iFileName);
    if (pluginName != null) {
      OLogManager.instance().info(this, "Uninstalling dynamic plugin '%s'...", iFileName);

      final OServerPluginInfo removedPlugin = activePlugins.remove(pluginName);
      if (removedPlugin != null)
        removedPlugin.shutdown();
    }
  }

  protected String updatePlugin(final File pluginFile) {
    final String pluginFileName = pluginFile.getName();

    if (!pluginFile.isDirectory() && !pluginFileName.endsWith(".jar") && !pluginFileName.endsWith(".zip"))
      // SKIP IT
      return null;

    OServerPluginInfo currentPluginData = getPluginByFile(pluginFileName);

    final long fileLastModified = pluginFile.lastModified();
    if (currentPluginData != null) {
      if (fileLastModified <= currentPluginData.getLoadedOn())
        // ALREADY LOADED, SKIPT IT
        return pluginFileName;

      // SHUTDOWN PREVIOUS INSTANCE
      try {
        currentPluginData.shutdown();
        activePlugins.remove(loadedPlugins.remove(pluginFileName));

      } catch (Exception e) {
        // IGNORE EXCEPTIONS
        OLogManager.instance().debug(this, "Error on shutdowning plugin '%s'...", e, pluginFileName);
      }
    }

    installDynamicPlugin(pluginFile);

    return pluginFileName;
  }

  private void installDynamicPlugin(final File pluginFile) {
    String pluginName = pluginFile.getName();

    final OServerPluginInfo currentPluginData;
    OLogManager.instance().info(this, "Installing dynamic plugin '%s'...", pluginName);

    URLClassLoader pluginClassLoader = null;
    try {
      final URL url = pluginFile.toURI().toURL();

      pluginClassLoader = new URLClassLoader(new URL[] { url });

      // LOAD PLUGIN.JSON FILE
      final URL r = pluginClassLoader.getResource("plugin.json");
      final InputStream pluginConfigFile = r.openStream();

      try {
        if (pluginConfigFile == null || pluginConfigFile.available() == 0) {
          OLogManager.instance().error(this, "Error on loading 'plugin.json' file for dynamic plugin '%s'", pluginName);
          throw new IllegalArgumentException(String.format("Error on loading 'plugin.json' file for dynamic plugin '%s'",
              pluginName));
        }

        final ODocument properties = new ODocument().fromJSON(pluginConfigFile);

        if (properties.containsField("name"))
          // OVERWRITE PLUGIN NAME
          pluginName = properties.field("name");

        final String pluginClass = properties.field("javaClass");

        final OServerPlugin pluginInstance;
        final Map<String, Object> parameters;

        if (pluginClass != null) {
          // CREATE PARAMETERS
          parameters = properties.field("parameters");
          final List<OServerParameterConfiguration> params = new ArrayList<OServerParameterConfiguration>();
          for (String paramName : parameters.keySet()) {
            params.add(new OServerParameterConfiguration(paramName, (String) parameters.get(paramName)));
          }
          final OServerParameterConfiguration[] pluginParams = params.toArray(new OServerParameterConfiguration[params.size()]);

          pluginInstance = startPluginClass(pluginClassLoader, pluginClass, pluginParams);
        } else {
          pluginInstance = null;
          parameters = null;
        }

        // REGISTER THE PLUGIN
        currentPluginData = new OServerPluginInfo(pluginName, (String) properties.field("version"),
            (String) properties.field("description"), (String) properties.field("web"), pluginInstance, parameters,
            pluginFile.lastModified(), pluginClassLoader);

      } finally {
        pluginConfigFile.close();
      }

      registerPlugin(currentPluginData);
      loadedPlugins.put(pluginFile.getName(), pluginName);

      registerStaticDirectory(currentPluginData);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on installing dynamic plugin '%s'", e, pluginName);
    }
  }

  protected void registerStaticDirectory(final OServerPluginInfo iPluginData) {
    Object pluginWWW = iPluginData.getParameter("www");
    if (pluginWWW == null)
      pluginWWW = iPluginData.getName();

    final OServerNetworkListener httpListener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    final OServerCommandGetStaticContent command = (OServerCommandGetStaticContent) httpListener
        .getCommand(OServerCommandGetStaticContent.class);

    if (command != null) {
      final URL wwwURL = iPluginData.getClassLoader().findResource("www/");

      final OCallable<Object, String> callback;
      if (wwwURL != null && iPluginData.getInstance() == null)
        callback = createStaticLinkCallback(iPluginData, wwwURL);
      else
        // LET TO THE COMMAND TO CONTROL IT
        callback = new OCallable<Object, String>() {
          @Override
          public Object call(final String iArgument) {
            return iPluginData.getInstance().getContent(iArgument);
          }
        };

      command.registerVirtualFolder(pluginWWW.toString(), callback);
    }
  }

  protected OCallable<Object, String> createStaticLinkCallback(final OServerPluginInfo iPluginData, final URL wwwURL) {
    return new OCallable<Object, String>() {
      @Override
      public Object call(final String iArgument) {
        String fileName = "www/" + iArgument;
        final URL url = iPluginData.getClassLoader().findResource(fileName);

        if (url != null) {
          final OServerCommandGetStaticContent.OStaticContent content = new OStaticContent();
          content.is = new BufferedInputStream(iPluginData.getClassLoader().getResourceAsStream(fileName));
          content.contentSize = -1;
          content.type = OServerCommandGetStaticContent.getContentType(url.getFile());
          return content;
        }
        return null;
      }
    };
  }

  @SuppressWarnings("unchecked")
  protected OServerPlugin startPluginClass(final URLClassLoader pluginClassLoader, final String iClassName,
      final OServerParameterConfiguration[] params) throws Exception {

    final Class<? extends OServerPlugin> classToLoad = (Class<? extends OServerPlugin>) Class.forName(iClassName, true,
        pluginClassLoader);
    final OServerPlugin instance = classToLoad.newInstance();

    // CONFIG()
    final Method configMethod = classToLoad.getDeclaredMethod("config", OServer.class, OServerParameterConfiguration[].class);
    configMethod.invoke(instance, server, params);

    // STARTUP()
    final Method startupMethod = classToLoad.getDeclaredMethod("startup");
    startupMethod.invoke(instance);

    return instance;
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String getName() {
    return "plugin-manager";
  }
}
