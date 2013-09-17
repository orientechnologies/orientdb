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

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OService;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerPlugin;

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

  public void config(OServer iServer) {
    server = iServer;
  }

  public void startup() {
    updatePlugins();

    // SCHEDULE A TIMER TASK FOR AUTO-RELOAD
    Orient.instance().getTimer().schedule(new TimerTask() {
      @Override
      public void run() {
        updatePlugins();
      }
    }, CHECK_DELAY, CHECK_DELAY);
  }

  public OServerPluginInfo getPlugin(final String iName) {
    return activePlugins.get(iName);
  }

  public String[] getPluginNames() {
    return activePlugins.keySet().toArray(new String[activePlugins.size()]);
  }

  public void registerPlugin(final OServerPluginInfo iPlugin) {
    final String pluginName = iPlugin.getName();

    if (activePlugins.contains(pluginName))
      throw new IllegalStateException("Plugin '" + pluginName + "' already registered");
    activePlugins.putIfAbsent(pluginName, iPlugin);
  }

  public Collection<OServerPluginInfo> getPlugins() {
    return activePlugins.values();
  }

  private void updatePlugins() {
    final File pluginsDirectory = new File("plugins/");
    if (!pluginsDirectory.exists())
      pluginsDirectory.mkdirs();

    final File[] plugins = pluginsDirectory.listFiles();

    final Map<String, OServerPluginInfo> currentDynamicPlugins = new HashMap<String, OServerPluginInfo>();
    for (Entry<String, OServerPluginInfo> entry : activePlugins.entrySet()) {
      if (entry.getValue().isDynamic())
        currentDynamicPlugins.put(entry.getKey(), entry.getValue());
    }

    for (File extension : plugins) {
      final String pluginName = updatePlugin(extension);
      if (pluginName != null)
        currentDynamicPlugins.remove(pluginName);
    }

    // REMOVE MISSING PLUGIN
    for (Entry<String, OServerPluginInfo> entry : currentDynamicPlugins.entrySet())
      uninstallPlugin(entry.getKey());
  }

  public void uninstallPlugin(final String iName) {
    OLogManager.instance().info(this, "Uninstalling dynamic plugin '%s'...", iName);

    final OServerPluginInfo removePlugin = activePlugins.remove(iName);
    if (removePlugin != null)
      removePlugin.getInstance().shutdown();
  }

  protected String updatePlugin(final File pluginFile) {
    final String pluginName = pluginFile.getName();

    if (!pluginFile.isDirectory() && !pluginName.endsWith(".jar"))
      // SKIP IT
      return null;

    OServerPluginInfo currentPluginData = activePlugins.get(pluginName);

    final long fileLastModified = pluginFile.lastModified();
    if (currentPluginData != null) {
      if (fileLastModified <= currentPluginData.getLoadedOn())
        // ALREADY LOADED, SKIPT IT
        return pluginName;

      // SHUTDOWN PREVIOUS INSTANCE
      try {
        currentPluginData.getInstance().sendShutdown();
      } catch (Exception e) {
        // IGNORE EXCEPTIONS
        OLogManager.instance().debug(this, "Error on shutdowning plugin '%s'...", e, pluginName);
      }
    }

    installDynamicPlugin(pluginFile);

    return pluginName;
  }

  private void installDynamicPlugin(final File pluginFile) {
    final String pluginName = pluginFile.getName();

    OServerPluginInfo currentPluginData;
    OLogManager.instance().info(this, "Installing dynamic plugin '%s'...", pluginName);

    try {
      final URLClassLoader pluginClassLoader = new URLClassLoader(new URL[] { pluginFile.toURI().toURL() }, this.getClass()
          .getClassLoader());

      // LOAD PLUGIN.PROPERTIES FILE
      final InputStream pluginPropertiesFile = pluginClassLoader.getResourceAsStream("plugin.properties");
      if (pluginPropertiesFile == null || pluginPropertiesFile.available() == 0)
        OLogManager.instance().error(this, "Error on loading plugin.properties for dynamic plugin '%s'",
            IllegalArgumentException.class, pluginName);

      final Properties properties = new Properties();
      properties.load(pluginPropertiesFile);

      final String pluginClass = properties.getProperty("class");

      List<OServerParameterConfiguration> params = new ArrayList<OServerParameterConfiguration>();

      for (Object prop : properties.keySet()) {
        final String propName = prop.toString();
        if (propName.startsWith("param."))
          params.add(new OServerParameterConfiguration(propName.substring("param.".length()), properties.getProperty(propName)));
      }

      final OServerParameterConfiguration[] pluginParams = params.toArray(new OServerParameterConfiguration[params.size()]);

      // REGISTER THE PLUGIN
      currentPluginData = new OServerPluginInfo(pluginName, startPluginClass(pluginClassLoader, pluginClass, pluginParams),
          pluginFile.lastModified());

      registerPlugin(currentPluginData);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on installing dynamic plugin '%s'", e, pluginName);
    }
  }

  @SuppressWarnings("unchecked")
  private OServerPlugin startPluginClass(final URLClassLoader pluginClassLoader, final String iClassName,
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
