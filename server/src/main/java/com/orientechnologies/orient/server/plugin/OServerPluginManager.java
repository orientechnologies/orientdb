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
import java.io.FilenameFilter;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OService;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

/**
 * Manages Server Extensions
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerPluginManager implements OService {
  private OServer server;

  public void config(OServer iServer) {
    server = iServer;
  }

  public void startup() {
    final File pluginsDirectory = new File("plugins/");
    if (!pluginsDirectory.exists())
      pluginsDirectory.mkdirs();

    final File[] plugins = pluginsDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".plugin");
      }
    });

    for (File extension : plugins) {
      loadPlugin(extension);
    }
  }

  protected void loadPlugin(final File plugin) {
    OLogManager.instance().config(this, "Loading plugin '%s'...", plugin.getName());

    try {
      final URLClassLoader pluginClassLoader = new URLClassLoader(new URL[] { plugin.toURI().toURL() }, this.getClass()
          .getClassLoader());

      // LOAD PLUGIN.PROPERTIES FILE
      final InputStream pluginPropertiesFile = pluginClassLoader.getResourceAsStream("plugin.properties");
      if (pluginPropertiesFile == null || pluginPropertiesFile.available() == 0)
        OLogManager.instance().error(this, "Error on loading plugin.properties", IllegalArgumentException.class);

      final Properties properties = new Properties();
      properties.load(pluginPropertiesFile);

      final String pluginClass = properties.getProperty("class");

      List<OServerParameterConfiguration> params = new ArrayList<OServerParameterConfiguration>();

      for (Object prop : properties.keySet()) {
        final String propName = prop.toString();
        if (propName.startsWith("prop."))
          params.add(new OServerParameterConfiguration(propName.substring("prop.".length()), properties.getProperty(propName)));
      }

      startPluginClass(pluginClassLoader, pluginClass, params);

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on loading plugin '%s'", e, plugin.getName());
    }
  }

  private Object startPluginClass(final URLClassLoader pluginClassLoader, final String iClassName,
      final List<OServerParameterConfiguration> params) throws Exception {

    final Class<?> classToLoad = Class.forName(iClassName, true, pluginClassLoader);
    final Method method = classToLoad.getDeclaredMethod("config");
    final Object instance = classToLoad.newInstance();
    return method.invoke(instance);
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String getName() {
    return "plugin-manager";
  }
}
