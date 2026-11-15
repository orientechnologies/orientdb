package com.orientechnologies.orient.server.plugin;

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.server.OServer;
import java.util.HashMap;
import java.util.Map;

public interface OServerPluginLoader<C> {

  String getName();

  String getPluginClassName();

  Class<C> getConfigClass();

  OServerPlugin newInstance();

  OServerPlugin newInstance(OServer context, C config);

  static Map<String, OServerPluginLoader> loaders() {
    Map<String, OServerPluginLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerPluginLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OServerPluginLoader> loadersByClassName() {
    Map<String, OServerPluginLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerPluginLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getPluginClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OServerPluginLoader> loadersByClassName(ClassLoader where) {
    Map<String, OServerPluginLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerPluginLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getPluginClassName(), loader);
    }
    return allLoaders;
  }
}
