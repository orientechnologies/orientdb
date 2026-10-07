package com.orientechnologies.orient.server.plugin;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OServerPluginLoader {

  String getName();

  OServerPlugin newInstance();

  static Map<String, OServerPluginLoader> loaders() {
    Map<String, OServerPluginLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerPluginLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }
}
