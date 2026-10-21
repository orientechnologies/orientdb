package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import java.util.HashMap;
import java.util.Map;

public interface OServerCommandLoader {

  String getCommandClass();

  String getName();

  OServerCommand newInstance(OServerCommandConfiguration config);

  static Map<String, OServerCommandLoader> loaders() {
    Map<String, OServerCommandLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerCommandLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OServerCommandLoader> loadersByClassName() {
    Map<String, OServerCommandLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerCommandLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getCommandClass(), loader);
    }
    return allLoaders;
  }

  static Map<String, OServerCommandLoader> loadersByClassName(ClassLoader where) {
    Map<String, OServerCommandLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OServerCommandLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getCommandClass(), loader);
    }
    return allLoaders;
  }
}
