package com.orientechnologies.orient.server.network.protocol;

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.server.OServer;
import java.util.HashMap;
import java.util.Map;

public interface ONetworkProtocolLoader {

  String getName();

  String getClassName();

  int getVersion();

  ONetworkProtocol newInstance(OServer server);

  static Map<String, ONetworkProtocolLoader> loaders() {
    Map<String, ONetworkProtocolLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(ONetworkProtocolLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, ONetworkProtocolLoader> loadersByClassName() {
    Map<String, ONetworkProtocolLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(ONetworkProtocolLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, ONetworkProtocolLoader> loadersByClassName(ClassLoader where) {
    Map<String, ONetworkProtocolLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(ONetworkProtocolLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
