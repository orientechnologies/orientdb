package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OAuditingServiceLoader {

  String getName();

  String getClassName();

  OAuditingService newInstance();

  static Map<String, OAuditingServiceLoader> loaders() {
    Map<String, OAuditingServiceLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OAuditingServiceLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OAuditingServiceLoader> loadersByClassName() {
    Map<String, OAuditingServiceLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OAuditingServiceLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OAuditingServiceLoader> loadersByClassName(ClassLoader where) {
    Map<String, OAuditingServiceLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OAuditingServiceLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
