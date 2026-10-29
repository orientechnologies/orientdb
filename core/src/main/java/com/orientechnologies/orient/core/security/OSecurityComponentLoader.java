package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OSecurityComponentLoader {

  String getName();

  String getClassName();

  OSecurityComponent newInstance();

  static Map<String, OSecurityComponentLoader> loaders() {
    Map<String, OSecurityComponentLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OSecurityComponentLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OSecurityComponentLoader> loadersByClassName() {
    Map<String, OSecurityComponentLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OSecurityComponentLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OSecurityComponentLoader> loadersByClassName(ClassLoader where) {
    Map<String, OSecurityComponentLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(
            OSecurityComponentLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
