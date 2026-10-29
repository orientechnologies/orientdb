package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OCredentialInterceptorLoader {

  String getName();

  String getClassName();

  OCredentialInterceptor newInstance();

  static Map<String, OCredentialInterceptorLoader> loaders() {
    Map<String, OCredentialInterceptorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OCredentialInterceptorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OCredentialInterceptorLoader> loadersByClassName() {
    Map<String, OCredentialInterceptorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OCredentialInterceptorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OCredentialInterceptorLoader> loadersByClassName(ClassLoader where) {
    Map<String, OCredentialInterceptorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(
            OCredentialInterceptorLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
