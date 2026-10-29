package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OSecurityAuthenticatorLoader {

  String getName();

  String getClassName();

  OSecurityAuthenticator newInstance();

  static Map<String, OSecurityAuthenticatorLoader> loaders() {
    Map<String, OSecurityAuthenticatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OSecurityAuthenticatorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OSecurityAuthenticatorLoader> loadersByClassName() {
    Map<String, OSecurityAuthenticatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OSecurityAuthenticatorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OSecurityAuthenticatorLoader> loadersByClassName(ClassLoader where) {
    Map<String, OSecurityAuthenticatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(
            OSecurityAuthenticatorLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
