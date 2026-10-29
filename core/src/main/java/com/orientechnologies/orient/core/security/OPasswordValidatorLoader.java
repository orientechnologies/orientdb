package com.orientechnologies.orient.core.security;

import com.orientechnologies.common.util.OClassLoaderHelper;
import java.util.HashMap;
import java.util.Map;

public interface OPasswordValidatorLoader {

  String getName();

  String getClassName();

  OPasswordValidator newInstance();

  static Map<String, OPasswordValidatorLoader> loaders() {
    Map<String, OPasswordValidatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OPasswordValidatorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OPasswordValidatorLoader> loadersByClassName() {
    Map<String, OPasswordValidatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OPasswordValidatorLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OPasswordValidatorLoader> loadersByClassName(ClassLoader where) {
    Map<String, OPasswordValidatorLoader> allLoaders = new HashMap<>();
    var loaders =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(
            OPasswordValidatorLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
