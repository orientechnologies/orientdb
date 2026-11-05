package com.orientechnologies.orient.core.hook;

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import java.util.HashMap;
import java.util.Map;

public interface OHookLoader {

  String getName();

  String getClassName();

  ORecordHook newLegacyHook(ODatabaseSession session);

  static Map<String, OHookLoader> loaders() {
    Map<String, OHookLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OHookLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OHookLoader> loadersByClassName() {
    Map<String, OHookLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OHookLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }

  static Map<String, OHookLoader> loadersByClassName(ClassLoader where) {
    Map<String, OHookLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OHookLoader.class, where);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getClassName(), loader);
    }
    return allLoaders;
  }
}
