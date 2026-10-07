package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.Orient;
import java.util.HashMap;
import java.util.Map;

public interface OrientDBLoader {

  String getScheme();

  OrientDBInternal load(String url, OrientDBConfig config, Orient orient);

  static Map<String, OrientDBLoader> loaders() {
    Map<String, OrientDBLoader> allLoaders = new HashMap<>();
    var loaders = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OrientDBLoader.class);
    while (loaders.hasNext()) {
      var loader = loaders.next();
      allLoaders.put(loader.getScheme(), loader);
    }
    return allLoaders;
  }
}
