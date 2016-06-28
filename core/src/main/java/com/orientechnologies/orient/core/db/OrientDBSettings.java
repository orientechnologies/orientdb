package com.orientechnologies.orient.core.db;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 27/06/16.
 */
public class OrientDBSettings {

  public Map<String, Object> configurations;

  public static OrientDBSettings fromMap(Map<String, Object> configurations) {
    OrientDBSettings settings = new OrientDBSettings();
    settings.configurations = configurations;
    return settings;
  }

  public static OrientDBSettings defaultSettings() {
    OrientDBSettings settings = new OrientDBSettings();
    settings.configurations = new HashMap<>();
    return settings;
  }

  public String getStorageMode() {
    String config = (String) configurations.get("mode");
    if (config == null)
      config = "rw";
    return config;
  }

}
