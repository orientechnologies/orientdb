package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OContextConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 27/06/16.
 */
public class OrientDBSettings {

  public OContextConfiguration configurations;

  public static OrientDBSettings fromMap(Map<String, Object> configurations) {
    OrientDBSettings settings = new OrientDBSettings();
    settings.configurations = new OContextConfiguration(configurations);
    return settings;
  }

  public static OrientDBSettings defaultSettings() {
    OrientDBSettings settings = new OrientDBSettings();
    settings.configurations = new OContextConfiguration();
    return settings;
  }

  public OContextConfiguration getConfigurations() {
    return configurations;
  }
}
