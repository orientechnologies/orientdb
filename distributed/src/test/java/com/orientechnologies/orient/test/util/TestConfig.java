package com.orientechnologies.orient.test.util;

import com.orientechnologies.orient.test.configs.K8sConfigs;

import java.util.List;

public interface TestConfig {
  List<String> getServerIds();

  String getLocalConfigFile(String serverId);

  K8sConfigs getK8sConfigs(String serverId);
}
