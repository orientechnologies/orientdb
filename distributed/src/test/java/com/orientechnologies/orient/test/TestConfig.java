package com.orientechnologies.orient.test;

import java.util.List;

public interface TestConfig {
  List<String> getServerIds();

  String getLocalConfigFile(String serverId);

  K8sServerConfig getK8sConfigs(String serverId);
}
