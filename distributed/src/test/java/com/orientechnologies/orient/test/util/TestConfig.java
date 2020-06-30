package com.orientechnologies.orient.test.util;

import java.util.List;
import java.util.Map;

public interface TestConfig {
  List<String> getServerIds();
  String getLocalConfigFile(String serverId);
  Map<String, String> getK8sConfigParams(String serverId);
}
