package com.orientechnologies.orient.test.configs;

import com.orientechnologies.orient.test.util.TestConfig;
import com.orientechnologies.orient.test.util.TestSetupUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDServerConfig implements TestConfig {
  public static final String SERVER0 = "server0";
  public static final String SERVER1 = "server1";
  public static final String SERVER2 = "server2";

  // Server config files used for each instance when using a local setup.
  private static Map<String, String> localServerConfigFiles =
      new HashMap<String, String>() {
        {
          put(SERVER0, "orientdb-simple-dserver-config-0.xml");
          put(SERVER1, "orientdb-simple-dserver-config-1.xml");
          put(SERVER2, "orientdb-simple-dserver-config-2.xml");
        }
      };
  // Configurations used for the deployment of each instance on Kubernetes
  private Map<String, K8sConfigs>    serverK8sConfigs       = new HashMap<>();

  @Override
  public List<String> getServerIds() {
    return Arrays.asList(SERVER0, SERVER1, SERVER2);
  }

  @Override
  public String getLocalConfigFile(String serverId) {
    return localServerConfigFiles.get(serverId);
  }

  @Override
  public K8sConfigs getK8sConfigs(String serverId) {
    K8sConfigs config = serverK8sConfigs.get(serverId);
    if (config == null) {
      config = TestSetupUtil.newK8sConfigs();
      config.setNodeName(serverId);
      config.setHazelcastConfig("/kubernetes/hazelcast.xml");
      config.setServerConfig("/kubernetes/orientdb-simple-dserver-config.xml");
      config.setDistributedDBConfig("/kubernetes/default-distributed-db-config.json");
      serverK8sConfigs.put(serverId, config);
    }
    return config;
  }
}
