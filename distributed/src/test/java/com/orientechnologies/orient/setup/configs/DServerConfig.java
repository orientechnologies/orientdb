package com.orientechnologies.orient.setup.configs;

import com.orientechnologies.orient.setup.K8sServerConfig;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetupUtil;

import java.util.*;

public class DServerConfig implements SetupConfig {
  public static final String SERVER0 = "0";
  public static final String SERVER1 = "1";
  public static final String SERVER2 = "2";

  // Server config files used for each instance when using a local setup.
  private static Map<String, String> localServerConfigFiles =
      new HashMap<String, String>() {
        {
          put(SERVER0, "orientdb-dserver-config-0.xml");
          put(SERVER1, "orientdb-dserver-config-1.xml");
          put(SERVER2, "orientdb-dserver-config-2.xml");
        }
      };
  // Configurations used for the deployment of each instance on Kubernetes
  private Map<String, K8sServerConfig> serverK8sConfigs = new HashMap<>();

  @Override
  public List<String> getServerIds() {
    return Arrays.asList(SERVER0, SERVER1, SERVER2);
  }

  @Override
  public String getLocalConfigFile(String serverId) {
    return localServerConfigFiles.get(serverId);
  }

  @Override
  public K8sServerConfig getK8sConfigs(String serverId) {
    K8sServerConfig config = serverK8sConfigs.get(serverId);
    if (config == null) {
      config = TestSetupUtil.newK8sConfigs();
      config.setNodeName(serverId);
      config.setHazelcastConfig("/kubernetes/hazelcast.xml");
      config.setServerConfig("/kubernetes/orientdb-simple-dserver-config.xml");
      config.setDistributedDBConfig("/kubernetes/default-distributed-db-config.json");
      config.setServerLogConfig("/kubernetes/orientdb-server-log.properties");
      config.setClientLogConfig("/kubernetes/orientdb-client-log.properties");
      serverK8sConfigs.put(serverId, config);
    }
    return config;
  }
}
