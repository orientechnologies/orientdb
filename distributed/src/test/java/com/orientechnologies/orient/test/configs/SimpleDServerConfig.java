package com.orientechnologies.orient.test.configs;

import com.orientechnologies.orient.test.util.TemplateKeys;
import com.orientechnologies.orient.test.util.TestConfig;

import java.util.*;

public class SimpleDServerConfig implements TestConfig {
  private static List<String> serverIds = Arrays.asList("server0", "server1", "server2");
  private static Map<String, String> serverConfigFiles =
      new HashMap<String, String>() {
        {
          put("server0", "orientdb-simple-dserver-config-0.xml");
          put("server1", "orientdb-simple-dserver-config-1.xml");
          put("server2", "orientdb-simple-dserver-config-2.xml");
        }
      };

  private static Map<String, String> serverK8sParams =
      new HashMap<String, String>() {
        {
          put(TemplateKeys.ORIENTDB_NODE_NAME, "");
          put(TemplateKeys.ORIENTDB_LABEL, "orientdb");
          put(TemplateKeys.ORIENTDB_HTTP_PORT, "2480");
          put(TemplateKeys.ORIENTDB_BINARY_PORT, "2424");
          put(TemplateKeys.ORIENTDB_HAZELCAST_PORT, "2434");
          put(
              TemplateKeys.ORIENTDB_DOCKER_IMAGE,
              "192.168.2.119:5000/pxsalehi/orientdb:3.1.1");
          put(TemplateKeys.ORIENTDB_CONFIG_CM, "");
          put(TemplateKeys.ORIENTDB_DB_VOL_SIZE, "2");
          put(TemplateKeys.ORIENTDB_HAZELCAST_CONFIG, "/kubernetes/hazelcast-0.xml");
          put(
              TemplateKeys.ORIENTDB_DISTRIBUTED_DB_CONFIG,
              "/kubernetes/default-distributed-db-config.json");
          put(
              TemplateKeys.ORIENTDB_SERVER_CONFIG,
              "/kubernetes/orientdb-simple-dserver-config-0.xml");
        }
      };
  private Map<String, Map<String, String>> nodeParams = new HashMap<>();

  @Override
  public List<String> getServerIds() {
    return serverIds;
  }

  @Override
  public String getLocalConfigFile(String serverId) {
    return serverConfigFiles.get(serverId);
  }

  @Override
  public Map<String, String> getK8sConfigParams(String serverId) {
    Map<String, String> params = nodeParams.get(serverId);
    if (params == null) {
      params = new HashMap<>(serverK8sParams);
      params.put(TemplateKeys.ORIENTDB_NODE_NAME, serverId);
      params.put(TemplateKeys.ORIENTDB_CONFIG_CM, serverId + "-cm");
      nodeParams.put(serverId, params);
    }
    return params;
  }
}
