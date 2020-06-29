package com.orientechnologies.orient.testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConfigs {

  public interface configs {
    List<String> getServerIds();
    String getLocalConfigFile(String serverId);
    Map<String, String> getK8sConfigParams(String serverId);
  }

  public static class SimpleDServer implements configs {
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
            put(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME, "");
            put(StatefulSetTemplateKeys.ORIENTDB_LABEL, "orientdb");
            put(StatefulSetTemplateKeys.ORIENTDB_HTTP_PORT, "2480");
            put(StatefulSetTemplateKeys.ORIENTDB_BINARY_PORT, "2424");
            put(StatefulSetTemplateKeys.ORIENTDB_HAZELCAST_PORT, "2434");
            put(
                StatefulSetTemplateKeys.ORIENTDB_DOCKER_IMAGE,
                "192.168.2.119:5000/pxsalehi/orientdb:3.1.1");
            put(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM, "");
            put(StatefulSetTemplateKeys.ORIENTDB_DB_VOL_SIZE, "2");
            put(StatefulSetTemplateKeys.ORIENTDB_HAZELCAST_CONFIG, "/kubernetes/hazelcast-0.xml");
            put(
                StatefulSetTemplateKeys.ORIENTDB_DISTRIBUTED_DB_CONFIG,
                "/kubernetes/default-distributed-db-config.json");
            put(
                StatefulSetTemplateKeys.ORIENTDB_SERVER_CONFIG,
                "/kubernetes/orientdb-simple-dserver-config-0.xml");
          }
        };
    private Map<String, Map<String, String>> nodeParams = new HashMap<>();

    @Override public List<String> getServerIds() {
      return new ArrayList<>(serverConfigFiles.keySet());
    }

    @Override public String getLocalConfigFile(String serverId) {
      return serverConfigFiles.get(serverId);
    }

    @Override public Map<String, String> getK8sConfigParams(String serverId) {
      Map<String, String> params = nodeParams.get(serverId);
      if (params == null) {
        params = new HashMap<>(serverK8sParams);
        params.put(StatefulSetTemplateKeys.ORIENTDB_NODE_NAME, serverId);
        params.put(StatefulSetTemplateKeys.ORIENTDB_CONFIG_CM, serverId + "-cm");
        nodeParams.put(serverId, params);
      }
      return params;
    }
  }
}
