package com.orientechnologies.orient.server.distributed.config;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.config.OMulticastConfguration;
import com.orientechnologies.orient.core.db.config.OMulticastConfigurationBuilder;
import com.orientechnologies.orient.core.db.config.ONodeConfigurationBuilder;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedNetworkMulticastConfiguration;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ODistributedConfig {

  static Map<String, BiConsumer<String, OServerDistributedConfiguration>> configurators = new HashMap<>();

  static {

    configurators.put("ORIENTDB_DISTRIBUTED_ENABLED", (value, config) -> config.enabled = Boolean.parseBoolean(value));
    configurators.put("ORIENTDB_DISTRIBUTED_NODE_NAME", (value, config) -> config.nodeName = value);
    configurators.put("ORIENTDB_DISTRIBUTED_QUORUM", (value, config) -> config.quorum = Integer.parseInt(value));
    configurators.put("ORIENTDB_DISTRIBUTED_GROUP_NAME", (value, config) -> config.group.name = value);
    configurators.put("ORIENTDB_DISTRIBUTED_GROUP_PASSWORD", (value, config) -> config.group.password = value);
    configurators.put("ORIENTDB_DISTRIBUTED_NETWORK_MULTICAST_ENABLED",
        (value, config) -> config.network.multicast.enabled = Boolean.parseBoolean(value));
    configurators.put("ORIENTDB_DISTRIBUTED_NETWORK_MULTICAST_IP", (value, config) -> config.network.multicast.ip = value);
    configurators.put("ORIENTDB_DISTRIBUTED_NETWORK_MULTICAST_PORT",
        (value, config) -> config.network.multicast.port = Integer.parseInt(value));
    configurators.put("ORIENTDB_DISTRIBUTED_NETWORK_MULTICAST_DISCOVERY_PORTS",
        (value, config) -> config.network.multicast.discoveryPorts = Arrays.stream(value.split(",")).map(Integer::parseInt)
            .mapToInt(i -> i).toArray());

  }

  public static OServerDistributedConfiguration fromEnv(OServerDistributedConfiguration distributed)
      throws OConfigurationException {
    final OServerDistributedConfiguration config;
    if (distributed == null) {
      config = new OServerDistributedConfiguration();
      config.enabled = false;
    } else {
      config = distributed;
    }

    configurators.entrySet().stream().forEach((entry) -> {

      String env = System.getenv(entry.getKey());

      if (env != null) {
        entry.getValue().accept(env, config);
      }

    });

    validateConfiguration(config);

    return config;
  }

  public static void validateConfiguration(OServerDistributedConfiguration configuration) throws OConfigurationException {

    if (configuration.enabled) {

      if (configuration.nodeName == null) {
        throw new OConfigurationException("Node name not specified in the configuration");
      }

      if (configuration.group.name == null) {
        throw new OConfigurationException("Group name not specified in the configuration");
      }
      if (configuration.group.password == null) {
        throw new OConfigurationException("Group password not specified in the configuration");
      }
      if (configuration.quorum == null) {
        throw new OConfigurationException("Quorum not specified in the configuration");
      }

      if (configuration.network.multicast.enabled) {

        if (configuration.network.multicast.ip == null) {
          throw new OConfigurationException("Address not specified in the configuration of multicast");
        }

        if (configuration.network.multicast.port == null) {
          throw new OConfigurationException("Address not specified in the configuration of multicast");
        }

        if (configuration.network.multicast.discoveryPorts == null) {
          throw new OConfigurationException("Address not specified in the configuration of multicast");
        }

      }
    }
  }

  public static OrientDBConfig buildConfig(OContextConfiguration contextConfiguration,
      OServerDistributedConfiguration distributed) {

    OrientDBConfigBuilder builder = OrientDBConfig.builder().fromContext(contextConfiguration);

    ONodeConfigurationBuilder nodeConfigurationBuilder = builder.getNodeConfigurationBuilder();

    nodeConfigurationBuilder.setNodeName(distributed.nodeName).setQuorum(distributed.quorum).setGroupName(distributed.group.name)
        .setGroupPassword(distributed.group.password);

    OServerDistributedNetworkMulticastConfiguration multicast = distributed.network.multicast;

    nodeConfigurationBuilder.setMulticast(
        OMulticastConfguration.builder().setEnabled(multicast.enabled).setIp(multicast.ip).setPort(multicast.port)
            .setDiscoveryPorts(multicast.discoveryPorts).build());

    return builder.build();
  }
}
