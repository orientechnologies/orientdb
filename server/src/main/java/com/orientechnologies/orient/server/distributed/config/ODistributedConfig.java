package com.orientechnologies.orient.server.distributed.config;

import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.config.OLocalBinaryListenersConfig;
import com.orientechnologies.orient.core.db.config.OMulticastConfguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedNetworkMulticastConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

public class ODistributedConfig {

  public static OServerDistributedConfiguration fromEnv(OServerDistributedConfiguration distributed)
      throws OConfigurationException {
    final OServerDistributedConfiguration config;
    if (distributed == null) {
      config = new OServerDistributedConfiguration();
      config.setEnabled(false);
    } else {
      config = distributed;
    }

    validateConfiguration(config);

    return config;
  }

  public static void validateConfiguration(OServerDistributedConfiguration configuration)
      throws OConfigurationException {

    if (configuration.getEnabled()) {

      if (configuration.getNodeName() == null) {
        throw new OConfigurationException("Node name not specified in the configuration");
      }

      if (configuration.getGroup().getName() == null) {
        throw new OConfigurationException("Group name not specified in the configuration");
      }
      if (configuration.getGroup().getPassword() == null) {
        throw new OConfigurationException("Group password not specified in the configuration");
      }
      if (configuration.getQuorum() == null) {
        throw new OConfigurationException("Quorum not specified in the configuration");
      }

      if (configuration.getNetwork().getMulticast().isEnabled()) {

        if (configuration.getNetwork().getMulticast().getIp() == null) {
          throw new OConfigurationException(
              "Address not specified in the configuration of multicast");
        }

        if (configuration.getNetwork().getMulticast().getPort() == null) {
          throw new OConfigurationException(
              "Address not specified in the configuration of multicast");
        }

        if (configuration.getNetwork().getMulticast().getDiscoveryPorts() == null) {
          throw new OConfigurationException(
              "Address not specified in the configuration of multicast");
        }
      }
    }
  }

  public static OrientDBConfigBuilder buildNodeConfig(
      OrientDBConfigBuilder configBuilder,
      OServerDistributedConfiguration distributed,
      OServerConfiguration configuration) {
    var nodeConfigurationBuilder = configBuilder.getNodeConfigurationBuilder();
    nodeConfigurationBuilder
        .setNodeName(distributed.getNodeName())
        .setQuorum(distributed.getQuorum())
        .setGroupName(distributed.getGroup().getName())
        .setGroupPassword(distributed.getGroup().getPassword());

    OServerDistributedNetworkMulticastConfiguration multicast =
        distributed.getNetwork().getMulticast();

    nodeConfigurationBuilder.setMulticast(
        OMulticastConfguration.builder()
            .setEnabled(multicast.isEnabled())
            .setIp(multicast.getIp())
            .setPort(multicast.getPort())
            .setDiscoveryPorts(multicast.getDiscoveryPorts())
            .build());

    var listenerBuilder = OLocalBinaryListenersConfig.builder();
    for (OServerNetworkListenerConfiguration listener : configuration.getNetwork().getListeners()) {
      if ("ONetworkProtocolBinary".equals(listener.getProtocol())) {
        listenerBuilder.addListener(
            listener.getIpAddress(), OServerNetworkListener.getPorts(listener.getPortRange()));
      }
    }
    nodeConfigurationBuilder = nodeConfigurationBuilder.setListeners(listenerBuilder.build());

    return configBuilder;
  }
}
