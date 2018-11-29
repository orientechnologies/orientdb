package com.orientechnologies.agent.services.metrics.server.network;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OMeter;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Enrico Risa on 18/07/2018.
 */
public class OrientDBServerNetworkMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry  registry;

  private OMeter networkRequests;

  private AtomicLong connections = new AtomicLong(0);

  private AtomicLong sockets = new AtomicLong(0);

  public OrientDBServerNetworkMetrics(OEnterpriseServer server, OMetricsRegistry registry) {

    this.server = server;
    this.registry = registry;
  }

  @Override
  public void start() {

    this.networkRequests = this.registry
        .meter(OGlobalMetrics.SERVER_NETWORK_REQUESTS.name, OGlobalMetrics.SERVER_NETWORK_REQUESTS.description, "Events/seconds");

    server.registerConnectionListener(new OEnterpriseConnectionListener() {

      @Override
      public void onClientConnection(OClientConnection iConnection) {
        connections.incrementAndGet();
      }

      @Override
      public void onClientDisconnection(OClientConnection iConnection) {
        connections.decrementAndGet();
      }

      @Override
      public void onAfterClientRequest(OClientConnection iConnection, byte iRequestType) {
        networkRequests.mark();
      }

      @Override
      public void onSocketAccepted(ONetworkProtocol protocol) {
        sockets.incrementAndGet();
      }

      @Override
      public void onSocketDestroyed(ONetworkProtocol protocol) {
        sockets.decrementAndGet();
      }
    });

    this.registry.gauge(OGlobalMetrics.SERVER_NETWORK_SESSIONS.name, OGlobalMetrics.SERVER_NETWORK_SESSIONS.description,
        () -> connections.get());

    this.registry.gauge(OGlobalMetrics.SERVER_NETWORK_SOCKETS.name, OGlobalMetrics.SERVER_NETWORK_SOCKETS.description,
        () -> sockets.get());

  }

  @Override
  public void stop() {
    this.registry.remove(OGlobalMetrics.SERVER_NETWORK_REQUESTS.name);
    this.registry.remove(OGlobalMetrics.SERVER_NETWORK_SESSIONS.name);
    this.registry.remove(OGlobalMetrics.SERVER_NETWORK_SOCKETS.name);
  }
}
