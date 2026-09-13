package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OProxyServer implements OServerPlugin {
  private static final OLogger logger = OLogManager.instance().logger(OProxyServer.class);
  protected int bufferSize = 16384;

  protected List<OProxyServerListener> serverThreads = new ArrayList<OProxyServerListener>();
  protected volatile boolean running = false;
  protected boolean waitUntilRemotePortsAreOpen = false;
  protected OProxyServerConfig config;
  protected int readTimeout = 300;

  public OProxyServer() {}

  @Override
  public String getName() {
    return "proxy";
  }

  @Override
  public void startup() {
    if (!config.isEnabled()) return;

    running = true;

    for (Map.Entry<Integer, Integer> ports : config.getPorts().entrySet()) {
      final int localPort = ports.getKey();
      final int remotePort = ports.getValue();

      logger.info(
          "Proxy server: configuring proxy connection from localhost:%d -> %s:%d...",
          localPort, config.getRemoteHost(), remotePort);

      try {
        final OProxyServerListener serverThread =
            new OProxyServerListener(this, localPort, remotePort);
        serverThread.start();
        serverThreads.add(serverThread);

      } catch (Exception e) {
        logger.error("Proxy server: error on starting proxy server", e);
      }
    }
  }

  protected void onMessage(
      final boolean request,
      final int fromPort,
      final int toPort,
      final byte[] buffer,
      final int size) {}

  @Override
  public void shutdown() {
    running = false;
    for (OProxyServerListener t : serverThreads) t.sendShutdown();
  }

  @Override
  public void config(final OServer server, final OServerParameterConfiguration[] params) {
    config = OProxyServerConfig.fromParameters(params);
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isWaitUntilRemotePortsAreOpen() {
    return waitUntilRemotePortsAreOpen;
  }

  public void setWaitUntilRemotePortsAreOpen(boolean waitUntilRemotePortsAreOpen) {
    this.waitUntilRemotePortsAreOpen = waitUntilRemotePortsAreOpen;
  }

  public String formatBytes(final byte[] request, final int total) {
    if ("none".equalsIgnoreCase(config.getTracing())) return "";

    final StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < total; ++i) {
      if (i > 0) buffer.append(',');

      if ("byte".equalsIgnoreCase(config.getTracing())) buffer.append(request[i]);
      else if ("hex".equalsIgnoreCase(config.getTracing()))
        buffer.append(String.format("0x%x", request[i]));
    }
    return buffer.toString();
  }

  public void setPorts(final String portsAsString) {
    this.config.setPorts(portsAsString);
  }

  public void setTracing(String tracing) {
    this.config.setTracing(tracing);
  }

  public String getTracing() {
    return this.config.getTracing();
  }

  public String getRemoteHost() {
    return this.config.getRemoteHost();
  }
}
