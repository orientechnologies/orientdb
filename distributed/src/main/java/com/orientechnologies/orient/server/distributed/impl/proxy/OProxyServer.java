package com.orientechnologies.orient.server.distributed.impl.proxy;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OProxyServer extends OServerPluginAbstract {
  protected boolean enabled = true;
  protected String remoteHost = "localhost";
  protected Map<Integer, Integer> ports = new HashMap<Integer, Integer>();
  protected int bufferSize = 16384;

  protected List<OProxyServerListener> serverThreads = new ArrayList<OProxyServerListener>();
  protected volatile boolean running = false;
  protected String tracing = "byte";
  protected int readTimeout = 300;
  protected boolean waitUntilRemotePortsAreOpen = false;

  public OProxyServer() {}

  @Override
  public String getName() {
    return "proxy";
  }

  @Override
  public void startup() {
    if (!enabled) return;

    running = true;

    for (Map.Entry<Integer, Integer> ports : this.ports.entrySet()) {
      final int localPort = ports.getKey();
      final int remotePort = ports.getValue();

      OLogManager.instance()
          .info(
              this,
              "Proxy server: configuring proxy connection from localhost:%d -> %s:%d...",
              localPort,
              remoteHost,
              remotePort);

      try {
        final OProxyServerListener serverThread =
            new OProxyServerListener(this, localPort, remotePort);
        serverThread.start();
        serverThreads.add(serverThread);

      } catch (Exception e) {
        OLogManager.instance().error(this, "Proxy server: error on starting proxy server", e);
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
    for (OServerParameterConfiguration param : params) {
      if (param.name.equalsIgnoreCase("enabled")) enabled = Boolean.parseBoolean(param.value);
      else if (param.name.equalsIgnoreCase("remoteHost")) remoteHost = param.value;
      else if (param.name.equalsIgnoreCase("tracing")) {
        if (!"none".equalsIgnoreCase(param.value)
            && !"byte".equalsIgnoreCase(param.value)
            && !"hex".equalsIgnoreCase(param.value))
          OLogManager.instance().error(this, "Invalid tracing value: %s", null, param.value);
        else tracing = param.value;

      } else if (param.name.equalsIgnoreCase("ports")) {
        setPorts(param.value);
      }
    }
  }

  public void setPorts(final String portsAsString) {
    ports.clear();

    final String[] pairs = portsAsString.split(",");
    for (String pair : pairs) {
      final String[] fromTo = pair.split("->");
      if (fromTo.length != 2)
        throw new OConfigurationException(
            "Proxy server: port configuration is not valid. Format: portFrom->portTo");
      ports.put(Integer.parseInt(fromTo[0]), Integer.parseInt(fromTo[1]));
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public String getTracing() {
    return tracing;
  }

  public void setTracing(String tracing) {
    this.tracing = tracing;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  public String getRemoteHost() {
    return remoteHost;
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
    if ("none".equalsIgnoreCase(tracing)) return "";

    final StringBuilder buffer = new StringBuilder();
    for (int i = 0; i < total; ++i) {
      if (i > 0) buffer.append(',');

      if ("byte".equalsIgnoreCase(tracing)) buffer.append(request[i]);
      else if ("hex".equalsIgnoreCase(tracing)) buffer.append(String.format("0x%x", request[i]));
    }
    return buffer.toString();
  }
}
