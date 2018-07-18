package com.orientechnologies.enterprise.server;

import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public class OEnterpriseServerImpl implements OEnterpriseServer, OServerPlugin {

  private OServer server;

  private List<OEnterpriseConnectionListener> listeners = new ArrayList<>();

  public OEnterpriseServerImpl(OServer server) {
    this.server = server;
    server.getPluginManager().registerPlugin(new OServerPluginInfo("Enterprise Server", null, null, null, this, null, 0, null));
  }

  @Override
  public void registerConnectionListener(OEnterpriseConnectionListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterConnectionListener(OEnterpriseConnectionListener listener) {
    listeners.remove(listener);
  }

  @Override
  public Map<String, String> getAvailableStorageNames() {
    return server.getAvailableStorageNames();
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void startup() {

  }

  @Override
  public void shutdown() {
    listeners.clear();
  }

  @Override
  public void onClientConnection(OClientConnection oClientConnection) {

    this.listeners.forEach((l) -> l.onClientConnection(oClientConnection));
  }

  @Override
  public void onClientDisconnection(OClientConnection oClientConnection) {
    this.listeners.forEach((l) -> l.onClientDisconnection(oClientConnection));
  }

  @Override
  public void onBeforeClientRequest(OClientConnection oClientConnection, byte b) {
    this.listeners.forEach((l) -> l.onBeforeClientRequest(oClientConnection, b));
  }

  @Override
  public void onAfterClientRequest(OClientConnection oClientConnection, byte b) {
    this.listeners.forEach((l) -> l.onAfterClientRequest(oClientConnection, b));
  }

  @Override
  public void onClientError(OClientConnection oClientConnection, Throwable throwable) {
    this.listeners.forEach((l) -> l.onClientError(oClientConnection, throwable));
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] oServerParameterConfigurations) {
  }

  @Override
  public void sendShutdown() {

  }

  @Override
  public Object getContent(String s) {
    return null;
  }
}
