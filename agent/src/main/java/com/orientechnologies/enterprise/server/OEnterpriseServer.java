package com.orientechnologies.enterprise.server;

import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.server.OClientConnection;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public interface OEnterpriseServer {

  void registerConnectionListener(OEnterpriseConnectionListener listener);

  void unregisterConnectionListener(OEnterpriseConnectionListener listener);

  void registerDatabaseListener(OEnterpriseStorageListener listener);

  void unRegisterDatabaseListener(OEnterpriseStorageListener listener);

  void registerFunction(OSQLFunction function);

  void unregisterFunction(String function);

  default Map<String, String> getAvailableStorageNames() {
    return Collections.emptyMap();
  }

  void shutdown();

  List<OClientConnection> getConnections();

  OrientDBInternal getDatabases();
}
