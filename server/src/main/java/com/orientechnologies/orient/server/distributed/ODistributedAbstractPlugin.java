/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.replication.OStorageReplicator;

/**
 * Abstract plugin to manage the distributed environment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODistributedAbstractPlugin extends OServerHandlerAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener, ORecordHook {
  protected boolean                         enabled               = true;
  protected String                          alias                 = null;
  protected long                            offlineBuffer         = -1;
  protected Map<String, ODocument>          databaseConfiguration = new ConcurrentHashMap<String, ODocument>();
  protected Map<String, OStorageReplicator> replicators           = new ConcurrentHashMap<String, OStorageReplicator>();

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value)) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("alias"))
        alias = param.value;
      else if (param.name.startsWith("db."))
        databaseConfiguration.put(param.name.substring("db.".length()), (ODocument) new ODocument().fromJSON(param.value.trim()));
    }

    // CHECK THE CONFIGURATION
    if (!databaseConfiguration.containsKey("*"))
      throw new OConfigurationException("Invalid cluster configuration: cannot find settings for the default replication as 'db.*'");
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    super.startup();
    Orient.instance().addDbLifecycleListener(this);
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    Orient.instance().removeDbLifecycleListener(this);

    super.shutdown();
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabase iDatabase) {
    final ODocument cfg = getServerDatabaseConfiguration(iDatabase.getName());
    if ((Boolean) cfg.field("replication")) {
      createReplicator(iDatabase.getName());

      if (iDatabase instanceof ODatabaseComplex<?>)
        ((ODatabaseComplex<?>) iDatabase).registerHook(this);

      Object defCluster = cfg.field("defaultCluster");
      if (defCluster != null)
        iDatabase.set(ATTRIBUTES.DEFAULTCLUSTERID, defCluster);
    }
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabase iDatabase) {
    if (iDatabase instanceof ODatabaseComplex<?>)
      ((ODatabaseComplex<?>) iDatabase).unregisterHook(this);
  }

  @Override
  public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final OStorageReplicator replicator = replicators.get(db.getName());
    if (replicator != null)
      return replicator.distributeOperation(iType, iRecord);

    return false;
  }

  @Override
  public void sendShutdown() {
    super.sendShutdown();
  }

  @Override
  public String getName() {
    return "cluster";
  }

  public String getLocalNodeId() {
    return alias;
  }

  public ODocument getDatabaseConfiguration(final String iDatabaseName) {
    // NOT FOUND: GET BY CONFIGURATION ON LOCAL NODE
    ODocument cfg = databaseConfiguration.get(iDatabaseName);
    if (cfg == null)
      // NOT FOUND: GET THE DEFAULT ONE
      cfg = databaseConfiguration.get("*");

    return cfg;
  }

  public ODocument getServerDatabaseConfiguration(final String iDatabaseName) {
    final ODocument cfg = getDatabaseConfiguration(iDatabaseName);
    Map<String, Object> perServerCfg = cfg.field("servers[" + getLocalNodeId() + "]");
    if (perServerCfg == null)
      // NOT FOUND: GET THE DEFAULT ONE
      perServerCfg = cfg.field("servers[*]");
    return new ODocument(perServerCfg);
  }

  public void setDefaultDatabaseConfiguration(final String iDatabaseName, final ODocument iConfiguration) {
    databaseConfiguration.put(iDatabaseName, iConfiguration);
  }

  public long getOfflineBuffer() {
    return offlineBuffer;
  }

  public void setOfflineBuffer(long offlineBuffer) {
    this.offlineBuffer = offlineBuffer;
  }

  protected void createReplicator(final String iDatabaseName) {
    if (!replicators.containsKey(iDatabaseName))
      replicators.put(iDatabaseName, new OStorageReplicator(this, iDatabaseName));
  }
}
