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

import java.util.HashMap;
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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

/**
 * Abstract plugin to manage the distributed environment.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class ODistributedAbstractPlugin extends OServerHandlerAbstract implements ODistributedServerManager,
    ODatabaseLifecycleListener, ORecordHook {
  protected boolean                           enabled               = true;
  protected String                            alias                 = null;
  protected long                              offlineBuffer         = -1;
  protected Map<String, ODocument>            databaseConfiguration = new ConcurrentHashMap<String, ODocument>();
  protected Map<String, OStorageSynchronizer> synchronizers         = new HashMap<String, OStorageSynchronizer>();

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    oServer.setVariable("ODistributedAbstractPlugin", this);

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
        databaseConfiguration.put(param.name.substring("db.".length()),
            (ODocument) new ODocument().fromJSON(param.value.trim(), "noMap"));
    }

    // CHECK THE CONFIGURATION
    if (!databaseConfiguration.containsKey("*"))
      throw new OConfigurationException(
          "Invalid cluster configuration: cannot find settings for the default synchronization as 'db.*'");
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
    final ODocument cfg = getLocalDatabaseConfiguration(iDatabase.getName());
    if (cfg == null)
      return;

    final Boolean synch = (Boolean) cfg.field("synchronization");
    if (synch == null || synch) {
      getDatabaseSynchronizer(iDatabase.getName(), null);

      Object defCluster = cfg.field("defaultCluster");
      if (defCluster != null)
        iDatabase.set(ATTRIBUTES.DEFAULTCLUSTERID, defCluster);
//
//      final ODocument classes = cfg.field("classes");
//      if (classes != null) {
//        for (String className : classes.fieldNames()) {
//          final ODocument clazz = classes.field(className);
//          if (clazz != null) {
//            final String defaultCluster = clazz.field("defaultCluster");
//            int clusterId = ((ODatabaseComplex<?>) iDatabase).getClusterIdByName(defaultCluster);
//
//            if (clusterId == -1) {
//              // CREATE THE NEW CLUSTER ID
//              final CLUSTER_TYPE clusterType = iDatabase.getStorage() instanceof OStorageLocal ? CLUSTER_TYPE.PHYSICAL
//                  : CLUSTER_TYPE.MEMORY;
//              clusterId = ((ODatabaseComplex<?>) iDatabase).addCluster(defaultCluster, clusterType);
//            }
//
//            OClass cls = ((ODatabaseComplex<?>) iDatabase).getMetadata().getSchema().getClass(className);
//            if (cls == null)
//              // CREATE THE CLASS WITH THE CLUSTER ID AS DEFAULT
//              cls = ((ODatabaseComplex<?>) iDatabase).getMetadata().getSchema().createClass(className, clusterId);
//            else
//              cls.setDefaultClusterId(clusterId);
//
//          }
//        }
//      }

      if (iDatabase instanceof ODatabaseComplex<?>)
        ((ODatabaseComplex<?>) iDatabase).registerHook(this);
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

    final OStorageSynchronizer synchronizer;
    synchronized (synchronizers) {
      synchronizer = synchronizers.get(db.getName());
    }
    if (synchronizer != null)
      return synchronizer.distributeOperation(iType, iRecord);

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

  public ODocument getLocalDatabaseConfiguration(final String iDatabaseName) {
    final ODocument cfg = getDatabaseConfiguration(iDatabaseName);
    ODocument perServerCfg = cfg.field("servers[" + alias + "]");
    if (perServerCfg == null)
      // NOT FOUND: GET THE DEFAULT ONE
      perServerCfg = cfg.field("servers[*]");
    return perServerCfg;
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

  public OStorageSynchronizer getDatabaseSynchronizer(final String iDatabaseName, final String iNodeId) {
    synchronized (synchronizers) {
      OStorageSynchronizer sync = synchronizers.get(iDatabaseName);
      if (sync == null) {
        sync = new OStorageSynchronizer(this, iDatabaseName);
        synchronizers.put(iDatabaseName, sync);
      }

      if (iNodeId != null)
        sync.getLog(iNodeId);
      return sync;
    }
  }
}
