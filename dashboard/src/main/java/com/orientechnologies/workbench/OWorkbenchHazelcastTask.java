package com.orientechnologies.workbench;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.ee.common.OWorkbenchPasswordGet;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Created by enricorisa on 20/05/14.
 */
public class OWorkbenchHazelcastTask extends TimerTask {

  private final OWorkbenchPlugin         handler;

  private Map<String, OMonitoredCluster> clusters = new HashMap<String, OMonitoredCluster>();

  public OWorkbenchHazelcastTask(final OWorkbenchPlugin iHandler) {
    this.handler = iHandler;
  }

  @Override
  public void run() {
    OLogManager.instance().info(this, "WORKBENCH contacting hazelcast cluster...");
    String osql = "select from Cluster ";
    OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);
    final List<ODocument> response = this.handler.getDb().query(osqlQuery);
    for (ODocument cluster : response) {
      String clusterName = cluster.field("name");
      if (clusters.get(clusterName) == null) {
        try {
          OMonitoredCluster monitoredCluster = new OMonitoredCluster(handler, cluster);
          clusters.put(clusterName, monitoredCluster);
        } catch (Exception e) {

        }

      }
    }

  }

}
