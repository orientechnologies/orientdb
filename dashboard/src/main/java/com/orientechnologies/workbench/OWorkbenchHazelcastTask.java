package com.orientechnologies.workbench;

import java.util.*;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Created by enricorisa on 20/05/14.
 */
public class OWorkbenchHazelcastTask extends TimerTask {

  private final OWorkbenchPlugin handler;

  public OWorkbenchHazelcastTask(final OWorkbenchPlugin iHandler) {
    this.handler = iHandler;
  }

  @Override
  public void run() {
    OLogManager.instance().info(this, "WORKBENCH looking for hazelcast cluster...");
    String osql = "select from Cluster ";
    OSQLQuery<ORecordSchemaAware> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware>(osql);
    final List<ODocument> response = this.handler.getDb().query(osqlQuery);
    for (ODocument cluster : response) {
      String clusterName = cluster.field("name");
      String status = cluster.field("status");
      if (!handler.hasCluster(clusterName)) {
        try {
          OMonitoredCluster monitoredCluster = new OMonitoredCluster(handler, cluster);
          handler.addCluster(monitoredCluster);
        } catch (Exception e) {

        }

      }
    }

  }

}
