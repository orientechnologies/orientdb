package com.orientechnologies.workbench;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by enricorisa on 20/05/14.
 */
public class OWorkbenchHazelcastTask extends TimerTask {

  private final OWorkbenchPlugin handler;

  protected AtomicBoolean        pause;

  public OWorkbenchHazelcastTask(final OWorkbenchPlugin iHandler) {
    this.handler = iHandler;
    pause = new AtomicBoolean(false);
  }

  @Override
  public void run() {

    if (!pause.get()) {
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

  public void pause() {
    pause.set(true);
  }

  public void resume() {
    pause.set(false);
  }
}
