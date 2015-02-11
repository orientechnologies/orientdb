package com.orientechnologies.workbench;

import java.io.IOException;
import java.util.Map;
import java.util.TimerTask;

/**
 * Created by Enrico Risa on 11/02/15.
 */
public class OWorkbenchRealtimeTask extends TimerTask {
  private OWorkbenchPlugin monitor;

  public OWorkbenchRealtimeTask(OWorkbenchPlugin oWorkbenchPlugin) {

    this.monitor = oWorkbenchPlugin;
  }

  @Override
  public void run() {

    for (Map.Entry<String, OMonitoredServer> s : monitor.getMonitoredServers()) {

      try {
        s.getValue().getRealtime().fetch();
      } catch (IOException e) {

      }
    }

  }
}
