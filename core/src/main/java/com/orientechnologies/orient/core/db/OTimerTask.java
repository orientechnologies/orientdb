package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.log.OLogManager;
import java.util.TimerTask;

public class OTimerTask extends TimerTask {

  private final Runnable task;

  public OTimerTask(Runnable task) {
    this.task = task;
  }

  @Override
  public void run() {
    try {
      this.task.run();
    } catch (Exception ex) {
      OLogManager.instance().warn(this, "Exception running scheduled task", ex);
    } catch (Error err) {
      OLogManager.instance().error(this, "Error running scheduled task", err);
    } catch (Throwable th) {
      OLogManager.instance().error(this, "Error running scheduled task", th);
    }
  }
}
