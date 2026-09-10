package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.log.OLogger;
import java.util.TimerTask;

public class OTimerTask extends TimerTask {
  private static final OLogger logger = OLogger.get(OTimerTask.class);
  private final Runnable task;

  public OTimerTask(Runnable task) {
    this.task = task;
  }

  @Override
  public void run() {
    try {
      this.task.run();
    } catch (Exception ex) {
      logger.warn("Exception running scheduled task", ex);
    } catch (Error err) {
      logger.error("Error running scheduled task", err);
    } catch (Throwable th) {
      logger.error("Error running scheduled task", th);
    }
  }
}
