/*
 * Copyright 2010-2012 henryzhao81@gmail.com
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

package com.orientechnologies.orient.server.schedule;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.schedule.OScheduler;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

/**
 * Author : henryzhao81@gmail.com Mar 28, 2013
 */

public class OScheduleHandler extends OServerHandlerAbstract {
  private static int          MAX_POOL_SIZE = 21;                                         // 1 for TimerThread
  private ExecutorService     executor      = Executors.newFixedThreadPool(MAX_POOL_SIZE);
  protected String            databaseName  = "";
  protected String            user          = "admin";
  protected String            pass          = "admin";
  protected boolean           isEnabled     = false;
  private final static String BASEPATH      = "${ORIENTDB_HOME}/databases/";

  public OScheduleHandler() {
  }

  @Override
  public String getName() {
    return "scheduler";
  }

  @Override
  public void config(final OServer iServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("databaseName")) {
        this.databaseName = param.value;
      }
      if (param.name.equalsIgnoreCase("user")) {
        this.user = param.value;
      }
      if (param.name.equalsIgnoreCase("pass")) {
        this.pass = param.value;
      }
      if (param.name.equalsIgnoreCase("enabled")) {
        this.isEnabled = Boolean.parseBoolean(param.value);
      }
    }
  }

  @Override
  public void startup() {
    if (!isEnabled)
      return;
    TimerThread tThread = new TimerThread(this);
    executor.execute(tThread);
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  public void executeSchedule(long referenceTimeMillis) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    Map<String, OScheduler> schedulers = db.getMetadata().getSchedulerListener().getSchedulers();
    Iterator<String> sKeys = schedulers.keySet().iterator();
    while (sKeys.hasNext()) {
      String key = sKeys.next();
      OScheduler scheduler = schedulers.get(key);
      if (OLogManager.instance().isDebugEnabled()) {
        OLogManager.instance().debug(this, "check : " + scheduler.toString());
      }
      OSchedulingPattern pattern = new OSchedulingPattern(scheduler.getSchedulingRule());
      if (pattern.match(TimeZone.getDefault(), referenceTimeMillis) && scheduler.isStarted()) {
        executor.execute(scheduler);
      }
    }
  }

  public ODatabaseDocument getDatabase() {
    ODatabaseDocument db = null;
    try {
      String url = OSystemVariableResolver.resolveSystemVariables(OFileUtils.getPath(new File(BASEPATH + this.databaseName)
          .getPath()));
      if (this.exists(url)) {
        db = new ODatabaseDocumentTx("local:" + url).open(this.user, this.pass);
      } else {
        db = null;
        OLogManager.instance().error(this, "database pharos not exist");
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      db = null;
      OLogManager.instance().error(this, "failed to open database");
    }
    return db;
  }

  private boolean exists(String path) {
    return new File(path + "/default.0.oda").exists();
  }
}

class TimerThread extends Thread {
  OScheduleHandler  handler;
  ODatabaseDocument db = null;

  public TimerThread(OScheduleHandler handler) {
    this.handler = handler;
  }

  public void run() {
    OLogManager.instance().warn(this, "Schedule Timer Started");
    long millis = System.currentTimeMillis();
    long nextMinute = ((millis / 60000) + 1) * 60000;
    while (true) {
      long sleepTime = (nextMinute - System.currentTimeMillis());
      if (sleepTime > 0) {
        try {
          safeSleep(sleepTime);
        } catch (InterruptedException e) {
          OLogManager.instance().error(this, "exit timer thread " + e.getMessage());
          break;
        }
      }
      millis = System.currentTimeMillis();
      try {
        if (!isInterrupted() && db != null)
          handler.executeSchedule(millis);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "error on execute schedule " + t.getMessage());
        if (db != null)
          db.close();
        db = null;
      }
      nextMinute = ((millis / 60000) + 1) * 60000;
      // set to thread local, delay sometime for Database fully initialized
      if (db == null)
        db = handler.getDatabase();
    }
    if (db != null)
      db.close();
    OLogManager.instance().warn(this, "Schedule Timer Ended");
  }

  /**
   * It has been reported that the {@link Thread#sleep(long)} method sometimes exits before the requested time has passed. This one
   * offers an alternative that sometimes could sleep a few millis more than requested, but never less.
   * 
   * @param millis
   *          The length of time to sleep in milliseconds.
   * @throws InterruptedException
   *           If another thread has interrupted the current thread. The <i>interrupted status</i> of the current thread is cleared
   *           when this exception is thrown.
   * @see Thread#sleep(long)
   */
  private void safeSleep(long millis) throws InterruptedException {
    long done = 0;
    do {
      long before = System.currentTimeMillis();
      sleep(millis - done);
      long after = System.currentTimeMillis();
      done += (after - before);
    } while (done < millis);
  }
}
