/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
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

package com.orientechnologies.orient.core.schedule;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduler.STATUS;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents an instance of a scheduled event.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */
public class OScheduledEvent extends ODocumentWrapper {
  public static final String CLASS_NAME = "OSchedule";

  public static final String PROP_NAME = "name";
  public static final String PROP_RULE = "rule";
  public static final String PROP_ARGUMENTS = "arguments";
  public static final String PROP_STATUS = "status";
  public static final String PROP_FUNC = "function";
  public static final String PROP_STARTTIME = "starttime";
  public static final String PROP_EXEC_ID = "nextExecId";

  private OFunction function;
  private final AtomicBoolean running;
  private OCronExpression cron;
  private volatile TimerTask timer;
  private final AtomicLong nextExecutionId;

  /** Creates a scheduled event object from a configuration. */
  public OScheduledEvent(final ODocument doc) {
    super(doc);
    running = new AtomicBoolean(false);
    nextExecutionId = new AtomicLong(getNextExecutionId());
    getFunction();
    try {
      cron = new OCronExpression(getRule());
    } catch (ParseException e) {
      OLogManager.instance().error(this, "Error on compiling cron expression " + getRule(), e);
    }
  }

  public void interrupt() {
    synchronized (this) {
      final TimerTask t = timer;
      timer = null;
      if (t != null) t.cancel();
    }
  }

  public OFunction getFunction() {
    final OFunction fun = getFunctionSafe();
    if (fun == null) throw new OCommandScriptException("Function cannot be null");
    return fun;
  }

  public String getRule() {
    return document.field(PROP_RULE);
  }

  public String getName() {
    return document.field(PROP_NAME);
  }

  public long getNextExecutionId() {
    Long value = document.field(PROP_EXEC_ID);
    return value != null ? value : 0;
  }

  public String getStatus() {
    return document.field(PROP_STATUS);
  }

  public Map<Object, Object> getArguments() {
    return document.field(PROP_ARGUMENTS);
  }

  public Date getStartTime() {
    return document.field(PROP_STARTTIME);
  }

  public boolean isRunning() {
    return this.running.get();
  }

  public OScheduledEvent schedule(String database, String user, OrientDBInternal orientDB) {
    if (isRunning()) {
      interrupt();
    }
    ScheduledTimerTask task = new ScheduledTimerTask(this, database, user, orientDB);
    task.schedule();
    timer = task;
    return this;
  }

  @Override
  public String toString() {
    return "OSchedule [name:"
        + getName()
        + ",rule:"
        + getRule()
        + ",current status:"
        + getStatus()
        + ",func:"
        + getFunctionSafe()
        + ",started:"
        + getStartTime()
        + "]";
  }

  @Override
  public void fromStream(final ODocument iDocument) {
    super.fromStream(iDocument);
    try {
      cron.buildExpression(getRule());
    } catch (ParseException e) {
      OLogManager.instance().error(this, "Error on compiling cron expression " + getRule(), e);
    }
  }

  private void setRunning(boolean running) {
    this.running.set(running);
  }

  private OFunction getFunctionSafe() {
    if (function == null) {
      final Object funcDoc = document.field(PROP_FUNC);
      if (funcDoc != null) {
        if (funcDoc instanceof OFunction) {
          function = (OFunction) funcDoc;
          // OVERWRITE FUNCTION ID
          document.field(PROP_FUNC, function.getId());
        } else if (funcDoc instanceof ODocument) function = new OFunction((ODocument) funcDoc);
        else if (funcDoc instanceof ORecordId) function = new OFunction((ORecordId) funcDoc);
      }
    }
    return function;
  }

  private static class ScheduledTimerTask extends TimerTask {

    private final OScheduledEvent event;
    private final String database;
    private final String user;
    private final OrientDBInternal orientDB;

    private ScheduledTimerTask(
        OScheduledEvent event, String database, String user, OrientDBInternal orientDB) {
      this.event = event;
      this.database = database;
      this.user = user;
      this.orientDB = orientDB;
    }

    public void schedule() {
      synchronized (this) {
        event.nextExecutionId.incrementAndGet();
        Date now = new Date();
        long time = event.cron.getNextValidTimeAfter(now).getTime();
        long delay = time - now.getTime();
        orientDB.scheduleOnce(this, delay);
      }
    }

    @Override
    public void run() {
      orientDB.execute(
          database,
          user,
          db -> {
            runTask(db);
            return null;
          });
    }

    private void runTask(ODatabaseSession db) {
      event.reload();

      if (event.running.get()) {
        OLogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName() + "' is already running",
                null);
        return;
      }

      if (event.function == null) {
        OLogManager.instance()
            .error(
                this,
                "Error: The scheduled event '" + event.getName() + "' has no configured function",
                null);
        return;
      }

      try {
        event.setRunning(true);

        OLogManager.instance()
            .info(
                this,
                "Checking for the execution of the scheduled event '%s' executionId=%d...",
                event.getName(),
                event.nextExecutionId.get());
        try {
          boolean executeEvent = executeEvent();
          if (executeEvent) {
            OLogManager.instance()
                .info(
                    this,
                    "Executing scheduled event '%s' executionId=%d...",
                    event.getName(),
                    event.nextExecutionId.get());
            executeEventFunction();
          }

        } finally {
          event.setRunning(false);
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during execution of scheduled function", e);
      } finally {
        if (event.timer != null) {
          // RE-SCHEDULE THE NEXT EVENT
          event.schedule(database, user, orientDB);
        }
      }
    }

    private boolean executeEvent() {
      for (int retry = 0; retry < 10; ++retry) {
        try {
          if (isEventAlreadyExecuted()) break;

          event.document.field(PROP_STATUS, STATUS.RUNNING);
          event.document.field(PROP_STARTTIME, System.currentTimeMillis());
          event.document.field(PROP_EXEC_ID, event.nextExecutionId.get());

          event.document.save();

          // OK
          return true;
        } catch (ONeedRetryException e) {
          event.document.reload(null, true);
          // CONCURRENT UPDATE, PROBABLY EXECUTED BY ANOTHER SERVER
          if (isEventAlreadyExecuted()) break;

          OLogManager.instance()
              .info(
                  this,
                  "Cannot change the status of the scheduled event '%s' executionId=%d, retry %d",
                  e,
                  event.getName(),
                  event.nextExecutionId.get(),
                  retry);

        } catch (ORecordNotFoundException e) {
          OLogManager.instance()
              .info(
                  this,
                  "Scheduled event '%s' executionId=%d not found on database, removing event",
                  e,
                  event.getName(),
                  event.nextExecutionId.get());
          event.interrupt();
          break;
        } catch (Exception e) {
          // SUSPEND EXECUTION
          OLogManager.instance()
              .error(
                  this,
                  "Error during starting of scheduled event '%s' executionId=%d",
                  e,
                  event.getName(),
                  event.nextExecutionId.get());

          event.interrupt();
          break;
        }
      }
      return false;
    }

    private void executeEventFunction() {
      Object result = null;
      try {
        result = event.function.execute(event.getArguments());
      } finally {
        OLogManager.instance()
            .info(
                this,
                "Scheduled event '%s' executionId=%d completed with result: %s",
                event.getName(),
                event.nextExecutionId.get(),
                result);
        for (int retry = 0; retry < 10; ++retry) {
          try {
            event.document.field(PROP_STATUS, STATUS.WAITING);
            event.document.save();
          } catch (ONeedRetryException e) {
            event.document.reload(null, true);
          } catch (Exception e) {
            OLogManager.instance()
                .error(this, "Error on saving status for event '%s'", e, event.getName());
          }
        }
      }
    }

    private boolean isEventAlreadyExecuted() {
      final ORecord rec = event.document.getIdentity().getRecord();
      if (rec == null)
        // SKIP EXECUTION BECAUSE THE EVENT WAS DELETED
        return true;

      final ODocument updated = rec.reload();

      final Long currentExecutionId = updated.field(PROP_EXEC_ID);
      if (currentExecutionId == null) return false;

      if (currentExecutionId >= event.nextExecutionId.get()) {
        OLogManager.instance()
            .info(
                this,
                "Scheduled event '%s' with id %d is already running (current id=%d)",
                event.getName(),
                event.nextExecutionId.get(),
                currentExecutionId);
        // ALREADY RUNNING
        return true;
      }
      return false;
    }
  }
}
