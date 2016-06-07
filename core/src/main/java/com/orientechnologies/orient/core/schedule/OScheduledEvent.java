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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OScheduler.STATUS;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;

/**
 * Represents an instance of a scheduled event.
 *
 * @author Luca Garulli
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */

public class OScheduledEvent extends ODocumentWrapper {
  public final static String  CLASS_NAME     = "OSchedule";

  public static final String  PROP_NAME      = "name";
  public static final String  PROP_RULE      = "rule";
  public static final String  PROP_ARGUMENTS = "arguments";
  public static final String  PROP_STATUS    = "status";
  public static final String  PROP_FUNC      = "function";
  public static final String  PROP_STARTTIME = "starttime";
  public static final String  PROP_EXEC_ID   = "nextExecId";

  private ODatabaseDocument db;

  private OFunction           function;
  private boolean             isRunning      = false;
  private OCronExpression     cron;
  private volatile TimerTask  timer;
  private long                nextExecutionId;

  private class ScheduledTimer extends TimerTask {
    @Override
    public void run() {
      if (isRunning) {
        OLogManager.instance().error(this, "Error: The scheduled event '" + getName() + "' is already running");
        return;
      }

      if (function == null) {
        OLogManager.instance().error(this, "Error: The scheduled event '" + getName() + "' has no configured function");
        return;
      }

      try {

        executeFunction();

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (timer != null) {
          // RE-SCHEDULE THE NEXT EVENT
          schedule();
        }
      }
    }
  }

  /**
   * Creates a scheduled event object from a configuration.
   */
  public OScheduledEvent(final ODocument doc) {
    super(doc);
    getFunction();
    try {
      cron = new OCronExpression(getRule());
    } catch (ParseException e) {
      OLogManager.instance().error(this, "Error on compiling cron expression " + getRule());
    }
  }

  public void interrupt() {
    final TimerTask t = timer;
    timer = null;
    if (t != null)
      t.cancel();
  }

  public OFunction getFunction() {
    final OFunction fun = getFunctionSafe();
    if (fun == null)
      throw new OCommandScriptException("Function cannot be null");
    return fun;
  }

  public String getRule() {
    return document.field(PROP_RULE);
  }

  public String getName() {
    return document.field(PROP_NAME);
  }

  public Long getNextExecutionId() {
    final Long value = document.field(PROP_EXEC_ID);
    if (value == null)
      return 0l;
    return value;
  }

  @Override
  public <RET extends ODocumentWrapper> RET save() {
    if (db == null)
      bindDb();
    else
      db.activateOnCurrentThread();

    return super.save();
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
    return this.isRunning;
  }

  public OScheduledEvent schedule() {
    if (timer != null)
      timer.cancel();

    bindDb();

    timer = new ScheduledTimer();
    nextExecutionId = getNextExecutionId() + 1;
    Orient.instance().scheduleTask(timer, cron.getNextValidTimeAfter(new Date()), 0);
    return this;
  }

  public String toString() {
    return "OSchedule [name:" + getName() + ",rule:" + getRule() + ",current status:" + getStatus() + ",func:" + getFunctionSafe()
        + ",started:" + getStartTime() + "]";
  }

  @Override
  public void fromStream(final ODocument iDocument) {
    super.fromStream(iDocument);
    bindDb();
    try {
      cron.buildExpression(getRule());
    } catch (ParseException e) {
      OLogManager.instance().error(this, "Error on compiling cron expression " + getRule());
    }
  }

  private Object executeFunction() {
    isRunning = true;

    OLogManager.instance().info(this, "Checking for the execution of the scheduled event '%s' executionId=%d...", getName(),
        nextExecutionId);

    Object result = null;
    try {
      if (db != null)
        db.activateOnCurrentThread();

      boolean executeEvent = false;
      for (int retry = 0; retry < 10; ++retry) {
        try {
          if (isEventAlreadyExecuted())
            break;

          document.field(PROP_STATUS, STATUS.RUNNING);
          document.field(PROP_STARTTIME, System.currentTimeMillis());
          document.field(PROP_EXEC_ID, nextExecutionId);
          document.save();

          // OK
          executeEvent = true;
          break;

        } catch (ONeedRetryException e) {

          // CONCURRENT UPDATE, PROBABLY EXECUTED BY ANOTHER SERVER
          if (isEventAlreadyExecuted())
            break;

          OLogManager.instance().info(this, "Cannot change the status of the scheduled event '%s' executionId=%d, retry %d",
              getName(), nextExecutionId, retry);

        } catch (ORecordNotFoundException e) {
          OLogManager.instance().info(this, "Scheduled event '%s' executionId=%d not found on database, removing event", getName(),
              nextExecutionId);

          timer = null;
          break;
        } catch (Throwable e) {
          // SUSPEND EXECUTION
          OLogManager.instance().error(this, "Error during starting of scheduled event '%s' executionId=%d", e, getName(),
              nextExecutionId);

          timer = null;
          break;
        }
      }

      if (!executeEvent)
        return null;

      OLogManager.instance().info(this, "Executing scheduled event '%s' executionId=%d...", getName(), nextExecutionId);
      try {
        result = function.execute(getArguments());
      } finally {
        OLogManager.instance().info(this, "Scheduled event '%s' executionId=%d completed with result: %s", getName(),
            nextExecutionId, result);
        document.field(PROP_STATUS, STATUS.WAITING);
        document.save();
      }

    } finally {
      isRunning = false;
    }

    return result;
  }

  private boolean isEventAlreadyExecuted() {
    final ODocument updated = document.getIdentity().getRecord().reload();

    final Long currentExecutionId = updated.field(PROP_EXEC_ID);
    if( currentExecutionId == null )
      return false;

    if (currentExecutionId >= nextExecutionId) {
      OLogManager.instance().info(this, "Scheduled event '%s' with id %d is already running (current id=%d)", getName(),
          nextExecutionId, currentExecutionId);
      // ALREADY RUNNING
      return true;
    }
    return false;
  }

  private void bindDb() {
    final ODatabaseDocumentInternal tlDb = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (tlDb != null && !tlDb.isClosed())
      this.db = ((ODatabaseDocumentTx) tlDb).copy();
  }

  private OFunction getFunctionSafe() {
    if (function == null) {
      final Object funcDoc = document.field(PROP_FUNC);
      if (funcDoc != null) {
        if (funcDoc instanceof OFunction) {
          function = (OFunction) funcDoc;
          // OVERWRITE FUNCTION ID
          document.field(PROP_FUNC, function.getId());
        } else if (funcDoc instanceof ODocument)
          function = new OFunction((ODocument) funcDoc);
        else if (funcDoc instanceof ORecordId)
          function = new OFunction((ORecordId) funcDoc);
      }
    }
    return function;
  }
}
