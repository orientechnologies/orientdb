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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.*;

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OSchedulerListener.SCHEDULER_STATUS;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * @author henryzhao81-at-gmail.com
 * @since Mar 28, 2013
 */

public class OScheduler extends ODocumentWrapper implements Runnable {
  public final static String        CLASSNAME      = "OSchedule";

  public static String              PROP_NAME      = "name";
  public static String              PROP_RULE      = "rule";
  public static String              PROP_ARGUMENTS = "arguments";
  public static String              PROP_STATUS    = "status";
  public static String              PROP_FUNC      = "function";
  public static String              PROP_STARTTIME = "starttime";
  public static String              PROP_STARTED   = "start";

  private OFunction                 function;
  private boolean                   isRunning      = false;
  private ODatabaseDocumentInternal db;

  public OScheduler(ODocument doc) {
    // To check presence of function
    getFunction();
    bindDb();
  }

  protected void bindDb() {
    this.db = ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public void fromStream(ODocument iDocument) {
    super.fromStream(iDocument);
    bindDb();
  }

  public OFunction getFunctionSafe() {
    if (function == null) {
      ODocument funcDoc = document.field(PROP_FUNC);
      if (funcDoc != null)
        function = new OFunction(funcDoc);
    }
    return function;
  }

  public OFunction getFunction() {
    OFunction fun = getFunctionSafe();
    if (fun == null)
      throw new OCommandScriptException("function cannot be null");
    return fun;
  }

  public String getSchedulingRule() {
    return document.field(PROP_RULE);
  }

  public String getSchduleName() {
    return document.field(PROP_NAME);
  }

  public boolean isStarted() {
    Boolean started = document.field(PROP_STARTED);
    return started == null ? false : started;
  }

  public void setStarted(boolean started) {
    document.field(PROP_STARTED, started);
  }

  public String getStatus() {
    return document.field(PROP_STATUS);
  }

  public void setStatus(String status) {
    document.field(status, PROP_STATUS);
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

  public String toString() {
    String str = "OSchedule <name:" + getSchduleName() + ",rule:" + getSchedulingRule() + ",current status:" + getStatus()
        + ",func:" + getFunctionSafe() + ",start:" + isStarted() + ">";
    return str;
  }

  @Override
  public void run() {
    if (this.function == null)
      return;

    isRunning = true;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    final Date date = new Date(System.currentTimeMillis());
    OLogManager.instance().warn(this, "execute : " + this.toString() + " at " + sdf.format(date));
    if (db != null)
      ODatabaseRecordThreadLocal.INSTANCE.set(db);

    this.document.field(PROP_STATUS, SCHEDULER_STATUS.RUNNING);
    this.document.field(PROP_STARTTIME, System.currentTimeMillis());
    this.document.save();
    OScriptManager scriptManager = null;
    Bindings binding = null;

    scriptManager = Orient.instance().getScriptManager();
    final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry = scriptManager.acquireDatabaseEngine(db.getName(),
        function.getLanguage());
    final ScriptEngine scriptEngine = entry.object;
    try {
      binding = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);

      scriptManager.bind(binding, (ODatabaseDocumentTx) db, null, getArguments());

      if (this.function.getLanguage() == null)
        throw new OConfigurationException("Database function '" + this.function.getName() + "' has no language");
      final String funcStr = scriptManager.getFunctionDefinition(this.function);
      if (funcStr != null) {
        try {
          scriptEngine.eval(funcStr);
        } catch (ScriptException e) {
          scriptManager.throwErrorMessage(e, funcStr);
        }
      }
      if (scriptEngine instanceof Invocable) {
        final Invocable invocableEngine = (Invocable) scriptEngine;
        Object[] args = null;
        Map<Object, Object> iArgs = getArguments();
        if (iArgs != null) {
          args = new Object[iArgs.size()];
          int i = 0;
          for (Entry<Object, Object> arg : iArgs.entrySet())
            args[i++] = arg.getValue();
        } else {
          args = OCommonConst.EMPTY_OBJECT_ARRAY;
        }
        invocableEngine.invokeFunction(this.function.getName(), args);
      }
    } catch (ScriptException e) {
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", this.function.getName(), e.getColumnNumber()), e);
    } catch (NoSuchMethodException e) {
      throw OException
          .wrapException(new OCommandScriptException("Error on execution of the script", this.function.getName(), 0), e);
    } catch (OCommandScriptException e) {
      throw e;
    } catch (Exception ex) {
      throw OException.wrapException(new OCommandScriptException("Unknown Exception", this.function.getName(), 0), ex);
    } finally {
      if (scriptManager != null && binding != null)
        scriptManager.unbind(binding, null, getArguments());

      scriptManager.releaseDatabaseEngine(function.getLanguage(), db.getName(), entry);

      OLogManager.instance().warn(this, "Job : " + this.toString() + " Finished!");
      isRunning = false;
      this.document.field(PROP_STATUS, SCHEDULER_STATUS.WAITING);
      this.document.save();
    }
  }
}
