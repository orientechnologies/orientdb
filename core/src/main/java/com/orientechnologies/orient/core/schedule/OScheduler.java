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

package com.orientechnologies.orient.core.schedule;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptDocumentDatabaseWrapper;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.OScriptOrientWrapper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OSchedulerListener.SCHEDULER_STATUS;

/**
 * Author : henryzhao81@gmail.com Mar 28, 2013
 */

public class OScheduler implements Runnable {
  public final static String  CLASSNAME      = "OSchedule";

  public static String        PROP_NAME      = "name";
  public static String        PROP_RULE      = "rule";
  public static String        PROP_ARGUMENTS = "arguments";
  public static String        PROP_STATUS    = "status";
  public static String        PROP_FUNC      = "function";
  public static String        PROP_STARTTIME = "starttime";
  public static String        PROP_STARTED   = "start";

  private String              name;
  private String              rule;
  private Map<Object, Object> iArgs;
  private String              status;
  private OFunction           function;
  private Date                startTime;
  private ODocument           document;
  private ODatabaseRecord     db;
  private boolean             started;
  private boolean             isRunning      = false;

  public OScheduler(ODocument doc) {
    this.name = doc.field(PROP_NAME);
    this.rule = doc.field(PROP_RULE);
    this.iArgs = doc.field(PROP_ARGUMENTS);
    this.status = doc.field(PROP_STATUS);
    // this.runAtStart = doc.field(PROP_RUN_ON_START) == null ? false : ((Boolean)doc.field(PROP_RUN_ON_START));
    this.started = doc.field(PROP_STARTED) == null ? false : ((Boolean) doc.field(PROP_STARTED));
    ODocument funcDoc = doc.field(PROP_FUNC);
    if (funcDoc != null)
      function = new OFunction(funcDoc);
    else
      throw new OCommandScriptException("function cannot be null");
    this.startTime = doc.field(PROP_STARTTIME);
    this.document = doc;
    this.db = doc.getDatabase();
  }

  public String getSchedulingRule() {
    return this.rule;
  }

  public String getSchduleName() {
    return this.name;
  }

  public boolean isStarted() {
    return this.started;
  }

  public void setStarted(boolean started) {
    this.started = started;
  }

  public String getStatus() {
    return status;
  }

  public Map<Object, Object> arguments() {
    return this.iArgs;
  }

  public OFunction getFunction() {
    return this.function;
  }

  public Date getStartTime() {
    return this.startTime;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  public void resetDocument(ODocument doc) {
    this.document = doc;
    this.name = doc.field(PROP_NAME);
    this.rule = doc.field(PROP_RULE);
    this.iArgs = doc.field(PROP_ARGUMENTS);
    this.status = doc.field(PROP_STATUS);
    this.started = doc.field(PROP_STARTED) == null ? false : ((Boolean) doc.field(PROP_STARTED));
    ODocument funcDoc = doc.field(PROP_FUNC);
    if (funcDoc != null)
      function = new OFunction(funcDoc);
    else
      throw new OCommandScriptException("function cannot be null");
    this.startTime = doc.field(PROP_STARTTIME);
    this.db = doc.getDatabase();
  }

  public String toString() {
    String str = "OSchedule <name:" + this.name + ",rule:" + this.rule + ",current status:" + this.status + ",func:"
        + this.function.getName() + ",start:" + this.isStarted() + ">";
    return str;
  }

  @Override
  public void run() {
    isRunning = true;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    Date date = new Date(System.currentTimeMillis());
    OLogManager.instance().warn(this, "execute : " + this.toString() + " at " + sdf.format(date));
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    this.document.field(PROP_STATUS, SCHEDULER_STATUS.RUNNING);
    this.document.field(PROP_STARTTIME, System.currentTimeMillis());
    this.document.save();
    OScriptManager scriptManager = null;
    Bindings binding = null;
    try {
      if (this.function == null)
        return;
      if (db != null && !(db instanceof ODatabaseRecordTx))
        db = db.getUnderlying();
      scriptManager = Orient.instance().getScriptManager();
      final ScriptEngine scriptEngine = scriptManager.getEngine(this.function.getLanguage());
      binding = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);

      for (OScriptInjection i : scriptManager.getInjections())
        i.bind(binding);
      binding.put("doc", this.document);
      if (db != null)
        binding.put("db", new OScriptDocumentDatabaseWrapper((ODatabaseRecordTx) db));
      binding.put("orient", new OScriptOrientWrapper(db));
      if (iArgs != null) {
        for (Entry<Object, Object> a : iArgs.entrySet()) {
          binding.put(a.getKey().toString(), a.getValue());
        }
        binding.put("params", iArgs.values().toArray());
      } else {
        binding.put("params", new Object[0]);
      }

      if (this.function.getLanguage() == null)
        throw new OConfigurationException("Database function '" + this.function.getName() + "' has no language");
      final String funcStr = scriptManager.getFunctionDefinition(this.function);
      if (funcStr != null) {
        try {
          scriptEngine.eval(funcStr);
        } catch (ScriptException e) {
          scriptManager.getErrorMessage(e, funcStr);
        }
      }
      if (scriptEngine instanceof Invocable) {
        final Invocable invocableEngine = (Invocable) scriptEngine;
        Object[] args = null;
        if (iArgs != null) {
          args = new Object[iArgs.size()];
          int i = 0;
          for (Entry<Object, Object> arg : iArgs.entrySet())
            args[i++] = arg.getValue();
        }
        invocableEngine.invokeFunction(this.function.getName(), args);
      }
    } catch (ScriptException e) {
      throw new OCommandScriptException("Error on execution of the script", this.function.getName(), e.getColumnNumber(), e);
    } catch (NoSuchMethodException e) {
      throw new OCommandScriptException("Error on execution of the script", this.function.getName(), 0, e);
    } catch (OCommandScriptException e) {
      throw e;
    } catch (Exception ex) {
      throw new OCommandScriptException("Unknown Exception", this.function.getName(), 0, ex);
    } finally {
      if (scriptManager != null && binding != null)
        scriptManager.unbind(binding);
      OLogManager.instance().warn(this, "Job : " + this.toString() + " Finished!");
      isRunning = false;
      this.document.field(PROP_STATUS, SCHEDULER_STATUS.WAITING);
      this.document.save();
    }
  }
}
