/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.ORetryQueryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stored function. It contains language and code to execute as a function. The execute() takes
 * parameters. The function is state-less, so can be used by different threads.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OFunction extends ODocumentWrapper {
  public static final String CLASS_NAME = "OFunction";
  private OCallable<Object, Map<Object, Object>> callback;

  /** Creates a new function. */
  public OFunction() {
    super(CLASS_NAME);
    setLanguage("SQL");
  }

  /**
   * Creates a new function wrapping the saved document.
   *
   * @param iDocument Document to assign
   */
  public OFunction(final ODocument iDocument) {
    super(iDocument);
  }

  /**
   * Loads a function.
   *
   * @param iRid RID of the function to load
   */
  public OFunction(final ORecordId iRid) {
    super(iRid);
  }

  public String getName() {
    return document.field("name");
  }

  public OFunction setName(final String iName) {
    document.field("name", iName);
    return this;
  }

  public String getCode() {
    return document.field("code");
  }

  public OFunction setCode(final String iCode) {
    document.field("code", iCode);
    return this;
  }

  public String getLanguage() {
    return document.field("language");
  }

  public OFunction setLanguage(final String iLanguage) {
    document.field("language", iLanguage);
    return this;
  }

  public List<String> getParameters() {
    return document.field("parameters");
  }

  public OFunction setParameters(final List<String> iParameters) {
    document.field("parameters", iParameters);
    return this;
  }

  public boolean isIdempotent() {
    final Boolean idempotent = document.field("idempotent");
    return idempotent != null && idempotent;
  }

  public OFunction setIdempotent(final boolean iIdempotent) {
    document.field("idempotent", iIdempotent);
    return this;
  }

  public OCallable<Object, Map<Object, Object>> getCallback() {
    return callback;
  }

  public OFunction setCallback(final OCallable<Object, Map<Object, Object>> callback) {
    this.callback = callback;
    return this;
  }

  @Deprecated
  public Object execute(final Object... iArgs) {
    return executeInContext(null, iArgs);
  }

  @Deprecated
  public Object executeInContext(OCommandContext iContext, final Object... iArgs) {
    if (iContext == null) {
      iContext = new OBasicCommandContext();
    }
    final List<String> params = getParameters();

    // CONVERT PARAMETERS IN A MAP
    Map<Object, Object> args = null;

    if (iArgs.length > 0) {
      args = new LinkedHashMap<Object, Object>();
      for (int i = 0; i < iArgs.length; ++i) {
        // final Object argValue =
        // ORecordSerializerStringAbstract.getTypeValue(iArgs[i].toString());
        final Object argValue = iArgs[i];

        if (params != null && i < params.size()) args.put(params.get(i), argValue);
        else args.put("param" + i, argValue);
      }
    }

    if (callback != null) {
      // EXECUTE CALLBACK
      return callback.call(args);
    }

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) iContext.getDatabase();
    if (database == null) {
      database = ODatabaseRecordThreadLocal.instance().get();
    }

    OScriptExecutor executor =
        database
            .getSharedContext()
            .getOrientDB()
            .getScriptManager()
            .getCommandManager()
            .getScriptExecutor(getLanguage());

    return executor.executeFunction(iContext, getName(), args);
  }

  @Deprecated
  public Object executeInContext(OCommandContext iContext, final Map<String, Object> iArgs) {
    if (iContext == null) {
      iContext = new OBasicCommandContext();
    }
    // CONVERT PARAMETERS IN A MAP
    final Map<Object, Object> args = new LinkedHashMap<Object, Object>();

    if (iArgs.size() > 0) {
      // PRESERVE THE ORDER FOR PARAMETERS (ARE USED AS POSITIONAL)
      final List<String> params = getParameters();
      for (String p : params) {
        args.put(p, iArgs.get(p));
      }
    }

    if (callback != null) {
      // EXECUTE CALLBACK
      return callback.call(args);
    }

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) iContext.getDatabase();
    if (database == null) {
      database = ODatabaseRecordThreadLocal.instance().get();
    }

    OScriptExecutor executor =
        database
            .getSharedContext()
            .getOrientDB()
            .getScriptManager()
            .getCommandManager()
            .getScriptExecutor(getLanguage());

    return executor.executeFunction(iContext, getName(), args);
  }

  @Deprecated
  public Object execute(final Map<Object, Object> iArgs) {
    final long start = Orient.instance().getProfiler().startChrono();
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    Object result;
    while (true) {
      try {
        if (callback != null) return callback.call(iArgs);

        OScriptExecutor executor =
            database
                .getSharedContext()
                .getOrientDB()
                .getScriptManager()
                .getCommandManager()
                .getScriptExecutor(getLanguage());

        result = executor.execute(database, getCode(), iArgs);

        break;

      } catch (ONeedRetryException | ORetryQueryException ignore) {
      }
    }

    if (Orient.instance().getProfiler().isRecording())
      Orient.instance()
          .getProfiler()
          .stopChrono(
              "db." + ODatabaseRecordThreadLocal.instance().get().getName() + ".function.execute",
              "Time to execute a function",
              start,
              "db.*.function.execute");

    return result;
  }

  public ORID getId() {
    return document.getIdentity();
  }

  @Override
  public String toString() {
    return getName();
  }
}
