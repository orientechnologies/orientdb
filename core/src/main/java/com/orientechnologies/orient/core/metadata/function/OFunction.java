/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.command.script.OCommandScript;
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
 * Stored function. It contains language and code to execute as a function. The execute() takes parameters. The function is
 * state-less, so can be used by different threads.
 * 
 * @author Luca Garulli
 * 
 */
public class OFunction extends ODocumentWrapper {
  public static final String CLASS_NAME = "OFunction";

  /**
   * Creates a new function.
   */
  public OFunction() {
    super(CLASS_NAME);
    setLanguage("SQL");
  }

  /**
   * Creates a new function wrapping the saved document.
   * 
   * @param iDocument
   *          Document to assign
   */
  public OFunction(final ODocument iDocument) {
    super(iDocument);
  }

  /**
   * Loads a function.
   * 
   * @param iRid
   *          RID of the function to load
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

  public Object execute(final Object... iArgs) {
    return executeInContext(null, iArgs);
  }

  public Object executeInContext(final OCommandContext iContext, final Object... iArgs) {
    final OCommandExecutorFunction command = new OCommandExecutorFunction();
    command.parse(new OCommandFunction(getName()));

    final List<String> params = getParameters();

    // CONVERT PARAMETERS IN A MAP
    Map<Object, Object> args = null;

    if (iArgs.length > 0) {
      args = new LinkedHashMap<Object, Object>();
      for (int i = 0; i < iArgs.length; ++i) {
        // final Object argValue = ORecordSerializerStringAbstract.getTypeValue(iArgs[i].toString());
        final Object argValue = iArgs[i];

        if (params != null && i < params.size())
          args.put(params.get(i), argValue);
        else
          args.put("param" + i, argValue);
      }
    }

    return command.executeInContext(iContext, args);
  }

  public Object executeInContext(final OCommandContext iContext, final Map<String, Object> iArgs) {
    final OCommandExecutorFunction command = new OCommandExecutorFunction();
    command.parse(new OCommandFunction(getName()));

    // CONVERT PARAMETERS IN A MAP
    final Map<Object, Object> args = new LinkedHashMap<Object, Object>();

    if (iArgs.size() > 0) {
      // PRESERVE THE ORDER FOR PARAMETERS (ARE USED AS POSITIONAL)
      final List<String> params = getParameters();
      for (String p : params) {
        args.put(p, iArgs.get(p));
      }
    }

    return command.executeInContext(iContext, args);
  }

  public Object execute(final Map<Object, Object> iArgs) {
    final long start = Orient.instance().getProfiler().startChrono();

    Object result;
    while (true) {
      try {
        final OCommandExecutorScript command = new OCommandExecutorScript();
        command.parse(new OCommandScript(getLanguage(), getCode()));

        result = command.execute(iArgs);
        break;
      } catch (ORetryQueryException e) {
        continue;
      }
    }

    if (Orient.instance().getProfiler().isRecording())
      Orient
          .instance()
          .getProfiler()
          .stopChrono("db." + ODatabaseRecordThreadLocal.INSTANCE.get().getName() + ".function.execute",
              "Time to execute a function", start, "db.*.function.execute");

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
