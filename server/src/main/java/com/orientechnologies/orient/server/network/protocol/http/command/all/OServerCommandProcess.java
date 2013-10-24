/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.network.protocol.http.command.all;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.processor.OProcessorManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponseWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandProcess extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES     = { "GET|process/*", "POST|process/*" };
  private String                path      = "";
  private String                extension = "";

  public OServerCommandProcess(final OServerCommandConfiguration iConfig) {
    for (OServerEntryConfiguration cfg : iConfig.parameters) {
      if ("path".equalsIgnoreCase(cfg.name))
        path = cfg.value;
      else if ("extension".equalsIgnoreCase(cfg.name))
        extension = cfg.value;
    }

    OProcessorManager.getInstance().register("composable", new OComposableProcessor().setPath(path).setExtension(extension));
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: process/<database>/<template-name>[/param]*");
    iRequest.data.commandInfo = "Processes a transformation returning a JSON";

    final String name = parts[2];

    ODatabaseDocumentTx db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final Object[] args = new String[parts.length - 3];
      for (int i = 3; i < parts.length; ++i)
        args[i - 3] = parts[i];

      // BIND CONTEXT VARIABLES
      final OCommandContext context = new OBasicCommandContext();
      int argIdx = 0;
      for (Object arg : args)
        context.setVariable("arg" + (argIdx++), arg);

      context.setVariable("session", OHttpSessionManager.getInstance().getSession(iRequest.sessionId));
      context.setVariable("request", new OHttpRequestWrapper(iRequest, (String[]) args));
      context.setVariable("response", new OHttpResponseWrapper(iResponse));

      final String debugMode = iRequest.getParameter("debug");
      if (debugMode != null && Boolean.parseBoolean(debugMode))
        context.setVariable("debugMode", Boolean.TRUE);

      final OComposableProcessor processEngine = (OComposableProcessor) OProcessorManager.getInstance().get("composable");
      Object result = processEngine.processFromFile(name, context, iRequest.httpMethod.equals("GET"));

      if (result instanceof ODocument)
        result = ((ODocument) result).field("result");

      iResponse.writeResult(result, "");

    } catch (OProcessException e) {
      // EXCEPTION
      OLogManager.instance().error(this, "[process] exception during process of %s", e, name);

      final StringBuilder msg = new StringBuilder();
      for (Exception currentException = e; currentException != null; currentException = (Exception) currentException.getCause()) {
        if (msg.length() > 0)
          msg.append("\n");
        msg.append(currentException.getMessage());
      }

      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          msg.toString(), null, true);

    } finally {
      if (db != null)
        db.close();
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
