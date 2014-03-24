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

import java.io.IOException;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponseWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public abstract class OServerCommandAbstractLogic extends OServerCommandAuthenticatedDbAbstract {
  protected abstract String[] init(OHttpRequest iRequest, OHttpResponse iResponse);

  protected abstract void handleResult(OHttpRequest iRequest, OHttpResponse iResponse, Object iResult) throws InterruptedException,
      IOException;

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] parts = init(iRequest, iResponse);
    ODatabaseDocumentTx db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(parts[2]);
      if (f == null)
        throw new IllegalArgumentException("Function '" + parts[2] + "' is not configured");

      if (iRequest.httpMethod.equalsIgnoreCase("GET") && !f.isIdempotent()) {
        iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
            "GET method is not allowed to execute function '" + parts[2]
                + "' because has been declared as non idempotent. Use POST instead.", null, true);
        return false;
      }

      final Object[] args = new String[parts.length - 3];
      for (int i = 3; i < parts.length; ++i)
        args[i - 3] = parts[i];

      // BIND CONTEXT VARIABLES
      final OBasicCommandContext context = new OBasicCommandContext();
      context.setVariable("session", OHttpSessionManager.getInstance().getSession(iRequest.sessionId));
      context.setVariable("request", new OHttpRequestWrapper(iRequest, (String[]) args));
      context.setVariable("response", new OHttpResponseWrapper(iResponse));

      handleResult(iRequest, iResponse, f.executeInContext(context, args));

    } catch (OCommandScriptException e) {
      // EXCEPTION

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
}
