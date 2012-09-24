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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponseWrapper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandFunction extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|function/*", "POST|function/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 3, "Syntax error: function/<database>/<name>[/param]*");

    iRequest.data.commandInfo = "Execute a function";

    ODatabaseDocumentTx db = null;
    Object result = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      // FORCE RELOADING
      db.getMetadata().getFunctionManager().load();

      final OFunction f = db.getMetadata().getFunctionManager().getFunction(parts[2]);
      if (f == null)
        throw new IllegalArgumentException("Function '" + parts[2] + "' is not configured");

      if (iRequest.method.equalsIgnoreCase("GET") && !f.isIdempotent()) {
        iResponse.sendTextContent(iRequest, OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, null,
            OHttpUtils.CONTENT_TEXT_PLAIN, "GET method is not allowed to execute function '" + parts[2]
                + "' because has been declared as non idempotent. Use POST instead.", true, null);
        return false;
      }

      final Object[] args = new Object[parts.length - 3];
      for (int i = 3; i < parts.length; ++i)
        args[i - 3] = parts[i];

      // BIND CONTEXT VARIABLES
      final Map<String, Object> context = new HashMap<String, Object>();
      context.put("request", new OHttpRequestWrapper(iRequest));
      context.put("response", new OHttpResponseWrapper(iResponse));

      result = f.executeInContext(context, args);

      if (result != null) {
        if (result instanceof ODocument && ((ODocument) result).isEmbedded()) {
          iResponse.sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, null,
              OHttpUtils.CONTENT_JSON, ((ODocument) result).toJSON(), true, null);
        } else
          iResponse.sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, null,
              OHttpUtils.CONTENT_TEXT_PLAIN, result, true, null);
      } else
        iResponse.sendTextContent(iRequest, OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", null, OHttpUtils.CONTENT_TEXT_PLAIN, null,
            true, null);

    } catch (OCommandScriptException e) {
      // EXCEPTION

      final StringBuilder msg = new StringBuilder();
      for (Exception currentException = e; currentException != null; currentException = (Exception) currentException.getCause()) {
        if (msg.length() > 0)
          msg.append("\n");
        msg.append(currentException.getMessage());
      }

      iResponse.sendTextContent(iRequest, OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, null,
          OHttpUtils.CONTENT_TEXT_PLAIN, msg.toString(), true, null);

    } finally {
      if (db != null)
        OSharedDocumentDatabase.release(db);
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
