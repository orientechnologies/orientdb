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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OServerCommandPostCommand extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|command/*", "POST|command/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 3,
        "Syntax error: command/<database>/<language>/<command-text>[/limit][/<fetchPlan>]");

    // TRY TO GET THE COMMAND FROM THE URL, THEN FROM THE CONTENT
    final String language = urlParts.length > 2 ? urlParts[2].trim() : "sql";
    String text = urlParts.length > 3 ? urlParts[3].trim() : iRequest.content;
    final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4].trim()) : -1;
    String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
    final String accept = iRequest.getHeader("accept");

    Object params = null;
    String mode = "resultset";

    if (iRequest.content != null && !iRequest.content.isEmpty()) {
      // CONTENT REPLACES TEXT
      if (iRequest.content.startsWith("{")) {
        // JSON PAYLOAD
        final ODocument doc = new ODocument().fromJSON(iRequest.content);
        text = doc.field("command");
        params = doc.field("parameters");
        if (doc.containsField("mode"))
          mode = doc.field("mode");

        if (params instanceof Collection) {
          final Object[] paramArray = new Object[((Collection) params).size()];
          ((Collection) params).toArray(paramArray);
          params = paramArray;
        }
      } else {
        text = iRequest.content;
      }
    }

    if (text == null)
      throw new IllegalArgumentException("text cannot be null");

    iRequest.data.commandInfo = "Command";
    iRequest.data.commandDetail = text;

    ODatabaseDocument db = null;

    Object response;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OCommandRequestText cmd = (OCommandRequestText) OCommandManager.instance().getRequester(language);

      cmd.setText(text);
      cmd.setLimit(limit);
      cmd.setFetchPlan(fetchPlan);

      final OCommandExecutor executor = OCommandManager.instance().getExecutor(cmd);
      executor.setContext(cmd.getContext());
      executor.setProgressListener(cmd.getProgressListener());
      executor.parse(cmd);

      if (!executor.isIdempotent() && iRequest.httpMethod.equals("GET"))
        throw new OCommandExecutionException("Cannot execute non idempotent command using HTTP GET");

      // REQUEST CAN'T MODIFY THE RESULT, SO IT'S CACHEABLE
      cmd.setCacheableResult(true);

      if (params == null) {
        response = db.command(cmd).execute();
      } else {
        response = db.command(cmd).execute(params);
      }

      fetchPlan = executor.getFetchPlan();

      String format = null;
      if (iRequest.parameters.get("format") != null)
        format = iRequest.parameters.get("format");

      if (fetchPlan != null)
        if (format != null)
          format += ",fetchPlan:" + fetchPlan;
        else
          format = "fetchPlan:" + fetchPlan;

      Map<String, Object> additionalContent = null;

      final List<String> tips = (List<String>) executor.getContext().getVariable("tips");
      if (tips != null) {
        additionalContent = new HashMap<String, Object>(1);
        additionalContent.put("warnings", tips);
      }

      iResponse.writeResult(response, format, accept, additionalContent, mode);

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
