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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

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

    if (iRequest.content != null)
      // CONTENT REPLACES TEXT
      text = iRequest.content;

    if (text == null)
      throw new IllegalArgumentException("text cannot be null");

    iRequest.data.commandInfo = "Command";
    iRequest.data.commandDetail = text;

    ODatabaseDocumentTx db = null;

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

      response = executor.execute(null);
      
      fetchPlan = executor.getFetchPlan();

      final String format = fetchPlan != null ? "fetchPlan:" + fetchPlan : null;

      iResponse.writeResult(response, format);

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
