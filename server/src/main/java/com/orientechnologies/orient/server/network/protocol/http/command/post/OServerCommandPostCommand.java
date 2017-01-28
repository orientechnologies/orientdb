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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.util.*;

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

    try {
      db = getProfiledDatabaseInstance(iRequest);
      OResultSet result = executeStatement(language,text, params, db);
      int i = 0;
      List response = new ArrayList();
      while (result.hasNext()) {
        if (limit >= 0 && i >= limit) {
          break;
        }
        response.add(result.next().toElement());
        i++;
      }
      result.close();

      String format = null;
      Map<String, Object> additionalContent = new HashMap<>();

      if (iRequest.getHeader("TE") != null)
        iResponse.setStreaming(true);

      iResponse.writeResult(response, format, accept, additionalContent, mode);

    } finally {
      if (db != null) {
        db.activateOnCurrentThread();
        db.close();
      }
    }

    return false;
  }

  protected OResultSet executeStatement(String language, String text, Object params, ODatabaseDocument db) {
    OResultSet result;
    if (params instanceof Map) {
      result = db.command(text, (Map) params);
    } else if (params instanceof Object[]) {
      result = db.command(text, (Object[]) params);
    } else {
      result = db.command(text, params);
    }
    return result;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
