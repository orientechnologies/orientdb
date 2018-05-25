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

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.*;
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
    int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4].trim()) : -1;
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
      OStatement stm = parseStatement(language, text, db);
      OResultSet result = executeStatement(language, text, params, db);
      limit = getLimitFromStatement(stm, limit);
      String localFetchPlan = getFetchPlanFromStatement(stm);
      if (localFetchPlan != null) {
        fetchPlan = localFetchPlan;
      }
      int i = 0;
      List response = new ArrayList();
      while (result.hasNext()) {
        if (limit >= 0 && i >= limit) {
          break;
        }
        response.add(result.next());
        i++;
      }

      Map<String, Object> additionalContent = new HashMap<>();

      result.getExecutionPlan().ifPresent(x -> additionalContent.put("executionPlan", x.toResult().toElement()));

      result.close();

      String format = null;
      if (fetchPlan != null) {
        format = "fetchPlan:" + fetchPlan;
      }

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

  public static String getFetchPlanFromStatement(OStatement statement) {
    if (statement instanceof OSelectStatement) {
      OFetchPlan fp = ((OSelectStatement) statement).getFetchPlan();
      if (fp != null) {
        return fp.toString().substring("FETCHPLAN ".length());
      }
    } else if (statement instanceof OMatchStatement) {
      return ((OMatchStatement) statement).getFetchPlan();
    }
    return null;
  }

  public static OStatement parseStatement(String language, String text, ODatabaseDocument db) {
    try {
      if (language != null && language.equalsIgnoreCase("sql")) {
        return OSQLEngine.parse(text, (ODatabaseDocumentInternal) db);
      }
    } catch (Exception e) {
    }
    return null;
  }

  public static int getLimitFromStatement(OStatement statement, int previousLimit) {
    try {
      OLimit limit = null;
      if (statement instanceof OSelectStatement) {
        limit = ((OSelectStatement) statement).getLimit();
      } else if (statement instanceof OMatchStatement) {
        limit = ((OMatchStatement) statement).getLimit();
      } else if (statement instanceof OTraverseStatement) {
        limit = ((OTraverseStatement) statement).getLimit();
      }
      if (limit != null) {
        return limit.getValue(new OBasicCommandContext());
      }

    } catch (Exception e) {
    }
    return previousLimit;
  }

  protected OResultSet executeStatement(String language, String text, Object params, ODatabaseDocument db) {
    OResultSet result;
    if ("sql".equalsIgnoreCase(language)) {
      if (params instanceof Map) {
        result = db.command(text, (Map) params);
      } else if (params instanceof Object[]) {
        result = db.command(text, (Object[]) params);
      } else {
        result = db.command(text, params);
      }
    } else {
      if (params instanceof Map) {
        result = db.execute(language, text, (Map) params);
      } else if (params instanceof Object[]) {
        result = db.execute(language, text, (Object[]) params);
      } else {
        result = db.execute(language, text, params);
      }
    }
    return result;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
