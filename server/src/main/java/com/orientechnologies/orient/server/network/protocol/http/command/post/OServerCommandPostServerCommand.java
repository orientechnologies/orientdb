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

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.util.*;

public class OServerCommandPostServerCommand extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = {"POST|servercommand"};

  public OServerCommandPostServerCommand() {
    super("server.command");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: servercommand");

    // TRY TO GET THE COMMAND FROM THE URL, THEN FROM THE CONTENT
    final String language = urlParts.length > 2 ? urlParts[2].trim() : "sql";
    String text = urlParts.length > 3 ? urlParts[3].trim() : iRequest.getContent();
    int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4].trim()) : -1;
    String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
    final String accept = iRequest.getHeader("accept");

    Object params = null;
    String mode = "resultset";

    boolean returnExecutionPlan = true;

    long begin = System.currentTimeMillis();
    if (iRequest.getContent() != null && !iRequest.getContent().isEmpty()) {
      // CONTENT REPLACES TEXT
      if (iRequest.getContent().startsWith("{")) {
        // JSON PAYLOAD
        final ODocument doc = new ODocument().fromJSON(iRequest.getContent());
        text = doc.field("command");
        params = doc.field("parameters");

        if ("false".equalsIgnoreCase("" + doc.field("returnExecutionPlan"))) {
          returnExecutionPlan = false;
        }

        if (params instanceof Collection) {
          final Object[] paramArray = new Object[((Collection) params).size()];
          ((Collection) params).toArray(paramArray);
          params = paramArray;
        }
      } else {
        text = iRequest.getContent();
      }
    }

    if ("false".equalsIgnoreCase("" + iRequest.getHeader("return-execution-plan"))) {
      returnExecutionPlan = false;
    }

    if (text == null) throw new IllegalArgumentException("text cannot be null");

    iRequest.getData().commandInfo = "Command";
    iRequest.getData().commandDetail = text;

    OResultSet result = executeStatement(language, text, params);

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
    if (returnExecutionPlan) {
      result
          .getExecutionPlan()
          .ifPresent(x -> additionalContent.put("executionPlan", x.toResult().toElement()));
    }

    result.close();
    long elapsedMs = System.currentTimeMillis() - begin;

    String format = null;
    if (fetchPlan != null) {
      format = "fetchPlan:" + fetchPlan;
    }

    if (iRequest.getHeader("TE") != null) iResponse.setStreaming(true);

    additionalContent.put("elapsedMs", elapsedMs);
    iResponse.writeResult(response, format, accept, additionalContent, mode);

    return false;
  }

  protected OResultSet executeStatement(String language, String text, Object params) {
    OResultSet result;

    OrientDB odb = this.server.getContext();
    if (params instanceof Map) {
      result = odb.execute(text, (Map) params);
    } else if (params instanceof Object[]) {
      result = odb.execute(text, (Object[]) params);
    } else {
      result = odb.execute(text, params);
    }
    return result;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
