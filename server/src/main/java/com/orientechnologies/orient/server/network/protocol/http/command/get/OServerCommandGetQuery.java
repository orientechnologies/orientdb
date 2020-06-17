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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCommand;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OServerCommandGetQuery extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = {"GET|query/*"};

  @Override
  @SuppressWarnings("unchecked")
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts =
        checkSyntax(
            iRequest.getUrl(),
            4,
            "Syntax error: query/<database>/sql/<query-text>[/<limit>][/<fetchPlan>].<br>Limit is optional and is set to 20 by default. Set to 0 to have no limits.");

    int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;
    String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
    final String text = urlParts[3];
    final String accept = iRequest.getHeader("accept");

    iRequest.getData().commandInfo = "Query";
    iRequest.getData().commandDetail = text;

    ODatabaseDocument db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      OStatement stm = OServerCommandPostCommand.parseStatement("SQL", text, db);
      OResultSet result = db.query(text, new Object[] {});
      limit = OServerCommandPostCommand.getLimitFromStatement(stm, limit);
      String localFetchPlan = OServerCommandPostCommand.getFetchPlanFromStatement(stm);
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

      result
          .getExecutionPlan()
          .ifPresent(x -> additionalContent.put("executionPlan", x.toResult().toElement()));

      result.close();

      String format = null;
      if (fetchPlan != null) {
        format = "fetchPlan:" + fetchPlan;
      }

      iResponse.writeRecords(response, fetchPlan, null, accept, additionalContent);

    } finally {
      if (db != null) db.close();
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
