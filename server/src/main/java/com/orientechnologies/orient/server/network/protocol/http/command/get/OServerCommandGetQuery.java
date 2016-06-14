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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OServerCommandGetQuery extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|query/*" };

  @Override
  @SuppressWarnings("unchecked")
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(
        iRequest.url,
        4,
        "Syntax error: query/<database>/sql/<query-text>[/<limit>][/<fetchPlan>].<br>Limit is optional and is set to 20 by default. Set to 0 to have no limits.");

    final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;
    String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
    final String text = urlParts[3];
    final String accept = iRequest.getHeader("accept");

    iRequest.data.commandInfo = "Query";
    iRequest.data.commandDetail = text;

    ODatabaseDocument db = null;

    final List<OIdentifiable> response;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OQueryAbstract command = new OSQLSynchQuery<ODocument>(text, limit).setFetchPlan(fetchPlan);

      // REQUEST CAN'T MODIFY THE RESULT, SO IT'S CACHEABLE
      command.setCacheableResult(true);

      response = (List<OIdentifiable>) db.query(command);
      fetchPlan = command.getFetchPlan();

      Map<String, Object> additionalContent = null;

      final List<String> tips = (List<String>) command.getContext().getVariable("tips");
      if (tips != null) {
        additionalContent = new HashMap<String, Object>(1);
        additionalContent.put("warnings", tips);
      }

      iResponse.writeRecords(response, fetchPlan, null, accept, additionalContent);

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
