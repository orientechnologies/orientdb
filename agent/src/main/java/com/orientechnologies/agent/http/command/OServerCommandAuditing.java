/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class OServerCommandAuditing extends OServerCommandDistributedScope {
  private static final String[] NAMES = {"GET|auditing/*", "POST|auditing/*"};

  public OServerCommandAuditing(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_SECURITY.toString(), server);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: auditing/<db>/<action>");

    iRequest.getData().commandInfo = "Auditing information";

    String db = parts[1];
    String action = parts[2];

    if (isLocalNode(iRequest)) {
      if ("GET".equals(iRequest.getHttpMethod())) {
        if (action.equalsIgnoreCase("config")) {
          doGet(iRequest, iResponse, parts[1]);
        }
      } else if ("POST".equals(iRequest.getHttpMethod())) {

        if (action.equalsIgnoreCase("config")) {
          doPost(iRequest, iResponse, db);
        } else if (action.equalsIgnoreCase("query")) {
          doGetData(iRequest, iResponse, db);
        }
      }
    } else {
      proxyRequest(iRequest, null);
    }
    return false;
  }

  private void doGetData(OHttpRequest iRequest, OHttpResponse iResponse, String db)
      throws IOException, InterruptedException {

    ODocument params = new ODocument().fromJSON(iRequest.getContent());

    String query = buildQuery(params);

    Collection<OResult> documents =
        server
            .getSystemDatabase()
            .executeWithDB(
                (session) -> {
                  try (OResultSet results = session.query(query, params.toMap())) {
                    return results.stream().collect(Collectors.toList());
                  }
                });

    iResponse.writeResult(documents);
  }

  private String buildQuery(ODocument params) {
    String query =
        String.format(
            "select user as username,* from OAuditingLog :where order by date desc limit :limit");

    List<String> whereConditions = new ArrayList<String>();
    Integer limit = params.field("limit");

    if (isNotNullNotEmpty(params, "operation")) {
      whereConditions.add("operation = :operation");
    }
    if (isNotNullNotEmpty(params, "user")) {
      whereConditions.add("user = :user");
    }
    if (isNotNullNotEmpty(params, "record")) {
      whereConditions.add("record = :record");
    }
    if (isNotNullNotEmpty(params, "db")) {
      whereConditions.add("database = :db");
    }
    if (isNull(params, "db")) {
      whereConditions.add("database is null");
    }
    if (isNotNullNotEmpty(params, "note")) {
      String note = params.field("note");
      note = "%" + note + "%";
      params.field("note", note);
      whereConditions.add("note LIKE :note");
    }
    if (params.containsField("fromDate")) {
      whereConditions.add("date > :fromDate");
    }
    if (params.containsField("toDate")) {
      whereConditions.add("date < :toDate");
    }
    query = query.replace(":where", buildWhere(whereConditions));

    query = query.replace(":limit", "" + limit);
    return query;
  }

  private boolean isNull(ODocument params, String db) {
    return params.containsField(db) && params.field(db) == null;
  }

  private boolean isNotNullNotEmpty(ODocument params, String field) {

    boolean valid = params.field(field) != null;
    if (valid) {
      Object val = params.field(field);

      if (val instanceof String) {
        valid = !((String) val).isEmpty();
      }
    }
    return valid;
  }

  private String buildWhere(List<String> whereConditions) {
    String where = "";
    int i = 0;
    for (String whereCondition : whereConditions) {
      if (i != 0) {
        where += " and ";
      } else {
        where += "where ";
      }
      where += whereCondition;
      i++;
    }
    return where;
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String db)
      throws InterruptedException, IOException {

    ODocument config = new ODocument().fromJSON(iRequest.getContent(), "noMap");
    iRequest.setDatabaseName(db);

    ODatabaseDocument dbDoc = null;

    try {
      dbDoc = getProfiledDatabaseInstance(iRequest);

      if (server.getSecurity().getAuditing() != null)
        server.getSecurity().getAuditing().changeConfig(dbDoc.getUser(), db, config);

      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          "OK",
          OHttpUtils.CONTENT_JSON,
          config.toJSON("prettyPrint"),
          null);
    } finally {
      if (dbDoc != null) dbDoc.close();
    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String db) throws IOException {
    iRequest.setDatabaseName(db);

    ODocument config = null;
    if (server.getSecurity().getAuditing() != null) {
      config = server.getSecurity().getAuditing().getConfig(db);
    } else {
      config = new ODocument();
    }

    iResponse.send(
        OHttpUtils.STATUS_OK_CODE,
        "OK",
        OHttpUtils.CONTENT_JSON,
        config.toJSON("prettyPrint"),
        null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
