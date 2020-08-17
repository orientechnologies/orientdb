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

import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OServerCommandGetListDatabases extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = {"GET|listDatabases"};

  public OServerCommandGetListDatabases() {
    super("server.listDatabases");
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws IOException {
    return authenticate(iRequest, iResponse, false);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    checkSyntax(iRequest.getUrl(), 1, "Syntax error: server");

    iRequest.getData().commandInfo = "Server status";

    final ODocument result = new ODocument();

    // We copy the returned set so that we can modify it, and we use a LinkedHashSet to preserve the
    // ordering.
    java.util.Set<String> storageNames =
        new java.util.LinkedHashSet(server.getAvailableStorageNames().keySet());

    // This just adds the system database if the guest user has the specified permission
    // (server.listDatabases.system).
    if (server.getSecurity() != null
        && server
            .getSecurity()
            .isAuthorized(OServerConfiguration.GUEST_USER, "server.listDatabases.system")) {
      storageNames.add(OSystemDatabase.SYSTEM_DB_NAME);
    }

    // ORDER DATABASE NAMES (CASE INSENSITIVE)
    final List<String> orderedStorages = new ArrayList<String>(storageNames);
    Collections.sort(
        orderedStorages,
        new Comparator<String>() {
          @Override
          public int compare(final String o1, final String o2) {
            return o1.toLowerCase().compareTo(o2.toLowerCase());
          }
        });

    result.field("databases", orderedStorages);
    iResponse.writeRecord(result);

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
