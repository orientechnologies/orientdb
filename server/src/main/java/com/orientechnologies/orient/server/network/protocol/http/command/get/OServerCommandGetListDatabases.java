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

import java.io.IOException;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetListDatabases extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|listDatabases" };

  public OServerCommandGetListDatabases() {
    super("server.listDatabases");
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws IOException {
    return authenticate(iRequest, iResponse, false);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 1, "Syntax error: server");

    iRequest.data.commandInfo = "Server status";

    final ODocument result = new ODocument();
      
    // We copy the returned set so that we can modify it, and we use a LinkedHashSet to preserve the ordering.
    java.util.Set<String> storageNames = new java.util.LinkedHashSet(server.getAvailableStorageNames().keySet());
      
    // This just adds the system database if the guest user has the specified permission (server.listDatabases.system).
    if (server.getSecurity() != null && server.getSecurity().isAuthorized(OServerConfiguration.GUEST_USER, "server.listDatabases.system")) {
      if (server.getSecurity().systemDbExists()) {
        storageNames.add(server.getSecurity().getSystemDbName());
      }
    }
      
    result.field("databases", storageNames);
    iResponse.writeRecord(result);   

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
