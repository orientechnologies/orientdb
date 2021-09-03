/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.net.URL;
import java.net.URLConnection;

public class OServerCommandPostInstallDatabase extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = {"POST|installDatabase"};

  public OServerCommandPostInstallDatabase() {
    super("database.create");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.getUrl(), 1, "Syntax error: installDatabase");
    iRequest.getData().commandInfo = "Import database";
    try {
      final String url = iRequest.getContent();
      final String name = getDbName(url);
      if (name != null) {
        if (server.getContext().exists(name)) {
          throw new ODatabaseException("Database named '" + name + "' already exists: ");
        } else {
          final URL uri = new URL(url);
          final URLConnection conn = uri.openConnection();
          conn.setRequestProperty("User-Agent", "OrientDB-Studio");
          conn.setDefaultUseCaches(false);
          server
              .getDatabases()
              .networkRestore(
                  name,
                  conn.getInputStream(),
                  () -> {
                    return null;
                  });
          try (ODatabaseSession session = server.getDatabases().openNoAuthorization(name)) {}

          iResponse.send(
              OHttpUtils.STATUS_OK_CODE,
              OHttpUtils.STATUS_OK_DESCRIPTION,
              OHttpUtils.CONTENT_TEXT_PLAIN,
              null,
              null);
        }
      } else {
        throw new IllegalArgumentException("Could not find database name");
      }
    } catch (Exception e) {
      throw e;
    }
    return false;
  }

  protected String getDbName(final String url) {
    String name = null;
    if (url != null) {
      int idx = url.lastIndexOf("/");
      if (idx != -1) {
        name = url.substring(idx + 1).replace(".zip", "");
      }
    }
    return name;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
