/*
 *
 *  * Copyright 2014 Orient Technologies.
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

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.File;
import java.net.URL;
import java.net.URLConnection;

public class OServerCommandPostInstallDatabase extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "POST|installDatabase" };

  public OServerCommandPostInstallDatabase() {
    super("database.create");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 1, "Syntax error: installDatabase");
    iRequest.data.commandInfo = "Import database";
    try {
      final String url = iRequest.content;
      final String name = getDbName(url);
      if (name != null) {

        final String folder = server.getDatabaseDirectory() + File.separator + name;
        final File f = new File(folder);
        if (f.exists() && OLocalPaginatedStorage.exists(folder)) {
          throw new ODatabaseException("Database named '" + name + "' already exists: ");
        } else {
          f.mkdirs();
          final URL uri = new URL(url);
          final URLConnection conn = uri.openConnection();
          conn.setRequestProperty("User-Agent", "OrientDB-Studio");
          conn.setDefaultUseCaches(false);
          OZIPCompressionUtil.uncompressDirectory(conn.getInputStream(), folder, new OCommandOutputListener() {
            @Override
            public void onMessage(String iText) {
            }
          });
          iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
        }
      } else {
        throw new IllegalArgumentException("Could not find database name");
      }
    } catch (Exception e) {
      throw e;
    } finally {
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
