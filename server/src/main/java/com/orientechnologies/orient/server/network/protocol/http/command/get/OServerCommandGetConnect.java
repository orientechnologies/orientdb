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

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;

public class OServerCommandGetConnect extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = {"GET|connect/*", "HEAD|connect/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: connect/<database>[/<user>/<password>]");

    urlParts[1] = urlParts[1].replace(DBNAME_DIR_SEPARATOR, '/');

    iRequest.getData().commandInfo = "Connect";
    iRequest.getData().commandDetail = urlParts[1];

    iResponse.send(
        OHttpUtils.STATUS_OK_NOCONTENT_CODE,
        OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION,
        OHttpUtils.CONTENT_TEXT_PLAIN,
        null,
        null);
    return false;
  }

  @Override
  public boolean beforeExecute(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    final String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: connect/<database>[/<user>/<password>]");

    if (urlParts == null || urlParts.length < 3) return super.beforeExecute(iRequest, iResponse);

    // USER+PASSWD AS PARAMETERS
    setNoCache(iResponse);
    return true;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
