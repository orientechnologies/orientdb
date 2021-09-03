/*
 *
 *  * Copyright 2017 Orient Technologies.
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

package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.io.IOException;

/** This command is called in order to know if the running instance of orientdb is EE or not. */
public class OServerCommandIsEnterprise extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = {"GET|isEE"};

  public OServerCommandIsEnterprise() {
    super("server.listDatabases");
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: isEE");

    if ("GET".equalsIgnoreCase(iRequest.getHttpMethod())) {
      doGet(iRequest, iResponse, parts);
    }

    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts)
      throws IOException {

    if ("isEE".equalsIgnoreCase(parts[0])) {

      ODocument context = Orient.instance().getProfiler().getContext();

      if (context.getProperty("enterprise") == null) {
        context.setProperty("enterprise", false);
      }
      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          "OK",
          OHttpUtils.CONTENT_JSON,
          context.toJSON("prettyPrint"),
          null);

    } else {
      throw new IllegalArgumentException("");
    }
  }
}
