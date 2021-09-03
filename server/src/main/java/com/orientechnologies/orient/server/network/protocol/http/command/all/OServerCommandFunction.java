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
package com.orientechnologies.orient.server.network.protocol.http.command.all;

import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import java.io.IOException;

public class OServerCommandFunction extends OServerCommandAbstractLogic {
  private static final String[] NAMES = {"GET|function/*", "POST|function/*"};

  public OServerCommandFunction() {}

  public OServerCommandFunction(final OServerCommandConfiguration iConfig) {}

  @Override
  public String[] init(final OHttpRequest iRequest, final OHttpResponse iResponse) {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: function/<database>/<name>[/param]*");
    iRequest.getData().commandInfo = "Execute a function";
    return parts;
  }

  @Override
  protected void handleResult(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final Object iResult)
      throws InterruptedException, IOException {
    iResponse.writeResult(iResult);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
