/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http.command.options;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandOptions extends OServerCommandAbstract {
  private static final String[] NAMES = { "OPTIONS|*" };

  public OServerCommandOptions() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    iRequest.data.commandInfo = "HTTP Options";
    iRequest.data.commandDetail = iRequest.url;

    iResponse
        .send(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_TEXT_PLAIN,
            null,
            "Access-Control-Allow-Methods: POST, GET, PUT, DELETE, OPTIONS\r\nAccess-Control-Max-Age: 1728000\r\nAccess-Control-Allow-Headers: if-modified-since, content-type, authorization, x-requested-with");
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
