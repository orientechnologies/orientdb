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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.StringWriter;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetProfiler extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|profiler/*" };

  public OServerCommandGetProfiler() {
    super("server.profiler");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: profiler/<query>/[<from>/<to>]");

    iRequest.data.commandInfo = "Profiler information";

    try {
      StringWriter jsonBuffer = new StringWriter();
      OJSONWriter json = new OJSONWriter(jsonBuffer);

      final String to = parts.length > 2 ? parts[2] : null;
      final String from = parts.length > 3 ? parts[3] : null;
      json.append(Orient.instance().getProfiler().toJSON(parts[1], from, to));

      sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, jsonBuffer.toString());
    } catch (Exception e) {
      sendTextContent(iRequest, OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, null,
          OHttpUtils.CONTENT_TEXT_PLAIN, e);
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
