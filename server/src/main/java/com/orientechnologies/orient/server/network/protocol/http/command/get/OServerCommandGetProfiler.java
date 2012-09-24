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
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetProfiler extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|profiler/*" };

  public OServerCommandGetProfiler() {
    super("server.profiler");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: profiler/<command>/[<config>]|[<from>/<to>]");

    iRequest.data.commandInfo = "Profiler information";

    try {

      final String command = parts[1];
      if (command.equalsIgnoreCase("start")) {
        Orient.instance().getProfiler().startRecording();
        iResponse.sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, "Recording started");

      } else if (command.equalsIgnoreCase("stop")) {
        Orient.instance().getProfiler().stopRecording();
        iResponse.sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, "Recording stopped");

      } else if (command.equalsIgnoreCase("configure")) {
        Orient.instance().getProfiler().configure(parts[2]);
        iResponse.sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, "Profiler configured with: " + parts[2]);

      } else if (command.equalsIgnoreCase("status")) {
        final String status = Orient.instance().getProfiler().isRecording() ? "on" : "off";
        iResponse.sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, status);

      } else {
        final String from = parts.length > 2 ? parts[2] : null;
        final String to = parts.length > 3 ? parts[3] : null;

        StringWriter jsonBuffer = new StringWriter();
        OJSONWriter json = new OJSONWriter(jsonBuffer);
        json.append(Orient.instance().getProfiler().toJSON(command, from, to));

        iResponse.sendTextContent(OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, jsonBuffer.toString());
      }

    } catch (Exception e) {
      iResponse.sendTextContent(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, null, OHttpUtils.CONTENT_TEXT_PLAIN,
          e);
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
