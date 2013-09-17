/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import java.io.StringWriter;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandGetProfiler extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|profiler/*", "POST|profiler/*" };

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
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Recording started", null);

      } else if (command.equalsIgnoreCase("stop")) {
        Orient.instance().getProfiler().stopRecording();
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Recording stopped", null);

      } else if (command.equalsIgnoreCase("configure")) {
        Orient.instance().getProfiler().configure(parts[2]);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, "Profiler configured with: " + parts[2],
            null);

      } else if (command.equalsIgnoreCase("status")) {
        final String status = Orient.instance().getProfiler().isRecording() ? "on" : "off";
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, status, null);

      } else if (command.equalsIgnoreCase("metadata")) {
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, Orient.instance().getProfiler().metadataToJSON(),
            null);

      } else {
        final String par1 = parts.length > 2 ? parts[2] : null;
        final String par2 = parts.length > 3 ? parts[3] : null;

        StringWriter jsonBuffer = new StringWriter();
        OJSONWriter json = new OJSONWriter(jsonBuffer);
        json.append(Orient.instance().getProfiler().toJSON(command, par1, par2));

        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, jsonBuffer.toString(), null);
      }

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
