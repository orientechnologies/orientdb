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
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

/**
 * Maintains information about current HTTP response.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpResponse {
  public static final String JSON_FORMAT   = "type,indent:2,rid,version,attribSameRow,class";
  public static final char[] URL_SEPARATOR = { '/' };

  private final OutputStream out;
  private String             headers;

  public OHttpResponse(final OutputStream iOutStream) {
    out = iOutStream;
  }

  public void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
      final String iContentType, final Object iContent) throws IOException {
    sendTextContent(iRequest, iCode, iReason, iHeaders, iContentType, iContent, true);
  }

  public void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
      final String iContentType, final Object iContent, final boolean iKeepAlive) throws IOException {
    sendTextContent(iRequest, iCode, iReason, iHeaders, iContentType, iContent, iKeepAlive, null);
  }

  public void sendTextContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iHeaders,
      final String iContentType, final Object iContent, final boolean iKeepAlive, final String[] iAdditionalHeaders)
      throws IOException {
    final String content;
    final String contentType;
    if ((iRequest.parameters != null) && iRequest.parameters.containsKey(OHttpUtils.CALLBACK_PARAMETER_NAME)) {
      final String callbackFunction = iRequest.parameters.get(OHttpUtils.CALLBACK_PARAMETER_NAME);
      content = callbackFunction + "(" + iContent + ")";
      contentType = "text/javascript";
    } else {
      content = iContent != null ? iContent.toString() : null;
      contentType = iContentType;
    }

    final boolean empty = content == null || content.length() == 0;

    sendStatus(iRequest, empty && iCode == 200 ? 204 : iCode, iReason);
    sendResponseHeaders(iRequest, contentType, iKeepAlive);

    if (iAdditionalHeaders != null)
      for (String h : iAdditionalHeaders)
        writeLine(iRequest, h);

    if (iHeaders != null)
      writeLine(iRequest, iHeaders);

    final String sessId = iRequest.sessionId != null ? iRequest.sessionId : "-";

    writeLine(iRequest, "Set-Cookie: " + OHttpUtils.OSESSIONID + "=" + sessId + "; Path=/; HttpOnly");

    final byte[] binaryContent = empty ? null : OBinaryProtocol.string2bytes(content);

    writeLine(iRequest, OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

    writeLine(iRequest, null);

    if (binaryContent != null)
      out.write(binaryContent);
    out.flush();
  }

  public void sendStatus(final OHttpRequest iRequest, final int iStatus, final String iReason) throws IOException {
    writeLine(iRequest, iRequest.httpVersion + " " + iStatus + " " + iReason);
  }

  public void sendResponseHeaders(final OHttpRequest iRequest, final String iContentType) throws IOException {
    sendResponseHeaders(iRequest, iContentType, true);
  }

  public void sendResponseHeaders(final OHttpRequest iRequest, final String iContentType, final boolean iKeepAlive)
      throws IOException {
    if (headers != null)
      writeLine(iRequest, headers);

    writeLine(iRequest, "Date: " + new Date());
    writeLine(iRequest, "Content-Type: " + iContentType + "; charset=" + iRequest.executor.getResponseCharSet());
    writeLine(iRequest, "Server: " + iRequest.data.serverInfo);
    writeLine(iRequest, "Connection: " + (iKeepAlive ? "Keep-Alive" : "close"));

    // INCLUDE COMMON CUSTOM HEADERS
    if (iRequest.executor.getAdditionalResponseHeaders() != null)
      for (String h : iRequest.executor.getAdditionalResponseHeaders())
        writeLine(iRequest, h);
  }

  public void writeLine(final OHttpRequest iRequest, final String iContent) throws IOException {
    writeContent(iRequest, iContent);
    out.write(OHttpUtils.EOL);
  }

  public void writeContent(final OHttpRequest iRequest, final String iContent) throws IOException {
    if (iContent != null)
      out.write(OBinaryProtocol.string2bytes(iContent));
  }

  public void sendRecordsContent(final OHttpRequest iRequest, final List<OIdentifiable> iRecords) throws IOException {
    sendRecordsContent(iRequest, iRecords, null);
  }

  public void sendRecordsContent(final OHttpRequest iRequest, final List<OIdentifiable> iRecords, final String iFetchPlan)
      throws IOException {
    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
    json.beginObject();

    final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;

    // WRITE RECORDS
    json.beginCollection(1, true, "result");
    formatCollection(iRecords, buffer, format);
    json.endCollection(1, true);

    json.endObject();

    sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
  }

  public void formatCollection(final List<OIdentifiable> iRecords, final StringWriter buffer, final String format) {
    if (iRecords != null) {
      int counter = 0;
      String objectJson;
      for (OIdentifiable rec : iRecords) {
        if (rec != null)
          try {
            objectJson = rec.getRecord().toJSON(format);

            if (counter++ > 0)
              buffer.append(", ");

            buffer.append(objectJson);
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
          }
      }
    }
  }

  public void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord) throws IOException {
    sendRecordContent(iRequest, iRecord, null);
  }

  public void sendRecordContent(final OHttpRequest iRequest, final ORecord<?> iRecord, String iFetchPlan) throws IOException {
    final String format = iFetchPlan != null ? JSON_FORMAT + ",fetchPlan:" + iFetchPlan : JSON_FORMAT;
    if (iRecord != null)
      sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, iRecord.toJSON(format));
  }

  public void sendBinaryContent(final OHttpRequest iRequest, final int iCode, final String iReason, final String iContentType,
      final InputStream iContent, final long iSize) throws IOException {
    sendStatus(iRequest, iCode, iReason);
    sendResponseHeaders(iRequest, iContentType);
    writeLine(iRequest, OHttpUtils.HEADER_CONTENT_LENGTH + (iSize));
    writeLine(iRequest, null);

    if (iContent != null) {
      int b;
      while ((b = iContent.read()) > -1)
        out.write(b);
    }

    out.flush();
  }

  /**
   * Stores additional headers to send
   * 
   * @param iHeader
   */
  public void setHeader(final String iHeader) {
    headers = iHeader;
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public void flush() throws IOException {
    out.flush();
  }
}
