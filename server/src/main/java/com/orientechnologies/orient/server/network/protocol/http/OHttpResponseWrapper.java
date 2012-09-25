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
import java.io.OutputStream;
import java.util.List;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Wrapper to use the HTTP response in functions and scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHttpResponseWrapper {
  private final OHttpResponse response;

  /**
   * @param iResponse
   */
  public OHttpResponseWrapper(final OHttpResponse iResponse) {
    response = iResponse;
  }

  public String getHeader() {
    return response.headers;
  }

  public OHttpResponseWrapper setHeader(final String iHeader) {
    response.setHeader(iHeader);
    return this;
  }

  public String getCharacterSet() {
    return response.characterSet;
  }

  public OHttpResponseWrapper setCharacterSet(final String iCharacterSet) {
    response.characterSet = iCharacterSet;
    return this;
  }

  public String getHttpVersion() {
    return response.httpVersion;
  }

  public String[] getAdditionalResponseHeaders() {
    return response.additionalHeaders;
  }

  public OutputStream getOutputStream() {
    return response.getOutputStream();
  }

  public void sendStatus(int iStatus, String iReason) throws IOException {
    response.sendStatus(iStatus, iReason);
  }

  public void sendResponseHeaders(String iContentType) throws IOException {
    response.sendResponseHeaders(iContentType);
  }

  public void sendResponseHeaders(String iContentType, boolean iKeepAlive) throws IOException {
    response.sendResponseHeaders(iContentType, iKeepAlive);
  }

  public void writeLine(String iContent) throws IOException {
    response.writeLine(iContent);
  }

  public void writeContent(String iContent) throws IOException {
    response.writeContent(iContent);
  }

  public void sendRecordsContent(List<OIdentifiable> iRecords) throws IOException {
    response.sendRecordsContent(iRecords);
  }

  public void sendRecordsContent(List<OIdentifiable> iRecords, String iFetchPlan) throws IOException {
    response.sendRecordsContent(iRecords, iFetchPlan);
  }

  public void sendRecordContent(ORecord<?> iRecord) throws IOException {
    response.sendRecordContent(iRecord);
  }

  public void sendRecordContent(ORecord<?> iRecord, String iFetchPlan) throws IOException {
    response.sendRecordContent(iRecord, iFetchPlan);
  }

  public void flush() throws IOException {
    response.flush();
  }

  public void sendTextContent(int iCode, String iReason, String iContentType, Object iContent) throws IOException {
    response.sendTextContent(iCode, iReason, iContentType, iContent, null);
  }

  public void sendTextContent(int iCode, String iReason, String iContentType, Object iContent, String iHeaders) throws IOException {
    response.sendTextContent(iCode, iReason, iContentType, iContent, iHeaders);
  }

  public void sendTextContent(int iCode, String iReason, String iContentType, Object iContent, String iHeaders, boolean iKeepAlive)
      throws IOException {
    response.sendTextContent(iCode, iReason, iContentType, iContent, iHeaders, iKeepAlive);
  }
}
