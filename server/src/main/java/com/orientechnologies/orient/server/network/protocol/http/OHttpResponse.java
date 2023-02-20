package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OClientConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;

public interface OHttpResponse {

  String JSON_FORMAT =
      "type,indent:-1,rid,version,attribSameRow,class,keepTypes,alwaysFetchEmbeddedDocuments";

  void send(int iCode, String iReason, String iContentType, Object iContent, String iHeaders)
      throws IOException;

  void writeStatus(int iStatus, String iReason) throws IOException;

  void writeHeaders(String iContentType) throws IOException;

  void writeHeaders(String iContentType, boolean iKeepAlive) throws IOException;

  void writeLine(String iContent) throws IOException;

  void writeContent(String iContent) throws IOException;

  void writeResult(Object result) throws InterruptedException, IOException;

  void writeResult(Object iResult, String iFormat, String iAccept)
      throws InterruptedException, IOException;

  void writeResult(
      Object iResult, String iFormat, String iAccept, Map<String, Object> iAdditionalProperties)
      throws InterruptedException, IOException;

  void writeResult(
      Object iResult,
      String iFormat,
      String iAccept,
      Map<String, Object> iAdditionalProperties,
      String mode)
      throws InterruptedException, IOException;

  void writeRecords(Object iRecords) throws IOException;

  void writeRecords(Object iRecords, String iFetchPlan) throws IOException;

  void writeRecords(Object iRecords, String iFetchPlan, String iFormat, String accept)
      throws IOException;

  void writeRecords(
      Object iRecords,
      String iFetchPlan,
      String iFormat,
      String accept,
      Map<String, Object> iAdditionalProperties)
      throws IOException;

  void writeRecords(
      Object iRecords,
      String iFetchPlan,
      String iFormat,
      String accept,
      Map<String, Object> iAdditionalProperties,
      String mode)
      throws IOException;

  void formatMultiValue(Iterator<?> iIterator, Writer buffer, String format) throws IOException;

  void writeRecord(ORecord iRecord) throws IOException;

  void writeRecord(ORecord iRecord, String iFetchPlan, String iFormat) throws IOException;

  void sendStream(int iCode, String iReason, String iContentType, InputStream iContent, long iSize)
      throws IOException;

  void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      InputStream iContent,
      long iSize,
      String iFileName)
      throws IOException;

  void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      InputStream iContent,
      long iSize,
      String iFileName,
      Map<String, String> additionalHeaders)
      throws IOException;

  void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      String iFileName,
      OCallable<Void, OChunkedResponse> iWriter)
      throws IOException;

  // Compress content string
  byte[] compress(String jsonStr);

  /** Stores additional headers to send */
  void setHeader(String iHeader);

  OutputStream getOutputStream();

  void flush() throws IOException;

  String getContentType();

  void setContentType(String contentType);

  String getContentEncoding();

  void setContentEncoding(String contentEncoding);

  void setStaticEncoding(String contentEncoding);

  void setSessionId(String sessionId);

  String getContent();

  void setContent(String content);

  int getCode();

  void setCode(int code);

  void setJsonErrorResponse(boolean jsonErrorResponse);

  void setStreaming(boolean streaming);

  String getHttpVersion();

  OutputStream getOut();

  String getHeaders();

  void setHeaders(String headers);

  String[] getAdditionalHeaders();

  void setAdditionalHeaders(String[] additionalHeaders);

  String getCharacterSet();

  void setCharacterSet(String characterSet);

  String getServerInfo();

  void setServerInfo(String serverInfo);

  String getSessionId();

  String getCallbackFunction();

  void setCallbackFunction(String callbackFunction);

  String getStaticEncoding();

  boolean isSendStarted();

  void setSendStarted(boolean sendStarted);

  boolean isKeepAlive();

  void setKeepAlive(boolean keepAlive);

  boolean isJsonErrorResponse();

  OClientConnection getConnection();

  void setConnection(OClientConnection connection);

  boolean isStreaming();

  void setSameSiteCookie(boolean sameSiteCookie);

  boolean isSameSiteCookie();

  OContextConfiguration getContextConfiguration();

  void addHeader(String name, String value);

  Map<String, String> getHeadersMap();

  void checkConnection() throws IOException;
}
