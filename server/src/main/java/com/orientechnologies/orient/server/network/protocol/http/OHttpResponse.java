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
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OClientConnection;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Maintains information about current HTTP response.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OHttpResponse {
  public static final String JSON_FORMAT =
      "type,indent:-1,rid,version,attribSameRow,class,keepTypes,alwaysFetchEmbeddedDocuments";
  public static final char[] URL_SEPARATOR = {'/'};
  protected static final Charset utf8 = StandardCharsets.UTF_8;

  private final String httpVersion;
  private final OutputStream out;
  private final OContextConfiguration contextConfiguration;
  private String headers;
  private String[] additionalHeaders;
  private String characterSet;
  private String contentType;
  private String serverInfo;

  private final Map<String, String> headersMap = new HashMap<>();

  private String sessionId;
  private String callbackFunction;
  private String contentEncoding;
  private String staticEncoding;
  private boolean sendStarted = false;
  private String content;
  private int code;
  private boolean keepAlive = true;
  private boolean jsonErrorResponse = true;
  private boolean sameSiteCookie = true;
  private OClientConnection connection;
  private boolean streaming = OGlobalConfiguration.NETWORK_HTTP_STREAMING.getValueAsBoolean();

  public OHttpResponse(
      final OutputStream iOutStream,
      final String iHttpVersion,
      final String[] iAdditionalHeaders,
      final String iResponseCharSet,
      final String iServerInfo,
      final String iSessionId,
      final String iCallbackFunction,
      final boolean iKeepAlive,
      OClientConnection connection,
      OContextConfiguration contextConfiguration) {
    setStreaming(
        contextConfiguration.getValueAsBoolean(OGlobalConfiguration.NETWORK_HTTP_STREAMING));
    out = iOutStream;
    httpVersion = iHttpVersion;
    setAdditionalHeaders(iAdditionalHeaders);
    setCharacterSet(iResponseCharSet);
    setServerInfo(iServerInfo);
    setSessionId(iSessionId);
    setCallbackFunction(iCallbackFunction);
    setKeepAlive(iKeepAlive);
    this.setConnection(connection);
    this.contextConfiguration = contextConfiguration;
  }

  public abstract void send(
      int iCode, String iReason, String iContentType, Object iContent, String iHeaders)
      throws IOException;

  public abstract void writeStatus(int iStatus, String iReason) throws IOException;

  public void writeHeaders(final String iContentType) throws IOException {
    writeHeaders(iContentType, true);
  }

  public void writeHeaders(final String iContentType, final boolean iKeepAlive) throws IOException {
    if (getHeaders() != null) {
      writeLine(getHeaders());
    }

    // Set up a date formatter that prints the date in the Http-date format as
    // per RFC 7231, section 7.1.1.1
    SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

    writeLine("Date: " + sdf.format(new Date()));
    writeLine("Content-Type: " + iContentType + "; charset=" + getCharacterSet());
    writeLine("Server: " + getServerInfo());
    writeLine("Connection: " + (iKeepAlive ? "Keep-Alive" : "close"));

    // SET CONTENT ENCDOING
    if (getContentEncoding() != null && getContentEncoding().length() > 0) {
      writeLine("Content-Encoding: " + getContentEncoding());
    }

    // INCLUDE COMMON CUSTOM HEADERS
    if (getAdditionalHeaders() != null) {
      for (String h : getAdditionalHeaders()) {
        writeLine(h);
      }
    }
  }

  public void writeLine(final String iContent) throws IOException {
    writeContent(iContent);
    getOut().write(OHttpUtils.EOL);
  }

  public void writeContent(final String iContent) throws IOException {
    if (iContent != null) {
      getOut().write(iContent.getBytes(utf8));
    }
  }

  public void writeResult(final Object result) throws InterruptedException, IOException {
    writeResult(result, null, null, null);
  }

  public void writeResult(Object iResult, final String iFormat, final String iAccept)
      throws InterruptedException, IOException {
    writeResult(iResult, iFormat, iAccept, null);
  }

  public void writeResult(
      Object iResult,
      final String iFormat,
      final String iAccept,
      final Map<String, Object> iAdditionalProperties)
      throws InterruptedException, IOException {
    writeResult(iResult, iFormat, iAccept, iAdditionalProperties, null);
  }

  public void writeResult(
      Object iResult,
      final String iFormat,
      final String iAccept,
      final Map<String, Object> iAdditionalProperties,
      final String mode)
      throws InterruptedException, IOException {
    if (iResult == null) {
      send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
    } else {
      final Object newResult;

      if (iResult instanceof Map) {
        ODocument doc = new ODocument();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) iResult).entrySet()) {
          String key = keyFromMapObject(entry.getKey());
          doc.field(key, entry.getValue());
        }
        newResult = Collections.singleton(doc).iterator();
      } else if (OMultiValue.isMultiValue(iResult)
          && (OMultiValue.getSize(iResult) > 0
              && !((OMultiValue.getFirstValue(iResult) instanceof OIdentifiable)
                  || ((OMultiValue.getFirstValue(iResult) instanceof OResult))))) {
        newResult = Collections.singleton(new ODocument().field("value", iResult)).iterator();
      } else if (iResult instanceof OIdentifiable) {
        // CONVERT SINGLE VALUE IN A COLLECTION
        newResult = Collections.singleton(iResult).iterator();
      } else if (iResult instanceof Iterable<?>) {
        newResult = ((Iterable<OIdentifiable>) iResult).iterator();
      } else if (OMultiValue.isMultiValue(iResult)) {
        newResult = OMultiValue.getMultiValueIterator(iResult);
      } else {
        newResult = Collections.singleton(new ODocument().field("value", iResult)).iterator();
      }

      if (newResult == null) {
        send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
      } else {
        writeRecords(newResult, null, iFormat, iAccept, iAdditionalProperties, mode);
      }
    }
  }

  public void writeRecords(final Object iRecords) throws IOException {
    writeRecords(iRecords, null, null, null, null);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan) throws IOException {
    writeRecords(iRecords, iFetchPlan, null, null, null);
  }

  public void writeRecords(
      final Object iRecords, final String iFetchPlan, String iFormat, final String accept)
      throws IOException {
    writeRecords(iRecords, iFetchPlan, iFormat, accept, null);
  }

  public void writeRecords(
      final Object iRecords,
      final String iFetchPlan,
      String iFormat,
      final String accept,
      final Map<String, Object> iAdditionalProperties)
      throws IOException {
    writeRecords(iRecords, iFetchPlan, iFormat, accept, iAdditionalProperties, null);
  }

  public void writeRecords(
      final Object iRecords,
      final String iFetchPlan,
      String iFormat,
      final String accept,
      final Map<String, Object> iAdditionalProperties,
      final String mode)
      throws IOException {
    if (iRecords == null) {
      send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
      return;
    }
    final int size = OMultiValue.getSize(iRecords);
    final Iterator<Object> it = OMultiValue.getMultiValueIterator(iRecords);

    if (accept != null && accept.contains("text/csv")) {
      sendStream(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_CSV,
          "data.csv",
          new OCallable<Void, OChunkedResponse>() {

            @Override
            public Void call(final OChunkedResponse iArgument) {
              final LinkedHashSet<String> colNames = new LinkedHashSet<String>();
              final List<OElement> records = new ArrayList<OElement>();

              // BROWSE ALL THE RECORD TO HAVE THE COMPLETE COLUMN
              // NAMES LIST
              while (it.hasNext()) {
                final Object r = it.next();

                if (r instanceof OResult) {

                  OResult result = (OResult) r;
                  records.add(result.toElement());

                  result
                      .toElement()
                      .getSchemaType()
                      .ifPresent(x -> x.properties().forEach(prop -> colNames.add(prop.getName())));
                  for (String fieldName : result.getPropertyNames()) {
                    colNames.add(fieldName);
                  }

                } else if (r != null && r instanceof OIdentifiable) {
                  final ORecord rec = ((OIdentifiable) r).getRecord();
                  if (rec != null) {
                    if (rec instanceof ODocument) {
                      final ODocument doc = (ODocument) rec;
                      records.add(doc);

                      for (String fieldName : doc.fieldNames()) {
                        colNames.add(fieldName);
                      }
                    }
                  }
                }
              }

              final List<String> orderedColumns = new ArrayList<String>(colNames);

              try {
                // WRITE THE HEADER
                for (int col = 0; col < orderedColumns.size(); ++col) {
                  if (col > 0) iArgument.write(',');

                  iArgument.write(orderedColumns.get(col).getBytes());
                }
                iArgument.write(OHttpUtils.EOL);

                // WRITE EACH RECORD
                for (OElement doc : records) {
                  for (int col = 0; col < orderedColumns.size(); ++col) {
                    if (col > 0) {
                      iArgument.write(',');
                    }

                    Object value = doc.getProperty(orderedColumns.get(col));
                    if (value != null) {
                      if (!(value instanceof Number)) value = "\"" + value + "\"";

                      iArgument.write(value.toString().getBytes());
                    }
                  }
                  iArgument.write(OHttpUtils.EOL);
                }

                iArgument.flush();

              } catch (IOException e) {
                OLogManager.instance().error(this, "HTTP response: error on writing records", e);
              }

              return null;
            }
          });
    } else {
      if (iFormat == null) iFormat = JSON_FORMAT;
      else iFormat = JSON_FORMAT + "," + iFormat;

      final String sendFormat = iFormat;
      if (isStreaming()) {
        sendStream(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_JSON,
            null,
            iArgument -> {
              try {
                OutputStreamWriter writer = new OutputStreamWriter(iArgument);
                writeRecordsOnStream(iFetchPlan, sendFormat, iAdditionalProperties, it, writer);
                writer.flush();
              } catch (IOException e) {
                OLogManager.instance()
                    .error(this, "Error during writing of records to the HTTP response", e);
              }
              return null;
            });
      } else {
        final StringWriter buffer = new StringWriter();
        writeRecordsOnStream(iFetchPlan, iFormat, iAdditionalProperties, it, buffer);
        send(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_JSON,
            buffer.toString(),
            null);
      }
    }
  }

  private void writeRecordsOnStream(
      String iFetchPlan,
      String iFormat,
      Map<String, Object> iAdditionalProperties,
      Iterator<Object> it,
      Writer buffer)
      throws IOException {
    final OJSONWriter json = new OJSONWriter(buffer, iFormat);
    json.beginObject();

    final String format = iFetchPlan != null ? iFormat + ",fetchPlan:" + iFetchPlan : iFormat;

    // WRITE RECORDS
    json.beginCollection(-1, true, "result");
    formatMultiValue(it, buffer, format);
    json.endCollection(-1, true);

    if (iAdditionalProperties != null) {
      for (Map.Entry<String, Object> entry : iAdditionalProperties.entrySet()) {

        final Object v = entry.getValue();
        if (OMultiValue.isMultiValue(v)) {
          json.beginCollection(-1, true, entry.getKey());
          formatMultiValue(OMultiValue.getMultiValueIterator(v), buffer, format);
          json.endCollection(-1, true);
        } else json.writeAttribute(entry.getKey(), v);

        if (Thread.currentThread().isInterrupted()) break;
      }
    }

    json.endObject();
  }

  protected abstract void checkConnection() throws IOException;

  public void formatMultiValue(
      final Iterator<?> iIterator, final Writer buffer, final String format) throws IOException {
    if (iIterator != null) {
      int counter = 0;
      String objectJson;

      while (iIterator.hasNext()) {
        final Object entry = iIterator.next();
        if (entry != null) {
          if (counter++ > 0) {
            buffer.append(", ");
          }

          if (entry instanceof OResult) {
            objectJson = ((OResult) entry).toJSON();
            buffer.append(objectJson);
          } else if (entry instanceof OIdentifiable) {
            ORecord rec = ((OIdentifiable) entry).getRecord();
            if (rec != null) {
              try {
                objectJson = rec.toJSON(format);

                buffer.append(objectJson);
              } catch (Exception e) {
                OLogManager.instance()
                    .error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
              }
            }
          } else if (OMultiValue.isMultiValue(entry)) {
            buffer.append("[");
            formatMultiValue(OMultiValue.getMultiValueIterator(entry), buffer, format);
            buffer.append("]");
          } else {
            buffer.append(OJSONWriter.writeValue(entry, format));
          }
        }
        checkConnection();
      }
    }
  }

  public void writeRecord(final ORecord iRecord) throws IOException {
    writeRecord(iRecord, null, null);
  }

  public void writeRecord(final ORecord iRecord, final String iFetchPlan, String iFormat)
      throws IOException {
    if (iFormat == null) {
      iFormat = JSON_FORMAT;
    }

    final String format = iFetchPlan != null ? iFormat + ",fetchPlan:" + iFetchPlan : iFormat;
    if (iRecord != null) {
      send(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_JSON,
          iRecord.toJSON(format),
          OHttpUtils.HEADER_ETAG + iRecord.getVersion());
    }
  }

  public abstract void sendStream(
      int iCode, String iReason, String iContentType, InputStream iContent, long iSize)
      throws IOException;

  public abstract void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      InputStream iContent,
      long iSize,
      String iFileName)
      throws IOException;

  public abstract void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      InputStream iContent,
      long iSize,
      String iFileName,
      Map<String, String> additionalHeaders)
      throws IOException;

  public abstract void sendStream(
      int iCode,
      String iReason,
      String iContentType,
      String iFileName,
      OCallable<Void, OChunkedResponse> iWriter)
      throws IOException;

  // Compress content string
  public byte[] compress(String jsonStr) {
    if (jsonStr == null || jsonStr.length() == 0) {
      return null;
    }
    GZIPOutputStream gout = null;
    ByteArrayOutputStream baos = null;
    try {
      byte[] incoming = jsonStr.getBytes(StandardCharsets.UTF_8);
      baos = new ByteArrayOutputStream();
      gout = new GZIPOutputStream(baos, 16384); // 16KB
      gout.write(incoming);
      gout.finish();
      return baos.toByteArray();
    } catch (Exception ex) {
      OLogManager.instance().error(this, "Error on compressing HTTP response", ex);
    } finally {
      try {
        if (gout != null) {
          gout.close();
        }
        if (baos != null) {
          baos.close();
        }
      } catch (Exception ex) {
      }
    }
    return null;
  }

  /** Stores additional headers to send */
  @Deprecated
  public void setHeader(final String iHeader) {
    setHeaders(iHeader);
  }

  public OutputStream getOutputStream() {
    return getOut();
  }

  public void flush() throws IOException {
    getOut().flush();
    if (!isKeepAlive()) {
      getOut().close();
    }
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  public void setStaticEncoding(String contentEncoding) {
    this.staticEncoding = contentEncoding;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  public void setJsonErrorResponse(boolean jsonErrorResponse) {
    this.jsonErrorResponse = jsonErrorResponse;
  }

  private String keyFromMapObject(Object key) {
    if (key instanceof String) {
      return (String) key;
    }
    return "" + key;
  }

  public void setStreaming(boolean streaming) {
    this.streaming = streaming;
  }

  public String getHttpVersion() {
    return httpVersion;
  }

  public OutputStream getOut() {
    return out;
  }

  public String getHeaders() {
    return headers;
  }

  public void setHeaders(String headers) {
    this.headers = headers;
  }

  public String[] getAdditionalHeaders() {
    return additionalHeaders;
  }

  public void setAdditionalHeaders(String[] additionalHeaders) {
    this.additionalHeaders = additionalHeaders;
  }

  public String getCharacterSet() {
    return characterSet;
  }

  public void setCharacterSet(String characterSet) {
    this.characterSet = characterSet;
  }

  public String getServerInfo() {
    return serverInfo;
  }

  public void setServerInfo(String serverInfo) {
    this.serverInfo = serverInfo;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getCallbackFunction() {
    return callbackFunction;
  }

  public void setCallbackFunction(String callbackFunction) {
    this.callbackFunction = callbackFunction;
  }

  public String getStaticEncoding() {
    return staticEncoding;
  }

  public boolean isSendStarted() {
    return sendStarted;
  }

  public void setSendStarted(boolean sendStarted) {
    this.sendStarted = sendStarted;
  }

  public boolean isKeepAlive() {
    return keepAlive;
  }

  public void setKeepAlive(boolean keepAlive) {
    this.keepAlive = keepAlive;
  }

  public boolean isJsonErrorResponse() {
    return jsonErrorResponse;
  }

  public OClientConnection getConnection() {
    return connection;
  }

  public void setConnection(OClientConnection connection) {
    this.connection = connection;
  }

  public boolean isStreaming() {
    return streaming;
  }

  public void setSameSiteCookie(boolean sameSiteCookie) {
    this.sameSiteCookie = sameSiteCookie;
  }

  public boolean isSameSiteCookie() {
    return sameSiteCookie;
  }

  public OContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  public void addHeader(String name, String value) {
    headersMap.put(name, value);
  }

  public Map<String, String> getHeadersMap() {
    return Collections.unmodifiableMap(headersMap);
  }
}
