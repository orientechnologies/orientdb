/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.OClientConnection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Maintains information about current HTTP response.
 *
 * @author Luca Garulli
 */
public class OHttpResponse {
  public static final String   JSON_FORMAT       = "type,indent:-1,rid,version,attribSameRow,class,keepTypes,alwaysFetchEmbeddedDocuments";
  public static final char[]   URL_SEPARATOR     = { '/' };
  private static final Charset utf8              = Charset.forName("utf8");
  public final String          httpVersion;
  private final OutputStream   out;
  public String                headers;
  public String[]              additionalHeaders;
  public String                characterSet;
  public String                contentType;
  public String                serverInfo;

  public String                sessionId;
  public String                callbackFunction;
  public String                contentEncoding;
  public boolean               sendStarted       = false;
  public String                content;
  public int                   code;
  public boolean               keepAlive         = true;
  public boolean               jsonErrorResponse = true;
  public OClientConnection     connection;

  public OHttpResponse(final OutputStream iOutStream, final String iHttpVersion, final String[] iAdditionalHeaders,
      final String iResponseCharSet, final String iServerInfo, final String iSessionId, final String iCallbackFunction,
      final boolean iKeepAlive, OClientConnection connection) {
    out = iOutStream;
    httpVersion = iHttpVersion;
    additionalHeaders = iAdditionalHeaders;
    characterSet = iResponseCharSet;
    serverInfo = iServerInfo;
    sessionId = iSessionId;
    callbackFunction = iCallbackFunction;
    keepAlive = iKeepAlive;
    this.connection = connection;
  }

  public void send(final int iCode, final String iReason, final String iContentType, final Object iContent, final String iHeaders)
      throws IOException {
    if (sendStarted) {
      // AVOID TO SEND RESPONSE TWICE
      return;
    }
    sendStarted = true;

    if (callbackFunction != null) {
      content = callbackFunction + "(" + iContent + ")";
      contentType = "text/javascript";
    } else {
      if (content == null || content.length() == 0) {
        content = iContent != null ? iContent.toString() : null;
      }
      if (contentType == null || contentType.length() == 0) {
        contentType = iContentType;
      }
    }

    final boolean empty = content == null || content.length() == 0;

    if (this.code > 0) {
      writeStatus(this.code, iReason);
    } else {
      writeStatus(empty && iCode == 200 ? 204 : iCode, iReason);
    }
    writeHeaders(contentType, keepAlive);

    if (iHeaders != null) {
      writeLine(iHeaders);
    }

    if (sessionId != null)
      writeLine("Set-Cookie: " + OHttpUtils.OSESSIONID + "=" + sessionId + "; Path=/; HttpOnly");

    byte[] binaryContent = null;
    if (!empty) {
      if (contentEncoding != null && contentEncoding.equals(OHttpUtils.CONTENT_ACCEPT_GZIP_ENCODED)) {
        binaryContent = compress(content);
      } else {
        binaryContent = content.getBytes(utf8);
      }
    }

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

    writeLine(null);

    if (binaryContent != null) {
      out.write(binaryContent);
    }

    flush();
  }

  public void writeStatus(final int iStatus, final String iReason) throws IOException {
    writeLine(httpVersion + " " + iStatus + " " + iReason);
  }

  public void writeHeaders(final String iContentType) throws IOException {
    writeHeaders(iContentType, true);
  }

  public void writeHeaders(final String iContentType, final boolean iKeepAlive) throws IOException {
    if (headers != null) {
      writeLine(headers);
    }

    writeLine("Date: " + new Date());
    writeLine("Content-Type: " + iContentType + "; charset=" + characterSet);
    writeLine("Server: " + serverInfo);
    writeLine("Connection: " + (iKeepAlive ? "Keep-Alive" : "close"));

    // SET CONTENT ENCDOING
    if (contentEncoding != null && contentEncoding.length() > 0) {
      writeLine("Content-Encoding: " + contentEncoding);
    }

    // INCLUDE COMMON CUSTOM HEADERS
    if (additionalHeaders != null) {
      for (String h : additionalHeaders) {
        writeLine(h);
      }
    }
  }

  public void writeLine(final String iContent) throws IOException {
    writeContent(iContent);
    out.write(OHttpUtils.EOL);
  }

  public void writeContent(final String iContent) throws IOException {
    if (iContent != null) {
      out.write(iContent.getBytes(utf8));
    }
  }

  public void writeResult(Object iResult) throws InterruptedException, IOException {
    writeResult(iResult, null, null, null);
  }

  public void writeResult(Object iResult, final String iFormat, final String iAccept) throws InterruptedException, IOException {
    writeResult(iResult, iFormat, iAccept, null);
  }

  public void writeResult(Object iResult, final String iFormat, final String iAccept,
      final Map<String, Object> iAdditionalProperties) throws InterruptedException, IOException {
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
          && (OMultiValue.getSize(iResult) > 0 && !(OMultiValue.getFirstValue(iResult) instanceof OIdentifiable))) {
        newResult = Collections.singleton(new ODocument().field("value", iResult)).iterator();
      } else if (iResult instanceof OIdentifiable) {
        // CONVERT SIGLE VALUE IN A COLLECTION
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
        writeRecords(newResult, null, iFormat, iAccept, iAdditionalProperties);
      }
    }
  }

  public void writeRecords(final Object iRecords) throws IOException {
    writeRecords(iRecords, null, null, null, null);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan) throws IOException {
    writeRecords(iRecords, iFetchPlan, null, null, null);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan, String iFormat, final String accept) throws IOException {
    writeRecords(iRecords, iFetchPlan, iFormat, accept, null);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan, String iFormat, final String accept,
      final Map<String, Object> iAdditionalProperties) throws IOException {
    if (iRecords == null)
      return;

    final Iterator<Object> it = OMultiValue.getMultiValueIterator(iRecords);

    if (accept != null && accept.contains("text/csv")) {
      sendStream(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, "data.csv", new OCallable<Void, OChunkedResponse>() {

        @Override
        public Void call(final OChunkedResponse iArgument) {
          final LinkedHashSet<String> colNames = new LinkedHashSet<String>();
          final List<ODocument> records = new ArrayList<ODocument>();

          // BROWSE ALL THE RECORD TO HAVE THE COMPLETE COLUMN
          // NAMES LIST
          while (it.hasNext()) {
            final Object r = it.next();
            if (r != null && r instanceof OIdentifiable) {
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
              if (col > 0)
                iArgument.write(',');

              iArgument.write(orderedColumns.get(col).getBytes());
            }
            iArgument.write(OHttpUtils.EOL);

            // WRITE EACH RECORD
            for (ODocument doc : records) {
              for (int col = 0; col < orderedColumns.size(); ++col) {
                if (col > 0) {
                  iArgument.write(',');
                }

                Object value = doc.field(orderedColumns.get(col));
                if (value != null) {
                  if (!(value instanceof Number))
                    value = "\"" + value + "\"";

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
      if (iFormat == null)
        iFormat = JSON_FORMAT;
      else
        iFormat = JSON_FORMAT + "," + iFormat;

      final StringWriter buffer = new StringWriter();
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
          } else
            json.writeAttribute(entry.getKey(), v);
        }
      }

      json.endObject();
      send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    }
  }

  private void checkConnection() throws IOException {
    final Socket socket;
    if (connection.protocol == null || connection.protocol.getChannel() == null)
      socket = null;
    else
      socket = connection.protocol.getChannel().socket;
    if (socket == null || socket.isClosed() || socket.isInputShutdown()) {
      OLogManager.instance().debug(this, "[OHttpResponse] found and removed pending closed channel %d (%s)", connection, socket);
      throw new IOException("Connection is closed");
    }

  }

  public void formatMultiValue(final Iterator<?> iIterator, final StringWriter buffer, final String format) throws IOException {
    if (iIterator != null) {
      int counter = 0;
      String objectJson;

      while (iIterator.hasNext()) {
        final Object entry = iIterator.next();
        if (entry != null) {
          if (counter++ > 0) {
            buffer.append(", ");
          }

          if (entry instanceof OIdentifiable) {
            ORecord rec = ((OIdentifiable) entry).getRecord();
            if (rec != null) {
              try {
                objectJson = rec.toJSON(format);

                buffer.append(objectJson);
              } catch (Exception e) {
                OLogManager.instance().error(this, "Error transforming record " + rec.getIdentity() + " to JSON", e);
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

  public void writeRecord(final ORecord iRecord, final String iFetchPlan, String iFormat) throws IOException {
    if (iFormat == null) {
      iFormat = JSON_FORMAT;
    }

    final String format = iFetchPlan != null ? iFormat + ",fetchPlan:" + iFetchPlan : iFormat;
    if (iRecord != null) {
      send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, iRecord.toJSON(format),
          OHttpUtils.HEADER_ETAG + iRecord.getVersion());
    }
  }

  public void sendStream(final int iCode, final String iReason, final String iContentType, InputStream iContent, long iSize)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, null);
  }

  public void sendStream(final int iCode, final String iReason, final String iContentType, InputStream iContent, long iSize,
      final String iFileName) throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    if (iSize < 0) {
      // SIZE UNKNOWN: USE A MEMORY BUFFER
      final ByteArrayOutputStream o = new ByteArrayOutputStream();
      if (iContent != null) {
        int b;
        while ((b = iContent.read()) > -1) {
          o.write(b);
        }
      }

      byte[] content = o.toByteArray();

      iContent = new ByteArrayInputStream(content);
      iSize = content.length;
    }

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (iSize));
    writeLine(null);

    if (iContent != null) {
      int b;
      while ((b = iContent.read()) > -1) {
        out.write(b);
      }
    }

    flush();
  }

  public void sendStream(final int iCode, final String iReason, final String iContentType, final String iFileName,
      final OCallable<Void, OChunkedResponse> iWriter) throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");
    writeLine("Transfer-Encoding: chunked");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    writeLine(null);

    final OChunkedResponse chunkedOutput = new OChunkedResponse(this);
    iWriter.call(chunkedOutput);
    chunkedOutput.close();

    flush();
  }

  // Compress content string
  public byte[] compress(String jsonStr) {
    if (jsonStr == null || jsonStr.length() == 0) {
      return null;
    }
    GZIPOutputStream gout = null;
    ByteArrayOutputStream baos = null;
    try {
      byte[] incoming = jsonStr.getBytes("UTF-8");
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
    if (!keepAlive) {
      out.close();
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

}
