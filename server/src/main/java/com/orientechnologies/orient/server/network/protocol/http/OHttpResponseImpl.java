package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.server.OClientConnection;
import java.io.*;
import java.net.Socket;
import java.util.Map;

public class OHttpResponseImpl extends OHttpResponse {

  public OHttpResponseImpl(
      OutputStream iOutStream,
      String iHttpVersion,
      String[] iAdditionalHeaders,
      String iResponseCharSet,
      String iServerInfo,
      String iSessionId,
      String iCallbackFunction,
      boolean iKeepAlive,
      OClientConnection connection,
      OContextConfiguration contextConfiguration) {
    super(
        iOutStream,
        iHttpVersion,
        iAdditionalHeaders,
        iResponseCharSet,
        iServerInfo,
        iSessionId,
        iCallbackFunction,
        iKeepAlive,
        connection,
        contextConfiguration);
  }

  @Override
  public void send(
      final int iCode,
      final String iReason,
      final String iContentType,
      final Object iContent,
      final String iHeaders)
      throws IOException {
    if (isSendStarted()) {
      // AVOID TO SEND RESPONSE TWICE
      return;
    }
    setSendStarted(true);

    if (getCallbackFunction() != null) {
      setContent(getCallbackFunction() + "(" + iContent + ")");
      setContentType("text/javascript");
    } else {
      if (getContent() == null || getContent().length() == 0) {
        setContent(iContent != null ? iContent.toString() : null);
      }
      if (getContentType() == null || getContentType().length() == 0) {
        setContentType(iContentType);
      }
    }

    final boolean empty = getContent() == null || getContent().length() == 0;

    if (this.getCode() > 0) {
      writeStatus(this.getCode(), iReason);
    } else {
      writeStatus(empty && iCode == 200 ? 204 : iCode, iReason);
    }
    writeHeaders(getContentType(), isKeepAlive());

    if (iHeaders != null) {
      writeLine(iHeaders);
    }

    if (getSessionId() != null) {
      String sameSite = (isSameSiteCookie() ? "SameSite=Strict;" : "");
      writeLine(
          "Set-Cookie: "
              + OHttpUtils.OSESSIONID
              + "="
              + getSessionId()
              + "; Path=/; HttpOnly;"
              + sameSite);
    }

    byte[] binaryContent = null;
    if (!empty) {
      if (getContentEncoding() != null
          && getContentEncoding().equals(OHttpUtils.CONTENT_ACCEPT_GZIP_ENCODED)) {
        binaryContent = compress(getContent());
      } else {
        binaryContent = getContent().getBytes(utf8);
      }
    }

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

    writeLine(null);

    if (binaryContent != null) {
      getOut().write(binaryContent);
    }

    flush();
  }

  @Override
  public void writeStatus(final int iStatus, final String iReason) throws IOException {
    writeLine(getHttpVersion() + " " + iStatus + " " + iReason);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, null, null);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize,
      final String iFileName)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, iFileName, null);
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      InputStream iContent,
      long iSize,
      final String iFileName,
      Map<String, String> additionalHeaders)
      throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    if (additionalHeaders != null) {
      for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
        writeLine(String.format("%s: %s", entry.getKey(), entry.getValue()));
      }
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
        getOut().write(b);
      }
    }

    flush();
  }

  @Override
  public void sendStream(
      final int iCode,
      final String iReason,
      final String iContentType,
      final String iFileName,
      final OCallable<Void, OChunkedResponse> iWriter)
      throws IOException {
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

  @Override
  protected void checkConnection() throws IOException {
    final Socket socket;
    if (getConnection().getProtocol() == null || getConnection().getProtocol().getChannel() == null)
      socket = null;
    else socket = getConnection().getProtocol().getChannel().socket;
    if (socket == null || socket.isClosed() || socket.isInputShutdown()) {
      OLogManager.instance()
          .debug(
              this,
              "[OHttpResponse] found and removed pending closed channel %d (%s)",
              getConnection(),
              socket);
      throw new IOException("Connection is closed");
    }
  }
}
