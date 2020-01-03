package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.server.OClientConnection;

import java.io.OutputStream;

public class OHttpResponseImpl extends OHttpResponse {


  public OHttpResponseImpl(OutputStream iOutStream, String iHttpVersion, String[] iAdditionalHeaders, String iResponseCharSet,
      String iServerInfo, String iSessionId, String iCallbackFunction, boolean iKeepAlive, OClientConnection connection) {
    super(iOutStream, iHttpVersion, iAdditionalHeaders, iResponseCharSet, iServerInfo, iSessionId, iCallbackFunction, iKeepAlive,
        connection);
  }
}
