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
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

public abstract class OServerCommandAbstract implements OServerCommand {

  protected OServer server;

  /** Default constructor. Disable cache of content at HTTP level */
  public OServerCommandAbstract() {}

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    setNoCache(iResponse);
    return true;
  }

  @Override
  public boolean afterExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    return true;
  }

  protected String[] checkSyntax(
      final String iURL, final int iArgumentCount, final String iSyntax) {
    final List<String> parts =
        OStringSerializerHelper.smartSplit(
            iURL, OHttpResponse.URL_SEPARATOR, 1, -1, true, true, false, false);
    try {
      for (int i = 0; i < parts.size(); i++) {
        parts.set(i, URLDecoder.decode(parts.get(i), "UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(
          new OHttpRequestException("URL is encoded using format different from UTF-8"), e);
    }
    if (parts.size() < iArgumentCount) throw new OHttpRequestException(iSyntax);

    return parts.toArray(new String[parts.size()]);
  }

  public OServer getServer() {
    return server;
  }

  public void configure(final OServer server) {
    this.server = server;
  }

  protected void setNoCache(final OHttpResponse iResponse) {
    // DEFAULT = DON'T CACHE
    iResponse.setHeader(
        "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
    iResponse.addHeader("Cache-Control", "no-cache, no-store, max-age=0");
    iResponse.addHeader("Pragma", "no-cache");
  }

  protected boolean isJsonResponse(OHttpResponse response) {
    return response.isJsonErrorResponse();
  }

  protected void sendJsonError(
      OHttpResponse iResponse,
      final int iCode,
      final String iReason,
      final String iContentType,
      final Object iContent,
      final String iHeaders)
      throws IOException {
    ODocument response = new ODocument();
    ODocument error = new ODocument();
    error.field("code", iCode);
    error.field("reason", iReason);
    error.field("content", iContent);
    List<ODocument> errors = new ArrayList<ODocument>();
    errors.add(error);
    response.field("errors", errors);
    iResponse.send(
        iCode, iReason, OHttpUtils.CONTENT_JSON, response.toJSON("prettyPrint"), iHeaders);
  }
}
