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
package com.orientechnologies.orient.server.network.protocol.http.command;

import java.io.IOException;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;

public abstract class OServerCommandAbstract implements OServerCommand {

  protected OServer server;

  /**
   * Default constructor. Disable cache of content at HTTP level
   */
  public OServerCommandAbstract() {
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    // DEFAULT = DON'T CACHE
    iResponse.setHeader("Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
    return true;
  }

  @Override
  public boolean afterExecute(final OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    return true;
  }

  protected String[] checkSyntax(final String iURL, final int iArgumentCount, final String iSyntax) {
    final List<String> parts = OStringSerializerHelper.smartSplit(iURL, OHttpResponse.URL_SEPARATOR, 1, -1, true, true, false);
    if (parts.size() < iArgumentCount)
      throw new OHttpRequestException(iSyntax);

    final String[] array = new String[parts.size()];
    return decodeParts(parts.toArray(array));
  }

  /**
   * urldecode each request part return the same array instance
   * 
   * @param parts
   * @return
   */
  private String[] decodeParts(final String[] parts) {
    try {
      if (parts == null)
        return null;
      for (int i = 0; i < parts.length; i++) {
        String part = parts[i];
        if (part == null)
          continue;

        // NEEDS DECODING
        part = java.net.URLDecoder.decode(part, "UTF-8");
        parts[i] = part;
      }
      return parts;
    } catch (Exception ex) {
      throw new OException(ex);
    }
  }

  public OServer getServer() {
    return server;
  }

  public void configure(final OServer server) {
    this.server = server;
  }
}
