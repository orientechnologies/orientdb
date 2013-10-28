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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartBaseInputStream;

/**
 * Maintains information about current HTTP request.
 * 
 * @author Luca Garulli
 * 
 */
public class OHttpRequest {
  public final OContextConfiguration        configuration;
  public final InputStream                  in;
  public String                             authorization;
  public String                             sessionId;
  public String                             url;
  public Map<String, String>                parameters;
  public String                             httpMethod;
  public String                             httpVersion;
  public String                             contentType;
  public String                             contentEncoding;
  public String                             content;
  public OHttpMultipartBaseInputStream      multipartStream;
  public String                             boundary;
  public String                             databaseName;
  public boolean                            isMultipart;
  public String                             ifMatch;
  public String                             authentication;
  protected Map<String, String>             headers;

  public final ONetworkProtocolData         data;
  public final ONetworkProtocolHttpAbstract executor;

  public OHttpRequest(final ONetworkProtocolHttpAbstract iExecutor, final InputStream iInStream, final ONetworkProtocolData iData,
      final OContextConfiguration iConfiguration) {
    executor = iExecutor;
    in = iInStream;
    data = iData;
    configuration = iConfiguration;
  }

  public String getUser() {
    return authorization != null ? authorization.substring(0, authorization.indexOf(":")) : null;
  }

  public InputStream getInputStream() {
    return in;
  }

  public String getParameter(final String iName) {
    return parameters != null ? parameters.get(iName) : null;
  }

  public void addHeader(final String h) {
    if (headers == null)
      headers = new HashMap<String, String>();

    final int pos = h.indexOf(':');
    if (pos > -1) {
      headers.put(h.substring(0, pos).trim(), h.substring(pos + 1).trim());
    }
  }

  public String getHeader(final String iName) {
    return headers.get(iName);
  }

  public Set<Entry<String, String>> getHeaders() {
    return headers.entrySet();
  }
}
