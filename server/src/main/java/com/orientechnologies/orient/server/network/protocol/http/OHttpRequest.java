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

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.multipart.OHttpMultipartBaseInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Maintains information about current HTTP request.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OHttpRequest {
  private final OContextConfiguration configuration;
  private final InputStream in;
  private final ONetworkProtocolData data;
  private final ONetworkHttpExecutor executor;
  protected String content;
  protected Map<String, String> parameters;
  private String sessionId;
  protected String authorization;
  private String databaseName;

  public OHttpRequest(
      final ONetworkHttpExecutor iExecutor,
      final InputStream iInStream,
      final ONetworkProtocolData iData,
      final OContextConfiguration iConfiguration) {
    executor = iExecutor;
    in = iInStream;
    data = iData;
    configuration = iConfiguration;
  }

  public String getUser() {
    return getAuthorization() != null
        ? getAuthorization().substring(0, getAuthorization().indexOf(":"))
        : null;
  }

  public InputStream getInputStream() {
    return getIn();
  }

  public String getParameter(final String iName) {
    return getParameters() != null ? getParameters().get(iName) : null;
  }

  public void addHeader(final String h) {
    if (getHeaders() == null) setHeaders(new HashMap<String, String>());

    final int pos = h.indexOf(':');
    if (pos > -1) {
      getHeaders()
          .put(h.substring(0, pos).trim().toLowerCase(Locale.ENGLISH), h.substring(pos + 1).trim());
    }
  }

  public Map<String, String> getUrlEncodedContent() {
    if (getContent() == null || getContent().length() < 1) {
      return null;
    }
    HashMap<String, String> retMap = new HashMap<String, String>();
    String key;
    String value;
    try {
      String[] pairs = getContent().split("\\&");
      for (int i = 0; i < pairs.length; i++) {
        String[] fields = pairs[i].split("=");
        if (fields.length == 2) {
          key = URLDecoder.decode(fields[0], "UTF-8");
          value = URLDecoder.decode(fields[1], "UTF-8");
          retMap.put(key, value);
        }
      }
    } catch (UnsupportedEncodingException usEx) {
      // noop
    }
    return retMap;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getHeader(final String iName) {
    return getHeaders().get(iName.toLowerCase(Locale.ENGLISH));
  }

  public abstract Map<String, String> getHeaders();

  public String getRemoteAddress() {
    if (getData().caller != null) return getData().caller;
    return getExecutor().getRemoteAddress();
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public abstract String getUrl();

  public OContextConfiguration getConfiguration() {
    return configuration;
  }

  public InputStream getIn() {
    return in;
  }

  public ONetworkProtocolData getData() {
    return data;
  }

  public ONetworkHttpExecutor getExecutor() {
    return executor;
  }

  public String getAuthorization() {
    return authorization;
  }

  public void setAuthorization(String authorization) {
    this.authorization = authorization;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public abstract void setUrl(String url);

  public abstract String getHttpMethod();

  public abstract void setHttpMethod(String httpMethod);

  public abstract String getHttpVersion();

  public abstract void setHttpVersion(String httpVersion);

  public abstract String getContentType();

  public abstract void setContentType(String contentType);

  public abstract String getContentEncoding();

  public abstract void setContentEncoding(String contentEncoding);

  public abstract String getAcceptEncoding();

  public abstract void setAcceptEncoding(String acceptEncoding);

  public abstract OHttpMultipartBaseInputStream getMultipartStream();

  public abstract void setMultipartStream(OHttpMultipartBaseInputStream multipartStream);

  public abstract String getBoundary();

  public abstract void setBoundary(String boundary);

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public abstract boolean isMultipart();

  public abstract void setMultipart(boolean multipart);

  public abstract String getIfMatch();

  public abstract void setIfMatch(String ifMatch);

  public abstract String getAuthentication();

  public abstract void setAuthentication(String authentication);

  public abstract boolean isKeepAlive();

  public abstract void setKeepAlive(boolean keepAlive);

  public abstract void setHeaders(Map<String, String> headers);

  public abstract String getBearerTokenRaw();

  public abstract void setBearerTokenRaw(String bearerTokenRaw);

  public abstract OParsedToken getBearerToken();

  public abstract void setBearerToken(OParsedToken bearerToken);
}
