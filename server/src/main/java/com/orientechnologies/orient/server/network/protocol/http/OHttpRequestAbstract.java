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
public abstract class OHttpRequestAbstract implements OHttpRequest {
  private final OContextConfiguration configuration;
  private final InputStream in;
  private final ONetworkProtocolData data;
  private final ONetworkHttpExecutor executor;
  private String content;
  private Map<String, String> parameters;
  private String sessionId;
  private String authorization;
  private String databaseName;

  public OHttpRequestAbstract(
      final ONetworkHttpExecutor iExecutor,
      final InputStream iInStream,
      final ONetworkProtocolData iData,
      final OContextConfiguration iConfiguration) {
    executor = iExecutor;
    in = iInStream;
    data = iData;
    configuration = iConfiguration;
  }

  @Override
  public String getUser() {
    return getAuthorization() != null
        ? getAuthorization().substring(0, getAuthorization().indexOf(":"))
        : null;
  }

  @Override
  public InputStream getInputStream() {
    return getIn();
  }

  @Override
  public String getParameter(final String iName) {
    return getParameters() != null ? getParameters().get(iName) : null;
  }

  @Override
  public void addHeader(final String h) {
    if (getHeaders() == null) setHeaders(new HashMap<String, String>());

    final int pos = h.indexOf(':');
    if (pos > -1) {
      getHeaders()
          .put(h.substring(0, pos).trim().toLowerCase(Locale.ENGLISH), h.substring(pos + 1).trim());
    }
  }

  @Override
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

  @Override
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public Map<String, String> getParameters() {
    return parameters;
  }

  @Override
  public String getHeader(final String iName) {
    return getHeaders().get(iName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public abstract Map<String, String> getHeaders();

  @Override
  public String getRemoteAddress() {
    if (getData().caller != null) return getData().caller;
    return getExecutor().getRemoteAddress();
  }

  @Override
  public String getContent() {
    return content;
  }

  @Override
  public void setContent(String content) {
    this.content = content;
  }

  @Override
  public abstract String getUrl();

  @Override
  public OContextConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public InputStream getIn() {
    return in;
  }

  @Override
  public ONetworkProtocolData getData() {
    return data;
  }

  @Override
  public ONetworkHttpExecutor getExecutor() {
    return executor;
  }

  @Override
  public String getAuthorization() {
    return authorization;
  }

  @Override
  public void setAuthorization(String authorization) {
    this.authorization = authorization;
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  public abstract void setUrl(String url);

  @Override
  public abstract String getHttpMethod();

  @Override
  public abstract void setHttpMethod(String httpMethod);

  @Override
  public abstract String getHttpVersion();

  @Override
  public abstract void setHttpVersion(String httpVersion);

  @Override
  public abstract String getContentType();

  @Override
  public abstract void setContentType(String contentType);

  @Override
  public abstract String getContentEncoding();

  @Override
  public abstract void setContentEncoding(String contentEncoding);

  @Override
  public abstract String getAcceptEncoding();

  @Override
  public abstract void setAcceptEncoding(String acceptEncoding);

  @Override
  public abstract OHttpMultipartBaseInputStream getMultipartStream();

  @Override
  public abstract void setMultipartStream(OHttpMultipartBaseInputStream multipartStream);

  @Override
  public abstract String getBoundary();

  @Override
  public abstract void setBoundary(String boundary);

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public abstract boolean isMultipart();

  @Override
  public abstract void setMultipart(boolean multipart);

  @Override
  public abstract String getIfMatch();

  @Override
  public abstract void setIfMatch(String ifMatch);

  @Override
  public abstract String getAuthentication();

  @Override
  public abstract void setAuthentication(String authentication);

  @Override
  public abstract boolean isKeepAlive();

  @Override
  public abstract void setKeepAlive(boolean keepAlive);

  @Override
  public abstract void setHeaders(Map<String, String> headers);

  @Override
  public abstract String getBearerTokenRaw();

  @Override
  public abstract void setBearerTokenRaw(String bearerTokenRaw);

  @Override
  public abstract OParsedToken getBearerToken();

  @Override
  public abstract void setBearerToken(OParsedToken bearerToken);
}
