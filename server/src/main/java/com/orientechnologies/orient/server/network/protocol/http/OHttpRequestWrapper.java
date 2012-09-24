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

import java.util.Map;

/**
 * Wrapper to use the HTTP request in functions and scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHttpRequestWrapper {
  private OHttpRequest wrapped;

  public String getContent() {
    return wrapped.content;
  }

  public String getUser() {
    return wrapped.authorization != null ? wrapped.authorization.substring(0, wrapped.authorization.indexOf(":")) : null;
  }

  public String getContentType() {
    return wrapped.contentType;
  }

  public String getHttpVersion() {
    return wrapped.httpVersion;
  }

  public String getIfMatch() {
    return wrapped.ifMatch;
  }

  public boolean getisMultipart() {
    return wrapped.isMultipart;
  }

  public Map<String, String> getParameters() {
    return wrapped.parameters;
  }

  public String getSessionId() {
    return wrapped.sessionId;
  }

  public String getURL() {
    return wrapped.url;
  }
}
