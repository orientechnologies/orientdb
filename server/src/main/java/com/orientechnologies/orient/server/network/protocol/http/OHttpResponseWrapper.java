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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper to use the HTTP response in functions and scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHttpResponseWrapper {
  private final OHttpResponse response;

  /**
   * @param iResponse
   */
  public OHttpResponseWrapper(final OHttpResponse iResponse) {
    response = iResponse;
  }

  public String getHeader() {
    return response.headers;
  }

  public OHttpResponseWrapper setHeader(final String iHeader) {
    response.setHeader(iHeader);
    return this;
  }

  public String getCharacterSet() {
    return response.characterSet;
  }

  public OHttpResponseWrapper setCharacterSet(final String iCharacterSet) {
    response.characterSet = iCharacterSet;
    return this;
  }

  public String getHttpVersion() {
    return response.httpVersion;
  }

  public String[] getAdditionalResponseHeaders() {
    return response.additionalHeaders;
  }

  public OutputStream getOutputStream() {
    return response.getOutputStream();
  }

  public OHttpResponseWrapper flush() throws IOException {
    response.flush();
    return this;
  }
}
