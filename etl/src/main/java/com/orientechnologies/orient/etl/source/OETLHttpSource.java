/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.source;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.logging.Level;

/** Extracts data from HTTP endpoint. */
public class OETLHttpSource extends OETLAbstractSource {
  protected BufferedReader reader;
  protected String url;
  protected String method = "GET";
  protected HttpURLConnection conn;
  protected ODocument headers;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:[{url:{optional:false,description:'HTTP URL to fetch'}},"
                + "{httpMethod:{optional:true,description:'HTTP method to use between GET (default), POST, PUT, DELETE, HEAD'}}],"
                + "output:'String'}");
  }

  @Override
  public void configure(ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
    url = iConfiguration.field("url");
    if (url == null || url.isEmpty()) throw new OConfigurationException("HTTP Source missing URL");
    if (iConfiguration.containsField("method")) method = iConfiguration.field("method");

    if (iConfiguration.containsField("headers")) headers = iConfiguration.field("headers");
  }

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public String getName() {
    return "http";
  }

  @Override
  public void begin(ODatabaseDocument db) {
    try {
      final URL obj = new URL(url);
      conn = (HttpURLConnection) obj.openConnection();
      conn.setRequestMethod(method);

      if (headers != null)
        for (String k : headers.fieldNames()) conn.setRequestProperty(k, (String) headers.field(k));

      log(Level.FINE, "Connecting to %s (method=%s)", url, method);

      final int responseCode = conn.getResponseCode();

      log(Level.FINE, "Connected: response code %d", responseCode);

    } catch (Exception e) {
      throw new OETLSourceException(
          "[HTTP source] error on opening connection in " + method + " to URL: " + url, e);
    }
  }

  @Override
  public void end() {
    if (reader != null)
      try {
        reader.close();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during close of reader", e);
      }

    if (conn != null) conn.disconnect();
  }

  @Override
  public Reader read() {
    try {
      reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      return reader;
    } catch (Exception e) {
      throw new OETLSourceException(
          "[HTTP source] Error on reading by using " + method + " from URL: " + url, e);
    }
  }
}
