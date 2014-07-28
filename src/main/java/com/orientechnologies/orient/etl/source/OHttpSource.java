/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OAbstractETLComponent;
import com.orientechnologies.orient.etl.extractor.OExtractorException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Extracts data from HTTP endpoint.
 */
public class OHttpSource extends OAbstractETLComponent implements OSource {
  protected BufferedReader      reader;
  protected long                progressBytes = -1;
  protected long                total         = -1;

  protected String              url;
  protected String              httpMethod    = "GET";
  protected HttpURLConnection   conn;
  protected Map<String, String> headers       = new HashMap<String, String>();
  private long                  currentRow    = 0;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[{url:{optional:false,description:'HTTP URL to fetch'}},"
        + "{httpMethod:{optional:true,description:'HTTP method to use between GET (default), POST, PUT, DELETE, HEAD'}}],"
        + "output:'String'}");
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
  public void begin() {
    try {
      final URL obj = new URL(url);
      conn = (HttpURLConnection) obj.openConnection();
      conn.setRequestMethod(httpMethod);

      for (Map.Entry<String, String> entry : headers.entrySet())
        conn.setRequestProperty(entry.getKey(), entry.getValue());

      final int responseCode = conn.getResponseCode();

      reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

    } catch (Exception e) {
      throw new OExtractorException("Error on opening connection in " + httpMethod + " to URL: " + url, e);
    }
  }

  @Override
  public void end() {
    if (reader != null)
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    if (conn != null)
      conn.disconnect();
  }

  @Override
  public Reader read() {
    return reader;
  }
}
