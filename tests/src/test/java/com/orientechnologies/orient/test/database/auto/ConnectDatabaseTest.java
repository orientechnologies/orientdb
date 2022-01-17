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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(enabled = false)
public class ConnectDatabaseTest {
  private final String url;
  private final String databaseName;

  @Parameters(value = "url")
  public ConnectDatabaseTest(@Optional String iURL) {
    if (iURL == null) url = "remote:xxx/GratefulDeadConcerts";
    else url = iURL;

    if (url.contains("/")) databaseName = url.substring(url.lastIndexOf("/") + 1);
    else databaseName = url.substring(url.lastIndexOf(":") + 1);
  }

  public void connectWithDNS() throws IOException {
    if (!url.startsWith("remote:") || !isInternetAvailable()) return;

    OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED.setValue(true);
    try {
      final ODatabaseDocumentTx database =
          new ODatabaseDocumentTx("remote:orientechnologies.com/" + databaseName);
      database.open("admin", "admin");
      Assert.assertFalse(database.isClosed());
      database.close();
    } finally {
      OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED.setValue(false);
    }
  }

  protected boolean isInternetAvailable() {
    try {
      final URL url = new URL("http://orientdb.com");
      final HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
      urlConn.setConnectTimeout(1000 * 10); // mTimeout is in seconds
      urlConn.connect();
      if (urlConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return true;
      }
    } catch (final MalformedURLException e1) {
    } catch (final IOException e) {
    }
    return false;
  }
}
