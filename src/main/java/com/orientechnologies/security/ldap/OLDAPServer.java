/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.security.ldap;

import java.net.URI;
import java.net.URISyntaxException;

public class OLDAPServer {
  private String scheme;
  private String host;
  private int port;
  private boolean isAlias;

  public String getHostname() {
    return host;
  }

  public String getURL() {
    return String.format("%s://%s:%d", scheme, host, port);
  }

  // Replaces the current URL's host port with hostname and returns it.
  public String getURL(final String hostname) {
    return String.format("%s://%s:%d", scheme, hostname, port);
  }

  public boolean isAlias() {
    return isAlias;
  }

  public OLDAPServer(final String scheme, final String host, int port, boolean isAlias) {
    this.scheme = scheme;
    this.host = host;
    this.port = port;
    this.isAlias = isAlias;
  }

  public static OLDAPServer validateURL(final String url, boolean isAlias) {
    OLDAPServer server = null;

    try {
      URI uri = new URI(url);

      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      if (port == -1) port = 389; // Default to standard LDAP port.

      server = new OLDAPServer(scheme, host, port, isAlias);
    } catch (URISyntaxException se) {

    }

    return server;
  }
}
