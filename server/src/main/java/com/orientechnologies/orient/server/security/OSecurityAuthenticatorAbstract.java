/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

import javax.security.auth.Subject;

/**
 * Provides an abstract implementation of OSecurityAuthenticator.
 * 
 * @author S. Colin Leister
 * 
 */
public abstract class OSecurityAuthenticatorAbstract implements OSecurityAuthenticator {
  private String                      name          = "";
  private boolean                     debug         = false;
  private boolean                     enabled       = true;
  private boolean                     caseSensitive = true;
  private OServer                     server;
  private OServerConfigurationManager serverConfig;

  protected OServer getServer() {
    return server;
  }

  protected OServerConfigurationManager getServerConfig() {
    return serverConfig;
  }

  protected boolean isDebug() {
    return debug;
  }

  protected boolean isCaseSensitive() {
    return caseSensitive;
  }

  // OSecurityComponent
  public void active() {
  }

  // OSecurityComponent
  public void config(final OServer oServer, final OServerConfigurationManager serverCfg, final ODocument jsonConfig) {
    server = oServer;
    serverConfig = serverCfg;

    if (jsonConfig.containsField("name")) {
      name = jsonConfig.field("name");
    }

    if (jsonConfig.containsField("debug")) {
      debug = jsonConfig.field("debug");
    }

    if (jsonConfig.containsField("enabled")) {
      enabled = jsonConfig.field("enabled");
    }

    if (jsonConfig.containsField("caseSensitive")) {
      caseSensitive = jsonConfig.field("caseSensitive");
    }
  }

  // OSecurityComponent
  public void dispose() {
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }

  // OSecurityAuthenticator
  // databaseName may be null.
  public String getAuthenticationHeader(String databaseName) {
    String header;

    // Default to Basic.
    if (databaseName != null)
      header = "WWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
    else
      header = "WWW-Authenticate: Basic realm=\"OrientDB Server\"";

    return header;
  }

  public Subject getClientSubject() {
    return null;
  }

  // Returns the name of this OSecurityAuthenticator.
  public String getName() {
    return name;
  }

  public OServerUserConfiguration getUser(final String username) {
    return null;
  }

  public boolean isAuthorized(final String username, final String resource) {
    return false;
  }

  public boolean isSingleSignOnSupported() {
    return false;
  }

  protected boolean isPasswordValid(final OServerUserConfiguration user) {
    return user != null && user.password != null && !user.password.isEmpty();
  }
}
