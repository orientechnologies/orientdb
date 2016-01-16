/*
 *
 * Copyright 2011 Luca Molino (molino.luca--AT--gmail.com *
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
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.util.Arrays;

/**
 * @author luca.molino
 * 
 */
public class OServerCommandAuthProxy extends OServerCommandPatternAbstract {

  public static final String DATABASE_CONF     = "database";
  public static final String USERNAME_CONF     = "username";
  public static final String USERPASSWORD_CONF = "userpassword";
  private final String       authentication;
  private String             databaseName;
  private String             userName;
  private String             userPassword;

  public OServerCommandAuthProxy(OServerCommandConfiguration iConfig) {
    super(iConfig);
    if (iConfig.parameters.length != 3)
      throw new OConfigurationException("AuthProxy Command requires database access data.");

    userName = "";
    userPassword = "";
    for (OServerEntryConfiguration conf : iConfig.parameters) {
      if (conf.name.equals(USERNAME_CONF))
        userName = conf.value;
      else if (conf.name.equals(USERPASSWORD_CONF))
        userPassword = conf.value;
      else if (conf.name.equals(DATABASE_CONF))
        databaseName = conf.value;
    }
    authentication = userName + ":" + userPassword;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    iRequest.authorization = authentication;
    checkSyntax(iRequest.url, 3, "Syntax error: " + Arrays.toString(getNames()) + "/<nextCommand>/");
    iRequest.url = OHttpUtils.nextChainUrl(iRequest.url);

    // CHECK THE SESSION VALIDITY
    if (iRequest.sessionId == null || OServerCommandAuthenticatedDbAbstract.SESSIONID_LOGOUT.equals(iRequest.sessionId)
        || iRequest.sessionId.length() > 1 && OHttpSessionManager.getInstance().getSession(iRequest.sessionId) == null)
      // AUTHENTICATED: CREATE THE SESSION
      iRequest.sessionId = OHttpSessionManager.getInstance().createSession(databaseName, userName, userPassword);

    return true;
  }
}
