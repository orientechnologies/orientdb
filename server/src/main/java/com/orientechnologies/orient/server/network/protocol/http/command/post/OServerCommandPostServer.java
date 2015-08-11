/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;

public class OServerCommandPostServer extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "POST|server/*" };

  public OServerCommandPostServer() {
    super("server.settings");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: server/<setting-name>/<setting-value>");

    iRequest.data.commandInfo = "Change server settings";

    if (urlParts[1] == null || urlParts.length == 0)
      throw new IllegalArgumentException("setting-name is null or empty");

    final String settingName = urlParts[1];
    final String settingValue = urlParts[2];

    if (settingName.startsWith("configuration.")) {

      changeConfiguration(iResponse, settingName.substring("configuration.".length()), settingValue);

    } else if (settingName.startsWith("log.")) {

      changeLogLevel(iResponse, settingName.substring("log.".length()), settingValue);

    } else
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "setting-name '" + settingName + "' is not supported", null);

    return false;
  }

  private void changeConfiguration(final OHttpResponse iResponse, final String settingName, final String settingValue)
      throws IOException {
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(settingName);
    if (cfg != null) {
      final Object oldValue = cfg.getValue();

      cfg.setValue(settingValue);


      iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "Server global configuration '" + settingName + "' update successfully. Old value was '" + oldValue + "', new value is '"
              + settingValue + "'", null);
    } else
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "Server global configuration '" + settingName + "' is invalid", null);
  }

  private void changeLogLevel(final OHttpResponse iResponse, final String settingName, final String settingValue)
      throws IOException {
    if (settingName.equals("console"))
      OLogManager.instance().setConsoleLevel(settingValue);
    else if (settingName.equals("file"))
      OLogManager.instance().setFileLevel(settingValue);
    else
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
          "log name '" + settingName + "' is not supported. Use 'console' or 'log'", null);

    iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN,
        "Server log configuration '" + settingName + "' update successfully", null);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
