/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class OServerCommandConfiguration extends OServerCommandDistributedScope {

  private static final String[] NAMES = { "GET|configuration" };

  public OServerCommandConfiguration(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_CONFIGURATION.toString(), server);
  }

  @Override
  void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse) {

  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    checkSyntax(iRequest.getUrl(), 1, "Syntax error: configuration/");
    if (iRequest.getHttpMethod().equals("GET")) {
      return doGetRequest(iRequest, iResponse);
    }
    return false;
  }

  protected boolean doGetRequest(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

    if (isLocalNode(iRequest)) {
      try {
        String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
        if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
          config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

        config = OSystemVariableResolver.resolveSystemVariables(config);

        server.getConfiguration();

        File file2 = new File(config);

        FileInputStream input = new FileInputStream(file2);
        try {
          iResponse.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, "text/xml", input, file2.length());
        } finally {
          try {
            input.close();
          } catch (IOException e) {
            OLogManager.instance().info(this, "Failed to close input stream for " + file2);
          }
        }
      } catch (FileNotFoundException e) {
        iResponse
            .send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
      } catch (Exception e) {
        iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
      }
    } else {

    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
