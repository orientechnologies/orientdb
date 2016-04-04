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

import com.orientechnologies.agent.proxy.HttpProxyListener;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class OServerCommandConfiguration extends OServerCommandDistributedScope {

  private static final String[] NAMES = { "GET|configuration" };

  protected OServerCommandConfiguration(String iRequiredResource) {
    super(iRequiredResource);
  }

  public OServerCommandConfiguration() {
    super("server.configuration");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    checkSyntax(iRequest.getUrl(), 1, "Syntax error: configuration/");
    if (iRequest.httpMethod.equals("GET")) {
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
        FileInputStream input;

        File file2 = new File(config);
        input = new FileInputStream(file2);
        iResponse.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, "text/xml", input, file2.length());
      } catch (Exception e) {
        iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
      }
    } else {
      proxyRequest(iRequest, iResponse, new HttpProxyListener() {
        @Override
        public void onProxySuccess(OHttpRequest request, OHttpResponse response, InputStream is) throws IOException {
          response.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, "text/xml", is, -1);
        }

        @Override
        public void onProxyError(OHttpRequest request, OHttpResponse iResponse, InputStream is, int code, Exception e)
            throws IOException {
          iResponse.send(code, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_JSON, e, null);
        }
      });
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
