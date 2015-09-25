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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class OServerCommandConfiguration extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = { "GET|configuration/*", "PUT|configuration/*" };

  protected OServerCommandConfiguration(String iRequiredResource) {
    super(iRequiredResource);
  }

  public OServerCommandConfiguration() {
    super("server.configuration");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    checkSyntax(iRequest.getUrl(), 2, "Syntax error: configuration/");
    if (iRequest.httpMethod.equals("GET")) {
      return doGet(iRequest, iResponse);
    }
    if (iRequest.httpMethod.equals("PUT")) {
      return doPut(iRequest, iResponse);
    }
    return false;
  }

  protected boolean doGet(OHttpRequest iRequest, OHttpResponse iResponse) {
    String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
    if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
      config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);
    FileInputStream input;
    try {
      File file2 = new File(config);
      input = new FileInputStream(file2);
      iResponse.sendStream(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, "text/xml", input, file2.length());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
    }

    return false;
  }

  protected boolean doPut(OHttpRequest iRequest, OHttpResponse iResponse) {

    String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
    if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
      config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

    File file = new File(config);
    BufferedWriter output;
    try {
      output = new BufferedWriter(new FileWriter(file));
      output.write(iRequest.content);
      output.close();
      iResponse.writeRecord(new ODocument());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
