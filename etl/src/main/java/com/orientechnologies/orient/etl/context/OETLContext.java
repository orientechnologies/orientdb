/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.etl.context;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

/**
 * OETLContext extends OBasicCommandContext, in order to handle the following additional elements: -
 * message handler for application messages
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OETLContext extends OBasicCommandContext {

  private OPluginMessageHandler messageHandler;
  private final Map<String, OrientDB> contexts = new HashMap<String, OrientDB>();

  /**
   * By default the OETLContext in initialized with a message handler with: - verbosity level: 0
   * (NONE) --> nothing will be printed out - output stream: null - logging: OLogManager
   */
  public OETLContext() {
    this.messageHandler = new OETLMessageHandler(0);
  }

  public OPluginMessageHandler getMessageHandler() {
    return this.messageHandler;
  }

  public void setMessageHandler(OPluginMessageHandler messageHandler) {
    this.messageHandler = messageHandler;
  }

  /**
   * Prints the error message for a caught exception according to a level passed as argument. It's
   * composed of: - defined error message - exception message
   *
   * @param e
   * @param message
   * @param level
   * @return printedMessage
   */
  public String printExceptionMessage(Exception e, String message, String level) {

    if (e.getMessage() != null) message += "\n" + e.getClass().getName() + " - " + e.getMessage();
    else message += "\n" + e.getClass().getName();

    switch (level) {
      case "debug":
        this.messageHandler.debug(this, message);
        break;
      case "info":
        this.messageHandler.info(this, message);
        break;
      case "warn":
        this.messageHandler.warn(this, message);
        break;
      case "error":
        this.messageHandler.error(this, message);
        break;
    }

    return message;
  }

  /**
   * Builds the exception stack trace and prints it according to a level passed as argument.
   *
   * @param e
   * @param level
   * @return printedMessage
   */
  public String printExceptionStackTrace(Exception e, String level) {

    // copying the exception stack trace in the string
    Writer writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    String s = writer.toString();

    switch (level) {
      case "debug":
        this.messageHandler.debug(this, "\n" + s + "\n");
        break;
      case "info":
        this.messageHandler.info(this, "\n" + s + "\n");
        break;
      case "warn":
        this.messageHandler.warn(this, "\n" + s + "\n");
        break;
      case "error":
        this.messageHandler.error(this, "\n" + s + "\n");
        break;
    }

    return s;
  }

  public synchronized void registerOrientDB(OrientDB orientdb) {
    OrientDBInternal orientDBInternal = OrientDBInternal.extract(orientdb);
    this.contexts.put(
        "embedded:" + orientDBInternal.getBasePath(), orientDBInternal.newOrientDBNoClose());
  }

  public synchronized OrientDB getOrientDB(String url, String user, String password) {
    System.out.println("create context" + url);
    OrientDB orientDB = contexts.get(url);
    if (orientDB == null) {
      orientDB = new OrientDB(url, user, password, OrientDBConfig.defaultConfig());
      contexts.put(url, orientDB);
    }
    return orientDB;
  }
}
