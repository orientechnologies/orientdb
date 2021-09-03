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

package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.etl.context.OETLContextWrapper;
import com.orientechnologies.orient.etl.http.OServerCommandETL;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * OETLPlugin.
 *
 * @author Gabriele Ponzi
 * @email gabriele.ponzi--at--gmail.com
 */
public class OETLPlugin extends OServerPluginAbstract {

  private OServer server;

  public OETLPlugin() {}

  public OETLPlugin(OServer server) {
    this.server = server;
  }

  public void executeJob(
      String jsonConfig, String outDBConfigPath, OPluginMessageHandler messageHandler) {
    System.out.println("OrientDB etl v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    if (jsonConfig == null) {
      System.out.println("Syntax error, missing configuration file.");
    } else {
      String[] args = {outDBConfigPath};
      final OETLProcessor processor =
          new OETLProcessorConfigurator()
              .parseConfigAndParametersWithContext(server.getContext(), args);

      try {
        // overriding default message handler if the chosen verbosity level is different from the
        // default one
        if (messageHandler.getOutputManagerLevel()
            != OETLContextWrapper.getInstance().getMessageHandler().getOutputManagerLevel()) {
          OETLContextWrapper.getInstance().setMessageHandler(messageHandler);
        }

        // execute the job
        processor.execute();
      } finally {
        processor.close();
      }
    }
  }

  @Override
  public String getName() {
    return "etl";
  }

  @Override
  public void startup() {

    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null) throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandETL());
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
