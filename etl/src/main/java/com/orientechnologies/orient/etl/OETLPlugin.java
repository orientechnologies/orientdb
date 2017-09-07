package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.etl.http.OServerCommandETL;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by gabriele on 07/09/17.
 */
public class OETLPlugin extends OServerPluginAbstract {

  private OServer server;

  public OETLPlugin() {}

  public void executeJob(String[] args) {

    System.out.println("OrientDB etl v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    if (args.length == 0) {
      System.out.println("Syntax error, missing configuration file.");
    }
    else {
      final OETLProcessor processor = new OETLProcessorConfigurator().parseConfigAndParameters(args);
      processor.execute();
    }
  }

  @Override
  public String getName() {
    return "etl";
  }

  @Override
  public void startup() {

    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

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
