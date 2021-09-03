package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/** Created by frank on 21/01/2016. */
public class OServerFailingOnStarupPluginStub extends OServerPluginAbstract {

  @Override
  public void startup() {
    throw new ODistributedException("this plugin is not starting correctly");
  }

  @Override
  public String getName() {
    return "failing on startup plugin";
  }
}
