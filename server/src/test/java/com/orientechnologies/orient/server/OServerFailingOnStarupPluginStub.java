package com.orientechnologies.orient.server;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by frank on 21/01/2016.
 */
public class OServerFailingOnStarupPluginStub extends OServerPluginAbstract {

  @Override
  public void startup() {
    throw new OException("this plugin is not starting correctly");
  }

  @Override
  public String getName() {
    return "failing on startup plugin";
  }
}
