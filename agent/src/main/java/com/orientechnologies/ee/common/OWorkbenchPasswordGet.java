package com.orientechnologies.ee.common;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

public class OWorkbenchPasswordGet implements Callable<String>, Serializable {

  @Override
  public String call() throws Exception {
    OServer server = OServerMain.server();
    OServerUserConfiguration config = server.getConfiguration().getUser("root");
    return config.password;
  }
}
