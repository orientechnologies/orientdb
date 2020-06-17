package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;

public class OServerCommandGetPing extends OServerCommandAbstract {
  private static final String[] NAMES = {"GET|ping"};

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public OServerCommandGetPing() {}

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    iResponse.send(
        OHttpUtils.STATUS_OK_CODE,
        OHttpUtils.STATUS_OK_DESCRIPTION,
        OHttpUtils.CONTENT_TEXT_PLAIN,
        "pong",
        null);

    return false; // Is not a chained command.
  }
}
