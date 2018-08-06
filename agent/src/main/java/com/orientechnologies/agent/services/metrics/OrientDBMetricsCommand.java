package com.orientechnologies.agent.services.metrics;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.ByteArrayOutputStream;

/**
 * Created by Enrico Risa on 06/08/2018.
 */
public class OrientDBMetricsCommand extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = { "GET|metrics", "POST|metrics" };

  private OMetricsRegistry registry;

  public OrientDBMetricsCommand(OMetricsRegistry registry) {
    super("server.metrics");
    this.registry = registry;
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    registry.toJSON(buffer);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
