package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAbstract;
import java.io.StringWriter;

public class OServerCommandGetSSO extends OServerCommandAbstract {
  private static final String[] NAMES = {"GET|sso"};

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public OServerCommandGetSSO() {}

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    getJSON(iResponse);

    return false; // Is not a chained command.
  }

  private void getJSON(final OHttpResponse iResponse) {
    try {
      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer, OHttpResponse.JSON_FORMAT);

      json.beginObject();

      json.writeAttribute("enabled", getServer().getSecurity().isSingleSignOnSupported());

      json.endObject();

      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_JSON,
          buffer.toString(),
          null);
    } catch (Exception ex) {
      OLogManager.instance().error(this, "OServerCommandGetSSO.getJSON() Exception: %s", ex);
    }
  }
}
