package com.orientechnologies.orient.etl.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.io.IOException;

/** Created by gabriele on 27/02/17. */
public class OServerCommandETL extends OServerCommandAuthenticatedServerAbstract {

  private OETLHandler handler = new OETLHandler();
  private static final String[] NAMES = {"GET|etl/*", "POST|etl/*"};

  public OServerCommandETL() {
    super("server.profiler");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: auditing/<db>/<action>");

    if ("POST".equalsIgnoreCase(iRequest.getHttpMethod())) {
      doPost(iRequest, iResponse, parts);
    }
    if ("GET".equalsIgnoreCase(iRequest.getHttpMethod())) {
      doGet(iRequest, iResponse, parts);
    }
    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts)
      throws IOException {

    if ("status".equalsIgnoreCase(parts[1])) {
      ODocument status = handler.status();
      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          "OK",
          OHttpUtils.CONTENT_JSON,
          status.toJSON("prettyPrint"),
          null);
    } else {
      throw new IllegalArgumentException("");
    }
  }

  private void doPost(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts)
      throws IOException {

    if ("job".equalsIgnoreCase(parts[1])) {
      ODocument cfg = new ODocument().fromJSON(iRequest.getContent());
      handler.executeImport(cfg, super.server);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
    } else if ("save-config".equalsIgnoreCase(parts[1])) {
      ODocument args = new ODocument().fromJSON(iRequest.getContent());
      try {
        handler.saveConfiguration(args, super.server);
      } catch (IOException e) {
        throw new IOException(e);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, null, null);
    } else if ("list-configs".equalsIgnoreCase(parts[1])) {
      try {
        ODocument configsInfo = handler.listConfigurations(super.server);
        iResponse.send(
            OHttpUtils.STATUS_OK_CODE,
            "OK",
            OHttpUtils.CONTENT_JSON,
            configsInfo.toJSON("prettyPrint"),
            null);
      } catch (IOException e) {
        throw new IOException(e);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      throw new IllegalArgumentException("");
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
