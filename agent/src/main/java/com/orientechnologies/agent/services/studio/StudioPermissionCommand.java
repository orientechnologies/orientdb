package com.orientechnologies.agent.services.studio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by Enrico Risa on 29/08/2018. */
public class StudioPermissionCommand extends OServerCommandAuthenticatedServerAbstract {

  private static final String[] NAMES = {"GET|permissions/all", "GET|permissions/mine"};

  private OEnterpriseServer enterpriseServer;

  public StudioPermissionCommand(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_PERMISSIONS.toString());
    enterpriseServer = server;
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: metrics");

    if (iRequest.getHttpMethod().equalsIgnoreCase("GET")) {
      doGet(iRequest, iResponse, parts);
    }
    return false;
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts)
      throws IOException {

    if (parts.length == 2) {

      ObjectMapper mapper = new ObjectMapper();
      switch (parts[1]) {
        case "all":
          Map<String, Map<String, String>> output = new HashMap<>();
          Map<String, String> perm =
              Arrays.asList(EnterprisePermissions.values()).stream()
                  .collect(
                      Collectors.toMap(
                          EnterprisePermissions::getPermission,
                          EnterprisePermissions::getDescription));
          mapper = new ObjectMapper();
          output.put("permissions", perm);
          iResponse.send(
              OHttpUtils.STATUS_OK_CODE,
              OHttpUtils.STATUS_OK_DESCRIPTION,
              OHttpUtils.CONTENT_JSON,
              mapper.writeValueAsString(output),
              null);
          break;
        case "mine":
          String user = getUser(iRequest);

          Map<String, Set<String>> m = new HashMap<>();
          if (user != null) {
            Set<String> permissions =
                Arrays.asList(EnterprisePermissions.values()).stream()
                    .filter((c) -> server.getSecurity().isAuthorized(user, c.toString()))
                    .map((c) -> c.toString())
                    .collect(Collectors.toSet());
            m.put("permissions", permissions);
            iResponse.send(
                OHttpUtils.STATUS_OK_CODE,
                OHttpUtils.STATUS_OK_DESCRIPTION,
                OHttpUtils.CONTENT_JSON,
                mapper.writeValueAsString(m),
                null);

          } else {
            sendNotAuthorizedResponse(iRequest, iResponse);
          }
          break;

        default:
          throw new UnsupportedOperationException(String.format("% not supported", parts[1]));
      }
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
