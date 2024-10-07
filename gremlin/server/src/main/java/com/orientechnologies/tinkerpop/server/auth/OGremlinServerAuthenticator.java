package com.orientechnologies.tinkerpop.server.auth;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import java.util.Map;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;

/** Created by Enrico Risa on 07/09/2017. */
public class OGremlinServerAuthenticator extends SimpleAuthenticator {

  OServer server;

  private static final String PERMISSION = "gremlin.server";

  public static final ResourceGeneric GREMLIN =
      new ResourceGeneric("GREMLIN", PERMISSION) {
        private static final long serialVersionUID = 1L;
      };

  @Override
  public AuthenticatedUser authenticate(Map<String, String> credentials)
      throws AuthenticationException {

    if (!credentials.containsKey(PROPERTY_USERNAME))
      throw new IllegalArgumentException(
          String.format("Credentials must contain a %s", PROPERTY_USERNAME));
    if (!credentials.containsKey(PROPERTY_PASSWORD))
      throw new IllegalArgumentException(
          String.format("Credentials must contain a %s", PROPERTY_PASSWORD));

    final String username = credentials.get(PROPERTY_USERNAME);
    final String password = credentials.get(PROPERTY_PASSWORD);

    if (!server.authenticate(username, password, PERMISSION)) {
      throw new AuthenticationException("Username and/or password are incorrect");
    }
    return new AuthenticatedUser(username);
  }

  @Override
  public void setup(Map<String, Object> config) {
    server = OServerMain.server();
  }
}
