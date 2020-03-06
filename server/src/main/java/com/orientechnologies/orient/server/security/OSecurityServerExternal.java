package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

import java.util.HashMap;
import java.util.Map;

public class OSecurityServerExternal extends OSecurityExternal {
  private OServer server;

  public OSecurityServerExternal(OServer server) {
    this.server = server;
  }

  @Override
  public OUser getUser(ODatabaseSession session, String username) {
    OUser user = super.getUser(session, username);

    if (user == null && username != null) {
      OServerUserConfiguration serverUser = server.getUser(username);
      if (serverUser != null) {
        user = new OSystemUser(username, "null", "Server");
        user.addRole(createRole(serverUser));
      }
    }
    return user;
  }

  public ORole createRole(OServerUserConfiguration serverUser) {

    final ORole role = new ORole(serverUser.name, null, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);

    if (serverUser.resources.equalsIgnoreCase("*")) {
      createRoot(role);
    } else {
      mapPermission(role, serverUser);
    }

    return role;
  }

  private void mapPermission(ORole role, OServerUserConfiguration user) {

    String[] strings = user.resources.split(",");

    for (String string : strings) {
      ORule.ResourceGeneric generic = ORule.mapLegacyResourceToGenericResource(string);
      if (generic != null) {
        role.addRule(generic, null, ORole.PERMISSION_ALL);
      }
    }
  }

  private void createRoot(ORole role) {
    role.addRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.ALL, null, ORole.PERMISSION_ALL);
//      role.addRule(ORule.ResourceGeneric.ALL_CLASSES, null, ORole.PERMISSION_ALL).save();
    role.addRule(ORule.ResourceGeneric.CLASS, null, ORole.PERMISSION_ALL);
//      role.addRule(ORule.ResourceGeneric.ALL_CLUSTERS, null, ORole.PERMISSION_ALL).save();
    role.addRule(ORule.ResourceGeneric.CLUSTER, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.DATABASE, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.SCHEMA, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.COMMAND, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.COMMAND_GREMLIN, null, ORole.PERMISSION_ALL);
    role.addRule(ORule.ResourceGeneric.FUNCTION, null, ORole.PERMISSION_ALL);
    createSecurityPolicyWithBitmask(role, "*", ORole.PERMISSION_ALL);
  }

  public void createSecurityPolicyWithBitmask(OSecurityRole role, String resource, int legacyPolicy) {
    String policyName = "default_" + legacyPolicy;
    OSecurityPolicy policy = new OSecurityPolicy(new ODocument().field("name", policyName));
    policy.setCreateRule((legacyPolicy & ORole.PERMISSION_CREATE) > 0 ? "true" : "false");
    policy.setReadRule((legacyPolicy & ORole.PERMISSION_READ) > 0 ? "true" : "false");
    policy.setBeforeUpdateRule((legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
    policy.setAfterUpdateRule((legacyPolicy & ORole.PERMISSION_UPDATE) > 0 ? "true" : "false");
    policy.setDeleteRule((legacyPolicy & ORole.PERMISSION_DELETE) > 0 ? "true" : "false");
    policy.setExecuteRule((legacyPolicy & ORole.PERMISSION_EXECUTE) > 0 ? "true" : "false");
    addSecurityPolicy(role, resource, policy);
  }

  public void addSecurityPolicy(OSecurityRole role, String resource, OSecurityPolicy policy) {
    OElement roleDoc = role.getDocument();
    if (roleDoc == null) {
      return;
    }
    Map<String, OIdentifiable> policies = roleDoc.getProperty("policies");
    if (policies == null) {
      policies = new HashMap<>();
      roleDoc.setProperty("policies", policies);
    }
    policies.put(resource, policy.getElement());

  }

}
