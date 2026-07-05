package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.command.OAdminCommandContext;
import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.sql.parser.ODatabaseUserData;
import com.orientechnologies.orient.core.sql.parser.ONodeData;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OCreateDatabaseParameters {

  private List<ODatabaseUserData> users;
  private List<ONodeData> nodes;
  private OAdminCommandContext ctx;

  public OCreateDatabaseParameters(
      List<ODatabaseUserData> users, List<ONodeData> nodes, OAdminCommandContext ctx) {
    this.users = users;
    this.nodes = nodes;
    this.ctx = ctx;
  }

  public void postCreateOps(OrientDBInternal context, ODatabaseDocumentInternal session) {
    if (users != null && !users.isEmpty()) {
      for (ODatabaseUserData user : users) {
        user.executeCreate(session, ctx);
      }
    }
  }

  public Optional<Set<OAddNodeInfo>> members() {
    if (nodes == null || nodes.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(
          this.nodes.stream().map(n -> n.toAddNodeInfo(ctx)).collect(Collectors.toSet()));
    }
  }
}
