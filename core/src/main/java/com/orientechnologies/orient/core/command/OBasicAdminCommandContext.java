package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.util.Map;

public class OBasicAdminCommandContext extends OBasicCommandContext
    implements OAdminCommandContext {

  private OrientDBInternal server;

  public OBasicAdminCommandContext(OrientDBInternal server, Map<Object, Object> args) {
    this.server = server;
    setInputParameters(args);
  }

  public OBasicAdminCommandContext(OrientDBInternal server, Object[] args) {
    this.server = server;
    setArrayParameters(args);
  }

  public OrientDBInternal getGlobalContext() {
    return server;
  }
}
