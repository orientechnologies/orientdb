package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.util.Map;

public class OBasicServerCommandContext extends OBasicCommandContext
    implements OServerCommandContext {

  private OrientDBInternal server;

  public OBasicServerCommandContext(OrientDBInternal server, Map<Object, Object> args) {
    this.server = server;
    setInputParameters(args);
  }

  public OBasicServerCommandContext(OrientDBInternal server, Object[] args) {
    this.server = server;
    setArrayParameters(args);
  }

  public OrientDBInternal getServer() {
    return server;
  }
}
