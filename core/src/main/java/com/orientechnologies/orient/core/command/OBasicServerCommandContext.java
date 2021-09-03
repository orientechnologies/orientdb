package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBInternal;

public class OBasicServerCommandContext extends OBasicCommandContext
    implements OServerCommandContext {

  private OrientDBInternal server;

  public OBasicServerCommandContext() {}

  public OBasicServerCommandContext(OrientDBInternal server) {
    this.server = server;
  }

  public OrientDBInternal getServer() {
    return server;
  }

  public void setServer(OrientDBInternal server) {
    this.server = server;
  }
}
