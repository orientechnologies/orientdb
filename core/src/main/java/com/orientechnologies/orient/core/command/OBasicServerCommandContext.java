package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDB;

public class OBasicServerCommandContext extends OBasicCommandContext
    implements OServerCommandContext {

  private OrientDB server;

  public OBasicServerCommandContext() {}

  public OBasicServerCommandContext(OrientDB server) {
    this.server = server;
  }

  public OrientDB getServer() {
    return server;
  }

  public void setServer(OrientDB server) {
    this.server = server;
  }
}
