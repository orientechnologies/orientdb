package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.Map;

public class OBasicAdminCommandContext extends OBasicCommandContext
    implements OAdminCommandContext {

  private OrientDBEmbedded context;

  public OBasicAdminCommandContext(OrientDBEmbedded server, Map<Object, Object> args) {
    this.context = server;
    setInputParameters(args);
  }

  public OBasicAdminCommandContext(OrientDBEmbedded server, Object[] args) {
    this.context = server;
    setArrayParameters(args);
  }

  public OrientDBEmbedded getGlobalContext() {
    return context;
  }
}
