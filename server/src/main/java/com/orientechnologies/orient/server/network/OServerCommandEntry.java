package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OServerCommandEntry {

  private final OServerCommandLoader loader;
  private final String pattern;
  private final Map<String, String> parameters;
  private final boolean statefull;
  private OServerCommand instance;

  public OServerCommandEntry(
      OServerCommandLoader loader,
      String pattern,
      Map<String, String> parameters,
      boolean statefull) {
    super();
    this.loader = loader;
    this.pattern = pattern;
    this.parameters = parameters;
    this.statefull = statefull;
  }

  public OServerCommand instance(OServer server) {
    if (statefull) {
      return loader.newInstance(pattern, parameters);
    } else {
      synchronized (this) {
        if (instance == null) {
          instance = loader.newInstance(pattern, parameters);
          ;
        }
        return instance;
      }
    }
  }

  public OServerCommandLoader getLoader() {
    return loader;
  }

  public boolean isStatefull() {
    return statefull;
  }
}
