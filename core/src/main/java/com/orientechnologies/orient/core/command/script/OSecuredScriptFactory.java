package com.orientechnologies.orient.core.command.script;

import java.util.HashSet;
import java.util.Set;
import javax.script.ScriptEngineFactory;

public abstract class OSecuredScriptFactory implements ScriptEngineFactory {

  protected Set<String> packages = new HashSet<>();

  public void addAllowedPackages(Set<String> packages) {

    this.packages.addAll(packages);
  }

  public Set<String> getPackages() {
    return packages;
  }

  public void removeAllowedPackages(Set<String> packages) {
    this.packages.removeAll(packages);
  }
}
