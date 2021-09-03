package com.orientechnologies.orient.core.command.script.js;

import com.orientechnologies.orient.core.command.script.OSecuredScriptFactory;
import jdk.nashorn.api.scripting.ClassFilter;

public class OSecuredClassFilter implements ClassFilter {

  private OSecuredScriptFactory factory;

  public OSecuredClassFilter(final OSecuredScriptFactory factory) {
    this.factory = factory;
  }

  @Override
  public boolean exposeToScripts(String s) {
    return factory.getPackages().stream()
        .map(e -> s.matches(e))
        .filter(f -> f)
        .findFirst()
        .isPresent();
  }
}
