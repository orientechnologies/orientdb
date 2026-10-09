package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class OServerSideScriptInterpreterConfig {

  private boolean enabled;
  private Set<String> allowedLanguages = Collections.emptySet();
  private Set<String> allowedPackages = Collections.emptySet();

  public static OServerSideScriptInterpreterConfig fromParameters(
      OServerParameterConfiguration[] iParams) {
    OServerSideScriptInterpreterConfig config = new OServerSideScriptInterpreterConfig();
    for (OServerParameterConfiguration param : iParams) {
      if (param.getName().equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.getValue()))
          // ENABLE IT
          config.enabled = true;
      } else if (param.getName().equalsIgnoreCase("allowedLanguages")) {
        config.allowedLanguages =
            new HashSet<>(Arrays.asList(param.getValue().toLowerCase(Locale.ENGLISH).split(",")));
        if (config.allowedLanguages.contains("javascript")) {
          config.allowedLanguages.add("js");
        }
      } else if (param.getName().equalsIgnoreCase("allowedPackages")) {
        config.allowedPackages = new HashSet<>(Arrays.asList(param.getValue().split(",")));
      }
    }
    return config;
  }

  public Set<String> getAllowedLanguages() {
    return allowedLanguages;
  }

  public Set<String> getAllowedPackages() {
    return allowedPackages;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
