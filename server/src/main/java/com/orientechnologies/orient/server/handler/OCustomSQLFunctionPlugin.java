package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OCustomSQLFunctionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Server Plugin to register custom SQL functions.
 *
 * @author Fabrizio Fortino
 */
public class OCustomSQLFunctionPlugin extends OServerPluginAbstract {

  private static final char PREFIX_NAME_SEPARATOR = '_';

  private ODocument configuration;

  @Override
  public String getName() {
    return "custom-sql-functions-manager";
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    configuration = new ODocument();

    final File configFile =
        Arrays.stream(iParams)
            .filter(p -> p.name.equalsIgnoreCase("config"))
            .map(p -> p.value.trim())
            .map(OSystemVariableResolver::resolveSystemVariables)
            .map(File::new)
            .filter(File::exists)
            .findFirst()
            .orElseThrow(
                () ->
                    new OConfigurationException(
                        "Custom SQL functions configuration file not found"));

    try {
      String configurationContent = OIOUtils.readFileAsString(configFile);
      configurationContent = removeComments(configurationContent);
      configuration = new ODocument().fromJSON(configurationContent);
    } catch (IOException e) {
      throw OException.wrapException(
          new OConfigurationException(
              "Cannot load Custom SQL configuration file '"
                  + configFile
                  + "'. No custom functions will be disabled"),
          e);
    }
  }

  private String removeComments(String configurationContent) {
    if (configurationContent == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    String[] split = configurationContent.split("\n");
    boolean first = true;
    for (int i = 0; i < split.length; i++) {
      String row = split[i];
      if (row.trim().startsWith("//")) {
        continue;
      }
      if (!first) {
        result.append("\n");
      }
      result.append(row);
      first = false;
    }
    return result.toString();
  }

  @Override
  public void startup() {
    if (Boolean.TRUE.equals(configuration.field("enabled"))) {
      List<Map<String, String>> functions = configuration.field("functions");
      for (Map<String, String> function : functions) {
        final String prefix = function.get("prefix");
        final String clazz = function.get("class");
        if (prefix == null || clazz == null) {
          throw new OConfigurationException(
              "Unable to load functions without prefix and / or class ");
        }
        if (!prefix.matches("^[\\pL]+$")) {
          throw new OConfigurationException(
              "Unable to load functions with prefix '"
                  + prefix
                  + "'. Prefixes can be letters only");
        }

        try {
          Class functionsClass = Class.forName(clazz);
          OCustomSQLFunctionFactory.register(prefix + PREFIX_NAME_SEPARATOR, functionsClass);
        } catch (ClassNotFoundException e) {
          throw OException.wrapException(
              new OConfigurationException(
                  "Unable to load class " + clazz + " for custom functions with prefix " + prefix),
              e);
        }
      }
    }
  }
}
