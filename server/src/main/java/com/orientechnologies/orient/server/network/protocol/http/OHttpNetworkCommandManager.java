package com.orientechnologies.orient.server.network.protocol.http;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;

public class OHttpNetworkCommandManager {

  private static final String               URL_PART_PATTERN    = "([a-zA-Z0-9%:\\\\+]*)";
  private static final String               REST_PARAM_PATTERN  = "\\{[a-zA-Z0-9%:]*\\}";

  private final Map<String, OServerCommand> exactCommands      = new HashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> wildcardCommands   = new HashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> restCommands       = new HashMap<String, OServerCommand>();
  private final OHttpNetworkCommandManager  parent;
  private final OServer                     server;

  public OHttpNetworkCommandManager(final OServer iServer, final OHttpNetworkCommandManager iParent) {
    server = iServer;
    parent = iParent;
  }

  public Object getCommand(final String iName) {
    OServerCommand cmd = exactCommands.get(iName);

    if (cmd == null) {
      for (Entry<String, OServerCommand> entry : restCommands.entrySet()) {
        if (matches(entry.getKey(), iName)) {
          return entry.getValue();
        }
      }
    }
    if (cmd == null) {
      // TRY WITH WILDCARD COMMANDS
      // TODO: OPTIMIZE SEARCH!
      String partLeft, partRight;
      for (Entry<String, OServerCommand> entry : wildcardCommands.entrySet()) {
        final int wildcardPos = entry.getKey().indexOf('*');
        partLeft = entry.getKey().substring(0, wildcardPos);
        partRight = entry.getKey().substring(wildcardPos + 1);

        if (iName.startsWith(partLeft) && iName.endsWith(partRight)) {
          cmd = entry.getValue();
          break;
        }
      }
    }

    if (cmd == null && parent != null)
      cmd = (OServerCommand) parent.getCommand(iName);

    return cmd;
  }

  /**
   * Register all the names for the same instance.
   * 
   * @param iServerCommandInstance
   */
  public void registerCommand(final OServerCommand iServerCommandInstance) {
    for (String name : iServerCommandInstance.getNames())
      if (OStringSerializerHelper.contains(name, '{')) {
        restCommands.put(name, iServerCommandInstance);
      } else if (OStringSerializerHelper.contains(name, '*'))
        wildcardCommands.put(name, iServerCommandInstance);
      else
        exactCommands.put(name, iServerCommandInstance);
    iServerCommandInstance.configure(server);
  }

  private boolean matches(String urlPattern, String requestUrl) {
    String matcherUrl = urlPattern.replaceAll(REST_PARAM_PATTERN, URL_PART_PATTERN);

    if (!matcherUrl.substring(0, matcherUrl.indexOf('|') + 1).equals(requestUrl.substring(0, requestUrl.indexOf('|') + 1))) {
      return false;
    }
    matcherUrl = matcherUrl.substring(matcherUrl.indexOf('|') + 1);
    requestUrl = requestUrl.substring(requestUrl.indexOf('|') + 1);
    return requestUrl.matches(matcherUrl);
  }

  public Map<String, String> extractUrlTokens(String requestUrl) {
    Map<String, String> result = new HashMap<String, String>();
    String urlPattern = findUrlPattern(requestUrl);
    if (urlPattern == null) {
      return result;
    }
    String matcherUrl = urlPattern.replaceAll(REST_PARAM_PATTERN, URL_PART_PATTERN);

    matcherUrl = matcherUrl.substring(matcherUrl.indexOf('|') + 1);
    requestUrl = requestUrl.substring(requestUrl.indexOf('|') + 1);

    Pattern pattern = Pattern.compile(matcherUrl);
    Matcher matcher = pattern.matcher(requestUrl);
    if (matcher.find()) {
      Pattern templateUrlPattern = Pattern.compile(REST_PARAM_PATTERN);
      Matcher templateMatcher = templateUrlPattern.matcher(urlPattern);
      int i = 1;
      String key;
      while (templateMatcher.find()) {
        key = templateMatcher.group();
        key = key.substring(1);
        key = key.substring(0, key.length() - 1);
        String value = matcher.group(i++);
        result.put(key, value);
      }
    }
    return result;
  }

  protected String findUrlPattern(String requestUrl) {
    for (Entry<String, OServerCommand> entry : restCommands.entrySet()) {
      if (matches(entry.getKey(), requestUrl)) {
        return entry.getKey();
      }
    }
    if (parent == null) {
      return null;
    } else {
      return parent.findUrlPattern(requestUrl);
    }
  }
}
