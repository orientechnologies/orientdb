/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OHttpNetworkCommandManager {

  private static final String               URL_PART_PATTERN = "([a-zA-Z0-9%:\\\\+]*)";

  private final Map<String, OServerCommand> exactCommands    = new ConcurrentHashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> wildcardCommands = new ConcurrentHashMap<String, OServerCommand>();
  private final Map<String, OServerCommand> restCommands     = new ConcurrentHashMap<String, OServerCommand>();
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

  public Map<String, String> extractUrlTokens(String requestUrl) {
    Map<String, String> result = new HashMap<String, String>();
    String urlPattern = findUrlPattern(requestUrl);
    if (urlPattern == null) {
      return result;
    }
    String matcherUrl = OPatternConst.PATTERN_REST_URL.matcher(urlPattern).replaceAll(URL_PART_PATTERN);

    matcherUrl = matcherUrl.substring(matcherUrl.indexOf('|') + 1);
    requestUrl = requestUrl.substring(requestUrl.indexOf('|') + 1);

    Pattern pattern = Pattern.compile(matcherUrl);
    Matcher matcher = pattern.matcher(requestUrl);
    if (matcher.find()) {
      Matcher templateMatcher = OPatternConst.PATTERN_REST_URL.matcher(urlPattern);
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

  private boolean matches(String urlPattern, String requestUrl) {
    String matcherUrl = OPatternConst.PATTERN_REST_URL.matcher(urlPattern).replaceAll(URL_PART_PATTERN);

    if (!matcherUrl.substring(0, matcherUrl.indexOf('|') + 1).equals(requestUrl.substring(0, requestUrl.indexOf('|') + 1))) {
      return false;
    }
    matcherUrl = matcherUrl.substring(matcherUrl.indexOf('|') + 1);
    requestUrl = requestUrl.substring(requestUrl.indexOf('|') + 1);
    return requestUrl.matches(matcherUrl);
  }
}
