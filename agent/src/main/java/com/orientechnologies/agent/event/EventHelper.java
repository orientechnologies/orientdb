/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.event;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHelper {

  private final static String VAR_BEGIN  = "$";

  private final static String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.69 Safari/537.36";

  public static Object resolve(final Map<String, Object> body2name2, final Object iContent) {
    Object value = null;
    if (iContent instanceof String)
      value = resolveVariables((String) iContent, VAR_BEGIN, OSystemVariableResolver.VAR_END, new OVariableParserListener() {

        @Override
        public Object resolve(final String iVariable) {
          return body2name2.get(iVariable);
        }

      });
    else
      value = iContent;

    return value;
  }

  public static Object resolveVariables(final String iText, final String iBegin, final String iEnd,
      final OVariableParserListener iListener) {
    if (iListener == null)
      throw new IllegalArgumentException("Missed VariableParserListener listener");

    int beginPos = iText.lastIndexOf(iBegin);
    if (beginPos == -1)
      return iText;

    // int endPos = iText.indexOf(iEnd, beginPos + 1);
    // if (endPos == -1)
    // return iText;

    String pre = iText.substring(0, beginPos);
    // String var = iText.substring(beginPos + iBegin.length(), endPos);
    String var = iText.substring(beginPos + iBegin.length());
    // String post = iText.substring(endPos + iEnd.length());

    Object resolved = iListener.resolve(var);

    if (resolved == null) {
      OLogManager.instance().warn(null, "[OVariableParser.resolveVariables] Error on resolving property: %s", var);
      // resolved = "null";
    }

    if (pre.length() > 0) {
      final String path = pre + (resolved != null ? resolved.toString() : "");
      return resolveVariables(path, iBegin, iEnd, iListener);
    }

    return resolved;
  }

  public static String replaceText(Map<String, Object> body2name, String body) {

    String[] splitBody = body.split(" ");

    for (String word : splitBody) {
      String resolvedWord = (String) resolve(body2name, word);
      if (resolvedWord != null)
        body = body.replace(word, resolvedWord);
    }

    return body;
  }

  public static Map<String, Object> createConfiguration(ODocument what, Map<String, Object> body2name) {

    Map<String, Object> configuration = new HashMap<String, Object>();

    String profile = "default";
    String subject = what.field("subject");
    String address = what.field("toAddress");
    String fromAddress = what.field("fromAddress");
    String cc = what.field("cc");
    String bcc = what.field("bcc");
    String body = what.field("body");

    if (body != null) {
      body = replaceText(body2name, body);
      body = replaceMarkers(body);
    }
    configuration.put("to", address);
    configuration.put("from", fromAddress);
    if (what.field("profile") != null) {
      profile = what.field("profile");
    }
    configuration.put("profile", profile);
    configuration.put("message", body);
    configuration.put("cc", cc);
    configuration.put("bcc", bcc);
    configuration.put("subject", subject);

    return configuration;
  }

  public static String replaceMarkers(String text) {
    if (text != null) {
      text = text.replaceAll("&", "&amp;");
      text = text.replaceAll("���", "&egrave;");
      text = text.replaceAll("���", "&eacute;");
      text = text.replaceAll("���", "&ograve;");
      text = text.replaceAll("���", "&agrave;");
      text = text.replaceAll("���", "&ugrave;");
      text = text.replaceAll("���", "&igrave;");
      text = text.replaceAll("<", "&lt;");
      text = text.replaceAll(">", "&gt;");
      text = text.replaceAll("\u2018", "&lsquo;");
      text = text.replaceAll("\u2019", "&rsquo;");
      text = text.replaceAll("'", "&rsquo;");
      text = text.replaceAll("\n", "<br/>");
    }

    return text;
  }

  public static ODocument findOrCreateMailUserConfiguration(ODatabaseDocument database) {
    String sql = "select from UserConfiguration where user.name = 'admin'";
    OSQLQuery<ORecordSchemaAware> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware>(sql);
    final List<ODocument> response = database.query(osqlQuery);
    ODocument configuration = null;
    ODocument userconfiguration = null;

    if (response.size() == 1) {
      userconfiguration = response.get(0);
      configuration = userconfiguration.field("mailProfile");
    }
    // mail = OServerMain.server().getPluginByClass(OMailPlugin.class);
    if (configuration == null) {
      configuration = new ODocument("OMailProfile");
      configuration.field("user", "");
      configuration.field("password", "");
      configuration.field("enabled", true);
      configuration.field("starttlsEnable", true);
      configuration.field("auth", true);
      configuration.field("port", 25);
      configuration.field("host", "");
      configuration.field("dateFormat", "yyyy-MM-dd HH:mm:ss");
      configuration.field("@type", "d");

      sql = "select from OUser where name = 'admin'";
      osqlQuery = new OSQLSynchQuery<ORecordSchemaAware>(sql);
      final List<ODocument> users = database.query(osqlQuery);
      if (users.size() == 1) {
        userconfiguration = new ODocument("UserConfiguration");
        final ODocument ouserAdmin = users.get(0);
        userconfiguration.field("user", ouserAdmin);
        userconfiguration.field("mailProfile", configuration);
        userconfiguration.field("orientdbSite", "http://www.orientechnologies.com/");
        userconfiguration.save();
      } else {
        throw new OConfigurationException("user admin not found");
      }
    }

    return configuration;

  }

  public static void executeHttpRequest(ODocument what) throws MalformedURLException {

    String url = what.field("url");
    String method = what.field("method"); // GET POST
    String body = what.field("body"); // parameters

    try {
      HttpURLConnection con = null;
      URL obj = new URL(url);
      con = (HttpURLConnection) obj.openConnection();

      Collection<Map<String, Object>> headers = what.field("headers");
      con.setRequestMethod(method);
      // add request header
      con.setRequestProperty("User-Agent", USER_AGENT);

      // ADD CUSTOM HEADERS
      if (headers != null) {
        for (Map<String, Object> header : headers) {
          String name = (String) header.get("name");
          String value = (String) header.get("value");
          con.setRequestProperty(name, value);
        }
      }
      if ("GET".equalsIgnoreCase(method)) {
        doGet(con);
      } else if ("POST".equalsIgnoreCase(method)) {
        doPost(body, con);
      }
    } catch (Exception e) {
      OLogManager.instance().error(null, "Failed to execute request with config: %s", e, what.toJSON());
    }

  }

  private static void doPost(String parameters, HttpURLConnection con) throws IOException {
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(parameters);
    wr.flush();
    wr.close();
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    OLogManager.instance().info(null, "HTTP result: %s", response.toString());
    in.close();
  }

  private static void doGet(HttpURLConnection con) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }

    OLogManager.instance().debug(null, "HTTP result: %s", response.toString());
    in.close();
  }
}