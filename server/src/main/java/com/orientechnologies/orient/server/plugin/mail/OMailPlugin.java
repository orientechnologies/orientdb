/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.plugin.mail;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginConfigurable;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.script.Bindings;
import javax.script.ScriptEngine;

public class OMailPlugin extends OServerPluginAbstract
    implements OScriptInjection, OServerPluginConfigurable {
  private static final String CONFIG_PROFILE_PREFIX = "profile.";
  private static final String CONFIG_MAIL_PREFIX = "mail.";

  private ODocument configuration;

  private Map<String, OMailProfile> profiles = new HashMap<String, OMailProfile>();

  private String configFile = "${ORIENTDB_HOME}/config/mail.json";

  public OMailPlugin() {}

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {

    OrientDBInternal.extract(oServer.getContext()).getScriptManager().registerInjection(this);
  }

  public void writeConfiguration() throws IOException {}

  /**
   * Sends an email. Supports the following configuration: subject, message, to, cc, bcc, date,
   * attachments
   *
   * @param iMessage Configuration as Map<String,Object>
   * @throws ParseException
   */
  public void send(final Map<String, Object> iMessage) {
    OLogManager.instance().warn(this, "Mail send is non available in this OrientDB version");
  }

  @Override
  public void bind(ScriptEngine engine, Bindings binding, ODatabaseDocument database) {
    binding.put("mail", this);
  }

  @Override
  public void unbind(ScriptEngine engine, Bindings binding) {
    binding.put("mail", null);
  }

  @Override
  public String getName() {
    return "mail";
  }

  public Set<String> getProfileNames() {
    return profiles.keySet();
  }

  public OMailProfile getProfile(final String iName) {
    return profiles.get(iName);
  }

  public OMailPlugin registerProfile(final String iName, final OMailProfile iProfile) {
    return this;
  }

  @Override
  public ODocument getConfig() {
    return configuration;
  }

  @Override
  public void changeConfig(ODocument document) {}
}
