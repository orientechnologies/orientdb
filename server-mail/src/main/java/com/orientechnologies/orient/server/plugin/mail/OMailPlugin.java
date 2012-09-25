/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.plugin.mail;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

public class OMailPlugin extends OServerHandlerAbstract {
  private static final String       CONFIG_PROFILE_PREFIX = "profile.";
  private static final String       CONFIG_MAIL_PREFIX    = "mail.";

  private Map<String, OMailProfile> profiles              = new HashMap<String, OMailProfile>();

  public OMailPlugin() {
  }

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.startsWith(CONFIG_PROFILE_PREFIX)) {
        final String parts = param.name.substring(CONFIG_PROFILE_PREFIX.length());
        int pos = parts.indexOf('.');
        if (pos == -1)
          continue;

        final String profileName = parts.substring(0, pos);
        final String profileParam = parts.substring(pos + 1);

        OMailProfile profile = profiles.get(profileName);
        if (profile == null) {
          profile = new OMailProfile();
          profiles.put(profileName, profile);
        }

        if (profileParam.startsWith(CONFIG_MAIL_PREFIX)) {
          profile.properties.setProperty("mail." + profileParam.substring(CONFIG_MAIL_PREFIX.length()), param.value);
        }
      }
    }

    OLogManager.instance().info(this, "Mail plugin installed and active. Loaded %d profile(s): %s", profiles.size(),
        profiles.keySet());
  }

  @Override
  public void shutdown() {
  }

  public void sendEmail(final Map<String, Object> iConfiguration) {

  }

  @Override
  public String getName() {
    return "mail";
  }
}
