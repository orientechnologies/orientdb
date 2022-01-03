/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.security.syslog;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.MessageFormat;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.cloudbees.syslog.sender.UdpSyslogMessageSender;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.security.OSyslog;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Provides a default implementation for syslog access.
 *
 * @author S. Colin Leister
 */
public class ODefaultSyslog extends OServerPluginAbstract implements OSyslog {
  private boolean debug = false;
  private String hostname = "localhost";
  private int port = 514; // Default syslog UDP port.
  private String appName = "OrientDB";

  private UdpSyslogMessageSender messageSender;

  // OSecurityComponent

  @Override
  public void startup() {
    try {
      if (isEnabled()) {
        messageSender = new UdpSyslogMessageSender();
        // _MessageSender.setDefaultMessageHostname("myhostname");
        // _MessageSender.setDefaultAppName(_AppName);
        // _MessageSender.setDefaultFacility(Facility.USER);
        // _MessageSender.setDefaultSeverity(Severity.INFORMATIONAL);
        messageSender.setSyslogServerHostname(hostname);
        messageSender.setSyslogServerPort(port);
        messageSender.setMessageFormat(MessageFormat.RFC_3164); // optional, default is RFC 3164
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultSyslog.active()", ex);
    }
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    enabled = false;

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        enabled = Boolean.parseBoolean(param.value);
        if (!enabled)
          // IGNORE THE REST OF CFG
          return;
      } else if (param.name.equalsIgnoreCase("debug")) {
        debug = Boolean.parseBoolean(param.value);
      } else if (param.name.equalsIgnoreCase("hostname")) {
        hostname = param.value;
      } else if (param.name.equalsIgnoreCase("port")) {
        port = Integer.parseInt(param.value);
      } else if (param.name.equalsIgnoreCase("appName")) {
        appName = param.value;
      }
    }
  }

  @Override
  public void shutdown() {
    messageSender = null;
  }

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }

  // OSyslog
  public void log(final String operation, final String message) {
    log(operation, null, null, message);
  }

  // OSyslog
  public void log(final String operation, final String username, final String message) {
    log(operation, null, username, message);
  }

  // OSyslog
  public void log(
      final String operation, final String dbName, final String username, final String message) {
    try {
      if (messageSender != null) {
        SyslogMessage sysMsg = new SyslogMessage();

        sysMsg.setFacility(Facility.USER);
        sysMsg.setSeverity(Severity.INFORMATIONAL);

        sysMsg.setAppName(appName);

        // Sylog ignores these settings.
        // if(operation != null) sysMsg.setMsgId(operation);
        // if(dbName != null) sysMsg.setProcId(dbName);

        StringBuilder sb = new StringBuilder();

        if (operation != null) {
          sb.append("[");
          sb.append(operation);
          sb.append("] ");
        }

        if (dbName != null) {
          sb.append("Database: ");
          sb.append(dbName);
          sb.append(" ");
        }

        if (username != null) {
          sb.append("Username: ");
          sb.append(username);
          sb.append(" ");
        }

        if (message != null) {
          sb.append(message);
        }

        sysMsg.withMsg(sb.toString());

        messageSender.sendMessage(sysMsg);
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultSyslog.log()", ex);
    }
  }

  @Override
  public String getName() {
    return "syslog";
  }
}
