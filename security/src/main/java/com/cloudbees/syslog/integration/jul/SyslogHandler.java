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
package com.cloudbees.syslog.integration.jul;

import com.cloudbees.syslog.Facility;
import com.cloudbees.syslog.Severity;
import com.cloudbees.syslog.SyslogMessage;
import com.cloudbees.syslog.integration.jul.util.LevelHelper;
import com.cloudbees.syslog.integration.jul.util.LogManagerHelper;
import com.cloudbees.syslog.sender.SyslogMessageSender;
import com.cloudbees.syslog.sender.UdpSyslogMessageSender;
import java.io.Closeable;
import java.io.IOException;
import java.util.logging.*;

/** @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a> */
public class SyslogHandler extends AbstractHandler {

  private SyslogMessageSender syslogMessageSender;

  private String appName;
  private Facility facility = Facility.USER;
  private Severity severity = Severity.DEBUG;
  private String messageHostname;

  public SyslogHandler() {
    super();
    LogManager manager = LogManager.getLogManager();

    String cname = getClass().getName();

    UdpSyslogMessageSender udpSender = new UdpSyslogMessageSender();
    udpSender.setSyslogServerHostname(
        LogManagerHelper.getStringProperty(
            manager, cname + ".syslogServerHostname", SyslogMessageSender.DEFAULT_SYSLOG_HOST));
    udpSender.setSyslogServerPort(
        LogManagerHelper.getIntProperty(
            manager, cname + ".syslogServerPort", SyslogMessageSender.DEFAULT_SYSLOG_PORT));

    appName = LogManagerHelper.getStringProperty(manager, cname + ".appName", this.appName);
    udpSender.setDefaultAppName(appName);
    facility =
        Facility.fromLabel(
            LogManagerHelper.getStringProperty(
                manager, cname + ".facility", this.facility.label()));
    udpSender.setDefaultFacility(facility);
    severity =
        Severity.fromLabel(
            LogManagerHelper.getStringProperty(
                manager, cname + ".severity", this.severity.label()));
    udpSender.setDefaultSeverity(severity);
    messageHostname =
        LogManagerHelper.getStringProperty(
            manager, cname + ".messageHostname", this.messageHostname);
    udpSender.setDefaultMessageHostname(messageHostname);

    this.syslogMessageSender = udpSender;
  }

  public SyslogHandler(SyslogMessageSender syslogMessageSender) {
    this(syslogMessageSender, Level.INFO, null);
  }

  public SyslogHandler(SyslogMessageSender syslogMessageSender, Level level, Filter filter) {
    super(level, filter);
    this.syslogMessageSender = syslogMessageSender;
  }

  @Override
  protected Formatter getDefaultFormatter() {
    return new SyslogMessageFormatter();
  }

  @Override
  public void publish(LogRecord record) {
    if (!isLoggable(record)) return;

    String msg = getFormatter().format(record);

    Severity severity = LevelHelper.toSeverity(record.getLevel());
    if (severity == null) severity = this.severity;

    SyslogMessage message =
        new SyslogMessage()
            .withTimestamp(record.getMillis())
            .withSeverity(severity)
            .withAppName(this.appName)
            .withHostname(this.messageHostname)
            .withFacility(this.facility)
            .withMsg(msg);

    try {
      syslogMessageSender.sendMessage(message);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws SecurityException {
    if (syslogMessageSender instanceof Closeable) {
      try {
        ((Closeable) syslogMessageSender).close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public String getEncoding() {
    throw new IllegalStateException();
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public Facility getFacility() {
    return facility;
  }

  public void setFacility(Facility facility) {
    this.facility = facility;
  }

  public Severity getSeverity() {
    return severity;
  }

  public void setSeverity(Severity severity) {
    this.severity = severity;
  }

  public String getMessageHostname() {
    return messageHostname;
  }

  public void setMessageHostname(String messageHostname) {
    this.messageHostname = messageHostname;
  }

  public SyslogMessageSender getSyslogMessageSender() {
    return syslogMessageSender;
  }

  public void setSyslogMessageSender(SyslogMessageSender syslogMessageSender) {
    this.syslogMessageSender = syslogMessageSender;
  }
}
