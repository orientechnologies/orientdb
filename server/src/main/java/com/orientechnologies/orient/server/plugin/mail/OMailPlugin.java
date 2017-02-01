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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginConfigurable;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OMailPlugin extends OServerPluginAbstract implements OScriptInjection, OServerPluginConfigurable {
  private static final String       CONFIG_PROFILE_PREFIX = "profile.";
  private static final String       CONFIG_MAIL_PREFIX    = "mail.";

  private ODocument                 configuration;

  private Map<String, OMailProfile> profiles              = new HashMap<String, OMailProfile>();

  private String                    configFile            = "${ORIENTDB_HOME}/config/mail.json";

  public OMailPlugin() {
    Orient.instance().getScriptManager().registerInjection(this);
  }

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {

    configuration = new ODocument();

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        configuration.field("enabled", Boolean.parseBoolean(param.value));
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
          profile.put(profileParam, param.value);
        }
      }
    }
    List<Map<String, String>> profilesDocuments = new ArrayList<Map<String, String>>();
    for (String p : profiles.keySet()) {
      OMailProfile oMailProfile = profiles.get(p);
      Map<String, String> properties = new HashMap<String, String>();
      Set<String> strings = oMailProfile.stringPropertyNames();
      for (String string : strings) {
        properties.put(string, oMailProfile.getProperty(string));
      }
      properties.put("name", p);
      profilesDocuments.add(properties);
    }
    configuration.field("profiles", profilesDocuments);

    configure();

    OLogManager.instance().info(this, "Installing Mail plugin, loaded %d profile(s): %s", profiles.size(), profiles.keySet());
  }

  private void configure() {
    final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));
    if (f.exists()) {
      // READ THE FILE
      try {
        final String configurationContent = OIOUtils.readFileAsString(f);
        configuration = new ODocument().fromJSON(configurationContent);
      } catch (IOException e) {
        throw OException.wrapException(new OConfigurationException("Cannot load Mail configuration file '" + configFile
            + "'. Mail Plugin will be disabled"), e);
      }

    } else {
      try {
        f.getParentFile().mkdirs();
        f.createNewFile();
        OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));

        OLogManager.instance().info(this, "Mail plugin: migrated configuration to file '%s'", f);
      } catch (IOException e) {
        throw OException.wrapException(new OConfigurationException("Cannot create Mail plugin configuration file '" + configFile
            + "'. Mail Plugin will be disabled"), e);
      }
    }

    profiles.clear();

    Collection<Map<String, Object>> profilesInDocs = configuration.field("profiles");

    for (Map<String, Object> profile : profilesInDocs) {
      String name = (String) profile.get("name");
      OMailProfile p = profiles.get(name);
      if (p == null) {
        p = new OMailProfile();
        profiles.put(name, p);
      }

      for (String s : profile.keySet()) {
        if (!s.equalsIgnoreCase("name")) {
          p.put(s, profile.get(s).toString());
        }
      }
    }

  }

  public void writeConfiguration() throws IOException {

    final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));

    OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));
  }

  /**
   * Sends an email. Supports the following configuration: subject, message, to, cc, bcc, date, attachments
   *
   * @param iMessage
   *          Configuration as Map<String,Object>
   * @throws AddressException
   * @throws MessagingException
   * @throws ParseException
   */
  public void send(final Map<String, Object> iMessage) throws AddressException, MessagingException, ParseException {
    if (iMessage == null)
      throw new IllegalArgumentException("Configuration is null");

    final String profileName = (String) iMessage.get("profile");

    final OMailProfile profile = profiles.get(profileName);
    if (profile == null)
      throw new IllegalArgumentException("Mail profile '" + profileName + "' is not configured on server");

    // creates a new session with an authenticator
    Authenticator auth = new OSMTPAuthenticator((String) profile.getProperty("mail.smtp.user"),
        (String) profile.getProperty("mail.smtp.password"));


    final Session session = Session.getInstance(profile, auth);

    // creates a new e-mail message
    MimeMessage msg = new MimeMessage(session);

    final String from;
    if (iMessage.containsKey("from"))
      // GET THE 'FROM' FROM THE MESSAGE
      from = (String) iMessage.get("from");
    else
      // GET THE 'FROM' FROM PROFILE
      from = (String) profile.getProperty("mail.from");

    if (from != null)
      msg.setFrom(new InternetAddress(from));

    final String to = (String) iMessage.get("to");
    if (to != null && !to.isEmpty())
      msg.setRecipients(Message.RecipientType.TO, getEmails(to));

    final String cc = (String) iMessage.get("cc");
    if (cc != null && !cc.isEmpty())
      msg.setRecipients(Message.RecipientType.CC, getEmails(cc));

    final String bcc = (String) iMessage.get("bcc");
    if (bcc != null && !bcc.isEmpty())
      msg.setRecipients(Message.RecipientType.BCC, getEmails(bcc));

    msg.setSubject((String) iMessage.get("subject"));

    // DATE
    Object date = iMessage.get("date");
    final Date sendDate;
    if (date == null)
      // NOT SPECIFIED = NOW
      sendDate = new Date();
    else if (date instanceof Date)
      // PASSED
      sendDate = (Date) date;
    else {
      // FORMAT IT
      String dateFormat = (String) profile.getProperty("mail.date.format");
      if (dateFormat == null)
        dateFormat = "yyyy-MM-dd HH:mm:ss";
      sendDate = new SimpleDateFormat(dateFormat).parse(date.toString());
    }
    msg.setSentDate(sendDate);

    // creates message part
    MimeBodyPart messageBodyPart = new MimeBodyPart();
    messageBodyPart.setContent(iMessage.get("message"), "text/html");

    // creates multi-part
    Multipart multipart = new MimeMultipart();
    multipart.addBodyPart(messageBodyPart);

    final String[] attachments = (String[]) iMessage.get("attachments");
    // adds attachments
    if (attachments != null && attachments.length > 0) {
      for (String filePath : attachments) {
        addAttachment(multipart, filePath);
      }
    }

    // sets the multi-part as e-mail's content
    msg.setContent(multipart);

    // sends the e-mail
    Transport.send(msg);
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
    profiles.put(iName, iProfile);
    return this;
  }

  protected InternetAddress[] getEmails(final String iText) throws AddressException {
    if (iText == null)
      return null;

    final String[] items = iText.split(",");
    final InternetAddress[] addresses = new InternetAddress[items.length];
    for (int i = 0; i < items.length; ++i)
      addresses[i] = new InternetAddress(items[i]);
    return addresses;
  }

  /**
   * Adds a file as an attachment to the email's content
   *
   * @param multipart
   * @param filePath
   * @throws MessagingException
   */
  private void addAttachment(final Multipart multipart, final String filePath) throws MessagingException {
    MimeBodyPart attachPart = new MimeBodyPart();
    DataSource source = new FileDataSource(filePath);
    attachPart.setDataHandler(new DataHandler(source));
    attachPart.setFileName(new File(filePath).getName());
    multipart.addBodyPart(attachPart);
  }

  @Override
  public ODocument getConfig() {
    return configuration;
  }

  @Override
  public void changeConfig(ODocument document) {

    ODocument oldConfig = configuration;
    configuration = document;

    try {
      writeConfiguration();
    } catch (IOException e) {
      configuration = oldConfig;

      throw OException.wrapException(new OConfigurationException("Cannot Write Mail configuration file '" + configFile
          + "'. Restoring old configuration."), e);
    }
    configure();
  }
}
