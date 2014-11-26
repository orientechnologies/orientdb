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
package com.orientechnologies.orient.server.plugin.mail;

import com.orientechnologies.common.log.OLogManager;
 import com.orientechnologies.orient.core.Orient;
 import com.orientechnologies.orient.core.command.script.OScriptInjection;
 import com.orientechnologies.orient.server.OServer;
 import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
 import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

 import javax.activation.DataHandler;
 import javax.activation.DataSource;
 import javax.activation.FileDataSource;
 import javax.mail.Authenticator;
 import javax.mail.Message;
 import javax.mail.MessagingException;
 import javax.mail.Multipart;
 import javax.mail.Session;
 import javax.mail.Transport;
 import javax.mail.internet.AddressException;
 import javax.mail.internet.InternetAddress;
 import javax.mail.internet.MimeBodyPart;
 import javax.mail.internet.MimeMessage;
 import javax.mail.internet.MimeMultipart;
 import javax.script.Bindings;
 import java.io.File;
 import java.text.ParseException;
 import java.text.SimpleDateFormat;
 import java.util.Date;
 import java.util.HashMap;
 import java.util.Map;
 import java.util.Set;

public class OMailPlugin extends OServerPluginAbstract implements OScriptInjection {
   private static final String       CONFIG_PROFILE_PREFIX = "profile.";
   private static final String       CONFIG_MAIL_PREFIX    = "mail.";

   private Map<String, OMailProfile> profiles              = new HashMap<String, OMailProfile>();

   public OMailPlugin() {
     Orient.instance().getScriptManager().registerInjection(this);
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
           profile.put(profileParam, param.value);
         }
       }
     }

     OLogManager.instance().info(this, "Installing Mail plugin, loaded %d profile(s): %s", profiles.size(), profiles.keySet());
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
   public void bind(final Bindings binding) {
     binding.put("mail", this);
   }

   @Override
   public void unbind(final Bindings binding) {
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
 }
