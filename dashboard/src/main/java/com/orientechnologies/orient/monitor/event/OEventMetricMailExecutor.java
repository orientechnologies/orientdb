/*
 * Copyright 2010-2013 Orient Technologies LTD
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 */

package com.orientechnologies.orient.monitor.event;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.event.metric.OEventMetricExecutor;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.mail.OMailPlugin;
import com.orientechnologies.orient.server.plugin.mail.OMailProfile;

@EventConfig(when = "MetricsWhen", what = "MailWhat")
public class OEventMetricMailExecutor extends OEventMetricExecutor {

  @Override
  public void execute(ODocument source, ODocument when, ODocument what) {

    if (canExecute(source, when)) {
      mailEvent(what);
    }
  }

  public void mailEvent(ODocument what) {
    OMailPlugin mail = OServerMain.server().getPluginByClass(OMailPlugin.class);

    Map<String, Object> configuration = new HashMap<String, Object>();
    OMailProfile prof = new OMailProfile();
    prof.put("mail.smtp.user", "");
    prof.put("mail.smtp.password", "");
    String subject = what.field("subject");
    String address = what.field("toAddress");

    String body = what.field("body");
    configuration.put("to", address);
    configuration.put("profile", "default");
    configuration.put("message", subject);
    configuration.put("cc", address);
    configuration.put("bcc", address);
    configuration.put("subject", body);

    try {
      mail.send(configuration);
    } catch (AddressException e) {
      e.printStackTrace();
    } catch (MessagingException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    }

  }

}
