package com.orientechnologies.orient.monitor.event;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.monitor.event.metric.OEventLogExecutor;
import com.orientechnologies.orient.monitor.event.metric.OEventMetricExecutor;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.mail.OMailPlugin;
import com.orientechnologies.orient.server.plugin.mail.OMailProfile;

@EventConfig(when = "LogWhen", what = "MailWhat")
public class OEventLogMailExecutor extends OEventLogExecutor {

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {

		// pre-conditions
		if (canExecute(source, when)) {
			mailEvent(what);
		}
	}

	public void mailEvent(ODocument what) {
		OMailPlugin mail = OServerMain.server().getPluginByClass(
				OMailPlugin.class);

		Map<String, Object> configuration = new HashMap<String, Object>();
		OMailProfile prof = new OMailProfile();
		prof.properties.put("mail.smtp.user", "");
		prof.properties.put("mail.smtp.password", "");
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
