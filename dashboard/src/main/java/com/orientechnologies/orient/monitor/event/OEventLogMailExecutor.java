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
import java.util.List;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.monitor.event.metric.OEventLogExecutor;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.mail.OMailPlugin;
import com.orientechnologies.orient.server.plugin.mail.OMailProfile;

@EventConfig(when = "LogWhen", what = "MailWhat")
public class OEventLogMailExecutor extends OEventLogExecutor {

	private ODocument oUserConfiguration;
	private OMailPlugin mailPlugin;

	public OEventLogMailExecutor(ODatabaseDocumentTx database) {

		String sql = "select from UserConfiguration where user.name = 'admin'";
		OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(
				sql);
		final List<ODocument> response = database.query(osqlQuery);
		ODocument configuration = null;
		if (response.size() == 1) {
			configuration = response.get(0).field("mailProfile");
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
			configuration.field("host", "192.168.0.50");
			configuration.field("dateFormat", "yyyy-MM-dd HH:mm:ss");
			ODocument userconfiguration = response != null
					&& !response.isEmpty() ? response.get(0) : new ODocument(
					"UserConfiguration");

			sql = "select from OUser where name = 'admin'";
			osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(sql);
			final List<ODocument> users = database.query(osqlQuery);
			if (users.size() == 1) {
				final ODocument ouserAdmin = users.get(0);
				userconfiguration.field("user", ouserAdmin);
				userconfiguration.field("mailProfile", configuration);
				userconfiguration.save();
				database.commit();
			}
		}
		oUserConfiguration = configuration;
	}

	@Override
	public void execute(ODocument source, ODocument when, ODocument what) {

		// pre-conditions
		if (canExecute(source, when)) {
			mailEvent(what);
		}
	}

	public void mailEvent(ODocument what) {
		if (mailPlugin == null) {
			mailPlugin = OServerMain.server().getPluginByClass(
					OMailPlugin.class);

			OMailProfile enterpriseProfile = new OMailProfile();
			enterpriseProfile.put("mail.smtp.user",
					oUserConfiguration.field("user"));
			enterpriseProfile.put("mail.smtp.password",
					oUserConfiguration.field("password"));
			enterpriseProfile.put("mail.smtp.port",
					oUserConfiguration.field("port"));
			enterpriseProfile.put("mail.smtp.auth",
					oUserConfiguration.field("auth"));
			enterpriseProfile.put("mail.smtp.host",
					oUserConfiguration.field("host"));
			enterpriseProfile.put("enabled",
					oUserConfiguration.field("enabled"));
			enterpriseProfile.put("mail.smtp.starttls.enable",
					oUserConfiguration.field("starttlsEnable"));
			enterpriseProfile.put("mail.date.format",
					oUserConfiguration.field("dateFormat"));

			mailPlugin.registerProfile("enterprise", enterpriseProfile);
		}

		final Map<String, Object> configuration = new HashMap<String, Object>();
		String subject = what.field("subject");
		String address = what.field("toAddress");
		String fromAddress = what.field("fromAddress");
		String cc = what.field("cc");
		String bcc = what.field("bcc");
		String body = what.field("body");
		configuration.put("to", address);
		configuration.put("from", fromAddress);
		configuration.put("profile", "enterprise");
		configuration.put("message", body);
		configuration.put("cc", cc);
		configuration.put("bcc", bcc);
		configuration.put("subject", subject);

		try {
			mailPlugin.send(configuration);
		} catch (AddressException e) {
			e.printStackTrace();
		} catch (MessagingException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}
}
