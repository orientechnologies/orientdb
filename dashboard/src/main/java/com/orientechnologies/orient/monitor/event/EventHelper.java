package com.orientechnologies.orient.monitor.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.plugin.mail.OMailProfile;

public class EventHelper {

	public static Object resolve(final Map<String, Object> body2name2,
			final Object iContent) {
		Object value = null;
		if (iContent instanceof String)
			value = OVariableParser.resolveVariables((String) iContent,
					OSystemVariableResolver.VAR_BEGIN,
					OSystemVariableResolver.VAR_END,
					new OVariableParserListener() {

						@Override
						public Object resolve(final String iVariable) {
							return body2name2.get(iVariable);
						}

					});
		else
			value = iContent;

		return value;
	}

	public static String replaceText(Map<String, Object> body2name, String body) {

		String[] splitBody = body.split(" ");

		for (String word : splitBody) {
			String resolvedWord = (String) resolve(body2name, word);
			body = body.replace(word, resolvedWord);
		}

		return body;
	}

	public static Map<String, Object> createConfiguration(ODocument what,
			Map<String, Object> body2name) {

		Map<String, Object> configuration = new HashMap<String, Object>();

		String subject = what.field("subject");
		String address = what.field("toAddress");
		String fromAddress = what.field("fromAddress");
		String cc = what.field("cc");
		String bcc = what.field("bcc");
		String body = what.field("body");

		body = replaceText(body2name, body);

		configuration.put("to", address);
		configuration.put("from", fromAddress);
		configuration.put("profile", "enterprise");
		configuration.put("message", body);
		configuration.put("cc", cc);
		configuration.put("bcc", bcc);
		configuration.put("subject", subject);

		return configuration;
	}

	public static OMailProfile createOMailProfile(ODocument oUserConfiguration) {
		
		
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
		return enterpriseProfile;
	}

	public static ODocument findOrCreateMailUserConfiguration(ODatabaseDocumentTx database) {
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
		
		return configuration;
		
	}

}