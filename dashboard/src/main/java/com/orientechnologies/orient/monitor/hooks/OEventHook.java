package com.orientechnologies.orient.monitor.hooks;

import java.util.List;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.monitor.event.OEventController;
import com.orientechnologies.orient.monitor.event.OEventExecutor;

public class OEventHook extends ORecordHookAbstract {

	@Override
	public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onRecordAfterCreate(ORecord<?> iiRecord) {

		ODocument doc = (ODocument) iiRecord;

		final List<ODocument> triggers = doc.getDatabase().query(
				new OSQLSynchQuery<Object>("select from Event where when.name = '" + doc.field("name") + "'"));

		for (ODocument oDocument : triggers) {

			ODocument when = oDocument.field("when");
			ODocument what = oDocument.field("what");
			String classWhen = when.field("@class");
			String classWhat = what.field("@class");
			OEventExecutor executor = OEventController.getInstance().getExecutor(classWhen, classWhat);
			executor.execute(doc, when, what);
			/*
			 * if (what != null) { if (when.field("@class").equals("MetricsWhen") && toFireMetric(doc, when)) { if
			 * (what.field("@class").equals("MailWhat")) mailEvent(what); } }
			 */

		}

	}

	/*
	 * public void mailEvent(ODocument what) { OMailPlugin mail = OServerMain.server().getPluginByClass( OMailPlugin.class);
	 * 
	 * Map<String, Object> configuration = new HashMap<String, Object>(); OMailProfile prof = new OMailProfile();
	 * prof.properties.put("mail.smtp.user", ""); prof.properties.put("mail.smtp.password", ""); String subject =
	 * what.field("subject"); String address = what.field("toAddress");
	 * 
	 * String body = what.field("body"); configuration.put("to", address); configuration.put("profile", "default");
	 * configuration.put("message", subject); configuration.put("cc", address); configuration.put("bcc", address);
	 * configuration.put("subject", body);
	 * 
	 * // try { // mail.send(configuration); // } catch (AddressException e) { // e.printStackTrace(); // } catch (MessagingException
	 * e) { // e.printStackTrace(); // } catch (ParseException e) { // e.printStackTrace(); // }
	 * 
	 * }
	 */
}
