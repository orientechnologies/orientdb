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
package com.orientechnologies.workbench;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public final class OWorkbenchPurgeMetricLogHelper {

	/**
	 * 
	 * @param hour
	 * @param db
	 * <br>
	 *          delete metrics older than @hour
	 */
	public static void deleteMetrics(Integer hour, ODatabaseDocumentTx db) {
		if (hour != null && hour != 0) {

			String osql = "select from Metric where snapshot.dateFrom <= :dateFrom order by snapshot.dateFrom";
			final Map<String, Object> params = new HashMap<String, Object>();

			OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);

			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.HOUR, -hour);
			params.put("dateFrom", calendar.getTime());

			List<ODocument> metrics = db.query(osqlQuery, params);

			purgeMetricAndSnapshot(metrics);
		}
	}

	/**
	 * 
	 * @param hour
	 * @param db
	 * <br>
	 *          delete logs older than @hour
	 */
	public static void deleteLogs(Integer hour, ODatabaseDocumentTx db) {
		if (hour != null && hour != 0) {

			String osql = "select from Log where date <= :dateFrom";
			final Map<String, Object> params = new HashMap<String, Object>();

			OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);

			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.HOUR, -hour);
			params.put("dateFrom", calendar.getTime());

			List<ODocument> logs = db.query(osqlQuery, params);

			purgeLogs(logs);
		}
	}

	/**
	 * 
	 * @param db
	 * <br>
	 *          delete all metrics
	 */
	public static void purgeMetricNow(ODatabaseDocumentTx db) {
		purgeMetricNow(null, db);
	}

	/**
	 * 
	 * @param db
	 * <br>
	 *          delete all metrics
	 */
	public static void purgeMetricNow(ODocument server, ODatabaseDocumentTx db) {
		String osql = "select from Metric";
		Map<String, Object> params = new HashMap<String, Object>();
		if (server != null) {
			osql += " where snapshot.server = :server";
			params.put("server", server);
		}
		osql+= " order by snapshot.dateFrom";
		OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);

		List<ODocument> metrics = db.query(osqlQuery, params);

		purgeMetricAndSnapshot(metrics);

	}

	/**
	 * 
	 * @param db
	 * <br>
	 *          delete all logs
	 */
	public static void purgeLogsNow(ODatabaseDocumentTx db) {
		purgeLogsNow(null, db);

	}

	/**
	 * 
	 * @param db
	 * <br>
	 *          delete all logs
	 */
	public static void purgeLogsNow(ODocument server, ODatabaseDocumentTx db) {
		String osql = "select from Log";

		Map<String, Object> params = new HashMap<String, Object>();
		if (server != null) {
			osql += " where server = :server";
		}
		params.put("server", server);
		OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);

		List<ODocument> logs = db.query(osqlQuery, params);

		purgeLogs(logs);

	}

	public static void purgeConfigNow(ODocument server, ODatabaseDocumentTx db) {
		String osql = "select from MetricConfig";
		Map<String, Object> params = new HashMap<String, Object>();
		if (server != null) {
			osql += " where server = :server";
		}
		params.put("server", server);
		OSQLQuery<ORecordSchemaAware<?>> osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);
		List<ODocument> logs = db.query(osqlQuery, params);

		for (ODocument oDocument : new ArrayList<ODocument>(logs)) {
			osql = "select from UserConfiguration where metrics contains(:metric)";
			params.clear();
			params.put("metric", oDocument);
			osqlQuery = new OSQLSynchQuery<ORecordSchemaAware<?>>(osql);
			List<ODocument> usr = db.query(osqlQuery, params);
			for (ODocument oDocument2 : usr) {
				List<ODocument> metrics = oDocument2.field("metrics");
				metrics.remove(oDocument);
				oDocument2.save();
			}
		}
		purgeLogs(logs);

	}

	private static void purgeLogs(List<ODocument> logs) {

		for (ODocument doc : logs) {
			try {
				doc.delete();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void purgeMetricAndSnapshot(List<ODocument> metrics) {
		ODocument snapshot = null;
		if (metrics != null && !metrics.isEmpty()) {
			snapshot = metrics.get(0).field("snapshot");
		}

		for (ODocument doc : metrics) {
			try {
				ODocument snapshot2compare = doc.field("snapshot");
				if (!snapshot.getIdentity().equals(snapshot2compare.getIdentity())) {
					snapshot.delete();
					snapshot = snapshot2compare;
				}
				doc.delete();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		if (snapshot != null)
			snapshot.delete();
	}

}