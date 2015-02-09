/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.workbench.http;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.workbench.OMonitoredServer;
import com.orientechnologies.workbench.OWorkbenchPlugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.Map.Entry;

public class OServerCommandGetRealtimeMetrics extends OServerCommandAuthenticatedDbAbstract {
  public static final String    DISTRIBUTED = "distributed";
  public static final String    DB          = "db";
  public static final String    NODE        = "node";
  private static final String[] NAMES       = { "GET|metrics/*" };

  private OWorkbenchPlugin      monitor;
  public final String[]         fields      = { "min", "max", "value", "entries", "total", "last" };

  public OServerCommandGetRealtimeMetrics() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    if (monitor == null)
      monitor = OServerMain.server().getPluginByClass(OWorkbenchPlugin.class);

    final String[] parts = checkSyntax(iRequest.url, 7,
        "Syntax error: metrics/monitor/<server>/<databases>/<type>/<kind>/<names>/<limit>/<compress>/<from>/<to>");

    iRequest.data.commandInfo = "Retrieve metrics";

    try {
      final String serverName = parts[2];
      final String[] databases = parts[3].split(",");
      final String type = parts[4];
      final String metricKind = parts[5];
      final String[] metricNames = parts[6].split(",");
      String from = null;
      String to = null;
      String compress = "none";
      String limit = null;
      if (parts.length > 7) {
        limit = parts[7];
      }
      if (parts.length > 8) {
        compress = parts[8];
      }
      if (parts.length > 9) {
        from = parts[9];
      }
      if (parts.length > 10) {
        to = parts[10];
      }
      final OMonitoredServer server = monitor.getMonitoredServer(serverName);

      if (server == null)
        throw new IllegalArgumentException("Invalid server '" + serverName + "'");

      final Map<String, Object> result = new HashMap<String, Object>();

      if ("realtime".equalsIgnoreCase(type))
        sendRealtimeMetrics(iRequest, iResponse, metricKind, metricNames, server, databases, result);
      else if ("snapshot".equalsIgnoreCase(type)) {
        sendSnapshotMetrics(iRequest, iResponse, metricKind, metricNames, server, databases, limit, compress, from, to, result);
      }

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  protected void sendRealtimeMetrics(OHttpRequest request, OHttpResponse iResponse, final String iMetricKind,
      final String[] metricNames, final OMonitoredServer server, String[] databases, final Map<String, Object> result)
      throws MalformedURLException, IOException, InterruptedException {
    String[] dbs = databases;
    ODatabaseDocumentTx db = getProfiledDatabaseInstance(request);
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    if (databases.length == 1 && databases[0].equals("all")) {
      try {
        final Map<String, Object> mapDb = server.getRealtime().getInformation("system.databases");
        String dbInfo = (String) mapDb.get("system.databases");
        if (dbInfo != null) {
          dbs = dbInfo.split(",");
        } else {
          iResponse.writeResult(new ODocument(), "indent:6", null);
        }
      } catch (MalformedURLException e) {
        e.printStackTrace();
      } catch (IOException e) {

      }

    }

    String[] nodes = getNodeList(server);

    final String status = server.getConfiguration().field("status");
    String licenseInvalid = OWorkbenchPlugin.STATUS.LICENSE_INVALID.toString();
    String licenseExpired = OWorkbenchPlugin.STATUS.LICENSE_EXPIRED.toString();
    if (!licenseInvalid.equals(status) && !licenseExpired.equals(status)) {
      Map<String, String> aggregation = buildAssociation(server, metricNames, dbs, nodes);
      for (String metricName : expandMetric(server, metricNames, dbs, nodes)) {
        final Map<String, Object> metrics;

        if (iMetricKind.equals("chrono"))
          metrics = server.getRealtime().getChrono(metricName);
        else if (iMetricKind.equals("statistics"))
          metrics = server.getRealtime().getStatistic(metricName);
        else if (iMetricKind.equals("information"))
          metrics = server.getRealtime().getInformation(metricName);
        else if (iMetricKind.equals("counter"))
          metrics = server.getRealtime().getCounter(metricName);
        else
          throw new IllegalArgumentException("Unsupported type '" + iMetricKind + "'");

        if (metrics != null)
          for (Entry<String, Object> metric : metrics.entrySet()) {
            String key = aggregation.get(metric.getKey());
            if (key == null) {
              key = metric.getKey();
            }
            result.put(key, metric.getValue());
          }
      }
    }

    ODocument response = new ODocument().setAllowChainedAccess(false).fromMap(result);
    iResponse.writeResult(response, "indent:6", null);
  }

  private String[] getNodeList(OMonitoredServer server) {
    String[] nodes = new String[0];
    ODocument cluster = server.getConfiguration().field("cluster");
    String serverName = server.getConfiguration().field("name");
    if (cluster != null) {
      String cName = cluster.field("name");
      if (cName != null) {
        List<String> nodesName = new ArrayList<String>();
        for (OMonitoredServer s : monitor.getServersByClusterName(cName)) {
          String name = s.getConfiguration().field("name");
          if (!serverName.equals(name)) {
            nodesName.add(name);
          }
        }
        nodes = nodesName.toArray(nodes);
      }
    }
    return nodes;
  }

  protected void sendSnapshotMetrics(OHttpRequest iRequest, OHttpResponse iResponse, final String iMetricKind,
      final String[] metricNames, final OMonitoredServer server, String[] databases, String limit, String compress, String from,
      String to, final Map<String, Object> result) throws MalformedURLException, IOException, InterruptedException {
    String query = "select @class, snapshot.dateTo as dateTo,snapshot.dateFrom as dateFrom, name, entries, last, min, max, average,value,total from Metric where name in :names and snapshot.server.name = :sname ";
    String[] dbs = databases;
    ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    if (databases.length == 1 && databases[0].equals("all")) {
      try {
        final Map<String, Object> mapDb = server.getRealtime().getInformation("system.databases");
        String dbInfo = (String) mapDb.get("system.databases");
        dbs = dbInfo.split(",");
      } catch (MalformedURLException e) {
        e.printStackTrace();
      } catch (IOException e) {

      }

    }

    String[] nodes = getNodeList(server);

    final Map<String, Object> params = new HashMap<String, Object>();
    params.put("names", expandMetric(server, metricNames, dbs, nodes));
    params.put("sname", server.getConfiguration().field("name"));

    if (from != null) {
      query += "and snapshot.dateFrom >= :dateFrom ";
      params.put("dateFrom", from);
    }
    if (to != null) {
      query += "and snapshot.dateTo <= :dateTo";
      params.put("dateTo", to);
    }
    query += " order by dateFrom desc, name desc ";
    if (limit != null) {
      query += " LIMIT " + limit;
    }

    final String status = server.getConfiguration().field("status");
    List<ODocument> docs = new ArrayList<ODocument>();
    String licenseInvalid = OWorkbenchPlugin.STATUS.LICENSE_INVALID.toString();
    String licenseExpired = OWorkbenchPlugin.STATUS.LICENSE_EXPIRED.toString();
    if (!licenseInvalid.equals(status) && !licenseExpired.equals(status)) {
      docs = db.query(new OSQLSynchQuery<ORecordSchemaAware>(query), params);
    }
    Map<String, String> aggregation = buildAssociation(server, metricNames, dbs, nodes);

    if (compress.equals("none") || compress.equals("1")) {
      for (ODocument oDocument : docs) {
        oDocument.field("name", aggregation.get(oDocument.field("name")));
      }
    } else {
      docs = groupByTime(docs, aggregation, compress);
    }
    iResponse.writeResult(docs);
  }

  protected List<ODocument> groupByTime(List<ODocument> documents, Map<String, String> aggregation, String time) {
    List<ODocument> docs = new ArrayList<ODocument>();

    Map<Date, Map<String, List<ODocument>>> mapDocs = new LinkedHashMap<Date, Map<String, List<ODocument>>>();
    Date last = null;
    Calendar cal = Calendar.getInstance();

    for (ODocument oDocument : documents) {
      Date from = oDocument.field("dateFrom");
      if (last == null) {
        last = from;
      } else {
        cal.setTime(last);
        cal.add(Calendar.MINUTE, -new Integer(time));
        Date calculate = cal.getTime();
        if (calculate.before(from)) {

        } else {
          last = from;
        }
      }
      Map<String, List<ODocument>> firstDoc = mapDocs.get(last);
      if (firstDoc == null) {
        firstDoc = new LinkedHashMap<String, List<ODocument>>();
      }
      String field = oDocument.field("name");
      List<ODocument> dc = firstDoc.get(aggregation.get(field));
      if (dc == null) {
        dc = new ArrayList<ODocument>();
      }
      dc.add(oDocument);
      firstDoc.put(aggregation.get(field), dc);
      mapDocs.put(last, firstDoc);
    }

    for (Date d : mapDocs.keySet()) {

      for (String k : mapDocs.get(d).keySet()) {
        List<ODocument> doc = mapDocs.get(d).get(k);
        ODocument retDoc = new ODocument();
        for (ODocument oDocument : doc) {
          retDoc.field("class", oDocument.field("class"));
          retDoc.field("name", aggregation.get(oDocument.field("name")));
          retDoc.field("dateFrom", d);
          retDoc.field("dateTo", d);
          for (String entry : fields) {
            Long f = oDocument.field(entry);
            f = f != null ? f : 0l;
            Long sum = retDoc.field(entry);
            sum = sum != null ? sum : 0l;
            sum = sum + f;
            retDoc.field(entry, sum);
          }
        }
        for (String entry : fields) {
          Long sum = retDoc.field(entry);
          retDoc.field(entry, sum / doc.size());
        }
        docs.add(retDoc);
      }

    }
    return docs;
  }

  protected Map<String, String> buildAssociation(OMonitoredServer server, String[] metrics, String[] databases, String[] nodes) {
    Map<String, String> ass = new HashMap<String, String>();
    String[] dbFormatted;
    dbFormatted = databases;
    for (String m : metrics) {
      if (m.startsWith(DB)) {
        for (String db : dbFormatted) {
          String replace = m.replace("*", db);
          ass.put(replace, m);
        }
      } else if (m.startsWith(DISTRIBUTED)) {

        if (m.startsWith(DISTRIBUTED + "." + NODE)) {
          for (String node : nodes) {
            String replace = m.replace("*", node);
            ass.put(replace, m);
          }
        } else {
          for (String db : dbFormatted) {
            String replace = m.replace("*", db);
            ass.put(replace, m);
          }
        }
      } else {
        ass.put(m, m);
      }
    }
    return ass;
  }

  protected List<String> expandMetric(OMonitoredServer server, String[] metrics, String[] databases, String[] nodes) {
    String[] dbFormatted;
    List<String> finalMetrics = new ArrayList<String>();
    if (databases.length == 0)
      return Arrays.asList(metrics);

    dbFormatted = databases;

    for (String m : metrics) {
      if (m.startsWith(DB)) {
        for (String db : dbFormatted) {
          finalMetrics.add(m.replace("*", db));
        }
      } else if (m.startsWith(DISTRIBUTED)) {

        if (m.startsWith(DISTRIBUTED + "." + NODE)) {
          for (String name : nodes) {
            finalMetrics.add(m.replace("*", name));
          }
        } else {
          for (String db : dbFormatted) {
            finalMetrics.add(m.replace("*", db));
          }
        }

      } else {
        finalMetrics.add(m);
      }
    }

    return finalMetrics;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
