/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.agent.Utils;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;

import java.io.*;
import java.net.URLDecoder;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.*;

public class OServerCommandGetLog extends OServerCommandDistributedScope {

  private static final String[] NAMES = {"GET|log/*"};

  private static final String TAIL = "tail";

  private static final String FILE = "file";

  private static final String SEARCH = "search";

  private static final String ALLFILES = "files";

  SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

  public OServerCommandGetLog(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_LOG.toString(), server);
  }

  @Override
  void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    final String[] urlParts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: log/<type>?<value>");

    String type = urlParts[1]; // the type of the log tail search or file

    String value = iRequest.getParameter("searchvalue");

    String size = iRequest.getParameter("size");

    String logType = iRequest.getParameter("logtype");

    String dateFrom = iRequest.getParameter("dateFrom");

    String dateTo = iRequest.getParameter("dateTo");

    String selectedFile = iRequest.getParameter("file");

    if (isLocalNode(iRequest)) {
      Date dFrom = null;
      if (dateFrom != null) {
        dFrom = new Date(new Long(dateFrom));
      }

      Date dTo = null;
      if (dateTo != null) {
        dTo = new Date(new Long(dateTo));
      }

      Properties prop = new Properties();
      FilenameFilter filter = new FilenameFilter() {
        public boolean accept(File directory, String fileName) {
          return !fileName.endsWith(".lck");
        }
      };

      String env = Orient.getHomePath();
      FileInputStream inStream = new FileInputStream(env + "/config/orientdb-server-log.properties");
      try {
        prop.load(inStream);
      } finally {
        Utils.safeClose(this, inStream);
      }
      String logDir = (String) prop.get("java.util.logging.FileHandler.pattern");

      if (!logDir.startsWith("/")) {
        // RELATIVE PATH
        if (logDir.startsWith("../"))
          logDir = logDir.substring("../".length());

        logDir = env + "/" + logDir;
      }

      File directory = new File(logDir).getParentFile();
      File[] files = directory.listFiles(filter);
      if (files != null) {
        Arrays.sort(files);
      } else {
        throw new Exception("logs directory is empty or does not exist");
      }
      List<ODocument> subdocuments = new ArrayList<ODocument>();
      final ODocument result = new ODocument();

      String line = "";
      String dayToDoc = "";
      String hour = "";
      String typeToDoc = "";
      String info = "";

      ODocument doc = new ODocument();

      if (TAIL.equals(type)) {
        insertFromFile(value, size, logType, dFrom, dTo, files, subdocuments, line, dayToDoc, hour, typeToDoc, info, doc, files[0]);
      } else if (FILE.equals(type)) {
        for (int i = 0; i <= files.length - 1; i++) {
          if (files[i].getName().equals(selectedFile))
            insertFromFile(value, size, logType, dFrom, dTo, files, subdocuments, line, dayToDoc, hour, typeToDoc, info, doc,
                    files[i]);
        }
      } else if (SEARCH.equals(type)) {
        for (int i = 0; i <= files.length - 1; i++) {
          line = "";
          insertFromFile(value, size, logType, dFrom, dTo, files, subdocuments, line, dayToDoc, hour, typeToDoc, info, doc,
                  files[i]);
        }
      } else if (ALLFILES.equals(type)) {
        for (int i = 0; i <= files.length - 1; i++) {
          doc = new ODocument();
          files[i].getName();
          doc.field("name", files[i].getName());
          subdocuments.add(doc);

        }

        result.field("files", subdocuments);
        iResponse.writeRecord(result, null, "");
        return false;
      }

      iRequest.data.commandInfo = "Load log";

      result.field("logs", subdocuments);

      iResponse.writeRecord(result, null, "");

    } else {
      proxyRequest(iRequest, iResponse);

    }
    return false;
  }

  private boolean isCurrentNode(ODistributedServerManager manager, String node) {

    return node.equalsIgnoreCase(manager.getLocalNodeName());

  }

  private void insertFromFile(String value, String size, String logType, Date dFrom, Date dTo, File[] files,
                              List<ODocument> subdocuments, String line, String dayToDoc, String hour, String typeToDoc, String info, ODocument doc,
                              File file) throws FileNotFoundException, IOException {
    File f = file;
    FileReader in = new FileReader(f);
    BufferedReader br = new BufferedReader(in);
    try {
      Date day = null;
      while (line != null) {
        line = br.readLine();
        if (line != null) {
          String[] split = line.split(" ");
          if (split != null) {

            try {

              day = dateFormatter.parse(split[0]);

              // trying to create a Date
              if (doc.field("day") != null) {
                doc.field("info", info);
                doc.field("file", f.getName());
                checkInsert(value, logType, subdocuments, typeToDoc, info, doc, dFrom, day, hour, dTo);
                doc = new ODocument();
              }

              // Created new Doc
              dayToDoc = split[0];
              hour = split[1];

              typeToDoc = split[2];

              if (doc.field("day") == null) {
                doc = new ODocument();
                addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
              }
              line = line.replace(split[0], "");
              line = line.replace(split[1], "");
              line = line.replace(split[2], "");
              info = line;

            } catch (Exception e) {
              // stack trace
              info = info.concat(line);
            }
          }
        } else {
          if (doc.field("day") != null) {
            addFieldToDoc(dayToDoc, hour, typeToDoc, doc);
            doc.field("info", info);
            doc.field("file", f.getName());
            checkInsert(value, logType, subdocuments, typeToDoc, info, doc, dFrom, day, hour, dTo);
          }
        }
      }
    } finally {
      Utils.safeClose(this, br, in);
    }
  }

  private Time setTimeFromParameter(String hourFrom) throws UnsupportedEncodingException {
    Time hFrom;
    hourFrom = URLDecoder.decode(hourFrom, "UTF-8");
    String[] splitHourFrom = hourFrom.split(" ");
    String[] hoursMinutes = splitHourFrom[0].split(":");
    Integer h = new Integer(hoursMinutes[0]);
    if (splitHourFrom[1].equals("PM"))
      h = h + 12;
    Integer m = new Integer(hoursMinutes[1]);
    hFrom = new Time(h, m, 0);
    return hFrom;
  }

  private Time setTime(String hour) {
    if (hour == null)
      return null;
    Integer h = new Integer(hour.split(":")[0]);
    Integer m = new Integer(hour.split(":")[1]);
    Integer s = new Integer(0);
    Time result = new Time(h, m, s);
    return result;
  }

  private void checkInsert(String value, String logType, List<ODocument> subdocuments, String typeToDoc, String info, ODocument doc,
                           Date dFrom, Date day, String hour, Date dTo) {
    Time tHour = setTime(hour);

    Calendar cal = Calendar.getInstance();
    cal.setTime(day);
    cal.set(Calendar.HOUR_OF_DAY, tHour.getHours());
    cal.set(Calendar.MINUTE, tHour.getMinutes());
    cal.set(Calendar.SECOND, tHour.getSeconds());

    Date date = cal.getTime();
    if (value == null && logType == null && dFrom == null && dTo == null) {
      subdocuments.add(doc);
      return;
    }
    // check Date and Hour From
    if (dFrom != null && date.before(dFrom))
      return;

    // check Date and Hour TO
    if (dTo != null && date.after(dTo))
      return;

    if (value != null) {
      if (!(info.toLowerCase(Locale.ENGLISH).contains(value.toLowerCase(Locale.ENGLISH)))) {
        return;
      }
    }
    if (logType != null) {
      if (!(typeToDoc.equalsIgnoreCase(logType))) {
        return;
      }
    }
    subdocuments.add(doc);
    return;
  }

  private void addFieldToDoc(String dayToDoc, String hour, String typeToDoc, ODocument doc) {
    doc.field("day", dayToDoc);
    doc.field("hour", hour);
    doc.field("type", typeToDoc);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
