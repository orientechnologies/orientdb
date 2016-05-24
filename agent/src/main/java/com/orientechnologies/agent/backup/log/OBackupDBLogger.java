/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.backup.log;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.OSystemDatabase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 30/03/16.
 */
public class OBackupDBLogger implements OBackupLogger {

  OBackupLogFactory          factory;

  public static final String CLASS_NAME = "OBackupLog";

  public OBackupDBLogger() {
    initLogger();
    factory = new OBackupLogFactory();

  }

  private void initLogger() {

    getDatabase().executeInDBScope(new OCallable<Void, ODatabase>() {
      @Override
      public Void call(ODatabase db) {

        OSchema schema = db.getMetadata().getSchema();

        if (!schema.existsClass(CLASS_NAME)) {
          OClass clazz = schema.createClass(CLASS_NAME);
          clazz.createProperty("unitId", OType.LONG);
          clazz.createProperty("mode", OType.STRING);
          clazz.createProperty("txId", OType.LONG);
          clazz.createProperty("uuid", OType.STRING);
          clazz.createProperty("dbName", OType.STRING);
          clazz.createProperty("timestamp", OType.LONG);

        }
        return null;
      }
    }, "");

  }

  @Override
  public void log(final OBackupLog log) {

    getDatabase().executeInDBScope(new OCallable<Void, ODatabase>() {
      @Override
      public Void call(ODatabase iArgument) {
        ODocument document = log.toDoc();
        document.setClassName(CLASS_NAME);
        iArgument.save(document);
        return null;
      }
    }, "");

  }

  @Override
  public long nextOpId() {
    return System.currentTimeMillis();
  }

  @Override
  public OBackupLog findLast(final OBackupLogType op, final String uuid) throws IOException {

    String query = String.format("select from %s where op = :op and uuid = :uuid order by timestamp desc limit 1", CLASS_NAME);
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("op", op.toString());
        put("uuid", uuid);
      }
    };

    List<ODocument> results = (List<ODocument>) getDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "", query, params);

    if (results.size() > 0) {
      return factory.fromDoc(results.get(0));
    }

    return null;
  }

  @Override
  public OBackupLog findLast(final OBackupLogType op, final String uuid, final Long unitId) throws IOException {
    String query = String
        .format("select from %s where op = :op and uuid = :uuid and unitId = :unitId order by timestamp desc limit 1", CLASS_NAME);
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("op", op.toString());
        put("uuid", uuid);
        put("unitId", unitId);
      }
    };

    List<ODocument> results = (List<ODocument>) getDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "", query, params);

    if (results.size() > 0) {
      return factory.fromDoc(results.get(0));
    }
    return null;
  }

  @Override
  public List<OBackupLog> findByUUID(final String uuid, int page, final int pageSize, Map<String, String> params)
      throws IOException {

    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    String query = "";
    Map<String, Object> queryParams = new HashMap<String, Object>() {
      {
        put("uuid", uuid);
        put("limit", pageSize);
      }
    };
    if (params != null && params.size() > 0) {

      String from = params.get("from");
      String to = params.get("to");

      if (from != null && to != null) {
        queryParams.put("tsFrom", Long.parseLong(from));
        queryParams.put("tsTo", Long.parseLong(to));
        query = String.format(
            "select * from %s where uuid = :uuid and timestamp >= :tsFrom and timestamp <= :tsTo order by timestamp desc limit :limit",
            CLASS_NAME);
      }

    } else {
      query = String.format("select * from %s where  uuid = :uuid  order by timestamp desc limit :limit", CLASS_NAME);
    }

    final List<ODocument> results = (List<ODocument>) getDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "", query, queryParams);

    for (ODocument result : results) {
      logs.add(factory.fromDoc(result));
    }

    return logs;
  }

  @Override
  public List<OBackupLog> findByUUIDAndUnitId(final String uuid, final Long unitId, int page, final int pageSize,
      Map<String, String> params) throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    String query;
    Map<String, Object> queryParams = new HashMap<String, Object>() {
      {
        put("uuid", uuid);
        put("unitId", unitId);
      }
    };
    if (params != null && params.size() > 0) {
      query = String.format("select * from %s where uuid = :uuid and unitId = :unitId and op= :op order by timestamp desc ",
          CLASS_NAME);
      for (Map.Entry<String, String> entry : params.entrySet()) {
        queryParams.put(entry.getKey(), entry.getValue());
      }
    } else {
      query = String.format("select * from %s where uuid = :uuid and unitId = :unitId  order by timestamp desc ", CLASS_NAME);
    }
    List<ODocument> results = (List<ODocument>) getDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "", query, queryParams);
    for (ODocument result : results) {
      logs.add(factory.fromDoc(result));
    }
    return logs;
  }

  @Override
  public void deleteByUUIDAndUnitIdAndTimestamp(final String uuid, final Long unitId, final Long txId) throws IOException {

    final String selectQuery = String.format("select from %s where uuid = :uuid and unitId = :unitId and txId >= :txId",
        CLASS_NAME);

    final String query = String.format("delete from %s where uuid = :uuid and unitId = :unitId and txId >= :txId", CLASS_NAME);
    final Map<String, Object> queryParams = new HashMap<String, Object>() {
      {
        put("uuid", uuid);
        put("unitId", unitId);
        put("txId", txId);
      }
    };

    getDatabase().executeInDBScope(new OCallable<Void, ODatabase>() {
      @Override
      public Void call(ODatabase iArgument) {

        iArgument.command(new OSQLAsynchQuery(selectQuery, new OCommandResultListener() {
          @Override
          public boolean result(Object iRecord) {

            ODocument doc = (ODocument) iRecord;
            dropFile(doc);
            return false;
          }

          @Override
          public void end() {

          }

          @Override
          public Object getResult() {
            return null;
          }
        })).execute(queryParams);
        Object execute = iArgument.command(new OCommandSQL(query)).execute(queryParams);
        System.out.println(execute);
        return null;
      }
    }, "");

  }

  private void dropFile(ODocument doc) {

    OBackupLog oBackupLog = factory.fromDoc(doc);

    if (oBackupLog instanceof OBackupFinishedLog) {

      String path = "";
      try {
        path = ((OBackupFinishedLog) oBackupLog).getPath() + File.separator + ((OBackupFinishedLog) oBackupLog).fileName;
        File f = new File(path);
        f.delete();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error deleting file " + path, e);
      }
    }
  }

  @Override
  public List<OBackupLog> findAllLatestByUUID(final String uuid, int page, final int pageSize) throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("uuid", uuid);
        put("limit", pageSize);
      }
    };
    String query = String.format("select * from %s where  uuid = :uuid  group by unitId  order by timestamp desc limit :limit",
        CLASS_NAME);

    final List<ODocument> results = (List<ODocument>) getDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "", query, params);

    for (ODocument result : results) {
      logs.add(factory.fromDoc(result));
    }

    return logs;
  }

  public OSystemDatabase getDatabase() {
    return OServerMain.server().getSystemDatabase();
  }
}
