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

package com.orientechnologies.agent.services.backup.log;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.server.OSystemDatabase;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/** Created by Enrico Risa on 30/03/16. */
public class OBackupDBLogger implements OBackupLogger {

  private final OEnterpriseServer server;
  OBackupLogFactory factory;

  public static final String CLASS_NAME = "OBackupLog";

  public OBackupDBLogger(OEnterpriseServer server) {

    this.server = server;
    initLogger();
    factory = new OBackupLogFactory();
  }

  private void initLogger() {

    getDatabase()
        .executeInDBScope(
            db -> {
              OSchema schema = db.getMetadata().getSchema();

              if (!schema.existsClass(CLASS_NAME)) {
                OClass clazz = schema.createClass(CLASS_NAME);
                clazz.createProperty("unitId", OType.LONG);
                clazz.createProperty("mode", OType.STRING);
                clazz.createProperty("txId", OType.LONG);
                clazz.createProperty("uuid", OType.STRING);
                clazz.createProperty("dbName", OType.STRING);
                clazz.createProperty("timestamp", OType.DATETIME);
              }
              return null;
            });
  }

  @Override
  public OBackupLog log(final OBackupLog log) {

    return getDatabase()
        .executeWithDB(
            session -> {
              ODocument document = log.toDoc();
              document.setClassName(CLASS_NAME);
              ODocument saved = session.save(document);
              return factory.fromDoc(saved);
            });
  }

  @Override
  public long nextOpId() {
    return System.currentTimeMillis();
  }

  @Override
  public OBackupLog findLast(final OBackupLogType op, final String uuid) throws IOException {

    String query =
        String.format(
            "select from %s where op = :op and uuid = :uuid order by timestamp desc limit 1",
            CLASS_NAME);
    Map<String, Object> params =
        new HashMap<String, Object>() {
          {
            put("op", op.toString());
            put("uuid", uuid);
          }
        };

    Optional<OBackupLog> backupLog =
        getLogs(
            query,
            params,
            (results) ->
                results.stream()
                    .map((r) -> factory.fromDoc((ODocument) r.toElement()))
                    .findFirst());

    if (backupLog.isPresent()) {
      return backupLog.get();
    }

    return null;
  }

  public <T> T getLogs(
      String query, Map<String, Object> params, OCallable<T, OResultSet> callback) {

    final OSystemDatabase database = getDatabase();

    if (database != null) {
      return database.executeWithDB(
          (db) -> {
            try (OResultSet resultSet = db.query(query, params)) {
              return callback.call(resultSet);
            }
          });
    }
    return null;
  }

  @Override
  public OBackupLog findLast(final OBackupLogType op, final String uuid, final Long unitId)
      throws IOException {
    String query =
        String.format(
            "select from %s where op = :op and uuid = :uuid and unitId = :unitId order by timestamp desc limit 1",
            CLASS_NAME);
    Map<String, Object> params =
        new HashMap<String, Object>() {
          {
            put("op", op.toString());
            put("uuid", uuid);
            put("unitId", unitId);
          }
        };

    Optional<OBackupLog> logs =
        getLogs(
            query,
            params,
            (results) ->
                results.stream()
                    .map((r) -> factory.fromDoc((ODocument) r.toElement()))
                    .findFirst());

    if (logs.isPresent()) {
      return logs.get();
    }

    return null;
  }

  @Override
  public List<OBackupLog> findByUUID(
      final String uuid, int page, final int pageSize, Map<String, String> params)
      throws IOException {

    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    String query = "";
    Map<String, Object> queryParams =
        new HashMap<String, Object>() {
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
        query =
            String.format(
                "select * from %s where uuid = :uuid and timestamp >= :tsFrom and timestamp <= :tsTo order by timestamp desc limit :limit",
                CLASS_NAME);
      }

    } else {
      query =
          String.format(
              "select * from %s where  uuid = :uuid  order by timestamp desc limit :limit",
              CLASS_NAME);
    }

    return getLogs(
        query,
        queryParams,
        (results) ->
            results.stream()
                .map(r -> factory.fromDoc((ODocument) r.toElement()))
                .collect(Collectors.toList()));
  }

  @Override
  public List<OBackupLog> findByUUIDAndUnitId(
      final String uuid,
      final Long unitId,
      int page,
      final int pageSize,
      Map<String, String> params)
      throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    String query;
    Map<String, Object> queryParams =
        new HashMap<String, Object>() {
          {
            put("uuid", uuid);
            put("unitId", unitId);
          }
        };
    if (params != null && params.size() > 0) {
      query =
          String.format(
              "select * from %s where uuid = :uuid and unitId = :unitId and op= :op order by timestamp desc ",
              CLASS_NAME);
      for (Map.Entry<String, String> entry : params.entrySet()) {
        queryParams.put(entry.getKey(), entry.getValue());
      }
    } else {
      query =
          String.format(
              "select * from %s where uuid = :uuid and unitId = :unitId  order by timestamp desc ",
              CLASS_NAME);
    }
    return getLogs(
        query,
        queryParams,
        (results) ->
            results.stream()
                .map(r -> factory.fromDoc((ODocument) r.toElement()))
                .collect(Collectors.toList()));
  }

  @Override
  public void deleteByUUIDAndTimestamp(final String uuid, final Long timestamp) throws IOException {

    final String selectQuery =
        String.format(
            "select from %s where uuid = :uuid and timestamp <= :timestamp group by unitId order by timestamp asc",
            CLASS_NAME);

    final Map<String, Object> queryParams =
        new HashMap<String, Object>() {
          {
            put("uuid", uuid);
            put("timestamp", timestamp);
          }
        };

    getDatabase()
        .executeInDBScope(
            session -> {
              final List<Long> units = new ArrayList<Long>();

              session
                  .command(
                      new OSQLAsynchQuery(
                          selectQuery,
                          new OCommandResultListener() {
                            @Override
                            public boolean result(Object iRecord) {

                              ODocument doc = (ODocument) iRecord;

                              Long unitId = doc.field("unitId");
                              units.add(unitId);
                              return true;
                            }

                            @Override
                            public void end() {}

                            @Override
                            public Object getResult() {
                              return null;
                            }
                          }))
                  .execute(queryParams);

              for (Long unit : units) {
                try {
                  deleteByUUIDAndUnitId(uuid, unit);
                } catch (IOException e) {
                  OLogManager.instance().error(this, "Error deleting backup unit " + uuid, e);
                }
              }
              return null;
            });

    final String query =
        String.format("delete from %s where uuid = :uuid and unitId = :unitId", CLASS_NAME);
  }

  public void deleteByUUIDAndUnitId(final String uuid, final Long unitId) throws IOException {

    final String selectQuery =
        String.format("select from %s where uuid = :uuid and unitId = :unitId ", CLASS_NAME);

    final String query =
        String.format("delete from %s where uuid = :uuid and unitId = :unitId ", CLASS_NAME);
    final Map<String, Object> queryParams =
        new HashMap<String, Object>() {
          {
            put("uuid", uuid);
            put("unitId", unitId);
          }
        };

    getDatabase()
        .executeInDBScope(
            session -> {
              session
                  .command(
                      new OSQLAsynchQuery(
                          selectQuery,
                          new OCommandResultListener() {
                            @Override
                            public boolean result(Object iRecord) {

                              ODocument doc = (ODocument) iRecord;
                              dropFile(doc);
                              return true;
                            }

                            @Override
                            public void end() {}

                            @Override
                            public Object getResult() {
                              return null;
                            }
                          }))
                  .execute(queryParams);
              session.command(new OCommandSQL(query)).execute(queryParams);
              return null;
            });
  }

  @Override
  public void deleteByUUIDAndUnitIdAndTx(final String uuid, final Long unitId, final Long txId)
      throws IOException {

    final String selectQuery =
        String.format(
            "select from %s where uuid = :uuid and unitId = :unitId and txId >= :txId", CLASS_NAME);

    final String query =
        String.format(
            "delete from %s where uuid = :uuid and unitId = :unitId and txId >= :txId", CLASS_NAME);
    final Map<String, Object> queryParams =
        new HashMap<String, Object>() {
          {
            put("uuid", uuid);
            put("unitId", unitId);
            put("txId", txId);
          }
        };

    getDatabase()
        .executeInDBScope(
            session -> {
              session
                  .command(
                      new OSQLAsynchQuery(
                          selectQuery,
                          new OCommandResultListener() {
                            @Override
                            public boolean result(Object iRecord) {

                              ODocument doc = (ODocument) iRecord;
                              dropFile(doc);
                              return true;
                            }

                            @Override
                            public void end() {}

                            @Override
                            public Object getResult() {
                              return null;
                            }
                          }))
                  .execute(queryParams);
              session.command(new OCommandSQL(query)).execute(queryParams);
              return null;
            });
  }

  private void dropFile(ODocument doc) {

    OBackupLog oBackupLog = factory.fromDoc(doc);

    if (oBackupLog instanceof OBackupFinishedLog) {

      String path = "";
      String directory = ((OBackupFinishedLog) oBackupLog).getPath();
      path = directory + File.separator + ((OBackupFinishedLog) oBackupLog).getFileName();
      File f = new File(path);
      boolean deleted = f.delete();
      if (!deleted) {
        OLogManager.instance().warn(this, "Error deleting file: " + f.getName());
      }
      File dir = new File(directory);
      if (dir.isDirectory()) {
        if (dir.listFiles().length == 0) {
          deleted = dir.delete();
          if (!deleted) {
            OLogManager.instance().warn(this, "Error deleting file: " + f.getName());
          }
        }
      }
    }
  }

  @Override
  public List<OBackupLog> findAllLatestByUUID(final String uuid, int page, final int pageSize)
      throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    Map<String, Object> params =
        new HashMap<String, Object>() {
          {
            put("uuid", uuid);
            put("limit", pageSize);
          }
        };
    String query =
        String.format(
            "select * from %s where  uuid = :uuid  group by unitId  order by timestamp desc limit :limit",
            CLASS_NAME);

    return getLogs(
        query,
        params,
        (results) ->
            results.stream()
                .map(r -> factory.fromDoc((ODocument) r.toElement()))
                .collect(Collectors.toList()));
  }

  @Override
  public void deleteLog(final OBackupLog scheduled) {

    final String query =
        String.format(
            "delete from %s where uuid = :uuid and unitId = :unitId and txId = :txId  and timestamp = :timestamp",
            CLASS_NAME);
    final Map<String, Object> queryParams =
        new HashMap<String, Object>() {
          {
            put("uuid", scheduled.getUuid());
            put("unitId", scheduled.getUnitId());
            put("txId", scheduled.getTxId());
            put("timestamp", scheduled.getTimestamp());
          }
        };

    getDatabase()
        .executeInDBScope(
            session -> {
              session.command(new OCommandSQL(query)).execute(queryParams);
              return null;
            });
  }

  @Override
  public void updateLog(final OBackupLog log) {

    getDatabase()
        .executeInDBScope(
            session -> {
              ODocument document = log.toDoc();
              ODocument dbVersion = session.load(new ORecordId(log.getInternalId()));
              ORecordInternal.setIdentity(document, new ORecordId(log.getInternalId()));
              ORecordInternal.setVersion(document, dbVersion.getVersion());
              session.save(document);
              return null;
            });
  }

  @Override
  public OEnterpriseServer getServer() {
    return server;
  }

  public OSystemDatabase getDatabase() {
    return server.getSystemDatabase();
  }
}
