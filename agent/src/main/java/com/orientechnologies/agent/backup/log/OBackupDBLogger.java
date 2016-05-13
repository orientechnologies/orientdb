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

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServerMain;

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

    ODatabase<?> oDatabase = getDatabase();

    try {
      OSchema schema = oDatabase.getMetadata().getSchema();

      if (!schema.existsClass(CLASS_NAME)) {
        OClass clazz = schema.createClass(CLASS_NAME);
        clazz.createProperty("unitId", OType.LONG);
        clazz.createProperty("mode", OType.STRING);
        clazz.createProperty("txId", OType.LONG);
        clazz.createProperty("uuid", OType.STRING);
        clazz.createProperty("dbName", OType.STRING);
        clazz.createProperty("timestamp", OType.LONG);

      }
    } finally {
      oDatabase.close();
    }

  }

  @Override
  public void log(OBackupLog log) {

    ODatabase oDatabase = getDatabase();
    oDatabase.activateOnCurrentThread();
    ODocument document = log.toDoc();
    document.setClassName(CLASS_NAME);

    try {
      oDatabase.save(document);
    } finally {
      oDatabase.close();
    }

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
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    try {
      ODatabase database = getDatabase();
      List<ODocument> results = database.command(new OCommandSQL(query)).execute(params);
      if (results.size() > 0) {
        return factory.fromDoc(results.get(0));
      }
    } finally {
      if (old != null) {
        ODatabaseRecordThreadLocal.INSTANCE.set(old);
      }
    }
    return null;
  }

  @Override
  public OBackupLog findLast(final OBackupLogType op, final String uuid, final Long unitId) throws IOException {
    return null;
  }

  @Override
  public List<OBackupLog> findByUUID(String uuid, int page, int pageSize) throws IOException {

    List<OBackupLog> logs = new ArrayList<OBackupLog>();
    return logs;
  }

  @Override
  public List<OBackupLog> findByUUIDAndUnitId(final String uuid, final Long unitId, int page, final int pageSize) throws IOException {
    List<OBackupLog> logs = new ArrayList<OBackupLog>();

    String query = String.format("select * from %s where uuid = :uuid and unitId = :unitId  order by timestamp desc ", CLASS_NAME);
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("uuid", uuid);
        put("unitId", unitId);
      }
    };
    List<ODocument> results = getDatabase().command(new OCommandSQL(query)).execute(params);

    for (ODocument result : results) {
      logs.add(factory.fromDoc(result));
    }

    return logs;
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
    String query = String.format("select * from %s where  uuid = :uuid  group by unitId  order by timestamp desc limit :limit", CLASS_NAME);
    List<ODocument> results = getDatabase().command(new OCommandSQL(query)).execute(params);

    for (ODocument result : results) {
      logs.add(factory.fromDoc(result));
    }

    return logs;
  }

  public ODatabase getDatabase() {
    return OServerMain.server().getSecurity().openSystemDatabase();
  }
}
