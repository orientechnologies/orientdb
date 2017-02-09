/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.stresstest.workload;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.ODatabaseRepair;
import com.orientechnologies.orient.core.db.tool.ODatabaseTool;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;

import java.util.List;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OCRUDWorkload extends OBaseDocumentWorkload implements OCheckWorkload {

  public static final String CLASS_NAME = "StressTestCRUD";
  public static final String INDEX_NAME = CLASS_NAME + ".Index";

  static final String INVALID_FORM_MESSAGE = "CRUD workload must be in form of CxIxUxDxSx where x is a valid number.";
  static final String INVALID_NUMBERS      = "Reads, Updates and Deletes must be less or equals to the Creates";

  private int total = 0;

  private OWorkLoadResult createsResult = new OWorkLoadResult();
  private OWorkLoadResult readsResult   = new OWorkLoadResult();
  private OWorkLoadResult updatesResult = new OWorkLoadResult();
  private OWorkLoadResult deletesResult = new OWorkLoadResult();
  private OWorkLoadResult scansResult   = new OWorkLoadResult();
  private int creates;
  private int reads;
  private int updates;
  private int deletes;
  private int scans;

  public OCRUDWorkload() {
    connectionStrategy = OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
  }

  @Override
  public String getName() {
    return "CRUD";
  }

  @Override
  public void parseParameters(final String args) {
    final String ops = args.toUpperCase();
    char state = ' ';
    final StringBuilder number = new StringBuilder();

    for (int pos = 0; pos < ops.length(); ++pos) {
      final char c = ops.charAt(pos);

      if (c == 'C' || c == 'R' || c == 'U' || c == 'D' || c == 'S') {
        state = assignState(state, number, c);
      } else if (c >= '0' && c <= '9')
        number.append(c);
      else
        throw new IllegalArgumentException("Character '" + c + "' is not valid on CRUD workload. " + INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    total = creates + reads + updates + deletes + scans;

    if (reads > creates || updates > creates || deletes > creates)
      throw new IllegalArgumentException(INVALID_NUMBERS);

    if (total == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);

    createsResult.total = creates;
    readsResult.total = reads;
    updatesResult.total = updates;
    deletesResult.total = deletes;
    scansResult.total = scans;
  }

  @Override
  public void execute(final OStressTesterSettings settings, final ODatabaseIdentifier databaseIdentifier) {
    createSchema(databaseIdentifier);
    connectionStrategy = settings.loadBalancing;

    // PREALLOCATE THE LIST TO AVOID CONCURRENCY ISSUES
    final ORID[] records = new ORID[createsResult.total];

    executeOperation(databaseIdentifier, createsResult, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        final ODocument doc = createOperation(context.currentIdx);
        records[context.currentIdx] = doc.getIdentity();
        createsResult.current.incrementAndGet();
        return null;
      }
    });

    if (records.length != createsResult.total)
      throw new RuntimeException("Error on creating records: found " + records.length + " but expected " + createsResult.total);

    executeOperation(databaseIdentifier, scansResult, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        scanOperation(((OWorkLoadContext) context).getDb());
        scansResult.current.incrementAndGet();
        return null;
      }
    });

    executeOperation(databaseIdentifier, readsResult, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        readOperation(((OWorkLoadContext) context).getDb(), context.currentIdx);
        readsResult.current.incrementAndGet();
        return null;
      }
    });

    executeOperation(databaseIdentifier, updatesResult, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        updateOperation(((OWorkLoadContext) context).getDb(), records[context.currentIdx]);
        updatesResult.current.incrementAndGet();
        return null;
      }
    });

    executeOperation(databaseIdentifier, deletesResult, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        deleteOperation(((OWorkLoadContext) context).getDb(), records[context.currentIdx]);
        records[context.currentIdx] = null;
        deletesResult.current.incrementAndGet();
        return null;
      }
    });
  }

  protected void createSchema(ODatabaseIdentifier databaseIdentifier) {
    final ODatabase database = getDocumentDatabase(databaseIdentifier, OStorageRemote.CONNECTION_STRATEGY.STICKY);
    try {
      final OSchema schema = database.getMetadata().getSchema();
      if (!schema.existsClass(OCRUDWorkload.CLASS_NAME)) {
        final OClass cls = schema.createClass(OCRUDWorkload.CLASS_NAME);
        cls.createProperty("name", OType.STRING);
        // cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString(), "name");
        cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE.toString(), (OProgressListener) null, (ODocument) null, "AUTOSHARDING",
            new String[] { "name" });
      }
    } finally {
      database.close();
    }
  }

  @Override
  public String getPartialResult() {
    final long current =
        createsResult.current.get() + scansResult.current.get() + readsResult.current.get() + updatesResult.current.get()
            + deletesResult.current.get();

    return String
        .format("%d%% [Creates: %d%% - Scans: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]", ((int) (100 * current / total)),
            createsResult.total > 0 ? 100 * createsResult.current.get() / createsResult.total : 0,
            scansResult.total > 0 ? 100 * scansResult.current.get() / scansResult.total : 0,
            readsResult.total > 0 ? 100 * readsResult.current.get() / readsResult.total : 0,
            updatesResult.total > 0 ? 100 * updatesResult.current.get() / updatesResult.total : 0,
            deletesResult.total > 0 ? 100 * deletesResult.current.get() / deletesResult.total : 0);
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("- Created %d records in %.3f secs%s", createsResult.total, (createsResult.totalTime / 1000f),
        createsResult.toOutput(1)));

    buffer.append(String.format("\n- Scanned %d records in %.3f secs%s", scansResult.total, (scansResult.totalTime / 1000f),
        scansResult.toOutput(1)));

    buffer.append(String
        .format("\n- Read %d records in %.3f secs%s", readsResult.total, (readsResult.totalTime / 1000f), readsResult.toOutput(1)));

    buffer.append(String.format("\n- Updated %d records in %.3f secs%s", updatesResult.total, (updatesResult.totalTime / 1000f),
        updatesResult.toOutput(1)));

    buffer.append(String.format("\n- Deleted %d records in %.3f secs%s", deletesResult.total, (deletesResult.totalTime / 1000f),
        deletesResult.toOutput(1)));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("type", getName());

    json.field("creates", createsResult.toJSON(), OType.EMBEDDED);
    json.field("scans", scansResult.toJSON(), OType.EMBEDDED);
    json.field("reads", readsResult.toJSON(), OType.EMBEDDED);
    json.field("updates", updatesResult.toJSON(), OType.EMBEDDED);
    json.field("deletes", deletesResult.toJSON(), OType.EMBEDDED);

    return json.toJSON("");
  }

  public ODocument createOperation(final long n) {
    return (ODocument) ODatabaseDocumentTx.executeWithRetries(new OCallable<Object, Integer>() {
      @Override
      public Object call(Integer iArgument) {
        ODocument doc = new ODocument(CLASS_NAME);
        doc.field("name", "value" + n);
        doc.save();
        return doc;
      }
    }, 10);
  }

  public void readOperation(final ODatabase database, final long n) {
    final String query = String.format("SELECT FROM %s WHERE name = ?", CLASS_NAME);
    final List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(query)).execute("value" + n);
    if (result.size() != 1) {
      throw new RuntimeException(String.format("The query [%s] result size is %d. Expected size is 1.", query, result.size()));
    }
  }

  public void scanOperation(final ODatabase database) {
    final String query = String.format("SELECT count(*) FROM %s WHERE notexistent is null", CLASS_NAME);
    final List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(query)).execute();
    if (result.size() != 1) {
      throw new RuntimeException(String.format("The query [%s] result size is %d. Expected size is 1.", query, result.size()));
    }
  }

  public void updateOperation(final ODatabase database, final OIdentifiable rec) {
    ODatabaseDocumentTx.executeWithRetries(new OCallable<Object, Integer>() {
      @Override
      public Object call(Integer iArgument) {
        final ODocument doc = rec.getRecord();
        doc.field("updated", true);
        doc.save();
        return doc;
      }
    }, 10);
  }

  public void deleteOperation(final ODatabase database, final OIdentifiable rec) {
    ODatabaseDocumentTx.executeWithRetries(new OCallable<Object, Integer>() {
      @Override
      public Object call(Integer iArgument) {
        database.delete(rec.getIdentity());
        return null;
      }
    }, 10);
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'C')
      creates = Integer.parseInt(number.toString());
    else if (state == 'R')
      reads = Integer.parseInt(number.toString());
    else if (state == 'U')
      updates = Integer.parseInt(number.toString());
    else if (state == 'D')
      deletes = Integer.parseInt(number.toString());
    else if (state == 'S')
      scans = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  public int getCreates() {
    return createsResult.total;
  }

  public int getReads() {
    return readsResult.total;
  }

  public int getScans() {
    return scansResult.total;
  }

  public int getUpdates() {
    return updatesResult.total;
  }

  public int getDeletes() {
    return deletesResult.total;
  }

  @Override
  public void check(final ODatabaseIdentifier databaseIdentifier) {
    final ODatabaseDocument db = (ODatabaseDocument) getDocumentDatabase(databaseIdentifier,
        OStorageRemote.CONNECTION_STRATEGY.STICKY);
    final ODatabaseTool repair = new ODatabaseRepair().setDatabase(db);
    repair.run();
  }
}
