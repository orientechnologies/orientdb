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
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;

import java.util.ArrayList;
import java.util.List;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OCRUDWorkload extends OBaseDocumentWorkload {

  public static final String CLASS_NAME           = "StressTestCRUD";
  public static final String INDEX_NAME           = CLASS_NAME + ".Index";

  static final String        INVALID_FORM_MESSAGE = "CRUD workload must be in form of CxIxUxDx where x is a valid number.";
  static final String        INVALID_NUMBERS      = "Reads, Updates and Deletes must be less or equals to the Creates";

  private int                total                = 0;

  private OWorkLoadResult    createsResult        = new OWorkLoadResult();
  private OWorkLoadResult    readsResult          = new OWorkLoadResult();
  private OWorkLoadResult    updatesResult        = new OWorkLoadResult();
  private OWorkLoadResult    deletesResult        = new OWorkLoadResult();

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

      if (c == 'C' || c == 'R' || c == 'U' || c == 'D') {
        state = assignState(state, number, c);
      } else if (c >= '0' && c <= '9')
        number.append(c);
      else
        throw new IllegalArgumentException("Character '" + c + "' is not valid on CRUD workload. " + INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    total = createsResult.total + readsResult.total + updatesResult.total + deletesResult.total;

    if (readsResult.total > createsResult.total || updatesResult.total > createsResult.total
        || deletesResult.total > createsResult.total)
      throw new IllegalArgumentException(INVALID_NUMBERS);

    if (total == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);
  }

  @Override
  public void execute(final int concurrencyLevel, final ODatabaseIdentifier databaseIdentifier) {
    createSchema(databaseIdentifier);

    final ArrayList<ORID> records = new ArrayList<ORID>(createsResult.total);
    for (int i = 0; i < createsResult.total; ++i)
      records.add(null);

    executeOperation(databaseIdentifier, createsResult, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        final ODocument doc = createOperation(context.currentIdx);
        synchronized (records) {
          if (records.set(context.currentIdx, doc.getIdentity()) != null)
            throw new RuntimeException(
                "Error on creating record with id " + context.currentIdx + " because has been already created");
        }
        createsResult.current.incrementAndGet();
        return null;
      }
    });

    if (records.size() != createsResult.total)
      throw new RuntimeException("Error on creating records: found " + records.size() + " but expected " + createsResult.total);

    executeOperation(databaseIdentifier, readsResult, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        readOperation(((OWorkLoadContext) context).getDb(), context.currentIdx);
        readsResult.current.incrementAndGet();
        return null;
      }
    });

    executeOperation(databaseIdentifier, updatesResult, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        updateOperation(((OWorkLoadContext) context).getDb(), records.get(context.currentIdx));
        updatesResult.current.incrementAndGet();
        return null;
      }
    });

    executeOperation(databaseIdentifier, deletesResult, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        deleteOperation(((OWorkLoadContext) context).getDb(), records.get(context.currentIdx));
        records.set(context.currentIdx, null);
        deletesResult.current.incrementAndGet();
        return null;
      }
    });
  }

  protected void createSchema(ODatabaseIdentifier databaseIdentifier) {
    final ODatabase database = getDocumentDatabase(databaseIdentifier);
    try {
      final OSchema schema = database.getMetadata().getSchema();
      if (!schema.existsClass(OCRUDWorkload.CLASS_NAME)) {
        final OClass cls = schema.createClass(OCRUDWorkload.CLASS_NAME);
        cls.createProperty("name", OType.STRING);
        cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString(), (OProgressListener) null, (ODocument) null,
            "AUTOSHARDING", new String[] { "name" });
      }
    } finally {
      database.close();
    }
  }

  @Override
  public String getPartialResult() {
    final long current = createsResult.current.get() + readsResult.current.get() + updatesResult.current.get()
        + deletesResult.current.get();
    return String.format("%d%% [Creates: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]", ((int) (100 * current / total)),
        100 * createsResult.current.get() / createsResult.total, 100 * readsResult.current.get() / readsResult.total,
        100 * updatesResult.current.get() / updatesResult.total, 100 * deletesResult.current.get() / deletesResult.total);
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("- Created %d records in %.3f secs - %s", createsResult.total, (createsResult.totalTime / 1000f),
        createsResult.toOutput()));

    buffer.append(String.format("\n- Read    %d records in %.3f secs - %s", readsResult.total, (readsResult.totalTime / 1000f),
        readsResult.toOutput()));

    buffer.append(String.format("\n- Updated %d records in %.3f secs - %s", updatesResult.total, (updatesResult.totalTime / 1000f),
        updatesResult.toOutput()));

    buffer.append(String.format("\n- Deleted %d records in %.3f secs - %s", deletesResult.total, (deletesResult.totalTime / 1000f),
        deletesResult.toOutput()));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("creates", createsResult.toJSON(), OType.EMBEDDED);
    json.field("reads", createsResult.toJSON(), OType.EMBEDDED);
    json.field("updates", createsResult.toJSON(), OType.EMBEDDED);
    json.field("deletes", createsResult.toJSON(), OType.EMBEDDED);

    return json.toString();
  }

  public ODocument createOperation(final long n) {
    ODocument doc = new ODocument(CLASS_NAME);
    doc.field("name", getValue(n));
    doc.save();
    return doc;
  }

  public void readOperation(final ODatabase database, final long n) {
    final String query = String.format("SELECT FROM %s WHERE name = ?", CLASS_NAME);
    final List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(query)).execute(getValue(n));
    if (result.size() != 1) {
      throw new RuntimeException(String.format("The query [%s] result size is %d. Expected size is 1.", query, result.size()));
    }
  }

  public void updateOperation(final ODatabase database, final OIdentifiable rec) {
    final ODocument doc = rec.getRecord();
    doc.field("updated", true);
    doc.save();
  }

  public void deleteOperation(final ODatabase database, final OIdentifiable rec) {
    database.delete(rec.getIdentity());
  }

  private String getValue(final long n) {
    return "value" + n;
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'C')
      createsResult.total = Integer.parseInt(number.toString());
    else if (state == 'R')
      readsResult.total = Integer.parseInt(number.toString());
    else if (state == 'U')
      updatesResult.total = Integer.parseInt(number.toString());
    else if (state == 'D')
      deletesResult.total = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  public int getCreates() {
    return createsResult.total;
  }

  public int getReads() {
    return readsResult.total;
  }

  public int getUpdates() {
    return updatesResult.total;
  }

  public int getDeletes() {
    return deletesResult.total;
  }
}
