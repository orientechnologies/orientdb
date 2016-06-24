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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OCRUDWorkload extends OBaseWorkload {

  public static final String CLASS_NAME                         = "StressTestCRUD";
  public static final String INDEX_NAME                         = CLASS_NAME + ".Index";

  static final String        OPERATION_SET_INVALID_FORM_MESSAGE = "CRUD workload must be in form of CxIxUxDx where x is a valid number.";

  private int                total                              = 0;

  private int                creates                            = 0;
  private int                reads                              = 0;
  private int                updates                            = 0;
  private int                deletes                            = 0;

  private AtomicInteger      currentCreates                     = new AtomicInteger();
  private AtomicInteger      currentReads                       = new AtomicInteger();
  private AtomicInteger      currentUpdates                     = new AtomicInteger();
  private AtomicInteger      currentDeletes                     = new AtomicInteger();

  private OWorkLoadResult    createsResult;
  private OWorkLoadResult    readsResult;
  private OWorkLoadResult    updatesResult;
  private OWorkLoadResult    deletesResult;

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
        throw new IllegalArgumentException(
            "Character '" + c + "' is not valid on CRUD workload. " + OPERATION_SET_INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    total = creates + reads + updates + deletes;

    if (total == 0)
      throw new IllegalArgumentException(OPERATION_SET_INVALID_FORM_MESSAGE);
  }

  @Override
  public void execute(final int concurrencyLevel, final ODatabaseIdentifier databaseIdentifier) {
    createSchema(databaseIdentifier);

    final ArrayList<ORID> records = new ArrayList<ORID>(creates);
    for (int i = 0; i < creates; ++i)
      records.add(null);

    createsResult = executeOperation(databaseIdentifier, creates, concurrencyLevel, new OCallable<Void, OWorkLoadContext>() {
      @Override
      public Void call(final OWorkLoadContext context) {
        final ODocument doc = createOperation(context.currentIdx);
        synchronized (records) {
          if (records.set(context.currentIdx, doc.getIdentity()) != null)
            throw new RuntimeException(
                "Error on creating record with id " + context.currentIdx + " because has been already created");
        }
        currentCreates.incrementAndGet();
        return null;
      }
    });

    if (records.size() != creates)
      throw new RuntimeException("Error on creating records: found " + records.size() + " but expected " + creates);

    readsResult = executeOperation(databaseIdentifier, reads, concurrencyLevel, new OCallable<Void, OWorkLoadContext>() {
      @Override
      public Void call(final OWorkLoadContext context) {
        readOperation(context.db, context.currentIdx);
        currentReads.incrementAndGet();
        return null;
      }
    });

    updatesResult = executeOperation(databaseIdentifier, updates, concurrencyLevel, new OCallable<Void, OWorkLoadContext>() {
      @Override
      public Void call(final OWorkLoadContext context) {
        updateOperation(context.db, records.get(context.currentIdx));
        currentUpdates.incrementAndGet();
        return null;
      }
    });

    deletesResult = executeOperation(databaseIdentifier, deletes, concurrencyLevel, new OCallable<Void, OWorkLoadContext>() {
      @Override
      public Void call(final OWorkLoadContext context) {
        deleteOperation(context.db, records.get(context.currentIdx));
        records.set(context.currentIdx, null);
        currentDeletes.incrementAndGet();
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
//        cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString(), (OProgressListener) null, (ODocument) null,
//            "AUTOSHARDING", new String[] { "name" });
        cls.createIndex(INDEX_NAME, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString(), (OProgressListener) null, (ODocument) null,
            null, new String[] { "name" });
      }
    } finally {
      database.close();
    }
  }

  @Override
  public String getPartialResult() {
    final long current = currentCreates.get() + currentReads.get() + currentUpdates.get() + currentDeletes.get();
    return String.format("%d%% [Creates: %d%% - Reads: %d%% - Updates: %d%% - Deletes: %d%%]", ((int) (100 * current / total)),
        100 * currentCreates.get() / creates, 100 * currentReads.get() / reads, 100 * currentUpdates.get() / updates,
        100 * currentDeletes.get() / deletes);
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format(
        "\nCreated %d records in %.3f secs - Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th Perc: %.3fms - 99.9th Perc: %.3fms",
        creates, (createsResult.totalTime / 1000f), creates * 1000 / (float) createsResult.totalTime,
        createsResult.avgNs / 1000000f, createsResult.percentileAvg, createsResult.percentile99Ns / 1000000f,
        createsResult.percentile99_9Ns / 1000000f));

    buffer.append(String.format(
        "\nRead    %d records in %.3f secs - Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th perc: %.3fms - 99.9th Perc: %.3fms",
        reads, (readsResult.totalTime / 1000f), reads * 1000 / (float) readsResult.totalTime, readsResult.avgNs / 1000000f,
        readsResult.percentileAvg, readsResult.percentile99Ns / 1000000f, readsResult.percentile99_9Ns / 1000000f));

    buffer.append(String.format(
        "\nUpdated %d records in %.3f secs - Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th perc: %.3fms - 99.9th Perc: %.3fms",
        updates, (updatesResult.totalTime / 1000f), updates * 1000 / (float) updatesResult.totalTime,
        updatesResult.avgNs / 1000000f, updatesResult.percentileAvg, updatesResult.percentile99Ns / 1000000f,
        updatesResult.percentile99_9Ns / 1000000f));

    buffer.append(String.format(
        "\nDeleted %d records in %.3f secs - Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th perc: %.3fms - 99.9th Perc: %.3fms",
        deletes, (deletesResult.totalTime / 1000f), deletes * 1000 / (float) deletesResult.totalTime,
        deletesResult.avgNs / 1000000f, deletesResult.percentileAvg, deletesResult.percentile99Ns / 1000000f,
        deletesResult.percentile99_9Ns / 1000000f));

    return buffer.toString();
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
      creates = Integer.parseInt(number.toString());
    else if (state == 'R')
      reads = Integer.parseInt(number.toString());
    else if (state == 'U')
      updates = Integer.parseInt(number.toString());
    else if (state == 'D')
      deletes = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  public int getCreates() {
    return creates;
  }

  public int getReads() {
    return reads;
  }

  public int getUpdates() {
    return updates;
  }

  public int getDeletes() {
    return deletes;
  }
}
