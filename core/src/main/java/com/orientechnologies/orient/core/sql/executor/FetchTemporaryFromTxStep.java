package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;

/**
 * Created by luigidellaquila on 12/01/17.
 * <p>
 * Fetches temporary records (cluster id -1) from current transaction
 */
public class FetchTemporaryFromTxStep extends AbstractExecutionStep {

  private final String            className;
  private       Iterator<ORecord> txEntries;
  private       Object            order;

  public FetchTemporaryFromTxStep(OCommandContext ctx, String className) {
    super(ctx);
    this.className = className;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();

    return new OResultSet() {

      int currentElement = 0;

      @Override
      public boolean hasNext() {
        if (txEntries == null) {
          return false;
        }
        if (currentElement >= nRecords) {
          return false;
        }
        return txEntries.hasNext();
      }

      @Override
      public OResult next() {
        if (txEntries == null) {
          throw new IllegalStateException();
        }
        if (currentElement >= nRecords) {
          throw new IllegalStateException();
        }
        if (!txEntries.hasNext()) {
          throw new IllegalStateException();
        }
        ORecord record = txEntries.next();

        currentElement++;
        OResultInternal result = new OResultInternal();
        result.setElement(record);
        ctx.setVariable("$current", result);
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void init() {
    if (this.txEntries == null) {
      Iterable<? extends ORecordOperation> iterable = ctx.getDatabase().getTransaction().getAllRecordEntries();

      List<ORecord> records = new ArrayList<>();
      if (iterable != null) {
        for (ORecordOperation op : iterable) {
          ORecord record = op.getRecord();
          if (matchesClass(record, className))
            records.add(record);
        }
      }
      if (order == FetchFromClusterExecutionStep.ORDER_ASC) {
        Collections.sort(records, new Comparator<ORecord>() {
          @Override
          public int compare(ORecord o1, ORecord o2) {
            long p1 = o1.getIdentity().getClusterPosition();
            long p2 = o2.getIdentity().getClusterPosition();
            if (p1 == p2) {
              return 0;
            } else if (p1 > p2) {
              return 1;
            } else {
              return -1;
            }
          }
        });
      } else {
        Collections.sort(records, new Comparator<ORecord>() {
          @Override
          public int compare(ORecord o1, ORecord o2) {
            long p1 = o1.getIdentity().getClusterPosition();
            long p2 = o2.getIdentity().getClusterPosition();
            if (p1 == p2) {
              return 0;
            } else if (p1 > p2) {
              return -1;
            } else {
              return 1;
            }
          }
        });
      }
      this.txEntries = records.iterator();
    }
  }

  private boolean matchesClass(ORecord record, String className) {
    ORecord doc = record.getRecord();
    if (!(doc instanceof ODocument)) {
      return false;
    }

    OClass schema = ((ODocument) doc).getSchemaClass();
    if (schema.getName().equals(className)) {
      return true;
    }
    if (schema.isSubClassOf(className)) {
      return true;
    }
    return false;
  }

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ FETCH NEW RECORDS FROM CURRENT TRANSACTION SCOPE (if any)\n");
    return result.toString();
  }
}
