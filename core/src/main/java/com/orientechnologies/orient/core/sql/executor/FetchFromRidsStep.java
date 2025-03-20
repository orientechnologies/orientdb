package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 22/07/16. */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private Collection<ORecordId> rids;

  public FetchFromRidsStep(Collection<ORecordId> rids) {
    super();
    this.rids = rids;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return OExecutionStream.loadIterator((Iterator<OIdentifiable>) (Iterator) this.rids.iterator());
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    return OExecutionStepInternal.getIndent(ctx)
        + "+ FETCH FROM RIDs\n"
        + OExecutionStepInternal.getIndent(ctx)
        + "  "
        + rids;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (rids != null) {
      result.setProperty("rids", rids.stream().map(x -> x.toString()).collect(Collectors.toList()));
    }
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("rids") != null) {
        List<String> ser = fromResult.getProperty("rids");
        rids = ser.stream().map(x -> new ORecordId(x)).collect(Collectors.toList());
      }
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
