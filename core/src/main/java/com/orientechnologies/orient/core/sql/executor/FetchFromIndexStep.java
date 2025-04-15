package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidateOne;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStreamProducer;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBetweenCondition;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OInCondition;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** Created by luigidellaquila on 23/07/16. */
public class FetchFromIndexStep extends AbstractExecutionStep {

  protected OIndexCandidate candidate;
  private boolean orderAsc;

  public FetchFromIndexStep(OIndexCandidate candidate, boolean orderAsc, OCommandContext ctx) {
    super();
    this.candidate = candidate;
    this.orderAsc = orderAsc;

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    database.queryStartUsingViewIndex(candidate.getName());
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    List<OIndexStream> streams = candidate.getStreams(ctx, orderAsc);

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<OIndexStream> iter = streams.iterator();
          private final AtomicLong count = new AtomicLong(0);

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            Stream<ORawPair<Object, ORID>> s = iter.next().start(ctx);
            return OExecutionStream.resultIterator(
                s.map((nextEntry) -> readResult(ctx, nextEntry, count)).iterator());
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {
            List<OIndexStreamStat> stats = new ArrayList<>();
            for (OIndexStream stream : streams) {
              stats.add(stream.indexStats());
            }
            updateIndexStats(ctx, stats, count);
          }
        };
    return OExecutionStream.multipleStreams(res);
  }

  private OResult readResult(
      OCommandContext ctx, ORawPair<Object, ORID> nextEntry, AtomicLong count) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    count.incrementAndGet();
    Object key = nextEntry.first;
    OIdentifiable value = nextEntry.second;

    nextEntry = null;

    OResultInternal result = new OResultInternal();
    result.setProperty("key", convertKey(key));
    result.setProperty("rid", value);
    ctx.setCurrent(result);
    return result;
  }

  private static Object convertKey(Object key) {
    if (key instanceof OCompositeKey) {
      return new ArrayList<>(((OCompositeKey) key).getKeys());
    }
    return key;
  }

  private void updateIndexStats(
      OCommandContext ctx, List<OIndexStreamStat> indexStats, AtomicLong count) {
    // stats
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());
    String indexName = candidate.getName();
    OBooleanExpression condition = getKeyCondition();
    OBinaryCondition additionalRangeCondition = getAdditionalCondition();
    if (indexName == null) {
      return; // this could happen, if not inited yet
    }
    boolean range = false;
    int size = 0;

    if (condition != null) {
      if (condition instanceof OBinaryCondition) {
        size = 1;
      } else if (condition instanceof OBetweenCondition) {
        size = 1;
        range = true;
      } else if (condition instanceof OAndBlock) {
        OAndBlock andBlock = ((OAndBlock) condition);
        size = andBlock.getSubBlocks().size();
        OBooleanExpression lastOp = andBlock.getSubBlocks().get(andBlock.getSubBlocks().size() - 1);
        if (lastOp instanceof OBinaryCondition) {
          OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
          range = op.isRange();
        }
      } else if (condition instanceof OInCondition) {
        size = 1;
      }
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count.get());
    ((OBasicCommandContext) ctx).updateProfilerIndex(indexStats);
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String indexName = this.candidate.getName();
    // TODO: resolve source condition from candidate
    String keyCondition = "";
    String result = OExecutionStepInternal.getIndent(ctx) + "+ FETCH FROM INDEX " + indexName;
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }

    result += ("\n" + OExecutionStepInternal.getIndent(ctx) + "  " + keyCondition);

    return result;
  }

  @Override
  public void serializeToResult(OResultInternal result, OToResultContext ctx) {
    result.setProperty("index", getIndexName());
    if (getKeyCondition() != null) {
      result.setProperty("key", getKeyCondition().toString());
      if (getAdditionalCondition() != null) {
        result.setProperty("toKey", getAdditionalCondition().toString());
      }
    }
  }

  protected OBinaryCondition getAdditionalCondition() {
    // TODO: refactor with candidate in the references of this
    return null;
  }

  protected OBooleanExpression getKeyCondition() {
    // TODO: refactor with candidate in the references of this
    return null;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("indexName", getIndexName());
    if (getKeyCondition() != null) {
      result.setProperty("condition", getKeyCondition().serialize());
    }
    if (getAdditionalCondition() != null) {
      result.setProperty("additionalRangeCondition", getAdditionalCondition().serialize());
    }
    result.setProperty("orderAsc", orderAsc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      String indexName = fromResult.getProperty("indexName");

      OBooleanExpression condition = null;
      if (fromResult.getProperty("condition") != null) {
        condition = OBooleanExpression.deserializeFromOResult(fromResult.getProperty("condition"));
      }
      OBinaryCondition additionalRangeCondition = null;
      if (fromResult.getProperty("additionalRangeCondition") != null) {
        additionalRangeCondition = new OBinaryCondition(-1);
        additionalRangeCondition.deserialize(fromResult.getProperty("additionalRangeCondition"));
      }
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      OIndex index = db.getMetadata().getIndexManager().getIndex(indexName);
      // TODO: not used now, but need to serialize correctly the sources
      candidate = new OIndexCandidateOne(index.getName());
      orderAsc = fromResult.getProperty("orderAsc");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new FetchFromIndexStep(candidate, this.orderAsc, ctx);
  }

  public String getIndexName() {
    return candidate.getName();
  }
}
