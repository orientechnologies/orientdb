package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Created by luigidellaquila on 11/10/16. */
public class CartesianProductStep extends AbstractExecutionStep {

  private List<OInternalExecutionPlan> subPlans = new ArrayList<>();

  public CartesianProductStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    Stream<OResult[]> stream = null;
    OResult[] productTuple = new OResult[this.subPlans.size()];

    for (int i = 0; i < this.subPlans.size(); i++) {
      OInternalExecutionPlan ep = this.subPlans.get(i);
      final int pos = i;
      if (stream == null) {
        OExecutionStream es = ep.start(ctx);
        stream =
            es.stream(ctx)
                .map(
                    (value) -> {
                      productTuple[pos] = value;
                      return productTuple;
                    })
                .onClose(() -> es.close(ctx));
      } else {
        stream =
            stream.flatMap(
                (val) -> {
                  OExecutionStream es = ep.start(ctx);
                  return es.stream(ctx)
                      .map(
                          (value) -> {
                            val[pos] = value;
                            return val;
                          })
                      .onClose(() -> es.close(ctx));
                });
      }
    }
    Stream<OResult> finalStream = stream.map(this::produceResult);
    return OExecutionStream.resultIterator(finalStream.iterator())
        .onClose((context) -> finalStream.close());
  }

  private OResult produceResult(OResult[] path) {

    OResultInternal nextRecord = new OResultInternal();

    for (int i = 0; i < path.length; i++) {
      OResult res = path[i];
      for (String s : res.getPropertyNames()) {
        nextRecord.setProperty(s, res.getProperty(s));
      }
    }
    return nextRecord;
  }

  public void addSubPlan(OInternalExecutionPlan subPlan) {
    this.subPlans.add(subPlan);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String result = "";
    String ind = OExecutionStepInternal.getIndent(ctx);

    int[] blockSizes = new int[subPlans.size()];

    for (int i = 0; i < subPlans.size(); i++) {
      OInternalExecutionPlan currentPlan = subPlans.get(subPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(ctx);

      String[] partials = partial.split("\n");
      blockSizes[subPlans.size() - 1 - i] = partials.length + 2;
      result = "+-------------------------\n" + result;
      for (int j = 0; j < partials.length; j++) {
        String p = partials[partials.length - 1 - j];
        if (result.length() > 0) {
          result = appendPipe(p) + "\n" + result;
        } else {
          result = appendPipe(p);
        }
      }
      result = "+-------------------------\n" + result;
    }
    result = addArrows(result, blockSizes);
    result += foot(blockSizes);
    result = ind + result;
    result = result.replaceAll("\n", "\n" + ind);
    result = head(ctx, subPlans.size()) + "\n" + result;
    return result;
  }

  private String addArrows(String input, int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result.append("-");
          } else if (isPlus(col, subRow, block, blockSize)) {
            result.append("+");
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result.append("|");
          } else {
            result.append(" ");
          }
        }
        result.append(rows[rowNum]);
        result.append("\n");
        rowNum++;
      }
    }

    return result.toString();
  }

  private boolean isHorizontalRow(int col, int subRow, int block, int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    if (subRow == blockSize / 2) {
      return true;
    }
    return false;
  }

  private boolean isPlus(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      if (subRow == blockSize / 2) {
        return true;
      }
    }
    return false;
  }

  private boolean isVerticalRow(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      if (subRow > blockSize / 2) {
        return true;
      }
    } else if (col < block * 3 + 1 && col % 3 == 1) {
      return true;
    }

    return false;
  }

  private String head(OPrintContext ctx, int nItems) {
    String ind = OExecutionStepInternal.getIndent(ctx);
    String result = ind + "+ CARTESIAN PRODUCT";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }

  private String foot(int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < blockSizes.length; i++) {
      result.append(" V ");
    }
    return result.toString();
  }

  private String appendPipe(String p) {
    return "| " + p;
  }
}
