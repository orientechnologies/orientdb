package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsExecutionStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ParallelExecStep extends AbstractExecutionStep {
  private final List<OInternalExecutionPlan> subExecutionPlans;

  public ParallelExecStep(
      List<OInternalExecutionPlan> subExecuitonPlans,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecutionPlans = subExecuitonPlans;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return new OSubResultsExecutionStream(
        subExecutionPlans.stream().map((step) -> step.start()).iterator());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = "";
    String ind = OExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subExecutionPlans.size()];

    for (int i = 0; i < subExecutionPlans.size(); i++) {
      OInternalExecutionPlan currentPlan = subExecutionPlans.get(subExecutionPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subExecutionPlans.size() - 1 - i] = partials.length + 2;
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
    result = head(depth, indent, subExecutionPlans.size()) + "\n" + result;
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

  private String head(int depth, int indent, int nItems) {
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    return ind + "+ PARALLEL";
  }

  private String foot(int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < blockSizes.length; i++) {
      result.append(" V ");
    }
    return result.toString();
  }

  private String spaces(int num) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < num; i++) {
      result.append(" ");
    }
    return result.toString();
  }

  private String appendPipe(String p) {
    return "| " + p;
  }

  public List<OExecutionPlan> getSubExecutionPlans() {
    return (List) subExecutionPlans;
  }

  @Override
  public boolean canBeCached() {
    for (OInternalExecutionPlan plan : subExecutionPlans) {
      if (!plan.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new ParallelExecStep(
        subExecutionPlans.stream().map(x -> x.copy(ctx)).collect(Collectors.toList()),
        ctx,
        profilingEnabled);
  }
}
