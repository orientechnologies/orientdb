package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila
 */
public class ParallelExecStep extends AbstractExecutionStep {
  private final List<OInternalExecutionPlan> subExecuitonPlans;

  int current = 0;
  private OTodoResultSet currentResultSet = null;

  public ParallelExecStep(List<OInternalExecutionPlan> subExecuitonPlans, OCommandContext ctx) {
    super(ctx);
    this.subExecuitonPlans = subExecuitonPlans;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return new OTodoResultSet() {
      int localCount = 0;

      @Override public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(ctx, nRecords);
          if (currentResultSet == null) {
            return false;
          }
        }
        return true;
      }

      @Override public OResult next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        while (currentResultSet == null || !currentResultSet.hasNext()) {
          fetchNext(ctx, nRecords);
          if (currentResultSet == null) {
            throw new IllegalStateException();
          }
        }
        localCount++;
        return currentResultSet.next();
      }

      @Override public void close() {

      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  void fetchNext(OCommandContext ctx, int nRecords) {
    do {
      if (current > subExecuitonPlans.size()) {
        currentResultSet = null;
        return;
      }
      currentResultSet = subExecuitonPlans.get(current).fetchNext(nRecords);
      if (!currentResultSet.hasNext()) {
        current++;
      }
    } while (!currentResultSet.hasNext());
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {
    //TODO
  }

  @Override public void sendResult(Object o, Status status) {
    //TODO
  }

  @Override public String prettyPrint(int depth, int indent) {
    String result = "";
    String ind = OExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subExecuitonPlans.size()];

    for (int i = 0; i < subExecuitonPlans.size(); i++) {
      OInternalExecutionPlan currentPlan = subExecuitonPlans.get(subExecuitonPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subExecuitonPlans.size() - 1 - i] = partials.length + 2;
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
    result = head(depth, indent, subExecuitonPlans.size()) + "\n" + result;
    return result;
  }

  private String addArrows(String input, int[] blockSizes) {
    String result = "";
    String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result += "-";
          } else if (isPlus(col, subRow, block, blockSize)) {
            result += "+";
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result += "|";
          } else {
            result += " ";
          }
        }
        result += rows[rowNum] + "\n";
        rowNum++;
      }
    }

    return result;
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
    String result = "";
    for (int i = 0; i < blockSizes.length; i++) {
      result += " V ";//TODO
    }
    return result;
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
}
