package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;

/**
 * For internal use. It is used to keep info about an index range search, where the main condition
 * has the lower bound and the additional condition has the upper bound on last field only
 */
class IndexCondPair {

  protected OAndBlock mainCondition;
  protected OBinaryCondition additionalRange;

  public IndexCondPair(OAndBlock keyCondition, OBinaryCondition additionalRangeCondition) {
    this.mainCondition = keyCondition;
    this.additionalRange = additionalRangeCondition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexCondPair that = (IndexCondPair) o;

    if (mainCondition != null
        ? !mainCondition.equals(that.mainCondition)
        : that.mainCondition != null) return false;
    if (additionalRange != null
        ? !additionalRange.equals(that.additionalRange)
        : that.additionalRange != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = mainCondition != null ? mainCondition.hashCode() : 0;
    result = 31 * result + (additionalRange != null ? additionalRange.hashCode() : 0);
    return result;
  }
}
