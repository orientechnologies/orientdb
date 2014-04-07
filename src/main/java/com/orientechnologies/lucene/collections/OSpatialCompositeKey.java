package com.orientechnologies.lucene.collections;

import com.orientechnologies.orient.core.index.OCompositeKey;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.List;

/**
 * Created by enricorisa on 02/04/14.
 */
public class OSpatialCompositeKey extends OCompositeKey {

  private double           maxDistance;

  private SpatialOperation operation;

  public OSpatialCompositeKey(final List<?> keys) {
    super(keys);
  }

  public OSpatialCompositeKey setMaxDistance(double maxDistance) {
    this.maxDistance = maxDistance;
    return this;
  }

  public double getMaxDistance() {
    return maxDistance;
  }

  public OSpatialCompositeKey setOperation(SpatialOperation operation) {
    this.operation = operation;
    return this;
  }

  public SpatialOperation getOperation() {
    return operation;
  }
}
