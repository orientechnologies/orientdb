/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.collections;

import com.orientechnologies.orient.core.index.OCompositeKey;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.List;

public class OSpatialCompositeKey extends OLuceneCompositeKey {

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
