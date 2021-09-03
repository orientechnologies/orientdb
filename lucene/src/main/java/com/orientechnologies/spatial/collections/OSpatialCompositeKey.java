/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.collections;

import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import java.util.List;
import org.apache.lucene.spatial.query.SpatialOperation;

public class OSpatialCompositeKey extends OLuceneCompositeKey {

  private double maxDistance;

  private SpatialOperation operation;

  public OSpatialCompositeKey(final List<?> keys) {
    super(keys);
  }

  public double getMaxDistance() {
    return maxDistance;
  }

  public OSpatialCompositeKey setMaxDistance(double maxDistance) {
    this.maxDistance = maxDistance;
    return this;
  }

  public SpatialOperation getOperation() {
    return operation;
  }

  public OSpatialCompositeKey setOperation(SpatialOperation operation) {
    this.operation = operation;
    return this;
  }
}
