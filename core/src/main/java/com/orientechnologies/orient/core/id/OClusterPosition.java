/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.id;

/**
 * Abstraction of records position in cluster. You can think about it as about of n-bit number. Real instances of cluster position
 * should be created using {@link OClusterPositionFactory} factory. Cluster positions are immutable objects.
 * 
 * @author Andrey Lomakin
 * @since 12.11.12
 */
public abstract class OClusterPosition extends Number implements Comparable<OClusterPosition> {
  public final static OClusterPosition INVALID_POSITION = OClusterPositionFactory.INSTANCE.valueOf(-1);

  /**
   * Creates custer position with value which is higher than current value by one.
   * 
   * @return custer position with value which is higher than current value by one.
   */
  public abstract OClusterPosition inc();

  /**
   * Creates custer position with value which is less than current value by one.
   * 
   * @return custer position with value which is less than current value by one.
   */
  public abstract OClusterPosition dec();

  /**
   * @return <code>true</code> if current cluster position values does not equal to {@link #INVALID_POSITION}
   */
  public abstract boolean isValid();

  /**
   * @return <code>true</code> if record with current cluster position can be stored in DB. (non-negative value)
   */
  public abstract boolean isPersistent();

  /**
   * @return <code>true</code> if record with current cluster position can not be stored in DB. (negative value)
   */
  public abstract boolean isNew();

  /**
   * @return <code>true</code> if record with current cluster position is not intended to be stored in DB and used for internal
   *         (system) needs.
   */
  public abstract boolean isTemporary();

  /**
   * @return Serialized presentation of cluster position. Does not matter which value it holds it will be the same length equals to
   *         result of the function {@link com.orientechnologies.orient.core.id.OClusterPositionFactory#getSerializedSize()}
   */
  public abstract byte[] toStream();
}
