/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.concur.resource;

/**
 * Interface to manage resources in the pool.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OResourcePoolListener<K, V> {

  /**
   * Creates a new resource to be used and to be pooled when the client finishes with it.
   *
   * @return The new resource
   */
  V createNewResource(K iKey, Object... iAdditionalArgs);

  /**
   * Reuses the pooled resource.
   *
   * @return true if can be reused, otherwise false. In this case the resource will be removed from
   *     the pool
   */
  boolean reuseResource(K iKey, Object[] iAdditionalArgs, V iValue);
}
