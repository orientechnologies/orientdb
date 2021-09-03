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
package com.orientechnologies.orient.core.storage.impl.local;

/**
 * Interface for storage components that support freeze/release operations.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 1.5.1
 */
public interface OFreezableStorageComponent {

  /**
   * After this method finished it's execution, all threads that are going to perform data
   * modifications in storage should wait till {@link #release()} method will be called. This method
   * will wait till all ongoing modifications will be finished.
   *
   * @param throwException If <code>true</code> {@link
   *     com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *     exception will be thrown on call of methods that requires storage modification. Otherwise
   *     other threads will wait for {@link #release()} method call.
   */
  void freeze(boolean throwException);

  /**
   * After this method finished execution all threads that are waiting to perform data modifications
   * in storage will be awaken and will be allowed to continue their execution.
   */
  void release();
}
