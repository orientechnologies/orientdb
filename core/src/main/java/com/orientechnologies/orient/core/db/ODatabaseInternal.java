/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.core.db;

import java.util.concurrent.Callable;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage;

public interface ODatabaseInternal extends ODatabase {

  /**
   * Returns the underlying storage implementation.
   * 
   * @return The underlying storage implementation
   * @see OStorage
   */
  public OStorage getStorage();

  /**
   * Internal only: replace the storage with a new one.
   * 
   * @param iNewStorage
   *          The new storage to use. Usually it's a wrapped instance of the current cluster.
   */
  public void replaceStorage(OStorage iNewStorage);

  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock);


	public void resetInitialization();
}
