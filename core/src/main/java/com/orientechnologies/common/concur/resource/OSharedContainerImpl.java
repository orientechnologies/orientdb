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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared container that works with callbacks like closures. If the resource implements the {@link OSharedResource} interface then
 * the resource is locked until is removed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSharedContainerImpl implements OSharedContainer {
  protected Map<String, Object> sharedResources = new ConcurrentHashMap<String, Object>();

  public boolean existsResource(final String iName) {
    // BYPASS THE SYNCHRONIZED BLOCK BECAUSE THE MAP IS ALREADY SYNCHRONIZED
    return sharedResources.containsKey(iName);
  }

  public <T> T removeResource(final String iName) {
    synchronized (this) {
      T resource = (T) sharedResources.remove(iName);

      if (resource instanceof OSharedResource)
        ((OSharedResource) resource).releaseExclusiveLock();
      return resource;
    }
  }

  public <T> T getResource(final String iName, final Callable<T> iCallback) {
    T value = (T) sharedResources.get(iName);
    if (value == null) {
      // THE SYNCHRONIZED BLOCK I CREATES NEEDED ONLY TO PREVENT MULTIPLE CALL TO THE CALLBACK IN CASE OF CONCURRENT
      synchronized (this) {
        if (value == null) {
          // CREATE IT
          try {
            value = iCallback.call();
          } catch (Exception e) {
            throw OException.wrapException(new ODatabaseException("Error on creation of shared resource"), e);
          }
          
          if (value instanceof OSharedResource)
            ((OSharedResource) value).acquireExclusiveLock();

          sharedResources.put(iName, value);
        }
      }
    }

    return value;
  }

  public void clearResources() {
    synchronized (this) {
      for (Object resource : sharedResources.values()) {
        if (resource instanceof OCloseable)
          (((OCloseable) resource)).close();
      }

      sharedResources.clear();
    }
  }
}
