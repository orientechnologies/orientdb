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
package com.orientechnologies.common.concur.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;

/**
 * Shared container that works with callbacks like closures. If the resource implements the {@link OSharedResource} interface then
 * the resource is locked until is removed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSharedContainerImpl implements OSharedContainer {
  protected Map<String, Object> sharedResources = new HashMap<String, Object>();

  public synchronized boolean existsResource(final String iName) {
    return sharedResources.containsKey(iName);
  }

  public synchronized <T> T removeResource(final String iName) {
    T resource = (T) sharedResources.remove(iName);

    if (resource instanceof OSharedResource)
      ((OSharedResource) resource).releaseExclusiveLock();

    return resource;
  }

  public synchronized <T> T getResource(final String iName, final Callable<T> iCallback) {
    T value = (T) sharedResources.get(iName);
    if (value == null) {
      // CREATE IT
      try {
        value = iCallback.call();
      } catch (Exception e) {
        throw new OException("Error on creation of shared resource", e);
      }

      if (value instanceof OSharedResource)
        ((OSharedResource) value).acquireExclusiveLock();

      sharedResources.put(iName, value);
    }

    return value;
  }

  public synchronized void clearResources() {
    for (Object resource : sharedResources.values()) {
      if (resource instanceof OCloseable)
        (((OCloseable) resource)).close();
    }

    sharedResources.clear();
  }
}
