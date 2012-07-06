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
package com.orientechnologies.orient.core.storage.fs;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is mmap manager singleton factory. It used for getting mmap manager instance from any part of the system. This locator
 * know how to create mmap manager. If mmap manager already exist returns it.
 * 
 * @author Artem Loginov (logart) logart2007@gmail.com
 * 
 */
public class OMMapManagerLocator {

  private static final AtomicReference<OMMapManager> instanceRef = new AtomicReference<OMMapManager>(null);

  /**
   * This method returns instance of mmap manager.
   * 
   * @return mmap manager instance. If it is not exist create new one.
   */
  public static OMMapManager getInstance() {
    if (instanceRef.get() == null) {
			synchronized (instanceRef) {
				if (instanceRef.compareAndSet(null, createInstance())) {
					instanceRef.get().init();
				}
			}
    }
    return instanceRef.get();
  }

  /**
   * This method called from com.orientechnologies.orient.core.storage.fs.OMMapManagerLocator#getInstance() to create new mmap
   * manager and init it.
   * 
   * @return mmap manager instance.
   */
  private static OMMapManager createInstance() {
    final OMMapManager localInstance;
    if (OGlobalConfiguration.FILE_MMAP_USE_OLD_MANAGER.getValueAsBoolean()) {
      OLogManager.instance().config(null, "[OMMapManagerLocator.createInstance] Using old mmap manager!");
      localInstance = new OMMapManagerOld();
    } else {
      OLogManager.instance().config(null, "[OMMapManagerLocator.createInstance] Using new mmap manager!");
      localInstance = new OMMapManagerNew();
    }
    return localInstance;
  }
}
