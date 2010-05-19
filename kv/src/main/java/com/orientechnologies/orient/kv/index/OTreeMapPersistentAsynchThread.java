/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.kv.index;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;

/**
 * Thread manager that synchronize all the registered maps to the database. This allow a huge speed improvement since every single
 * change can be scheduled to be persisted in asynchronous ways after a configured time.
 * 
 * @author Luca Garulli
 * 
 */
public class OTreeMapPersistentAsynchThread extends OSoftThread {

	private long																	delay			= 0;
	private Set<OTreeMapPersistentAsynch<?, ?>>		maps			= new HashSet<OTreeMapPersistentAsynch<?, ?>>();
	private static OTreeMapPersistentAsynchThread	instance	= new OTreeMapPersistentAsynchThread();

	public OTreeMapPersistentAsynchThread setDelay(final int iDelay) {
		delay = iDelay;
		return this;
	}

	/**
	 * Register the map to be synchronized every X seconds.
	 * 
	 * @param iMap
	 */
	public synchronized void registerMap(final OTreeMapPersistentAsynch<?, ?> iMap) {
		maps.add(iMap);
	}

	@Override
	protected synchronized void execute() throws Exception {
		for (OTreeMapPersistentAsynch<?, ?> map : maps) {
			try {
				synchronized (map) {

					map.executeCommitChanges();

				}
			} catch (Throwable t) {
				OLogManager.instance().error(this, "Error on commit changes in tree map: " + map, t);
			}
		}
	}

	@Override
	protected void afterExecution() throws InterruptedException {
		pauseCurrentThread(delay);
	}

	public static OTreeMapPersistentAsynchThread getInstance() {
		return instance;
	}
}
