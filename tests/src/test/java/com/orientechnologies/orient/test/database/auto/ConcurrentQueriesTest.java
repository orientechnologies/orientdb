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
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class ConcurrentQueriesTest {
	private final static int	THREADS	= 10;
	protected String					url;
	private boolean						level1CacheEnabled;
	private boolean						level2CacheEnabled;
	private boolean						mvccEnabled;

	static class CommandExecutor implements Runnable {

		String	url;
		String	threadName;

		public CommandExecutor(String url, String iThreadName) {
			super();
			this.url = url;
			threadName = iThreadName;
		}

		public void run() {
			try {
				for (int i = 0; i < 50; i++) {
					ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");
					try {
						while (true) {
							try {
								List<ODocument> result = db.command(new OCommandSQL("select from Concurrent")).execute();
								System.out.println("Thread " + threadName + " result = " + result.size());
								break;
							} catch (ONeedRetryException e) {
								// e.printStackTrace();
								System.out.println("Retry...");
							}
						}
					} finally {
						db.close();
					}
				}

			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	@Parameters(value = "url")
	public ConcurrentQueriesTest(@Optional(value = "memory:test") String iURL) {
		url = iURL;
	}

	@BeforeClass
	public void init() {
		if ("memory:test".equals(url))
			new ODatabaseDocumentTx(url).create().close();

		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");
		db.getMetadata().getSchema().createClass("Concurrent");

		for (int i = 0; i < 1000; ++i) {
			db.newInstance("Concurrent").field("test", i).save();
		}
	}

	@Test
	public void concurrentCommands() throws Exception {
		Thread[] threads = new Thread[THREADS];
		System.out.println("Spanning " + THREADS + " threads...");
		for (int i = 0; i < THREADS; ++i) {
			threads[i] = new Thread(new CommandExecutor(url, "thread1"), "ConcurrentTest1");
		}

		System.out.println("Starting " + THREADS + " threads...");
		for (int i = 0; i < THREADS; ++i) {
			threads[i].start();
		}

		System.out.println("Waiting for " + THREADS + " threads...");
		for (int i = 0; i < THREADS; ++i) {
			threads[i].join();
		}
	}
}
