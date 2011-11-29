/**
 * 
 */
package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author Michael Hiess
 * 
 */
public class MultipleDBTest {

	private String	baseUrl;

	@Parameters(value = "url")
	public MultipleDBTest(String iURL) {
		baseUrl = iURL + "-";
	}

	@Test
	public void testObjectMultipleDBsThreaded() throws Exception {

		final int operations_write = 1000;
		final int operations_read = 1;
		final int dbs = 10;

		final Semaphore sem = new Semaphore(4, true);
		final AtomicInteger activeDBs = new AtomicInteger(dbs);
		final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

		for (int i = 0; i < dbs; i++) {

			sem.acquire();
			final String dbUrl = baseUrl + i;

			Thread t = new Thread(new Runnable() {

				public void run() {

					ODatabaseObjectTx tx = new ODatabaseObjectTx(dbUrl);

					try {
						ODatabaseHelper.deleteDatabase(tx);
						ODatabaseHelper.createDatabase(tx, dbUrl);
					} catch (IOException e) {
						e.printStackTrace();
					}

					try {
						System.out.println("(" + dbUrl + ") " + "Created");

						if (tx.isClosed()) {
							tx.open("admin", "admin");
						}
						tx.getEntityManager().registerEntityClass(DummyObject.class);

						// System.out.println("(" + dbUrl + ") " + "Registered: " + DummyObject.class);
						// System.out.println("(" + dbUrl + ") " + "Calling: " + operations + " operations");
						long start = System.currentTimeMillis();
						for (int j = 0; j < operations_write; j++) {
							DummyObject dummy = new DummyObject("name" + j);
							tx.save(dummy);

							Assert.assertEquals(dummy.getId().toString(), "#5:" + j);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + dbUrl + ") " + "Operations (WRITE) executed: " + (j + 1));
							}
						}
						long end = System.currentTimeMillis();

						String time = "(" + dbUrl + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						start = System.currentTimeMillis();
						for (int j = 0; j < operations_read; j++) {
							List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
							Assert.assertEquals(l.size(), operations_write);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + dbUrl + ") " + "Operations (READ) executed: " + j + 1);
							}
						}
						end = System.currentTimeMillis();

						time = "(" + dbUrl + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						tx.close();

						sem.release();
						activeDBs.decrementAndGet();
					} finally {
						try {
							ODatabaseHelper.deleteDatabase(tx);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});

			t.start();
		}

		while (activeDBs.get() != 0) {
			Thread.sleep(300);
		}

		// System.out.println("Times:");
		// for (String s : times) {
		// System.out.println(s);
		// }

		System.out.println("Test testObjectMultipleDBsThreaded ended");
	}

	@Test
	public void testDocumentMultipleDBsThreaded() throws Exception {

		final int operations_write = 1000;
		final int operations_read = 1;
		final int dbs = 10;

		final Semaphore sem = new Semaphore(4, true);
		final AtomicInteger activeDBs = new AtomicInteger(dbs);
		final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

		for (int i = 0; i < dbs; i++) {

			sem.acquire();
			final String dbUrl = baseUrl + i;

			Thread t = new Thread(new Runnable() {

				public void run() {

					ODatabaseDocumentTx tx = new ODatabaseDocumentTx(dbUrl);

					try {
						ODatabaseHelper.deleteDatabase(tx);
						ODatabaseHelper.createDatabase(tx, dbUrl);
					} catch (IOException e) {
						e.printStackTrace();
					}

					try {
						System.out.println("(" + dbUrl + ") " + "Created");

						if (tx.isClosed()) {
							tx.open("admin", "admin");
						}
						// tx.getEntityManager().registerEntityClass(DummyObject.class);

						// System.out.println("(" + dbUrl + ") " + "Registered: " + DummyObject.class);
						// System.out.println("(" + dbUrl + ") " + "Calling: " + operations + " operations");
						long start = System.currentTimeMillis();
						for (int j = 0; j < operations_write; j++) {
							// DummyObject dummy = new DummyObject("name" + j);
							// tx.save(dummy);

							ODocument dummy = new ODocument(tx, "DummyObject");
							dummy.field("name", "name" + j);

							tx.save(dummy);
							Assert.assertEquals(dummy.getIdentity().toString(), "#5:" + j);

							// Assert.assertEquals(dummy.getId().toString(), "#5:" + j);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + dbUrl + ") " + "Operations (WRITE) executed: " + (j + 1));
							}
						}
						long end = System.currentTimeMillis();

						String time = "(" + dbUrl + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						start = System.currentTimeMillis();
						for (int j = 0; j < operations_read; j++) {
							List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
							Assert.assertEquals(l.size(), operations_write);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + dbUrl + ") " + "Operations (READ) executed: " + j + 1);
							}
						}
						end = System.currentTimeMillis();

						time = "(" + dbUrl + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						tx.close();

						sem.release();
						activeDBs.decrementAndGet();
					} finally {
						try {
							ODatabaseHelper.deleteDatabase(tx);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});

			t.start();
		}

		while (activeDBs.get() != 0) {
			Thread.sleep(300);
		}

		// System.out.println("Times:");
		// for (String s : times) {
		// System.out.println(s);
		// }

		System.out.println("Test testDocumentMultipleDBsThreaded ended");
	}

}
