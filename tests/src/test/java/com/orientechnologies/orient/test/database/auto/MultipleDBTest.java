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
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORID;
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
						System.out.println("(" + getDbId(tx) + ") " + "Created");

						if (tx.isClosed()) {
							tx.open("admin", "admin");
						}
						tx.getEntityManager().registerEntityClass(DummyObject.class);

						// System.out.println("(" +getDbId( tx ) + ") " + "Registered: " + DummyObject.class);
						// System.out.println("(" +getDbId( tx ) + ") " + "Calling: " + operations + " operations");
						long start = System.currentTimeMillis();
						for (int j = 0; j < operations_write; j++) {
							DummyObject dummy = new DummyObject("name" + j);
							tx.save(dummy);

							Assert.assertEquals(((ORID) dummy.getId()).getClusterPosition(), j);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + getDbId(tx) + ") " + "Operations (WRITE) executed: " + (j + 1));
							}
						}
						long end = System.currentTimeMillis();

						String time = "(" + getDbId(tx) + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						start = System.currentTimeMillis();
						for (int j = 0; j < operations_read; j++) {
							List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
							Assert.assertEquals(l.size(), operations_write);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + getDbId(tx) + ") " + "Operations (READ) executed: " + j + 1);
							}
						}
						end = System.currentTimeMillis();

						time = "(" + getDbId(tx) + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
						System.out.println(time);
						times.add(time);

						tx.close();

						sem.release();
						activeDBs.decrementAndGet();
					} finally {
						try {
							System.out.println("(" + getDbId(tx) + ") " + "Dropping");
							System.out.flush();
							ODatabaseHelper.deleteDatabase(tx);
							System.out.println("(" + getDbId(tx) + ") " + "Dropped");
							System.out.flush();
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

			final String dbUrl = baseUrl + i;

			sem.acquire();
			Thread t = new Thread(new Runnable() {

				public void run() {

					ODatabaseDocumentTx tx = new ODatabaseDocumentTx(dbUrl);

					try {
						ODatabaseHelper.deleteDatabase(tx);
						System.out.println("Thread " + this + " is creating database " + dbUrl);
						System.out.flush();
						ODatabaseHelper.createDatabase(tx, dbUrl);
					} catch (IOException e) {
						e.printStackTrace();
					}

					try {
						System.out.println("(" + getDbId(tx) + ") " + "Created");
						System.out.flush();

						if (tx.isClosed()) {
							tx.open("admin", "admin");
						}
						// tx.getEntityManager().registerEntityClass(DummyObject.class);

						// System.out.println("(" +getDbId( tx ) + ") " + "Registered: " + DummyObject.class);
						// System.out.println("(" +getDbId( tx ) + ") " + "Calling: " + operations + " operations");
						long start = System.currentTimeMillis();
						for (int j = 0; j < operations_write; j++) {
							// DummyObject dummy = new DummyObject("name" + j);
							// tx.save(dummy);

							ODocument dummy = new ODocument("DummyObject");
							dummy.field("name", "name" + j);

							tx.save(dummy);
							Assert.assertEquals(((ORID) dummy.getIdentity()).getClusterPosition(), j);

							// Assert.assertEquals(dummy.getId().toString(), "#5:" + j);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + getDbId(tx) + ") " + "Operations (WRITE) executed: " + (j + 1));
								System.out.flush();
							}
						}
						long end = System.currentTimeMillis();

						String time = "(" + getDbId(tx) + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
						System.out.println(time);
						System.out.flush();

						times.add(time);

						start = System.currentTimeMillis();
						for (int j = 0; j < operations_read; j++) {
							List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
							Assert.assertEquals(l.size(), operations_write);

							if ((j + 1) % 20000 == 0) {
								System.out.println("(" + getDbId(tx) + ") " + "Operations (READ) executed: " + j + 1);
								System.out.flush();
							}
						}
						end = System.currentTimeMillis();

						time = "(" + getDbId(tx) + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
						System.out.println(time);
						System.out.flush();

						times.add(time);

					} finally {
						try {
							tx.close();

							System.out.println("Thread " + this + "  is dropping database " + dbUrl);
							System.out.flush();
							ODatabaseHelper.deleteDatabase(tx);

							sem.release();
							activeDBs.decrementAndGet();
						} catch (Exception e) {
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
		System.out.flush();
	}

	private String getDbId(ODatabase tx) {
		if (tx.getStorage() instanceof OStorageRemote)
			return tx.getURL() + " - sessionId: " + ((OStorageRemote) tx.getStorage()).getSessionId();
		else
			return tx.getURL();
	}

}
