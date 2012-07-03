package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 03.07.12
 */

@Test
public class ByteArrayKeyTest {
	private ODatabaseDocumentTx database;
	private OIndex<?> index;

	protected OIndex<?> getIndex() {
		return database.getMetadata().getIndexManager().getIndex("byte-array-index");
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
		OIndex<?> index = getIndex();

		if (index == null) {
			index = database.getMetadata().getIndexManager()
							.createIndex("byte-array-index", "UNIQUE", new OSimpleKeyIndexDefinition(OType.BINARY), null, null);
			this.index = index;
		} else {
			index = database.getMetadata().getIndexManager().getIndex("byte-array-index");
			this.index = index;
		}
	}

	@AfterMethod
	public void afterMethod() {
		database.close();
	}

	@Parameters(value = "url")
	public ByteArrayKeyTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	public void testUsage() {
		OIndex<?> index = getIndex();
		byte[] key1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 1 };
		ODocument doc1 = new ODocument().field("k", "key1");
		index.put(key1, doc1);

		byte[] key2 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 2 };
		ODocument doc2 = new ODocument().field("k", "key1");
		index.put(key2, doc2);

		Assert.assertEquals(index.get(key1), doc1);
		Assert.assertEquals(index.get(key2), doc2);
	}

	public void testTransactionalUsageWorks() {
		database.begin(OTransaction.TXTYPE.OPTIMISTIC);
		byte[] key3 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 3 };
		ODocument doc1 = new ODocument().field("k", "key3");
		index.put(key3, doc1);

		byte[] key4 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 4 };
		ODocument doc2 = new ODocument().field("k", "key4");
		index.put(key4, doc2);

		database.commit();

		Assert.assertEquals(index.get(key3), doc1);
		Assert.assertEquals(index.get(key4), doc2);
	}

	@Test(dependsOnMethods = {"testTransactionalUsageWorks"})
	public void testTransactionalUsageBreaks1() {
		database.begin(OTransaction.TXTYPE.OPTIMISTIC);
		OIndex<?> index = getIndex();
		byte[] key5 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 5 };
		ODocument doc1 = new ODocument().field("k", "key5");
		index.put(key5, doc1);

		byte[] key6 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 6 };
		ODocument doc2 = new ODocument().field("k", "key6");
		index.put(key6, doc2);

		database.commit();

		Assert.assertEquals(index.get(key5), doc1);
		Assert.assertEquals(index.get(key6), doc2);
	}

	@Test(dependsOnMethods = {"testTransactionalUsageWorks"})
	public void testTransactionalUsageBreaks2() {
		OIndex<?> index = getIndex();
		database.begin(OTransaction.TXTYPE.OPTIMISTIC);
		byte[] key7 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 7 };
		ODocument doc1 = new ODocument().field("k", "key7");
		index.put(key7, doc1);

		byte[] key8 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
						3, 4, 5, 6, 7, 8, 9, 0, 8 };
		ODocument doc2 = new ODocument().field("k", "key8");
		index.put(key8, doc2);

		database.commit();

		Assert.assertEquals(index.get(key7), doc1);
		Assert.assertEquals(index.get(key8), doc2);
	}
}