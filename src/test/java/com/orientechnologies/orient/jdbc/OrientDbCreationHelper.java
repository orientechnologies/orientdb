package com.orientechnologies.orient.jdbc;

import java.util.Calendar;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import static org.junit.Assert.assertTrue;

public class OrientDbCreationHelper {

	public static void loadDB(String dbUrl, int documents) {
		createDB(dbUrl);

		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
		db.open("admin", "admin");
		db.declareIntent(new OIntentMassiveInsert());

		for (int i = 1; i <= documents; i++) {
			ODocument doc = new ODocument(db, "Item");
			doc = create(i, doc);
			db.save(doc, "Item");

		}

		db.declareIntent(null);
		db.close();

	}

	public static void createDB(String dbUrl) {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);

		if (db.exists()) db.delete();

		db.create();

		OSchema schema = db.getMetadata().getSchema();

		OClass item = schema.createClass("Item");

		item.createProperty("stringKey", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
		item.createProperty("intKey", OType.INTEGER).createIndex(INDEX_TYPE.UNIQUE);
		item.createProperty("date", OType.DATE).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("time", OType.DATETIME).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("text", OType.STRING);
		item.createProperty("length", OType.LONG).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("published", OType.BOOLEAN).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("title", OType.STRING).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("author", OType.STRING).createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("tags", OType.EMBEDDEDLIST);

		schema.save();

		schema.reload();

		db.close();

		// verify schema
		db.open("admin", "admin");

		assertTrue(db.getMetadata().getSchema().existsClass("Item"));

		db.close();
	}

	public static ODocument create(int id, ODocument doc) {
		String itemKey = Integer.valueOf(id).toString();

		doc.setClassName("Item");
		doc.field("stringKey", itemKey);
		doc.field("intKey", id);
		String contents =
				"OrientDB is a deeply scalable Document-Graph DBMS with the flexibility of the Document databases and the power to manage links of the Graph databases. It can work in schema-less mode, schema-full or a mix of both. Supports advanced features such as ACID Transactions, Fast Indexes, Native and SQL queries. It imports and exports documents in JSON. Graphs of hundreads of linked documents can be retrieved all in memory in few milliseconds without executing costly JOIN such as the Relational DBMSs do. OrientDB uses a new indexing algorithm called MVRB-Tree, derived from the Red-Black Tree and from the B+Tree with benefits of both: fast insertion and ultra fast lookup. The transactional engine can run in distributed systems supporting up to 9.223.372.036 Billions of records for the maximum capacity of 19.807.040.628.566.084 Terabytes of data distributed on multiple disks in multiple nodes. OrientDB is FREE for any use. Open Source License Apache 2.0. ";
		doc.field("text", contents);
		doc.field("title", "orientDB");
		doc.field("length", contents.length());
		doc.field("published", (id % 2 > 0));
		doc.field("author", "anAuthor" + id);
		// doc.field("tags", asList("java", "orient", "nosql"),
		// OType.EMBEDDEDLIST);
		Calendar instance = Calendar.getInstance();

		instance.add(Calendar.HOUR_OF_DAY, -id);
		doc.field("date", instance.getTime(), OType.DATE);
		doc.field("time", instance.getTime(), OType.DATETIME);

		return doc;

	}

}
