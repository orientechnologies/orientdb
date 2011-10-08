package com.orientechnologies.orient.jdbc;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientDbCreationHelper {

	public static void loadDB(ODatabaseDocumentTx db, int documents) {

		db.declareIntent(new OIntentMassiveInsert());

		for (int i = 1; i <= documents; i++) {
			ODocument doc = new ODocument(db, "Item");
			doc = createItem(i, doc);
			db.save(doc, "Item");

		}

		db.declareIntent(null);

	}

	public static void createDB(ODatabaseDocumentTx db) {
		if (db.exists()) db.delete();

		db.create();

		OSchema schema = db.getMetadata()
			.getSchema();

		// item
		OClass item = schema.createClass("Item");

		item.createProperty("stringKey", OType.STRING)
			.createIndex(INDEX_TYPE.UNIQUE);
		item.createProperty("intKey", OType.INTEGER)
			.createIndex(INDEX_TYPE.UNIQUE);
		item.createProperty("date", OType.DATE)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("time", OType.DATETIME)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("text", OType.STRING);
		item.createProperty("length", OType.LONG)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("published", OType.BOOLEAN)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("title", OType.STRING)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("author", OType.STRING)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		item.createProperty("tags", OType.EMBEDDEDLIST);

		// class Article
		OClass article = schema.createClass("Article");

		article.createProperty("uuid", OType.INTEGER)
			.createIndex(INDEX_TYPE.UNIQUE);
		article.createProperty("date", OType.DATE)
			.createIndex(INDEX_TYPE.NOTUNIQUE);
		article.createProperty("title", OType.STRING);
		article.createProperty("content", OType.STRING);

		// author
		OClass author = schema.createClass("Author");

		author.createProperty("uuid", OType.INTEGER)
			.createIndex(INDEX_TYPE.UNIQUE);
		author.createProperty("name", OType.STRING)
			.setMin("3");
		author.createProperty("articles", OType.LINKLIST, article);

		// link article-->author
		article.createProperty("author", OType.LINK, author);

		schema.save();

		schema.reload();

	}

	public static ODocument createItem(int id, ODocument doc) {
		String itemKey = Integer.valueOf(id)
			.toString();

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
		Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		instance.add(Calendar.HOUR_OF_DAY, -id);
		Date time = instance.getTime();
		doc.field("date", time, OType.DATE);
		doc.field("time", time, OType.DATETIME);

		return doc;

	}

}
