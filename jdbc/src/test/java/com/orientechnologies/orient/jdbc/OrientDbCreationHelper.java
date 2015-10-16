package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

public class OrientDbCreationHelper {

  public static void loadDB(ODatabaseDocumentTx db, int documents) throws IOException {

    db.declareIntent(new OIntentMassiveInsert());

    for (int i = 1; i <= documents; i++) {
      ODocument doc = new ODocument();
      doc.setClassName("Item");
      doc = createItem(i, doc);
      db.save(doc, "Item");

    }

    db.declareIntent(null);

    createAuthorAndArticles(db, 50, 50);
    createArticleWithAttachmentSplitted(db);

  }

  public static void createSchemaDB(ODatabaseDocumentTx db) {

    OSchema schema = db.getMetadata().getSchema();

    // item
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

    // class Article
    OClass article = schema.createClass("Article");

    article.createProperty("uuid", OType.INTEGER).createIndex(INDEX_TYPE.UNIQUE);
    article.createProperty("date", OType.DATE).createIndex(INDEX_TYPE.NOTUNIQUE);
    article.createProperty("title", OType.STRING);
    article.createProperty("content", OType.STRING);
    // article.createProperty("attachment", OType.LINK);

    // author
    OClass author = schema.createClass("Author");

    author.createProperty("uuid", OType.LONG).createIndex(INDEX_TYPE.UNIQUE);
    author.createProperty("name", OType.STRING).setMin("3");
    author.createProperty("rating", OType.DOUBLE);
    author.createProperty("articles", OType.LINKLIST, article);

    // link article-->author
    article.createProperty("author", OType.LINK, author);

    schema.reload();

  }

  public static ODocument createItem(int id, ODocument doc) {
    String itemKey = Integer.valueOf(id).toString();

    doc.setClassName("Item");
    doc.field("stringKey", itemKey);
    doc.field("intKey", id);
    String contents = "OrientDB is a deeply scalable Document-Graph DBMS with the flexibility of the Document databases and the power to manage links of the Graph databases. "
        + "It can work in schema-less mode, schema-full or a mix of both. Supports advanced features such as ACID Transactions, Fast Indexes, Native and SQL queries."
        + " It imports and exports documents in JSON."
        + " Graphs of hundreads of linked documents can be retrieved all in memory in few milliseconds without executing costly JOIN such as the Relational DBMSs do. "
        + "OrientDB uses a new indexing algorithm called MVRB-Tree, derived from the Red-Black Tree and from the B+Tree with benefits of both: fast insertion and ultra fast lookup. "
        + "The transactional engine can run in distributed systems supporting up to 9.223.372.036 Billions of records for the maximum capacity of 19.807.040.628.566.084 Terabytes of data distributed on multiple disks in multiple nodes. "
        + "OrientDB is FREE for any use. Open Source License Apache 2.0. ";
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

  public static void createAuthorAndArticles(ODatabaseDocumentTx db, int totAuthors, int totArticles) throws IOException {
    int articleSerial = 0;
    for (int a = 1; a <= totAuthors; ++a) {
      ODocument author = new ODocument("Author");
      List<ODocument> articles = new ArrayList<ODocument>(totArticles);
      author.field("articles", articles);

      author.field("uuid", a);
      author.field("name", "Jay");
      author.field("rating", new Random().nextDouble());

      for (int i = 1; i <= totArticles; ++i) {
        ODocument article = new ODocument("Article");

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Date time = instance.getTime();
        article.field("date", time, OType.DATE);

        article.field("uuid", articleSerial++);
        article.field("title", "the title");
        article.field("content", "the content");
        article.field("attachment", loadFile(db, "./src/test/resources/file.pdf"));

        articles.add(article);
      }

      author.save();
    }
  }

  public static ODocument createArticleWithAttachmentSplitted(ODatabaseDocumentTx db) throws IOException {

    ODocument article = new ODocument("Article");
    Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    Date time = instance.getTime();
    article.field("date", time, OType.DATE);

    article.field("uuid", 1000000);
    article.field("title", "the title 2");
    article.field("content", "the content 2");
    if (new File("./src/test/resources/file.pdf").exists())
      article.field("attachment", loadFile(db, "./src/test/resources/file.pdf", 256));
    db.save(article);
    return article;
  }

  private static ORecordBytes loadFile(ODatabaseDocumentInternal database, String filePath) throws IOException {
    final File f = new File(filePath);
    if (f.exists()) {
      BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(f));
      ORecordBytes record = new ORecordBytes(database);
      record.fromInputStream(inputStream);
      return record;
    }

    return null;
  }

  private static List<ORID> loadFile(ODatabaseDocumentInternal database, String filePath, int bufferSize) throws IOException {
    File binaryFile = new File(filePath);
    long binaryFileLength = binaryFile.length();
    int numberOfRecords = (int) (binaryFileLength / bufferSize);
    int remainder = (int) (binaryFileLength % bufferSize);
    if (remainder > 0)
      numberOfRecords++;
    List<ORID> binaryChuncks = new ArrayList<ORID>(numberOfRecords);
    BufferedInputStream binaryStream = new BufferedInputStream(new FileInputStream(binaryFile));
    byte[] chunk;

    database.declareIntent(new OIntentMassiveInsert());
    ORecordBytes recordChunk;
    for (int i = 0; i < numberOfRecords; i++) {
      if (i == numberOfRecords - 1)
        chunk = new byte[remainder];
      else
        chunk = new byte[bufferSize];
      binaryStream.read(chunk);
      recordChunk = new ORecordBytes(database, chunk);
      database.save(recordChunk);
      binaryChuncks.add(recordChunk.getIdentity());
    }
    database.declareIntent(null);

    return binaryChuncks;
  }

}
