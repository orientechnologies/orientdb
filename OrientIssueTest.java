package orient.test;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;


public class OrientIssueTest {
    String cacheClass = "cacheTest";
    /**
     * The identifier field.
     */
    public static final String ID = "id";

    private ODatabaseDocumentPool dbObjectPool = null;
    /**
     * The version field.
     */
    public static final String VERSION = "version";

    public static final String GUID = "guid";

    public static final String RECORD_COUNT = "count";

    public static final String TID = "tid";

    @Before
    public void setUp() {

        ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:feeder.db");
        if (!db.exists()) {
            db.create();
        } else {
            clearDb();
        }
        db.close();
        dbObjectPool = new ODatabaseDocumentPool();
        dbObjectPool.setup(1, 25);
        createResourceClasses();
    }

    @After
    public void tearDown() {
        clearDb();
    }

    @Test
    public void testOrientIssue() {
        addResource("id", 1L, null, "value1", 10L, "guid1");
        Assert.assertEquals(null, getLatestTidAll("id"));
        Assert.assertEquals(null, getLatestTidAlone("id"));
        addResource("id", 2L, "1", "value2", 20L, "guid2");
        Assert.assertEquals("1", getLatestTidAll("id"));
        Assert.assertEquals("1", getLatestTidAlone("id"));
    }

    private boolean createResourceClasses() {
        boolean value = true;

        ODatabaseDocumentTx db = getConnection();
        try {

            // Check if the class exists.
            if (!db.getMetadata().getSchema().existsClass(cacheClass)) {
                // Create a class.
                db.getMetadata().getSchema().createClass(cacheClass).setOverSize(2);

                // Add a property ID.
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(ID, OType.STRING);

                // Add a property VERSION
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(VERSION, OType.LONG);

                // Add a property ZONE_ID.
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(GUID, OType.STRING);

                // Add a property RECORD_COUNT
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(RECORD_COUNT, OType.LONG);

                // Add a property TID
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(TID, OType.STRING);


                // Create unique index on ID
                db.getMetadata().getSchema().getClass(cacheClass)
                .createIndex("idIndex" + cacheClass,
                        INDEX_TYPE.NOTUNIQUE, ID);

                // Create unique index on {ID, VERSION}
                db.getMetadata().getSchema().getClass(cacheClass)
                .createIndex("idIndexVersion" + cacheClass,
                        INDEX_TYPE.UNIQUE, ID, VERSION);
                db.getMetadata().getSchema().save();
            }
        } catch (Exception ex) {
            value = false;
            // No action.
        } finally {
            releaseConnection(db);
        }
        return value;
    }
    /**
     * This method returns the document connection from the pool.
     *
     * @return The document connection.
     */
    final ODatabaseDocumentTx getConnection() {
        return dbObjectPool.acquire("local:feeder.db",
                "admin", "admin");
    }

    /**
     * This method release the document connection object back to the pool.
     *
     * @param db The document connection.
     */
    final void releaseConnection(final ODatabaseDocumentTx db) {
        db.declareIntent(null);
        dbObjectPool.release(db);
    }

    final void addResource(final String id,
            final Long version, final String tid, final String value,
            final Long count, final String guid) {
        ODatabaseDocumentTx db = getConnection();
        try {
            db.begin();
            ODocument doc = db.newInstance(cacheClass);
            doc.field(ID, id);
            doc.field(VERSION, version);
            doc.field("value", value);
            doc.field(TID, tid);
            doc.field(GUID, guid);
            doc.field(RECORD_COUNT, count);
            db.save(doc, cacheClass);
            db.commit();
        } finally {
            releaseConnection(db);
        }
    }

    private void clearDb() {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:feeder.db");
        db.open("admin", "admin");
        db.command(
                new OCommandSQL(
                "DELETE FROM cluster:cacheTest")).execute();
        db.close();
    }

    String getLatestTidAll(final String id) {
        OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(
                    "SELECT  FROM " + cacheClass
                    + " WHERE id = ? order by version desc LIMIT 1");
        ODatabaseDocumentTx db = getConnection();
        try  {
            List<ODocument> elements = db.command(query).execute(
                    id);
            if (!elements.isEmpty()) {
                return elements.get(0).field(TID);
            }
        } finally {
            releaseConnection(db);
        }
        return null;
    }

    String getLatestTidAlone(final String id) {
        OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(
                    "SELECT  " + TID + " FROM " + cacheClass
                    + " WHERE id = ? order by version desc LIMIT 1");
        ODatabaseDocumentTx db = getConnection();
        try  {
            List<ODocument> elements = db.command(query).execute(
                    id);
            if (!elements.isEmpty()) {
                return elements.get(0).field(TID);
            }
        } finally {
            releaseConnection(db);
        }
        return null;
    }
}

        ODatabaseDocumentTx db = getConnection();
        try {

            // Check if the class exists.
            if (!db.getMetadata().getSchema().existsClass(cacheClass)) {
                // Create a class.
                db.getMetadata().getSchema().createClass(cacheClass).setOverSize(2);

                // Add a property ID.
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(ID, OType.STRING);

                // Add a property VERSION
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(VERSION, OType.LONG);

                // Add a property ZONE_ID.
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(GUID, OType.STRING);

                // Add a property RECORD_COUNT
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(RECORD_COUNT, OType.LONG);

                // Add a property TID
                db.getMetadata().getSchema().getClass(cacheClass)
                .createProperty(TID, OType.STRING);


                // Create unique index on ID
                db.getMetadata().getSchema().getClass(cacheClass)
                .createIndex("idIndex" + cacheClass,
                        INDEX_TYPE.NOTUNIQUE, ID);

                // Create unique index on {ID, VERSION}
                db.getMetadata().getSchema().getClass(cacheClass)
                .createIndex("idIndexVersion" + cacheClass,
                        INDEX_TYPE.UNIQUE, ID, VERSION);
                db.getMetadata().getSchema().save();
            }
        } catch (Exception ex) {
            value = false;
            // No action.
        } finally {
            releaseConnection(db);
        }
        return value;
    }
    /**
     * This method returns the document connection from the pool.
     *
     * @return The document connection.
     */
    final ODatabaseDocumentTx getConnection() {
        return dbObjectPool.acquire("local:feeder.db",
                "admin", "admin");
    }

    /**
     * This method release the document connection object back to the pool.
     *
     * @param db The document connection.
     */
    final void releaseConnection(final ODatabaseDocumentTx db) {
        db.declareIntent(null);
        dbObjectPool.release(db);
    }

    final void addResource(final String id,
            final Long version, final String tid, final String value,
            final Long count, final String guid) {
        ODatabaseDocumentTx db = getConnection();
        try {
            db.begin();
            ODocument doc = db.newInstance(cacheClass);
            doc.field(ID, id);
            doc.field(VERSION, version);
            doc.field("value", value);
            doc.field(TID, tid);
            doc.field(GUID, guid);
            doc.field(RECORD_COUNT, count);
            db.save(doc, cacheClass);
            db.commit();
        } finally {
            releaseConnection(db);
        }
    }

    private void clearDb() {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:feeder.db");
        db.open("admin", "admin");
        db.command(
                new OCommandSQL(
                "DELETE FROM cluster:cacheTest")).execute();
        db.close();
    }

    String getLatestTidAll(final String id) {
        OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(
                    "SELECT  FROM " + cacheClass
                    + " WHERE id = ? order by version desc LIMIT 1");
        ODatabaseDocumentTx db = getConnection();
        try  {
            List<ODocument> elements = db.command(query).execute(
                    id);
            if (!elements.isEmpty()) {
                return elements.get(0).field(TID);
            }
        } finally {
            releaseConnection(db);
        }
        return null;
    }

    String getLatestTidAlone(final String id) {
        OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>("SELECT " + TID + " FROM " + cacheClass
                    + " WHERE id = ? order by version desc LIMIT 1");
        ODatabaseDocumentTx db = getConnection();
        try  {
            List<ODocument> elements = db.command(query).execute(
                    id);
            if (!elements.isEmpty()) {
                return elements.get(0).field(TID);
            }
        } finally {
            releaseConnection(db);
        }
        return null;
    }
}
