package com.orientechnologies.orient.test.internal;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class WrongUpdateTest {
  static {
    OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.setValue(-1);
  }

  private static final int NUM_RETRIES = 5;

  public static void main(String[] args) throws Throwable {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:indexExceptionTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();
    databaseDocumentTx.close();

    initializeExceptionRecordTable(databaseDocumentTx);

    System.out.println("Starting Writer...");
    try {
      for (int i = 0; i < 10000; i++) {
        String id = createExceptionRecord(i);
        updateRecord(id);
        updateRecord(id);
        updateRecord(id);
        checkRecord(id);
        if (i % 100 == 0)
          System.out.println(i + " records were created.");
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void checkRecord(String id) throws Exception {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:indexExceptionTest");
    database.open("admin", "admin");
    try {
      ODocument document = findById(database, id);
      if (!"test updated".equals(document.field("test"))) {
        throw new Exception("Found an value that was not updated");
      }
    } finally {
      if (!database.isClosed()) {
        database.close();
      }
    }
  }

  protected static void initializeExceptionRecordTable(ODatabaseDocumentTx database) {
    database.open("admin", "admin");
    OClass exceptionRecordClass = database.getMetadata().getSchema().createClass("ExceptionRecord");
    try {
      exceptionRecordClass.createProperty("id", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
      exceptionRecordClass.createProperty("status", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("approved", OType.BOOLEAN).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("recordType", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("groupNonException", OType.BOOLEAN).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("comment", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("jobId", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("dataflowName", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("username", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("timestamp", OType.LONG).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("stageLabel", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("groupColumn", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("lastModifiedBy", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("lastModified", OType.LONG).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      exceptionRecordClass.createProperty("data", OType.STRING);

    } finally {
      database.close();
    }
  }

  private static void updateRecord(String id) throws Throwable {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:indexExceptionTest");
    database.open("admin", "admin");
    try {

      int numRetries = 0;
      while (numRetries < NUM_RETRIES) {
        try {
          ODocument document = findById(database, id);
          document.field("test", "test updated");
          database.save(document);
          break;
        } catch (Throwable e) {
          System.out.println("********************************");
          System.out.println("Update Iteration=" + numRetries + ", id=" + id + ", " + e.toString());
          System.out.println("********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
    } finally {
      if (database != null && !database.isClosed()) {
        database.close();
      }
    }
  }

  private static ODocument findById(ODatabaseDocumentTx database, String id) throws Exception {
    List<ODocument> results = database.query(new OSQLSynchQuery<ODocument>("select * from ExceptionRecord where id = ?"), id);
    if (results.size() == 0) {
      throw new Exception("Person with ID=" + id + " not found");
    } else if (results.size() > 1) {
      throw new Exception("Query for ID=" + id + " returned " + results.size() + ", expected 1 value");
    }
    return results.get(0);
  }

  private static String createExceptionRecord(int i) throws Throwable {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:indexExceptionTest");
    String ID = UUID.randomUUID().toString();
    try {
      database.open("admin", "admin");
      int numRetries = 0;
      while (numRetries < NUM_RETRIES) {
        try {
          ODocument document = database.newInstance("ExceptionRecord");
          document.field("id", ID);
          document.field("data", DATA);
          document.field("status", "original");
          document.field("approved", false);
          document.field("groupNonException", false);
          document.field("comment", "");
          document.field("jobId", "1");
          document.field("dataflowName", "Error_Handling_V5");
          document.field("username", "admin");
          document.field("timestamp", 1380118717674L);
          document.field("stageLabel", "Exception Monitor");
          document.field("groupColumn", "");
          document.field("lastModifiedBy", "admin");
          document.field("lastModified", new Date());
          database.save(document);

          break;
        } catch (Throwable e) {
          System.out.println("********************************");
          System.out.println("Create Iteration=" + numRetries + ", id=" + ID + ", " + e.toString());
          System.out.println("********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
    } finally {
      if (database != null) {
        database.close();
      }
    }
    return ID;
  }

  private static String DATA = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n"
                                 + "<exceptionRecord>\r\n"
                                 + "    <data>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ClmntState</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">CA</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>EFFDTE</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ERR_CD</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">E0001</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>LOSS_DATE_or_Accident_Date</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">03/02/2013 00:00:00</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>db_POLICY</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>CLMNT</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">01</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>Accident_State_cd</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">04</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>Subline</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>TPA</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">SED</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>db_EFFDTE</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>Type_of_Injury</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ASLOB</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">170</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>TYPE_Addl_Info</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ERR_MSG</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">PolicyNumber 71GPP4967403 incorrect for this Claim</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ClmntAddr1</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">7753 NEWLINE AVE</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>PolEffDate</key>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>TYPE_Claimant</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">Y</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>ClassCode</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">000000</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>Pad_Policy</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">000000000000000000071GPP4967403</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>POLICY</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">71GPP4967403</value>\r\n"
                                 + "        </entry>\r\n"
                                 + "        <entry>\r\n"
                                 + "            <key>TYPE_Claim</key>\r\n"
                                 + "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">Y</value>\r\n"
                                 + "        </entry>\r\n" + "        <entry>\r\n" + "            <key>Body_Part_Cd</key>\r\n"
                                 + "        </entry>\r\n" + "    </data>\r\n"
                                 + "    <id>4b401635-d692-4430-a4b3-52681485e991</id>\r\n" + "    <metadata>\r\n"
                                 + "        <status>original</status>\r\n" + "        <recordtype>exception</recordtype>\r\n"
                                 + "        <comment></comment>\r\n" + "        <jobId>1</jobId>\r\n"
                                 + "        <dataflowName>Error_Handling_V5</dataflowName>\r\n"
                                 + "        <username>admin</username>\r\n" + "        <timestamp>1380118717674</timestamp>\r\n"
                                 + "        <stageLabel>Exception Monitor</stageLabel>\r\n"
                                 + "        <groupColumn></groupColumn>\r\n" + "        <isApproved>false</isApproved>\r\n"
                                 + "        <matchedConditions>\r\n" + "            <condition>\r\n"
                                 + "                <name>E0001</name>\r\n"
                                 + "                <metric>PolicyNumber incorrect for this Claim</metric>\r\n"
                                 + "                <domain>POLICY_NO</domain>\r\n" + "            </condition>\r\n"
                                 + "        </matchedConditions>\r\n" + "        <lastModifiedBy>admin</lastModifiedBy>\r\n"
                                 + "        <lastModified>2013-09-25T09:18:37.958-05:00</lastModified>\r\n"
                                 + "        <dataTypes/>\r\n" + "    </metadata>\r\n"
                                 + "    <readOnlyFields>Status.Description</readOnlyFields>\r\n"
                                 + "    <readOnlyFields>YYYY</readOnlyFields>\r\n" + "</exceptionRecord>\r\n" + "";
}
