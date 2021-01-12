package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import java.util.Locale;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ODatabaseMetadataUpdateListener {

  private OrientDB orientDB;
  private ODatabaseSession session;
  private int count;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    session = orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    count = 0;
    OMetadataUpdateListener listener =
        new OMetadataUpdateListener() {

          @Override
          public void onSchemaUpdate(String database, OSchemaShared schema) {
            count++;
            assertNotNull(schema);
          }

          @Override
          public void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager) {
            count++;
            assertNotNull(indexManager);
          }

          @Override
          public void onFunctionLibraryUpdate(String database) {
            count++;
          }

          @Override
          public void onSequenceLibraryUpdate(String database) {
            count++;
          }

          @Override
          public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
            count++;
            assertNotNull(update);
          }
        };

    ((ODatabaseDocumentInternal) session).getSharedContext().registerListener(listener);
  }

  @Test
  public void testSchemaUpdateListener() {
    session.createClass("test1");
    assertEquals(count, 1);
  }

  @Test
  public void testFunctionUpdateListener() {
    session.getMetadata().getFunctionLibrary().createFunction("some");
    assertEquals(count, 1);
  }

  @Test
  public void testSequenceUpdate() {
    try {
      session
          .getMetadata()
          .getSequenceLibrary()
          .createSequence("sequence1", OSequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (ODatabaseException exc) {
      Assert.assertTrue("Failed to create sequence", false);
    }
    assertEquals(count, 1);
  }

  @Test
  public void testIndexUpdate() {
    session
        .createClass("Some")
        .createProperty("test", OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    assertEquals(count, 3);
  }

  @Test
  public void testIndexConfigurationUpdate() {
    session.set(ODatabase.ATTRIBUTES.LOCALECOUNTRY, Locale.GERMAN);
    assertEquals(count, 1);
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
  }
}
