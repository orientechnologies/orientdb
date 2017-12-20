package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ODatabaseMetadataUpdateListener {

  private OrientDB         orientDB;
  private ODatabaseSession session;
  private int              count;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    session = orientDB.open("test", "admin", "admin");
    count = 0;
    OMetadataUpdateListener listener = new OMetadataUpdateListener() {

      @Override
      public void onSchemaUpdate(String database, OSchemaShared schema) {
        count++;
        assertNotNull(schema);
      }

      @Override
      public void onIndexManagerUpdate(String database, OIndexManager indexManager) {
        count++;
        assertNotNull(indexManager);
      }

      @Override
      public void onFunctionLibraryUpdate(String database, OFunctionLibrary oFunctionLibrary) {
        count++;
        assertNotNull(oFunctionLibrary);
      }

      @Override
      public void onSequenceLibraryUpdate(String database, OSequenceLibraryImpl oSequenceLibrary) {
        count++;
        assertNotNull(oSequenceLibrary);
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
    session.getMetadata().getSequenceLibrary().createSequence("sequence1", OSequence.SEQUENCE_TYPE.ORDERED, null);
    assertEquals(count, 1);
  }

  @Test
  public void testIndexUpdate() {
    session.createClass("Some").createProperty("test", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
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
