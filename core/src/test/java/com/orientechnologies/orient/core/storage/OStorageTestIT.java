package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class OStorageTestIT {
  private OrientDB orientDB;

  private static Path buildPath;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildPath = Paths.get(buildDirectory);
    Files.createDirectories(buildPath);
  }

  @Test
  public void testCreatedVersionIsStored() {
    orientDB = new OrientDB("embedded:" + buildPath.toFile().getAbsolutePath(), OrientDBConfig.defaultConfig());
    orientDB.create(OStorageTestIT.class.getSimpleName(), ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());

    final ODatabaseSession session = orientDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try (OResultSet resultSet = session.query("SELECT FROM metadata:storage")) {
      Assert.assertTrue(resultSet.hasNext());

      final OResult result = resultSet.next();
      Assert.assertEquals(OConstants.getVersion(), result.getProperty("createdAtVersion"));
    }
  }

  @After
  public void after() {
    orientDB.drop(OStorageTestIT.class.getSimpleName());
  }
}
