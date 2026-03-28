/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.services.backup;

import static org.junit.Assert.*;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for OBackupConfig focusing on the changeBackup UUID preservation bug.
 *
 * <p>Bug: OBackupConfig.changeBackup(uuid, doc) removes the old backup entry by UUID and then
 * pushes the new doc without writing the UUID back onto the doc. This means any client that does
 * not include the "uuid" field in the PUT request body will produce a backup entry with no UUID in
 * both the in-memory configuration and the persisted backups.json file.
 */
public class OBackupConfigTest {

  private File tempDir;
  private OBackupConfig config;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("backup-config-test").toFile();
    new File(tempDir, "config").mkdirs();
    // OBackupConfig.load() resolves "${ORIENTDB_HOME}/config/backups.json"
    System.setProperty("ORIENTDB_HOME", tempDir.getAbsolutePath());
    config = new OBackupConfig().load();
  }

  @After
  public void tearDown() {
    System.clearProperty("ORIENTDB_HOME");
    deleteRecursively(tempDir);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private ODocument buildBackupDoc() {
    ODocument modes = new ODocument();
    ODocument fullMode = new ODocument();
    fullMode.field("when", "0 0/1 * * * ?");
    modes.field("FULL_BACKUP", fullMode);

    ODocument doc = new ODocument();
    doc.field("dbName", "testDb");
    doc.field("directory", "/tmp/test-backup");
    doc.field("modes", modes);
    return doc;
  }

  private ODocument readBackupJsonFromDisk() throws IOException {
    File f = new File(tempDir, "config/backups.json");
    return new ODocument().fromJSON(OIOUtils.readFileAsString(f), "noMap");
  }

  private void deleteRecursively(File f) {
    if (f.isDirectory()) {
      for (File child : f.listFiles()) deleteRecursively(child);
    }
    f.delete();
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * Baseline: addAndPushBackup must assign a UUID and persist it to disk. This passes and
   * establishes the expected contract.
   */
  @Test
  public void testAddBackupPersistsUUID() throws IOException {
    ODocument doc = buildBackupDoc();
    ODocument added = config.addAndPushBackup(doc);

    String uuid = added.field(OBackupConfig.ID);
    assertNotNull("addAndPushBackup must generate a UUID", uuid);

    // Verify in-memory
    Collection<ODocument> backups = config.getConfig().field(OBackupConfig.BACKUPS);
    assertEquals(1, backups.size());
    assertEquals(uuid, backups.iterator().next().field(OBackupConfig.ID));

    // Verify on disk
    Collection<ODocument> diskBackups = readBackupJsonFromDisk().field(OBackupConfig.BACKUPS);
    assertEquals(1, diskBackups.size());
    assertEquals(uuid, diskBackups.iterator().next().field(OBackupConfig.ID));
  }

  /**
   * Demonstrates the bug: changeBackup does not copy the UUID parameter onto the replacement
   * document before persisting it.
   *
   * <p>When a PUT /backupManager/{uuid} request arrives, OServerCommandBackupManager parses the raw
   * request body into a fresh ODocument and passes it straight to changeBackup. If the client does
   * not include the "uuid" field in the body (which is a reasonable expectation given that the UUID
   * is already in the URL), the stored backup entry ends up with no UUID. This makes the entry
   * unreachable by any subsequent GET, PUT, or DELETE that keys on UUID.
   */
  @Test
  public void testChangeBackupPreservesUUID() throws IOException {
    // 1. Add a backup and capture the generated UUID.
    ODocument added = config.addAndPushBackup(buildBackupDoc());
    String uuid = added.field(OBackupConfig.ID);
    assertNotNull(uuid);

    // 2. Build an updated document that intentionally omits the UUID field,
    //    exactly as the HTTP handler does when it parses the raw PUT body.
    ODocument updatedDoc = buildBackupDoc(); // no "uuid" field
    assertNull(
        "Pre-condition: the update doc must not contain a UUID",
        updatedDoc.field(OBackupConfig.ID));

    // 3. Call changeBackup — the UUID is supplied via the url path parameter.
    config.changeBackup(uuid, updatedDoc);

    // 4. The in-memory configuration must still carry the original UUID.
    Collection<ODocument> memBackups = config.getConfig().field(OBackupConfig.BACKUPS);
    assertEquals("Backup count must remain 1 after a change", 1, memBackups.size());
    String memUuid = memBackups.iterator().next().field(OBackupConfig.ID);
    // BUG: memUuid is null because changeBackup never calls doc.field(ID, uuid)
    assertEquals(
        "UUID in in-memory configuration must match the original UUID after changeBackup",
        uuid,
        memUuid);
  }

  /**
   * Same bug expressed at the persistence layer: after changeBackup the UUID must be present in
   * backups.json, otherwise a server restart will reload entries with no UUID and all scheduler
   * lookups by UUID will silently fail.
   */
  @Test
  public void testChangeBackupPersistsUUIDToDisk() throws IOException {
    ODocument added = config.addAndPushBackup(buildBackupDoc());
    String uuid = added.field(OBackupConfig.ID);

    ODocument updatedDoc = buildBackupDoc(); // no "uuid" field
    config.changeBackup(uuid, updatedDoc);

    Collection<ODocument> diskBackups = readBackupJsonFromDisk().field(OBackupConfig.BACKUPS);
    assertEquals(
        "Backup count in backups.json must remain 1 after a change", 1, diskBackups.size());
    String diskUuid = diskBackups.iterator().next().field(OBackupConfig.ID);
    // BUG: diskUuid is null for the same reason
    assertEquals(
        "UUID in backups.json must match the original UUID after changeBackup", uuid, diskUuid);
  }
}
