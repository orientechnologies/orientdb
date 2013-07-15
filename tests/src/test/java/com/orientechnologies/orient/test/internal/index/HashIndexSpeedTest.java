package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class HashIndexSpeedTest extends SpeedTestMonoThread {
  @Override
  public void cycle() throws Exception {
  }
  // private ODatabaseDocumentTx databaseDocumentTx;
  // private OUniqueHashIndex hashIndex;
  // private MersenneTwisterFast random = new MersenneTwisterFast();
  // private O2QCache buffer;
  //
  // public HashIndexSpeedTest() {
  // super(5000000);
  // }
  //
  // @Override
  // @Test(enabled = false)
  // public void init() throws Exception {
  // String buildDirectory = System.getProperty("buildDirectory", ".");
  // if (buildDirectory == null)
  // buildDirectory = ".";
  //
  // databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/uniqueHashIndexTest");
  // if (databaseDocumentTx.exists()) {
  // databaseDocumentTx.open("admin", "admin");
  // databaseDocumentTx.drop();
  // }
  //
  // databaseDocumentTx.create();
  //
  // long maxMemory = 2L * 1024 * 1024 * 1024;
  // System.out.println("Max memory :" + maxMemory);
  // buffer = new O2QCache(maxMemory, 15000, ODirectMemoryFactory.INSTANCE.directMemory(), null,
  // OHashIndexBucket.MAX_BUCKET_SIZE_BYTES, (OStorageLocal) databaseDocumentTx.getStorage(), false);
  // hashIndex = new OUniqueHashIndex();
  //
  // hashIndex.create("uhashIndexTest", new OSimpleKeyIndexDefinition(OType.STRING), OMetadata.CLUSTER_INDEX_NAME, new int[0], true,
  // null);
  // }
  //
  // @Override
  // @Test(enabled = false)
  // public void cycle() throws Exception {
  // String key = "bsadfasfas" + random.nextInt();
  // hashIndex.put(key, new ORecordId(0, new OClusterPositionLong(0)));
  // }
  //
  // @Override
  // @Test(enabled = false)
  // public void deinit() throws Exception {
  // hashIndex.delete();
  // databaseDocumentTx.drop();
  // }
}
