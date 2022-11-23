/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class PostponedEngineStartTest {

  private static Orient ORIENT;

  private static OEngine ENGINE1;
  private static OEngine ENGINE2;
  private static OEngine FAULTY_ENGINE;

  @BeforeClass
  public static void before() {
    ORIENT =
        new Orient(false) {
          @Override
          public Orient startup() {
            ORIENT.registerEngine(ENGINE1 = new NamedEngine("engine1"));
            ORIENT.registerEngine(ENGINE2 = new NamedEngine("engine2"));
            ORIENT.registerEngine(FAULTY_ENGINE = new FaultyEngine());
            return this;
          }

          @Override
          public Orient shutdown() {
            ODatabaseDocumentTx.closeAll();
            return this;
          }
        };

    ORIENT.startup();
  }

  @Test
  public void test() {
    // XXX: There is a known problem in TestNG runner with hardly controllable test methods
    // interleaving from different
    // test classes. This test case touches internals of OrientDB runtime, interleaving with foreign
    // methods is not acceptable
    // here. So I just invoke "test" methods manually from a single test method.
    //
    // BTW, TestNG author says that is not a problem, we just need to split *ALL* our test classes
    // into groups and
    // make groups depend on each other in right order. I see many problems here: (a) we have to to
    // split into groups,
    // (b) we have to maintain all that zoo and (c) we lose the ability to run each test case
    // individually since
    // group dependency must be run before.

    testEngineShouldNotStartAtRuntimeStart();
    testGetEngineIfRunningShouldReturnNullEngineIfNotRunning();
    testGetRunningEngineShouldStartEngine();
    testEngineRestart();
    testStoppedEngineShouldStartAndCreateStorage();
    testGetRunningEngineShouldThrowIfEngineIsUnknown();
    testGetRunningEngineShouldThrowIfEngineIsUnableToStart();
  }

  // @Test
  public void testEngineShouldNotStartAtRuntimeStart() {
    final OEngine engine = ORIENT.getEngine(ENGINE1.getName());
    Assert.assertFalse(engine.isRunning());
  }

  // @Test(dependsOnMethods = "testEngineShouldNotStartAtRuntimeStart")
  public void testGetEngineIfRunningShouldReturnNullEngineIfNotRunning() {
    final OEngine engine = ORIENT.getEngineIfRunning(ENGINE1.getName());
    Assert.assertNull(engine);
  }

  // @Test(dependsOnMethods = "testGetEngineIfRunningShouldReturnNullEngineIfNotRunning")
  public void testGetRunningEngineShouldStartEngine() {
    final OEngine engine = ORIENT.getRunningEngine(ENGINE1.getName());
    Assert.assertNotNull(engine);
    Assert.assertTrue(engine.isRunning());
  }

  // @Test(dependsOnMethods = "testGetRunningEngineShouldStartEngine")
  public void testEngineRestart() {
    OEngine engine = ORIENT.getRunningEngine(ENGINE1.getName());
    engine.shutdown();
    Assert.assertFalse(engine.isRunning());

    engine = ORIENT.getEngineIfRunning(ENGINE1.getName());
    Assert.assertNull(engine);

    engine = ORIENT.getEngine(ENGINE1.getName());
    Assert.assertFalse(engine.isRunning());

    engine = ORIENT.getRunningEngine(ENGINE1.getName());
    Assert.assertTrue(engine.isRunning());
  }

  // @Test
  public void testStoppedEngineShouldStartAndCreateStorage() {
    OEngine engine = ORIENT.getEngineIfRunning(ENGINE2.getName());
    Assert.assertNull(engine);

    final OStorage storage =
        ENGINE2.createStorage(
            ENGINE2.getName() + ":storage",
            null,
            125 * 1024 * 1024,
            25 * 1024 * 1024,
            Integer.MAX_VALUE);

    Assert.assertNotNull(storage);

    engine = ORIENT.getRunningEngine(ENGINE2.getName());
    Assert.assertTrue(engine.isRunning());
  }

  //  @Test(expected = IllegalStateException.class)
  public void testGetRunningEngineShouldThrowIfEngineIsUnknown() {
    //    Assert.assertThrows(IllegalStateException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        ORIENT.getRunningEngine("unknown engine");
    //      }
    //    });
    try {
      ORIENT.getRunningEngine("unknown engine");
      Assert.fail();
    } catch (Exception e) {
      // exception expected
    }
  }

  // @Test(expected = IllegalStateException.class)
  public void testGetRunningEngineShouldThrowIfEngineIsUnableToStart() {
    OEngine engine = ORIENT.getEngine(FAULTY_ENGINE.getName());
    Assert.assertNotNull(engine);

    //    Assert.assertThrows(IllegalStateException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        ORIENT.getRunningEngine(FAULTY_ENGINE.getName());
    //      }
    //    });
    try {

      ORIENT.getRunningEngine(FAULTY_ENGINE.getName());

      engine = ORIENT.getEngine(FAULTY_ENGINE.getName());
      Assert.assertNull(engine);
      Assert.fail();
    } catch (Exception e) {
      // exception expected
    }
  }

  private static class NamedEngine extends OEngineAbstract {

    private final String name;

    public NamedEngine(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public OStorage createStorage(
        String iURL,
        Map<String, String> parameters,
        long maxWalSegSize,
        long doubleWriteLogMaxSegSize,
        int storageId) {
      return new OStorage() {

        @Override
        public List<String> backup(
            OutputStream out,
            Map<String, Object> options,
            Callable<Object> callable,
            OCommandOutputListener iListener,
            int compressionLevel,
            int bufferSize) {
          return null;
        }

        @Override
        public void restore(
            InputStream in,
            Map<String, Object> options,
            Callable<Object> callable,
            OCommandOutputListener iListener) {}

        @Override
        public String getClusterName(int clusterId) {
          return null;
        }

        @Override
        public boolean setClusterAttribute(int id, OCluster.ATTRIBUTES attribute, Object value) {
          return false;
        }

        @Override
        public String getCreatedAtVersion() {
          return null;
        }

        @Override
        public void open(
            String iUserName, String iUserPassword, OContextConfiguration contextConfiguration) {}

        @Override
        public void create(OContextConfiguration contextConfiguration) {}

        @Override
        public boolean exists() {
          return false;
        }

        @Override
        public void reload() {}

        @Override
        public void delete() {}

        @Override
        public void close() {}

        @Override
        public void close(boolean iForce, boolean onDelete) {}

        @Override
        public boolean isClosed() {
          return false;
        }

        @Override
        public OStorageOperationResult<ORawBuffer> readRecord(
            ORecordId iRid,
            String iFetchPlan,
            boolean iIgnoreCache,
            boolean prefetchRecords,
            ORecordCallback<ORawBuffer> iCallback) {
          return null;
        }

        @Override
        public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(
            ORecordId rid, String fetchPlan, boolean ignoreCache, int recordVersion)
            throws ORecordNotFoundException {
          return null;
        }

        @Override
        public OStorageOperationResult<Boolean> deleteRecord(
            ORecordId iRecordId, int iVersion, int iMode, ORecordCallback<Boolean> iCallback) {
          return null;
        }

        @Override
        public ORecordMetadata getRecordMetadata(ORID rid) {
          return null;
        }

        @Override
        public boolean cleanOutRecord(
            ORecordId recordId, int recordVersion, int iMode, ORecordCallback<Boolean> callback) {
          return false;
        }

        @Override
        public List<ORecordOperation> commit(OTransactionInternal iTx) {
          return null;
        }

        @Override
        public OClusterBasedStorageConfiguration getConfiguration() {
          return null;
        }

        @Override
        public int getClusters() {
          return 0;
        }

        @Override
        public Set<String> getClusterNames() {
          return null;
        }

        @Override
        public Collection<? extends OCluster> getClusterInstances() {
          return null;
        }

        @Override
        public int addCluster(String iClusterName, Object... iParameters) {
          return 0;
        }

        @Override
        public int addCluster(String iClusterName, int iRequestedId) {
          return 0;
        }

        @Override
        public boolean dropCluster(String iClusterName) {
          return false;
        }

        @Override
        public boolean dropCluster(int iId) {
          return false;
        }

        @Override
        public String getClusterNameById(int clusterId) {
          return null;
        }

        @Override
        public long getClusterRecordsSizeById(int clusterId) {
          return 0;
        }

        @Override
        public long getClusterRecordsSizeByName(String clusterName) {
          return 0;
        }

        @Override
        public String getClusterRecordConflictStrategy(int clusterId) {
          return null;
        }

        @Override
        public String getClusterEncryption(int clusterId) {
          return null;
        }

        @Override
        public boolean isSystemCluster(int clusterId) {
          return false;
        }

        @Override
        public long getLastClusterPosition(int clusterId) {
          return 0;
        }

        @Override
        public long getClusterNextPosition(int clusterId) {
          return 0;
        }

        @Override
        public OPaginatedCluster.RECORD_STATUS getRecordStatus(ORID rid) {
          return null;
        }

        @Override
        public long count(int iClusterId) {
          return 0;
        }

        @Override
        public long count(int iClusterId, boolean countTombstones) {
          return 0;
        }

        @Override
        public long count(int[] iClusterIds) {
          return 0;
        }

        @Override
        public long count(int[] iClusterIds, boolean countTombstones) {
          return 0;
        }

        @Override
        public long getSize() {
          return 0;
        }

        @Override
        public long countRecords() {
          return 0;
        }

        @Override
        public int getDefaultClusterId() {
          return 0;
        }

        @Override
        public void setDefaultClusterId(int defaultClusterId) {}

        @Override
        public int getClusterIdByName(String iClusterName) {
          return 0;
        }

        @Override
        public String getPhysicalClusterNameById(int iClusterId) {
          return null;
        }

        @Override
        public boolean checkForRecordValidity(OPhysicalPosition ppos) {
          return false;
        }

        @Override
        public String getName() {
          return null;
        }

        @Override
        public String getURL() {
          return null;
        }

        @Override
        public long getVersion() {
          return 0;
        }

        @Override
        public void synch() {}

        @Override
        public Object command(OCommandRequestText iCommand) {
          return null;
        }

        @Override
        public long[] getClusterDataRange(int currentClusterId) {
          return new long[0];
        }

        @Override
        public OPhysicalPosition[] higherPhysicalPositions(
            int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] lowerPhysicalPositions(
            int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] ceilingPhysicalPositions(
            int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] floorPhysicalPositions(
            int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public STATUS getStatus() {
          return null;
        }

        @Override
        public String getType() {
          return null;
        }

        @Override
        public OStorage getUnderlying() {
          return null;
        }

        @Override
        public boolean isRemote() {
          return false;
        }

        @Override
        public boolean isDistributed() {
          return false;
        }

        @Override
        public boolean isAssigningClusterIds() {
          return false;
        }

        @Override
        public OSBTreeCollectionManager getSBtreeCollectionManager() {
          return null;
        }

        @Override
        public OCurrentStorageComponentsFactory getComponentsFactory() {
          return null;
        }

        @Override
        public ORecordConflictStrategy getRecordConflictStrategy() {
          return null;
        }

        @Override
        public void setConflictStrategy(ORecordConflictStrategy iResolver) {}

        @Override
        public String incrementalBackup(String backupDirectory, OCallable<Void, Void> started) {
          return null;
        }

        @Override
        public boolean supportIncremental() {
          return false;
        }

        @Override
        public void fullIncrementalBackup(final OutputStream stream)
            throws UnsupportedOperationException {}

        @Override
        public void restoreFromIncrementalBackup(String filePath) {}

        @Override
        public void restoreFullIncrementalBackup(final InputStream stream)
            throws UnsupportedOperationException {}

        @Override
        public void shutdown() {}

        @Override
        public void setSchemaRecordId(String schemaRecordId) {}

        @Override
        public void setDateFormat(String dateFormat) {}

        @Override
        public void setTimeZone(TimeZone timeZoneValue) {}

        @Override
        public void setLocaleLanguage(String locale) {}

        @Override
        public void setCharset(String charset) {}

        @Override
        public void setIndexMgrRecordId(String indexMgrRecordId) {}

        @Override
        public void setDateTimeFormat(String dateTimeFormat) {}

        @Override
        public void setLocaleCountry(String localeCountry) {}

        @Override
        public void setClusterSelection(String clusterSelection) {}

        @Override
        public void setMinimumClusters(int minimumClusters) {}

        @Override
        public void setValidation(boolean validation) {}

        @Override
        public void removeProperty(String property) {}

        @Override
        public void setProperty(String property, String value) {}

        @Override
        public void setRecordSerializer(String recordSerializer, int version) {}

        @Override
        public void clearProperties() {}

        @Override
        public int[] getClustersIds(Set<String> filterClusters) {
          return null;
        }
      };
    }

    @Override
    public String getNameFromPath(String dbPath) {
      return dbPath;
    }
  }

  private static class FaultyEngine extends OEngineAbstract {
    @Override
    public String getName() {
      return FaultyEngine.class.getSimpleName();
    }

    @Override
    public void startup() {
      super.startup();
      throw new RuntimeException("oops");
    }

    @Override
    public OStorage createStorage(
        String iURL,
        Map<String, String> parameters,
        long maxWalSegSize,
        long doubleWriteLogMaxSegSize,
        int storageId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNameFromPath(String dbPath) {
      throw new UnsupportedOperationException();
    }
  }
}
