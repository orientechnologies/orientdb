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

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OInvalidDatabaseNameException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class StorageNamingTests {

  @Test
  public void testSpecialLettersOne() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      try {
        orientDB.create("name%", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersTwo() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      try {
        orientDB.create("na.me", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersThree() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDB.create("na_me$", ODatabaseType.MEMORY);
      orientDB.drop("na_me$");
    }
  }

  @Test
  public void commaInPathShouldBeAllowed() {
    new NamingTestStorage("/path/with/,/but/not/in/the/name");
    new NamingTestStorage("/,,,/,/,/name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void commaInNameShouldThrow() {

    new NamingTestStorage("/path/with/,/name/with,");

    //    Assert.assertThrows(IllegalArgumentException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        new NamingTestStorage("/name/with,");
    //      }
    //    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void name() throws Exception {
    new NamingTestStorage("/name/with,");
  }

  private static class NamingTestStorage extends OStorageAbstract {

    public NamingTestStorage(String name) {
      super(name, name, "rw");
    }

    @Override
    public void open(
        String iUserName, String iUserPassword, OContextConfiguration contextConfiguration) {}

    @Override
    public void create(OContextConfiguration contextConfiguration) {}

    @Override
    public String getClusterName(final int clusterId) {
      return null;
    }

    @Override
    public boolean setClusterAttribute(
        final int id, final OCluster.ATTRIBUTES attribute, final Object value) {
      return false;
    }

    @Override
    public boolean exists() {
      return false;
    }

    @Override
    public void reload() {}

    @Override
    public void delete() {}

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
    public String getCreatedAtVersion() {
      return null;
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
    public String getType() {
      return null;
    }

    @Override
    public boolean isRemote() {
      return false;
    }

    @Override
    public OSBTreeCollectionManager getSBtreeCollectionManager() {
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
    public List<String> backup(
        OutputStream out,
        Map<String, Object> options,
        Callable<Object> callable,
        OCommandOutputListener iListener,
        int compressionLevel,
        int bufferSize)
        throws IOException {
      return null;
    }

    @Override
    public void restore(
        InputStream in,
        Map<String, Object> options,
        Callable<Object> callable,
        OCommandOutputListener iListener)
        throws IOException {}

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
  }
}
