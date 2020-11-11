/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterV0;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterV1;
import com.orientechnologies.orient.core.storage.cluster.v2.OPaginatedClusterV2;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/8/13
 */
public final class OPaginatedClusterFactory {
  public static OPaginatedCluster createCluster(
      final String name,
      final int configurationVersion,
      final int binaryVersion,
      final OAbstractPaginatedStorage storage) {
    if (configurationVersion >= 0 && configurationVersion < 6) {
      throw new OStorageException(
          "You use deprecated version of storage cluster, "
              + "this version is not supported in current implementation. Please do export/import or recreate database.");
    }

    switch (binaryVersion) {
      case 0:
        return new OPaginatedClusterV0(name, storage);
      case 1:
        return new OPaginatedClusterV1(name, storage);
      case 2:
        return new OPaginatedClusterV2(name, storage);
      default:
        throw new IllegalStateException("Invalid binary version of cluster " + binaryVersion);
    }
  }

  public static OPaginatedCluster createCluster(
      final String name,
      final int binaryVersion,
      final OAbstractPaginatedStorage storage,
      final String dataExtension,
      final String cpmExtension,
      final String fsmExtension) {
    switch (binaryVersion) {
      case 0:
        throw new IllegalStateException(
            "Version 0 of cluster is not supported with given configuration");
      case 1:
        return new OPaginatedClusterV1(name, dataExtension, cpmExtension, storage);
      case 2:
        return new OPaginatedClusterV2(name, dataExtension, cpmExtension, fsmExtension, storage);
      default:
        throw new IllegalStateException("Invalid binary version of cluster " + binaryVersion);
    }
  }
}
