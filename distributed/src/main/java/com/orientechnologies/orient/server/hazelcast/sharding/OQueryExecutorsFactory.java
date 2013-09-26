/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.hazelcast.sharding;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.sql.*;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * Factory that determines and returns valid OQueryExecutor class instance based on query
 * 
 * @author edegtyarenko
 * @since 25.10.12 8:11
 */
public class OQueryExecutorsFactory {

  public static final OQueryExecutorsFactory INSTANCE             = new OQueryExecutorsFactory();

  private static final Set<Class>            ALWAYS_DISTRIBUTABLE = new HashSet<Class>(Arrays.<Class> asList(
                                                                      OCommandExecutorSQLCreateClass.class,// int
                                                                      OCommandExecutorSQLAlterClass.class,// null
                                                                      OCommandExecutorSQLTruncateClass.class,// long
                                                                      OCommandExecutorSQLDropClass.class,// boolean

                                                                      OCommandExecutorSQLCreateCluster.class,// int
                                                                      OCommandExecutorSQLAlterCluster.class,// null
                                                                      OCommandExecutorSQLTruncateCluster.class,// long
                                                                      OCommandExecutorSQLDropCluster.class,// boolean

                                                                      OCommandExecutorSQLCreateProperty.class,// int
                                                                      OCommandExecutorSQLAlterProperty.class,// null
                                                                      OCommandExecutorSQLDropProperty.class,// null

                                                                      OCommandExecutorSQLCreateIndex.class,// long
                                                                      OCommandExecutorSQLRebuildIndex.class,// long
                                                                      OCommandExecutorSQLDropIndex.class// null
                                                                      ));

  private OQueryExecutorsFactory() {
  }

  public OQueryExecutor getExecutor(OCommandRequestText iCommand, OStorageEmbedded wrapped, ServerInstance serverInstance,
      Set<Integer> undistributedClusters) {

    final OCommandExecutor executorDelegate = OCommandManager.instance().getExecutor(iCommand);
    executorDelegate.parse(iCommand);

    final OCommandExecutor actualExecutor = executorDelegate instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executorDelegate)
        .getDelegate() : executorDelegate;

    if (actualExecutor instanceof OCommandExecutorSQLSelect) {
      final OCommandExecutorSQLSelect selectExecutor = (OCommandExecutorSQLSelect) actualExecutor;
      if (isSelectDistributable(selectExecutor, undistributedClusters)) {
        return new ODistributedSelectQueryExecutor(iCommand, selectExecutor, wrapped, serverInstance);
      } else {
        return new OLocalQueryExecutor(iCommand, wrapped);
      }
    } else {
      if (isCommandDistributable(actualExecutor)) {
        return new ODistributedQueryExecutor(iCommand, wrapped, serverInstance);
      } else {
        return new OLocalQueryExecutor(iCommand, wrapped);
      }
    }
  }

  private static boolean isSelectDistributable(OCommandExecutorSQLSelect selectExecutor, Set<Integer> undistributedClusters) {
    for (Integer c : selectExecutor.getInvolvedClusters()) {
      if (undistributedClusters.contains(c)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isCommandDistributable(OCommandExecutor executor) {
    return ALWAYS_DISTRIBUTABLE.contains(executor.getClass());
  }
}
