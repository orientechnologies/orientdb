package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 22/06/17.
 */
public class OClassDistributed extends OClassEmbedded {

  private volatile int[] bestClusterIds;
  private volatile int   lastVersion;

  protected OClassDistributed(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  public OClassDistributed(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  public OClassDistributed(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }

  @Override
  protected OPropertyImpl createPropertyInstance(ODocument p) {
    return new OPropertyDistributed(this, p);
  }

  @Override
  protected OPropertyEmbedded createPropertyInstance(OGlobalProperty global) {
    return new OPropertyDistributed(this, global);
  }

  @Override
  public int getClusterForNewInstance(ODocument doc) {

    ODatabaseDocumentDistributed db = (ODatabaseDocumentDistributed) getDatabase();
    final OStorage storage = db.getStorage();
    if (!(storage instanceof ODistributedStorage))
      throw new IllegalStateException("Storage is not distributed");

    ODistributedServerManager manager = ((ODistributedStorage) storage).getDistributedManager();
    if (bestClusterIds == null)
      readConfiguration(db, manager);
    else {
      if (lastVersion != ((ODistributedStorage) storage).getConfigurationUpdated()) {
        // DISTRIBUTED CFG IS CHANGED: GET BEST CLUSTER AGAIN
        readConfiguration(db, manager);

        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "New cluster list for class '%s': %s (dCfgVersion=%d)", getName(), Arrays.toString(bestClusterIds), lastVersion);
      }
    }

    final int size = bestClusterIds.length;
    if (size == 0)
      return -1;

    if (size == 1)
      // ONLY ONE: RETURN IT
      return bestClusterIds[0];

    final int cluster = super.getClusterSelection().getCluster(this, bestClusterIds, doc);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Selected cluster %d for class '%s' from %s (threadId=%d dCfgVersion=%d)", cluster, getName(),
          Arrays.toString(bestClusterIds), Thread.currentThread().getId(), lastVersion);

    return cluster;
  }

  public ODistributedConfiguration readConfiguration(ODatabaseDocumentDistributed db, ODistributedServerManager manager) {
    if (isAbstract())
      throw new IllegalArgumentException("Cannot create a new instance of abstract class");

    int[] clusterIds = getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int c : clusterIds)
      clusterNames.add(db.getClusterNameById(c).toLowerCase(Locale.ENGLISH));

    ODistributedConfiguration cfg = manager.getDatabaseConfiguration(db.getName());

    List<String> bestClusters = cfg.getOwnedClustersByServer(clusterNames, manager.getLocalNodeName());
    if (bestClusters.isEmpty()) {
      // REBALANCE THE CLUSTERS
      final OModifiableDistributedConfiguration modifiableCfg = cfg.modify();
      manager.reassignClustersOwnership(manager.getLocalNodeName(), db.getName(), modifiableCfg, true);

      cfg = modifiableCfg;

      // RELOAD THE CLUSTER LIST TO GET THE NEW CLUSTER CREATED (IF ANY)
      db.activateOnCurrentThread();
      clusterNames.clear();
      clusterIds = getClusterIds();
      for (int c : clusterIds)
        clusterNames.add(db.getClusterNameById(c).toLowerCase(Locale.ENGLISH));

      bestClusters = cfg.getOwnedClustersByServer(clusterNames, manager.getLocalNodeName());

      if (bestClusters.isEmpty()) {
        // FILL THE MAP CLUSTER/SERVERS
        final StringBuilder buffer = new StringBuilder();
        for (String c : clusterNames) {
          if (buffer.length() > 0)
            buffer.append(" ");

          buffer.append(" ");
          buffer.append(c);
          buffer.append(":");
          buffer.append(cfg.getServers(c, null));
        }

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Cannot find best cluster for class '%s'. Configured servers for clusters %s are %s (dCfgVersion=%d)", getName(),
            clusterNames, buffer.toString(), cfg.getVersion());

        throw new ODatabaseException(
            "Cannot find best cluster for class '" + getName() + "' on server '" + manager.getLocalNodeName()
                + "' (clusterStrategy=" + getName() + " dCfgVersion=" + cfg.getVersion() + ")");
      }
    }

    db.activateOnCurrentThread();

    final int[] newBestClusters = new int[bestClusters.size()];
    int i = 0;
    for (String c : bestClusters)
      newBestClusters[i++] = db.getClusterIdByName(c);

    this.bestClusterIds = newBestClusters;
    final ODistributedStorage storage = db.getStorageDistributed();
    lastVersion = storage.getConfigurationUpdated();

    return cfg;
  }

}
