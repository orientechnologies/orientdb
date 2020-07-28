package com.orientechnologies.orient.distributed.impl.metadata;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributed;
import java.util.List;
import java.util.stream.Collectors;

/** Created by tglman on 22/06/17. */
public class OSchemaDistributed extends OSchemaEmbedded {

  public OSchemaDistributed(OSharedContext sharedContext) {
    super(sharedContext);
  }

  protected OClassImpl createClassInstance(String className, int[] clusterIds) {
    return new OClassDistributed(this, className, clusterIds);
  }

  @Override
  protected OClassImpl createClassInstance(ODocument c) {
    return new OClassDistributed(this, c, (String) c.field("name"));
  }

  public void acquireSchemaWriteLock(ODatabaseDocumentInternal database) {
    if (isRunLocal(database)) {
      return;
    }
    if (executeThroughDistributedStorage(database)) {
      ((OAutoshardedStorage) database.getStorage()).acquireDistributedExclusiveLock(0);
    }
    super.acquireSchemaWriteLock(database);
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database, final boolean iSave) {
    if (isRunLocal(database)) {
      return;
    }
    try {
      super.releaseSchemaWriteLock(database, iSave);
    } finally {
      if (executeThroughDistributedStorage(database)) {
        ((OAutoshardedStorage) database.getStorage()).releaseDistributedExclusiveLock();
      }
    }
  }

  protected void doDropClass(ODatabaseDocumentInternal database, String className) {
    if (executeThroughDistributedStorage(database)) {
      final StringBuilder cmd;
      cmd = new StringBuilder("drop class ");
      cmd.append(className);
      cmd.append(" unsafe");

      sendCommand(database, cmd.toString());
      if (!isRunLocal(database)) {
        dropClassInternal(database, className);
      }
    } else dropClassInternal(database, className);
  }

  protected void doDropView(ODatabaseDocumentInternal database, final String name) {
    final StringBuilder cmd;

    if (executeThroughDistributedStorage(database)) {
      cmd = new StringBuilder("drop view ");
      cmd.append(name);
      cmd.append(" unsafe");

      sendCommand(database, cmd.toString());
    }

    dropViewInternal(database, name);
  }

  protected void doRealCreateView(
      ODatabaseDocumentInternal database, OViewConfig config, int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    if (executeThroughDistributedStorage(database)) {
      StringBuilder cmd = new StringBuilder("create view ");
      cmd.append('`');
      cmd.append(config.getName());
      cmd.append('`');
      cmd.append(" from (");
      cmd.append(config.getQuery());
      cmd.append(") METADATA {");
      cmd.append(", updateIntervalSeconds: " + config.getUpdateIntervalSeconds());
      if (config.getWatchClasses() != null && config.getWatchClasses().size() > 0) {
        cmd.append(", watchClasses: [\"");
        cmd.append(config.getWatchClasses().stream().collect(Collectors.joining("\",\"")));
        cmd.append("\"]");
      }
      if (config.getNodes() != null && config.getNodes().size() > 0) {
        cmd.append(", nodes: [\"");
        cmd.append(config.getNodes().stream().collect(Collectors.joining("\",\"")));
        cmd.append("\"]");
      }
      if (config.getIndexes() != null && config.getIndexes().size() > 0) {
        cmd.append(", indexes: [");
        boolean first = true;
        for (OViewConfig.OViewIndexConfig index : config.getIndexes()) {
          if (!first) {
            cmd.append(", ");
          }
          cmd.append("{");
          cmd.append("type: \"" + index.getType() + "\"");
          if (index.getEngine() != null) {
            cmd.append(", engine: \"" + index.getEngine() + "\"");
          }
          cmd.append(", properties:{");
          boolean firstProp = true;
          for (OPair<String, OType> property : index.getProperties()) {
            if (!firstProp) {
              cmd.append(", ");
            }
            cmd.append("\"");
            cmd.append(property.key);
            cmd.append("\":\"");
            cmd.append(property.value);
            cmd.append("\"");
            firstProp = false;
          }
          cmd.append("  }");
          cmd.append("}");
          first = false;
        }
        cmd.append("]");
      }
      if (config.isUpdatable()) {
        cmd.append(", updatable: true");
      }
      if (config.getOriginRidField() != null) {
        cmd.append(", originRidField: \"");
        cmd.append(config.getOriginRidField());
        cmd.append("\"");
      }
      if (config.getUpdateStrategy() != null) {
        cmd.append(", updateStrategy: \"");
        cmd.append(config.getUpdateStrategy());
        cmd.append("\"");
      }

      cmd.append("}");
      if (!isRunLocal(database)) {
        createViewInternal(database, config, clusterIds);
      }

      sendCommand(database, cmd.toString());

    } else {
      createViewInternal(database, config, clusterIds);
    }
  }

  protected void doRealCreateClass(
      ODatabaseDocumentInternal database,
      String className,
      List<OClass> superClassesList,
      int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    if (executeThroughDistributedStorage(database)) {
      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      boolean first = true;
      for (OClass superClass : superClassesList) {
        // Filtering for null
        if (first) cmd.append(" extends ");
        else cmd.append(", ");
        cmd.append(superClass.getName());
        first = false;
      }

      if (clusterIds != null) {
        if (clusterIds.length == 1 && clusterIds[0] == -1) cmd.append(" abstract");
        else {
          cmd.append(" cluster ");
          for (int i = 0; i < clusterIds.length; ++i) {
            if (i > 0) cmd.append(',');
            else cmd.append(' ');
            cmd.append(clusterIds[i]);
          }
        }
      }
      if (!isRunLocal(database)) {
        createClassInternal(database, className, clusterIds, superClassesList);
      }
      sendCommand(database, cmd.toString());

    } else {
      createClassInternal(database, className, clusterIds, superClassesList);
    }
  }

  @Override
  public void sendCommand(ODatabaseDocumentInternal database, String command) {
    if (isDistributeVersionTwo(database)) {
      ((ODatabaseDocumentDistributed) database).sendDDLCommand(command);
    } else {
      super.sendCommand(database, command);
    }
  }

  private boolean isDistributeVersionTwo(ODatabaseDocumentInternal database) {
    return database.getConfiguration().getValueAsInteger(DISTRIBUTED_REPLICATION_PROTOCOL_VERSION)
        == 2;
  }

  protected boolean isRunLocal(ODatabaseDocumentInternal database) {
    return isDistributeVersionTwo(database) && executeThroughDistributedStorage(database);
  }
}
