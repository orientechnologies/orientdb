package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig.OViewIndexConfig.OIndexConfigProperty;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
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
    if (executeThroughDistributedStorage(database)) {
      ((ODatabaseDocumentDistributed) database).acquireDistributedExclusiveLock(0);
    } else {
      super.acquireSchemaWriteLock(database);
    }
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database, final boolean iSave) {
    try {
      if (!executeThroughDistributedStorage(database)) {
        super.releaseSchemaWriteLock(database, iSave);
      }
    } finally {
      if (executeThroughDistributedStorage(database)) {
        ((ODatabaseDocumentDistributed) database).releaseDistributedExclusiveLock();
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
      cmd.append(" updateIntervalSeconds: " + config.getUpdateIntervalSeconds());
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
          for (OIndexConfigProperty property : index.getProperties()) {
            if (!firstProp) {
              cmd.append(", ");
            }
            cmd.append("\"");
            cmd.append(property.getName());
            cmd.append("\":{\"type\":\"");
            cmd.append(property.getType());
            cmd.append("\", \"linkedType\":\"");
            cmd.append(property.getLinkedType());
            cmd.append("\", \"indexBy\":\"");
            cmd.append(property.getIndexBy());
            if (property.getCollate() != null) {
              cmd.append("\", \"collate\":\"");
              cmd.append(property.getName());
            }
            cmd.append("\"}");
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
      sendCommand(database, cmd.toString());

    } else {
      createClassInternal(database, className, clusterIds, superClassesList);
    }
  }

  @Override
  public void sendCommand(ODatabaseDocumentInternal database, String command) {
    ((ODatabaseDocumentDistributed) database).sendDDLCommand(command, false);
  }
}
