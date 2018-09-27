package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.*;

/**
 * Created by tglman on 22/06/17.
 */
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
      ((OAutoshardedStorage) database.getStorage()).acquireDistributedExclusiveLock(0);
    }
    super.acquireSchemaWriteLock(database);
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database, final boolean iSave) {
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
      final OStorage storage = database.getStorage();
      final StringBuilder cmd;
      cmd = new StringBuilder("drop class ");
      cmd.append(className);
      cmd.append(" unsafe");

      final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
      OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
      commandSQL.addExcludedNode(autoshardedStorage.getNodeId());
      database.command(commandSQL).execute();

      dropClassInternal(database, className);
    } else
      dropClassInternal(database, className);
  }

  protected void doDropView(ODatabaseDocumentInternal database, final String name) {
    final OStorage storage = database.getStorage();
    final StringBuilder cmd;

    if (executeThroughDistributedStorage(database)) {
      cmd = new StringBuilder("drop view ");
      cmd.append(name);
      cmd.append(" unsafe");

      final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
      OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
      commandSQL.addExcludedNode(autoshardedStorage.getNodeId());
      database.command(commandSQL).execute();
    }

    dropViewInternal(database, name);
  }

  protected void doRealCreateView(ODatabaseDocumentInternal database, OViewConfig config, int[] clusterIds)
      throws ClusterIdsAreEmptyException {
    if (executeThroughDistributedStorage(database)) {
      final OStorage storage = database.getStorage();
      StringBuilder cmd = new StringBuilder("create view ");
      cmd.append('`');
      cmd.append(config.getName());
      cmd.append('`');

      createViewInternal(database, config, clusterIds);

      cmd.append(" cluster ");
      for (int i = 0; i < clusterIds.length; ++i) {
        if (i > 0)
          cmd.append(',');
        else
          cmd.append(' ');
        cmd.append(clusterIds[i]);
      }

      final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
      OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
      commandSQL.addExcludedNode(autoshardedStorage.getNodeId());

      final Object res = database.command(commandSQL).execute();

    } else {
      createViewInternal(database, config, clusterIds);
    }
  }

  protected void doRealCreateClass(ODatabaseDocumentInternal database, String className, List<OClass> superClassesList,
      int[] clusterIds) throws ClusterIdsAreEmptyException {
    if (executeThroughDistributedStorage(database)) {
      final OStorage storage = database.getStorage();
      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      boolean first = true;
      for (OClass superClass : superClassesList) {
        // Filtering for null
        if (first)
          cmd.append(" extends ");
        else
          cmd.append(", ");
        cmd.append(superClass.getName());
        first = false;
      }

      if (clusterIds != null) {
        if (clusterIds.length == 1 && clusterIds[0] == -1)
          cmd.append(" abstract");
        else {
          cmd.append(" cluster ");
          for (int i = 0; i < clusterIds.length; ++i) {
            if (i > 0)
              cmd.append(',');
            else
              cmd.append(' ');
            cmd.append(clusterIds[i]);
          }
        }
      }

      createClassInternal(database, className, clusterIds, superClassesList);

      final OAutoshardedStorage autoshardedStorage = (OAutoshardedStorage) storage;
      OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
      commandSQL.addExcludedNode(autoshardedStorage.getNodeId());

      final Object res = database.command(commandSQL).execute();

    } else {
      createClassInternal(database, className, clusterIds, superClassesList);
    }
  }

}
