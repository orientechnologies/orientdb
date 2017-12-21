package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by tglman on 13/06/17.
 */
public class OSchemaRemote extends OSchemaShared {
  private AtomicBoolean skipPush = new AtomicBoolean(false);

  public OSchemaRemote() {
    super();
  }

  @Override
  public OClass getOrCreateClass(ODatabaseDocumentInternal database, String iClassName, OClass... superClasses) {
    if (iClassName == null)
      return null;

    acquireSchemaReadLock();
    try {
      OClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null)
        return cls;
    } finally {
      releaseSchemaReadLock();
    }

    OClass cls;

    int[] clusterIds = null;

    acquireSchemaWriteLock(database);
    try {
      cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null)
        return cls;

      cls = createClass(database, iClassName, clusterIds, superClasses);

      addClusterClassMap(cls);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return cls;
  }

  protected OClassImpl createClassInstance(ODocument c) {
    return new OClassRemote(this, c, (String) c.field("name"));
  }

  public OClass createClass(ODatabaseDocumentInternal database, final String className, int[] clusterIds, OClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + className + "'");
    OClass result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null)
      OClassImpl.checkParametersConflict(Arrays.asList(superClasses));

    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key))
        throw new OSchemaException("Class '" + className + "' already exists in current database");

      checkClustersAreAbsent(clusterIds);

      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      List<OClass> superClassesList = new ArrayList<OClass>();
      if (superClasses != null && superClasses.length > 0) {
        boolean first = true;
        for (OClass superClass : superClasses) {
          // Filtering for null
          if (superClass != null) {
            if (first)
              cmd.append(" extends ");
            else
              cmd.append(", ");
            cmd.append('`').append(superClass.getName()).append('`');
            first = false;
            superClassesList.add(superClass);
          }
        }
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

      database.command(cmd.toString());
      reload();

      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
        it.next().onCreateClass(database, result);

      for (Iterator<ODatabaseListener> it = database.getListeners().iterator(); it.hasNext(); )
        it.next().onCreateClass(database, result);

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  public OClass createClass(ODatabaseDocumentInternal database, final String className, int clusters, OClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + className + "'");

    OClass result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null)
      OClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key))
        throw new OSchemaException("Class '" + className + "' already exists in current database");

      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      List<OClass> superClassesList = new ArrayList<OClass>();
      if (superClasses != null && superClasses.length > 0) {
        boolean first = true;
        for (OClass superClass : superClasses) {
          // Filtering for null
          if (superClass != null) {
            if (first)
              cmd.append(" extends ");
            else
              cmd.append(", ");
            cmd.append(superClass.getName());
            first = false;
            superClassesList.add(superClass);
          }
        }
      }

      if (clusters == 0)
        cmd.append(" abstract");
      else {
        cmd.append(" clusters ");
        cmd.append(clusters);
      }

      database.command(cmd.toString());
      reload();
      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
        it.next().onCreateClass(database, result);

      for (Iterator<ODatabaseListener> it = database.getListeners().iterator(); it.hasNext(); )
        it.next().onCreateClass(database, result);

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  private void checkClustersAreAbsent(final int[] iClusterIds) {
    if (iClusterIds == null)
      return;

    for (int clusterId : iClusterIds) {
      if (clusterId < 0)
        continue;

      if (clustersToClasses.containsKey(clusterId))
        throw new OSchemaException(
            "Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));
    }
  }

  public void dropClass(ODatabaseDocumentInternal database, final String className) {

    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (className == null)
        throw new IllegalArgumentException("Class name is null");

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      OClass cls = classes.get(key);

      if (cls == null)
        throw new OSchemaException("Class '" + className + "' was not found in current database");

      if (!cls.getSubclasses().isEmpty())
        throw new OSchemaException("Class '" + className + "' cannot be dropped because it has sub classes " + cls.getSubclasses()
            + ". Remove the dependencies before trying to drop it again");

      final StringBuilder cmd = new StringBuilder("drop class ");
      cmd.append(className);
      cmd.append(" unsafe");
      database.command(cmd.toString());
      reload();

      // FREE THE RECORD CACHE
      database.getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public void acquireSchemaWriteLock(ODatabaseDocumentInternal database) {
    skipPush.set(true);
    super.acquireSchemaWriteLock(database);
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database, final boolean iSave) {
    super.releaseSchemaWriteLock(database, iSave);
    skipPush.set(false);
  }

  @Override
  public void checkEmbedded() {
    throw new OSchemaException("'Internal' schema modification methods can be used only inside of embedded database");
  }

  public void update(ODocument schema) {
    if (!skipPush.get()) {
      super.fromStream(schema);
    }
  }
}
