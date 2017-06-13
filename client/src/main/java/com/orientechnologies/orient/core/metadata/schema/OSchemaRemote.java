package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

import java.util.*;

/**
 * Created by tglman on 13/06/17.
 */
public class OSchemaRemote extends OSchemaShared {
  public OSchemaRemote(boolean classesAreDetectedByClusterId) {
    super(classesAreDetectedByClusterId);
  }

  @Override
  public OClass getOrCreateClass(String iClassName, OClass... superClasses) {
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

    acquireSchemaWriteLock();
    try {
      cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null)
        return cls;

      cls = createClass(iClassName, clusterIds, superClasses);

      addClusterClassMap(cls);
    } finally {
      releaseSchemaWriteLock();
    }

    return cls;
  }

  public OClass createClass(final String className, int[] clusterIds, OClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + className + "'");
    OClass result;

    final ODatabaseDocumentInternal db = getDatabase();

    db.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null)
      OClassImpl.checkParametersConflict(Arrays.asList(superClasses));

    acquireSchemaWriteLock();
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

      db.command(cmd.toString());
      reload();

      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
        it.next().onCreateClass(getDatabase(), result);

      for (Iterator<ODatabaseListener> it = db.getListeners().iterator(); it.hasNext(); )
        it.next().onCreateClass(getDatabase(), result);

    } finally {
      releaseSchemaWriteLock();
    }

    return result;
  }

  public OClass createClass(final String className, int clusters, OClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null)
      throw new OSchemaException(
          "Invalid class name found. Character '" + wrongCharacter + "' cannot be used in class name '" + className + "'");

    OClass result;

    final ODatabaseDocumentInternal db = getDatabase();
    db.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null)
      OClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    acquireSchemaWriteLock();
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

      db.command(cmd.toString());
      reload();
      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
        it.next().onCreateClass(getDatabase(), result);

      for (Iterator<ODatabaseListener> it = db.getListeners().iterator(); it.hasNext(); )
        it.next().onCreateClass(getDatabase(), result);

    } finally {
      releaseSchemaWriteLock();
    }

    return result;
  }

  private void checkClustersAreAbsent(final int[] iClusterIds) {
    if (!clustersCanNotBeSharedAmongClasses || iClusterIds == null)
      return;

    for (int clusterId : iClusterIds) {
      if (clusterId < 0)
        continue;

      if (clustersToClasses.containsKey(clusterId))
        throw new OSchemaException(
            "Cluster with id " + clusterId + " already belongs to class " + clustersToClasses.get(clusterId));
    }
  }

  public void dropClass(final String className) {
    final ODatabaseDocumentInternal db = getDatabase();

    acquireSchemaWriteLock();
    try {
      if (getDatabase().getTransaction().isActive())
        throw new IllegalStateException("Cannot drop a class inside a transaction");

      if (className == null)
        throw new IllegalArgumentException("Class name is null");

      getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

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
      db.command(cmd.toString());
      reload();

      // FREE THE RECORD CACHE
      getDatabase().getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock();
    }
  }

}
