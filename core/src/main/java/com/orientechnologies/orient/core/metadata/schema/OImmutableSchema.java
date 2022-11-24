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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/21/14
 */
public class OImmutableSchema implements OSchema {
  private final Map<Integer, OClass> clustersToClasses;
  private final Map<String, OClass> classes;
  private final Set<Integer> blogClusters;

  private final Map<Integer, OView> clustersToViews;
  private final Map<String, OView> views;

  public final int version;
  private final ORID identity;
  private final List<OGlobalProperty> properties;
  private final OClusterSelectionFactory clusterSelectionFactory;

  public OImmutableSchema(OSchemaShared schemaShared, ODatabaseDocumentInternal database) {
    assert schemaShared.getDocument().getInternalStatus() == ORecordElement.STATUS.LOADED;
    assert database.getSharedContext().getIndexManager().getDocument().getInternalStatus()
        == ORecordElement.STATUS.LOADED;

    version = schemaShared.getVersion();
    identity = schemaShared.getIdentity();
    clusterSelectionFactory = schemaShared.getClusterSelectionFactory();

    clustersToClasses = new HashMap<Integer, OClass>(schemaShared.getClasses(database).size() * 3);
    classes = new HashMap<String, OClass>(schemaShared.getClasses(database).size());

    for (OClass oClass : schemaShared.getClasses(database)) {
      final OImmutableClass immutableClass = new OImmutableClass(oClass, this);

      classes.put(immutableClass.getName().toLowerCase(Locale.ENGLISH), immutableClass);
      if (immutableClass.getShortName() != null)
        classes.put(immutableClass.getShortName().toLowerCase(Locale.ENGLISH), immutableClass);

      for (int clusterId : immutableClass.getClusterIds())
        clustersToClasses.put(clusterId, immutableClass);
    }

    properties = new ArrayList<OGlobalProperty>();
    for (OGlobalProperty globalProperty : schemaShared.getGlobalProperties())
      properties.add(globalProperty);

    for (OClass cl : classes.values()) {
      ((OImmutableClass) cl).init();
    }
    this.blogClusters =
        Collections.unmodifiableSet(new HashSet<Integer>(schemaShared.getBlobClusters()));

    clustersToViews = new HashMap<Integer, OView>(schemaShared.getViews(database).size() * 3);
    views = new HashMap<String, OView>(schemaShared.getViews(database).size());

    for (OView oClass : schemaShared.getViews(database)) {
      final OImmutableView immutableClass = new OImmutableView(oClass, this);

      views.put(immutableClass.getName().toLowerCase(Locale.ENGLISH), immutableClass);
      if (immutableClass.getShortName() != null)
        views.put(immutableClass.getShortName().toLowerCase(Locale.ENGLISH), immutableClass);

      for (int clusterId : immutableClass.getClusterIds())
        clustersToViews.put(clusterId, immutableClass);
    }
    for (OClass cl : views.values()) {
      ((OImmutableClass) cl).init();
    }
  }

  @Override
  public OImmutableSchema makeSnapshot() {
    return this;
  }

  @Override
  public int countClasses() {
    return classes.size();
  }

  @Override
  public int countViews() {
    return views.size();
  }

  @Override
  public OClass createClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createClass(String iClassName, OClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createClass(String iClassName, OClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createClass(String iClassName, OClass iSuperClass, int[] iClusterIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createClass(String className, int clusters, OClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createClass(String className, int[] clusterIds, OClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createAbstractClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createAbstractClass(String iClassName, OClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass createAbstractClass(String iClassName, OClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OSchema reload() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsClass(String iClassName) {
    return classes.containsKey(iClassName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public OClass getClass(Class<?> iClass) {
    if (iClass == null) return null;

    return getClass(iClass.getSimpleName());
  }

  @Override
  public OClass getClass(String iClassName) {
    if (iClassName == null) return null;

    OClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
    if (cls != null) return cls;

    return null;
  }

  @Override
  public OClass getOrCreateClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass getOrCreateClass(String iClassName, OClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass getOrCreateClass(String iClassName, OClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<OClass> getClasses() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    return new HashSet<OClass>(classes.values());
  }

  @Override
  public Collection<OView> getViews() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    return new HashSet<OView>(views.values());
  }

  @Override
  public void create() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public ORID getIdentity() {
    return new ORecordId(identity);
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    final int clusterId = getDatabase().getClusterIdByName(clusterName);
    final Set<OClass> result = new HashSet<OClass>();
    for (OClass c : classes.values()) {
      if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId)) result.add(c);
    }

    return result;
  }

  @Override
  public OClass getClassByClusterId(int clusterId) {
    return clustersToClasses.get(clusterId);
  }

  @Override
  public OView getViewByClusterId(int clusterId) {
    return clustersToViews.get(clusterId);
  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size()) return null;
    return properties.get(id);
  }

  @Override
  public List<OGlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  @Override
  public OGlobalProperty createGlobalProperty(String name, OType type, Integer id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public Set<Integer> getBlobClusters() {
    return blogClusters;
  }

  @Override
  public OView getView(String name) {
    if (name == null) return null;

    OView cls = views.get(name.toLowerCase(Locale.ENGLISH));
    if (cls != null) return cls;

    return null;
  }

  @Override
  public OView createView(String viewName, String statement) {
    throw new UnsupportedOperationException();
  }

  public OView createView(
      ODatabaseDocumentInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OView createView(OViewConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OView createView(OViewConfig config, ViewCreationListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsView(String name) {
    return views.containsKey(name.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public void dropView(String name) {
    throw new UnsupportedOperationException();
  }
}
