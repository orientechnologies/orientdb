/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.metadata.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/21/14
 */
public class OImmutableSchema implements OSchema {
  private final Map<Integer, OClass> clustersToClasses;
  private final Map<String, OClass>  classes;
  private final Set<Integer>         blogClusters;

  public final int                       version;
  private final ORID                     identity;
  private final boolean                  clustersCanNotBeSharedAmongClasses;
  private final List<OGlobalProperty>    properties;
  private final OClusterSelectionFactory clusterSelectionFactory;

  public OImmutableSchema(OSchemaShared schemaShared) {
    version = schemaShared.getVersion();
    identity = schemaShared.getIdentity();
    clustersCanNotBeSharedAmongClasses = schemaShared.isClustersCanNotBeSharedAmongClasses();
    clusterSelectionFactory = schemaShared.getClusterSelectionFactory();

    clustersToClasses = new HashMap<Integer, OClass>(schemaShared.getClasses().size() * 3);
    classes = new HashMap<String, OClass>(schemaShared.getClasses().size());

    for (OClass oClass : schemaShared.getClasses()) {
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
    this.blogClusters = Collections.unmodifiableSet(new HashSet<Integer>(schemaShared.getBlobClusters()));
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
  public OClass createClass(Class<?> iClass) {
    throw new UnsupportedOperationException();
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
  public OClass createAbstractClass(Class<?> iClass) {
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
  public <RET extends ODocumentWrapper> RET reload() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsClass(String iClassName) {
    return classes.containsKey(iClassName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public OClass getClass(Class<?> iClass) {
    if (iClass == null)
      return null;

    return getClass(iClass.getSimpleName());
  }

  @Override
  public OClass getClass(String iClassName) {
    if (iClassName == null)
      return null;

    OClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
    if (cls != null)
      return cls;

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
  public <RET extends ODocumentWrapper> RET save() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    final int clusterId = getDatabase().getClusterIdByName(clusterName);
    final Set<OClass> result = new HashSet<OClass>();
    for (OClass c : classes.values()) {
      if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId))
        result.add(c);
    }

    return result;
  }

  @Override
  public OClass getClassByClusterId(int clusterId) {
    if (!clustersCanNotBeSharedAmongClasses)
      throw new OSchemaException("This feature is not supported in current version of binary format.");

    return clustersToClasses.get(clusterId);

  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size())
      return null;
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

  @Override
  public void onPostIndexManagement() {
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public Set<Integer> getBlobClusters() {
    return blogClusters;
  }


}
