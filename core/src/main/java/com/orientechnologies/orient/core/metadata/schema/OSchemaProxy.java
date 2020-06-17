/*
 *
 *  *  Co
 *  yright 2014 OrientDB LTD (info(-at-)orientdb.com)
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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Proxy class to use the shared OSchemaShared instance. Before to delegate each operations it sets
 * the current database in the thread local.
 *
 * @author Luca
 */
@SuppressWarnings("unchecked")
public class OSchemaProxy extends OProxedResource<OSchemaShared> implements OSchema {

  public OSchemaProxy(final OSchemaShared iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public OImmutableSchema makeSnapshot() {
    return delegate.makeSnapshot(database);
  }

  public void create() {
    delegate.create(database);
  }

  public int countClasses() {
    return delegate.countClasses(database);
  }

  public int countViews() {
    return delegate.countViews(database);
  }

  public OClass createClass(final String iClassName) {
    return delegate.createClass(database, iClassName);
  }

  public OClass getOrCreateClass(final String iClassName) {
    return getOrCreateClass(iClassName, (OClass) null);
  }

  public OClass getOrCreateClass(final String iClassName, final OClass iSuperClass) {
    if (iClassName == null) return null;

    OClass cls = delegate.getClass(iClassName.toLowerCase(Locale.ENGLISH));
    if (cls != null) return cls;

    cls = delegate.getOrCreateClass(database, iClassName, iSuperClass);

    return cls;
  }

  @Override
  public OClass getOrCreateClass(String iClassName, OClass... superClasses) {
    return delegate.getOrCreateClass(database, iClassName, superClasses);
  }

  @Override
  public OClass createClass(final String iClassName, final OClass iSuperClass) {
    return delegate.createClass(database, iClassName, iSuperClass, (int[]) null);
  }

  @Override
  public OClass createClass(String iClassName, OClass... superClasses) {
    return delegate.createClass(database, iClassName, superClasses);
  }

  public OClass createClass(
      final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
    return delegate.createClass(database, iClassName, iSuperClass, iClusterIds);
  }

  @Override
  public OClass createClass(String className, int[] clusterIds, OClass... superClasses) {
    return delegate.createClass(database, className, clusterIds, superClasses);
  }

  @Override
  public OClass createAbstractClass(final String iClassName) {
    return delegate.createAbstractClass(database, iClassName);
  }

  @Override
  public OClass createAbstractClass(final String iClassName, final OClass iSuperClass) {
    return delegate.createAbstractClass(database, iClassName, iSuperClass);
  }

  @Override
  public OClass createAbstractClass(String iClassName, OClass... superClasses) {
    return delegate.createAbstractClass(database, iClassName, superClasses);
  }

  public void dropClass(final String iClassName) {
    delegate.dropClass(database, iClassName);
  }

  public boolean existsClass(final String iClassName) {
    if (iClassName == null) return false;

    return delegate.existsClass(iClassName.toLowerCase(Locale.ENGLISH));
  }

  public boolean existsView(final String name) {
    if (name == null) return false;

    return delegate.existsView(name.toLowerCase(Locale.ENGLISH));
  }

  public void dropView(final String name) {
    delegate.dropView(database, name);
  }

  public OClass getClass(final Class<?> iClass) {
    if (iClass == null) return null;

    return delegate.getClass(iClass);
  }

  public OClass getClass(final String iClassName) {
    if (iClassName == null) return null;

    return delegate.getClass(iClassName);
  }

  public Collection<OClass> getClasses() {
    return delegate.getClasses(database);
  }

  public Collection<OView> getViews() {
    return delegate.getViews(database);
  }

  @Deprecated
  public void load() {

    delegate.load(database);
  }

  public OView getView(final String name) {
    if (name == null) return null;

    return delegate.getView(name);
  }

  @Override
  public OView createView(String viewName, String statement) {
    return createView(database, viewName, statement, new HashMap<>());
  }

  public OView createView(
      ODatabaseDocumentInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata) {
    return delegate.createView(database, viewName, statement, metadata);
  }

  @Override
  public OView createView(OViewConfig config) {
    return delegate.createView(database, config);
  }

  public OView createView(OViewConfig config, ViewCreationListener listener) {
    return delegate.createView(database, config, listener);
  }

  public OSchema reload() {
    delegate.reload(database);
    return this;
  }

  public int getVersion() {

    return delegate.getVersion();
  }

  public ORID getIdentity() {

    return delegate.getIdentity();
  }

  @Deprecated
  public void close() {
    // DO NOTHING THE DELEGATE CLOSE IS MANAGED IN A DIFFERENT CONTEXT
  }

  public String toString() {

    return delegate.toString();
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(final String iClusterName) {
    return delegate.getClassesRelyOnCluster(database, iClusterName);
  }

  @Override
  public OClass createClass(String className, int clusters, OClass... superClasses) {
    return delegate.createClass(database, className, clusters, superClasses);
  }

  @Override
  public OClass getClassByClusterId(int clusterId) {
    return delegate.getClassByClusterId(clusterId);
  }

  @Override
  public OView getViewByClusterId(int clusterId) {
    return delegate.getViewByClusterId(clusterId);
  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    return delegate.getGlobalPropertyById(id);
  }

  @Override
  public List<OGlobalProperty> getGlobalProperties() {
    return delegate.getGlobalProperties();
  }

  public OGlobalProperty createGlobalProperty(String name, OType type, Integer id) {
    return delegate.createGlobalProperty(name, type, id);
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return delegate.getClusterSelectionFactory();
  }

  public Set<Integer> getBlobClusters() {
    return delegate.getBlobClusters();
  }

  public int addBlobCluster(final int clusterId) {
    return delegate.addBlobCluster(database, clusterId);
  }

  public void removeBlobCluster(String clusterName) {
    delegate.removeBlobCluster(database, clusterName);
  }
}
