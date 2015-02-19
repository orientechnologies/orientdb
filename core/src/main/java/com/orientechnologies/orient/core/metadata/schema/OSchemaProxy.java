/*
 *
 *  *  Co
 *  yright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Proxy class to use the shared OSchemaShared instance. Before to delegate each operations it sets the current database in the
 * thread local.
 * 
 * @author Luca
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaProxy extends OProxedResource<OSchemaShared> implements OSchema {

  public OSchemaProxy(final OSchemaShared iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public OImmutableSchema makeSnapshot() {
    setCurrentDatabaseInThreadLocal();
    return delegate.makeSnapshot();
  }

  public void create() {
    setCurrentDatabaseInThreadLocal();
    delegate.create();
  }

  public int countClasses() {
    return delegate.countClasses();
  }

  public OClass createClass(final Class<?> iClass) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClass);
  }

  public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClass, iDefaultClusterId);
  }

  public OClass createClass(final String iClassName) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClassName);
  }

  public OClass getOrCreateClass(final String iClassName) {
    return getOrCreateClass(iClassName, null);
  }

  public OClass getOrCreateClass(final String iClassName, final OClass iSuperClass) {
    if (iClassName == null)
      return null;

    OClass cls = delegate.getClass(iClassName.toLowerCase());
    if (cls != null)
      return cls;

    setCurrentDatabaseInThreadLocal();
    cls = delegate.getOrCreateClass(iClassName, iSuperClass);

    return cls;
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClassName, iSuperClass, (int[]) null);
  }

  public OClass createClass(final String iClassName, final int iDefaultClusterId) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClassName, iDefaultClusterId);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClassName, iSuperClass, iDefaultClusterId);
  }

  public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createClass(iClassName, iSuperClass, iClusterIds);
  }

  @Override
  public OClass createAbstractClass(final Class<?> iClass) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createAbstractClass(iClass);
  }

  @Override
  public OClass createAbstractClass(final String iClassName) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createAbstractClass(iClassName);
  }

  @Override
  public OClass createAbstractClass(final String iClassName, final OClass iSuperClass) {
    setCurrentDatabaseInThreadLocal();
    return delegate.createAbstractClass(iClassName, iSuperClass);
  }

  public void dropClass(final String iClassName) {
    setCurrentDatabaseInThreadLocal();
    delegate.dropClass(iClassName);
  }

  public boolean existsClass(final String iClassName) {
    if (iClassName == null)
      return false;

    return delegate.existsClass(iClassName.toLowerCase());
  }

  public OClass getClass(final Class<?> iClass) {
    if (iClass == null)
      return null;

    return delegate.getClass(iClass);
  }

  public OClass getClass(final String iClassName) {
    if (iClassName == null)
      return null;

    return delegate.getClass(iClassName);
  }

  public Collection<OClass> getClasses() {
    return delegate.getClasses();
  }

  public void load() {
    setCurrentDatabaseInThreadLocal();
    delegate.load();

  }

  public <RET extends ODocumentWrapper> RET reload() {
    setCurrentDatabaseInThreadLocal();
    delegate.reload();

    return (RET) delegate;
  }

  public <RET extends ODocumentWrapper> RET save() {
    setCurrentDatabaseInThreadLocal();
    return (RET) delegate.save();
  }

  public int getVersion() {
    setCurrentDatabaseInThreadLocal();
    return delegate.getVersion();
  }

  public ORID getIdentity() {
    setCurrentDatabaseInThreadLocal();
    return delegate.getIdentity();
  }

  public void close() {
  }

  public String toString() {
    setCurrentDatabaseInThreadLocal();
    return delegate.toString();
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(final String iClusterName) {
    return delegate.getClassesRelyOnCluster(iClusterName);
  }

  @Override
  public OClass getClassByClusterId(int clusterId) {
    return delegate.getClassByClusterId(clusterId);
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
  public boolean isFullCheckpointOnChange() {
    return delegate.isFullCheckpointOnChange();
  }

  @Override
  public void setFullCheckpointOnChange(boolean fullCheckpointOnChange) {
    delegate.setFullCheckpointOnChange(fullCheckpointOnChange);
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return delegate.getClusterSelectionFactory();
  }
}
