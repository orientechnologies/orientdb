/*
 *
 * Copyright 2013 Luca Molino (molino.luca--AT--gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.object.metadata.schema;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.entity.OEntityManager;
import javassist.util.proxy.Proxy;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.reflection.OReflectionHelper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;

/**
 * @author luca.molino
 * 
 */
public class OSchemaProxyObject implements OSchema {

  protected OSchema underlying;

  public OSchemaProxyObject(OSchema iUnderlying) {
    underlying = iUnderlying;
  }

  @Override
  public OImmutableSchema makeSnapshot() {
    return underlying.makeSnapshot();
  }

  @Override
  public int countClasses() {
    return underlying.countClasses();
  }

  @Override
  public OClass createClass(Class<?> iClass) {
    return underlying.createClass(iClass);
  }

  @Override
  public OClass createClass(Class<?> iClass, int iDefaultClusterId) {
    return underlying.createClass(iClass, iDefaultClusterId);
  }

  @Override
  public OClass createClass(String iClassName) {
    return underlying.createClass(iClassName);
  }

  @Override
  public OClass createClass(String iClassName, OClass iSuperClass) {
    return underlying.createClass(iClassName, iSuperClass);
  }

  @Override
  public OClass createClass(String iClassName, int iDefaultClusterId) {
    return underlying.createClass(iClassName, iDefaultClusterId);
  }

  @Override
  public OClass createClass(String iClassName, OClass iSuperClass, int iDefaultClusterId) {
    return underlying.createClass(iClassName, iSuperClass, iDefaultClusterId);
  }

  @Override
  public OClass createClass(String iClassName, OClass iSuperClass, int[] iClusterIds) {
    return underlying.createClass(iClassName, iSuperClass, iClusterIds);
  }

  @Override
  public OClass createAbstractClass(Class<?> iClass) {
    return underlying.createAbstractClass(iClass);
  }

  @Override
  public OClass createAbstractClass(String iClassName) {
    return underlying.createAbstractClass(iClassName);
  }

  @Override
  public OClass createAbstractClass(String iClassName, OClass iSuperClass) {
    return underlying.createAbstractClass(iClassName, iSuperClass);
  }

  @Override
  public void dropClass(String iClassName) {
    underlying.dropClass(iClassName);
  }

  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    return underlying.reload();
  }

  @Override
  public boolean existsClass(String iClassName) {
    return underlying.existsClass(iClassName);
  }

  @Override
  public OClass getClass(Class<?> iClass) {
    return underlying.getClass(iClass);
  }

  @Override
  public OClass getClass(String iClassName) {
    return underlying.getClass(iClassName);
  }

  @Override
  public OClass getOrCreateClass(String iClassName) {
    return underlying.getOrCreateClass(iClassName);
  }

  @Override
  public OClass getOrCreateClass(String iClassName, OClass iSuperClass) {
    return underlying.getOrCreateClass(iClassName, iSuperClass);
  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    return underlying.getGlobalPropertyById(id);
  }

  @Override
  public Collection<OClass> getClasses() {
    return underlying.getClasses();
  }

  @Override
  public void create() {
    underlying.create();
  }

  @Override
  @Deprecated
  public int getVersion() {
    return underlying.getVersion();
  }

  @Override
  public ORID getIdentity() {
    return underlying.getIdentity();
  }

  @Override
  public <RET extends ODocumentWrapper> RET save() {
    return underlying.save();
  }

  @Override
  public Set<OClass> getClassesRelyOnCluster(String iClusterName) {
    return underlying.getClassesRelyOnCluster(iClusterName);
  }

  public OSchema getUnderlying() {
    return underlying;
  }

  @Override
  public OClass getClassByClusterId(int clusterId) {
    return underlying.getClassByClusterId(clusterId);
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return underlying.getClusterSelectionFactory();
  }

  @Override
  public boolean isFullCheckpointOnChange() {
    return underlying.isFullCheckpointOnChange();
  }

  @Override
  public void setFullCheckpointOnChange(boolean fullCheckpointOnChange) {
    underlying.setFullCheckpointOnChange(fullCheckpointOnChange);
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * 
   * @param iPackageName
   *          The base package
   */
  public synchronized void generateSchema(final String iPackageName) {
    generateSchema(iPackageName, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * 
   * @param iPackageName
   *          The base package
   */
  public synchronized void generateSchema(final String iPackageName, final ClassLoader iClassLoader) {
    OLogManager.instance().debug(this, "Generating schema inside package: %s", iPackageName);

    List<Class<?>> classes = null;
    try {
      classes = OReflectionHelper.getClassesFor(iPackageName, iClassLoader);
    } catch (ClassNotFoundException e) {
      throw new OException(e);
    }
    for (Class<?> c : classes) {
      generateSchema(c);
    }
  }

  /**
   * Generate/updates the SchemaClass and properties from given Class<?>.
   * 
   * @param iClass
   *          :- the Class<?> to generate
   */
  public synchronized void generateSchema(final Class<?> iClass) {
    generateSchema(iClass, ODatabaseRecordThreadLocal.INSTANCE.get());
  }

  /**
   * Generate/updates the SchemaClass and properties from given Class<?>.
   * 
   * @param iClass
   *          :- the Class<?> to generate
   */
  public synchronized void generateSchema(final Class<?> iClass, ODatabaseDocument database) {
    if (iClass == null || iClass.isInterface() || iClass.isPrimitive() || iClass.isEnum() || iClass.isAnonymousClass())
      return;
    OObjectEntitySerializer.registerClass(iClass);
    OClass schema = database.getMetadata().getSchema().getClass(iClass);
    if (schema == null) {
      generateOClass(iClass, database);
    }
    List<String> fields = OObjectEntitySerializer.getClassFields(iClass);
    if (fields != null)
      for (String field : fields) {
        if (schema.existsProperty(field))
          continue;
        if (OObjectEntitySerializer.isVersionField(iClass, field) || OObjectEntitySerializer.isIdField(iClass, field))
          continue;
        Field f = OObjectEntitySerializer.getField(field, iClass);
        if (f.getType().equals(Object.class) || f.getType().equals(ODocument.class) || f.getType().equals(ORecordBytes.class)) {
          continue;
        }
        OType t = OObjectEntitySerializer.getTypeByClass(iClass, field, f);
        if (t == OType.CUSTOM){
          OEntityManager entityManager = OEntityManager.getEntityManagerByDatabaseURL(database.getURL());
          //if the target type is registered as entity, it should be linked instead of custom/serialized
          if (entityManager.getEntityClass(iClass.getName()) != null){
            t = OType.LINK;
          }
        }
        if (t == null) {
          if (f.getType().isEnum())
            t = OType.STRING;
          else {
            t = OType.LINK;
          }
        }
        switch (t) {

        case LINK:
          Class<?> linkedClazz = OObjectEntitySerializer.getSpecifiedLinkedType(f);
          if (linkedClazz == null)
            linkedClazz = f.getType();
          generateLinkProperty(database, schema, field, t, linkedClazz);
          break;
        case LINKLIST:
        case LINKMAP:
        case LINKSET:
          linkedClazz = OObjectEntitySerializer.getSpecifiedMultiLinkedType(f);
          if (linkedClazz == null)
            linkedClazz = OReflectionHelper.getGenericMultivalueType(f);
          if (linkedClazz != null)
            generateLinkProperty(database, schema, field, t, linkedClazz);
          break;

        case EMBEDDED:
          linkedClazz = f.getType();
          if (linkedClazz == null || linkedClazz.equals(Object.class) || linkedClazz.equals(ODocument.class)
              || f.getType().equals(ORecordBytes.class)) {
            continue;
          } else {
            generateLinkProperty(database, schema, field, t, linkedClazz);
          }
          break;

        case EMBEDDEDLIST:
        case EMBEDDEDSET:
        case EMBEDDEDMAP:
          linkedClazz = OReflectionHelper.getGenericMultivalueType(f);
          if (linkedClazz == null || linkedClazz.equals(Object.class) || linkedClazz.equals(ODocument.class)
              || f.getType().equals(ORecordBytes.class)) {
            continue;
          } else {
            if (OReflectionHelper.isJavaType(linkedClazz)) {
              schema.createProperty(field, t, OType.getTypeByClass(linkedClazz));
            } else if (linkedClazz.isEnum()) {
              schema.createProperty(field, t, OType.STRING);
            } else {
              generateLinkProperty(database, schema, field, t, linkedClazz);
            }
          }
          break;

        default:
          schema.createProperty(field, t);
          break;
        }
      }
  }

  /**
   * Checks if all registered entities has schema generated, if not it generates it
   */
  public synchronized void synchronizeSchema() {
    OObjectDatabaseTx database = ((OObjectDatabaseTx) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner());
    Collection<Class<?>> registeredEntities = database.getEntityManager().getRegisteredEntities();
    boolean automaticSchemaGeneration = database.isAutomaticSchemaGeneration();
    boolean reloadSchema = false;
    for (Class<?> iClass : registeredEntities) {
      if (Proxy.class.isAssignableFrom(iClass) || iClass.isEnum() || OReflectionHelper.isJavaType(iClass)
          || iClass.isAnonymousClass())
        return;

      if (!database.getMetadata().getSchema().existsClass(iClass.getSimpleName())) {
        database.getMetadata().getSchema().createClass(iClass.getSimpleName());
        reloadSchema = true;
      }

      for (Class<?> currentClass = iClass; currentClass != Object.class;) {

        if (automaticSchemaGeneration && !currentClass.equals(Object.class) && !currentClass.equals(ODocument.class)) {
          ((OSchemaProxyObject) database.getMetadata().getSchema()).generateSchema(currentClass, database.getUnderlying());
        }
        String iClassName = currentClass.getSimpleName();
        currentClass = currentClass.getSuperclass();

        if (currentClass == null || currentClass.equals(ODocument.class))
          // POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
          // ODOCUMENT FIELDS
          currentClass = Object.class;

        if (database != null && !database.isClosed() && !currentClass.equals(Object.class)) {
          OClass oSuperClass;
          OClass currentOClass = database.getMetadata().getSchema().getClass(iClassName);
          if (!database.getMetadata().getSchema().existsClass(currentClass.getSimpleName())) {
            oSuperClass = database.getMetadata().getSchema().createClass(currentClass.getSimpleName());
            reloadSchema = true;
          } else {
            oSuperClass = database.getMetadata().getSchema().getClass(currentClass.getSimpleName());
            reloadSchema = true;
          }

          if (currentOClass.getSuperClass() == null || !currentOClass.getSuperClass().equals(oSuperClass)) {
            currentOClass.setSuperClass(oSuperClass);
            reloadSchema = true;
          }

        }
      }
    }
    if (database != null && !database.isClosed() && reloadSchema) {
      database.getMetadata().getSchema().save();
      database.getMetadata().getSchema().reload();
    }
  }

  protected static void generateOClass(Class<?> iClass, ODatabaseDocument database) {
    boolean reloadSchema = false;
    for (Class<?> currentClass = iClass; currentClass != Object.class;) {
      String iClassName = currentClass.getSimpleName();
      currentClass = currentClass.getSuperclass();

      if (currentClass == null || currentClass.equals(ODocument.class))
        // POJO EXTENDS ODOCUMENT: SPECIAL CASE: AVOID TO CONSIDER
        // ODOCUMENT FIELDS
        currentClass = Object.class;

      if (ODatabaseRecordThreadLocal.INSTANCE.get() != null && !ODatabaseRecordThreadLocal.INSTANCE.get().isClosed()
          && !currentClass.equals(Object.class)) {
        OClass oSuperClass;
        OClass currentOClass = database.getMetadata().getSchema().getClass(iClassName);
        if (!database.getMetadata().getSchema().existsClass(currentClass.getSimpleName())) {
          oSuperClass = database.getMetadata().getSchema().createClass(currentClass.getSimpleName());
          reloadSchema = true;
        } else {
          oSuperClass = database.getMetadata().getSchema().getClass(currentClass.getSimpleName());
          reloadSchema = true;
        }

        if (currentOClass.getSuperClass() == null || !currentOClass.getSuperClass().equals(oSuperClass)) {
          currentOClass.setSuperClass(oSuperClass);
          reloadSchema = true;
        }

      }
    }
    if (reloadSchema) {
      database.getMetadata().getSchema().save();
      database.getMetadata().getSchema().reload();
    }
  }

  protected static void generateLinkProperty(ODatabaseDocument database, OClass schema, String field, OType t, Class<?> linkedClazz) {
    OClass linkedClass = database.getMetadata().getSchema().getClass(linkedClazz);
    if (linkedClass == null) {
      OObjectEntitySerializer.registerClass(linkedClazz);
      linkedClass = database.getMetadata().getSchema().getClass(linkedClazz);
    }
    schema.createProperty(field, t, linkedClass);
  }

  @Override
  public List<OGlobalProperty> getGlobalProperties() {
    return underlying.getGlobalProperties();
  }

  public OGlobalProperty createGlobalProperty(String name, OType type, Integer id) {
    return underlying.createGlobalProperty(name, type, id);
  }

}
