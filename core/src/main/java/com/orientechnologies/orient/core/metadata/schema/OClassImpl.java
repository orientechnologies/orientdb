/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.metadata.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.annotation.OBeforeSerialization;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Schema Class implementation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OClassImpl extends ODocumentWrapperNoClass implements OClass {
  private static final long              serialVersionUID = 1L;

  protected OSchemaShared                owner;
  protected String                       name;
  protected Class<?>                     javaClass;
  protected final Map<String, OProperty> properties       = new LinkedHashMap<String, OProperty>();

  protected int[]                        clusterIds;
  protected int                          defaultClusterId = -1;
  protected OClassImpl                   superClass;
  protected int[]                        polymorphicClusterIds;
  protected List<OClass>                 baseClasses;
  protected float                        overSize         = 0f;
  protected String                       shortName;
  protected boolean                      strictMode       = false;                                 // @SINCE v1.0rc8
  protected boolean                      abstractClass    = false;                                 // @SINCE v1.2.0
  protected Map<String, String>          customFields;
  private static final Iterator<OClass>  EMPTY_CLASSES    = new ArrayList<OClass>().iterator();

  /**
   * Constructor used in unmarshalling.
   */
  public OClassImpl() {
  }

  /**
   * Constructor used in unmarshalling.
   */
  protected OClassImpl(final OSchemaShared iOwner) {
    document = new ODocument();
    owner = iOwner;
  }

  /**
   * Constructor used in unmarshalling.
   */
  protected OClassImpl(final OSchemaShared iOwner, final ODocument iDocument) {
    document = iDocument;
    owner = iOwner;
  }

  protected OClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
    this(iOwner);
    name = iName;
    setClusterIds(iClusterIds);
    setPolymorphicClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == -1)
      abstractClass = true;
  }

  public <T> T newInstance() throws InstantiationException, IllegalAccessException {
    if (javaClass == null)
      throw new IllegalArgumentException("Cannot create an instance of class '" + name + "' since no Java class was specified");

    return (T) javaClass.newInstance();
  }

  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    return (RET) owner.reload();
  }

  public String getCustom(final String iName) {
    if (customFields == null)
      return null;

    return customFields.get(iName);
  }

  public void setCustomInternal(final String iName, final String iValue) {
    if (customFields == null)
      customFields = new HashMap<String, String>();
    if (iValue == null || "null".equalsIgnoreCase(iValue))
      customFields.remove(iName);
    else
      customFields.put(iName, iValue);
  }

  public OClassImpl setCustom(final String iName, final String iValue) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s custom %s=%s", getName(), iName, iValue);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setCustomInternal(iName, iValue);
    return this;
  }

  public Map<String, String> getCustomInternal() {
    if (customFields != null)
      return Collections.unmodifiableMap(customFields);
    return null;
  }

  public void removeCustom(final String iName) {
    setCustom(iName, null);
  }

  public void clearCustom() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s custom clear", getName());
    getDatabase().command(new OCommandSQL(cmd)).execute();
    clearCustomInternal();
  }

  public void clearCustomInternal() {
    customFields = null;
  }

  public Set<String> getCustomKeys() {
    if (customFields != null)
      return customFields.keySet();
    return new HashSet<String>();
  }

  @SuppressWarnings("resource")
  public void validateInstances() {
    ODatabaseComplex<?> current = getDatabase().getDatabaseOwner();

    while (current != null && current.getUnderlying() instanceof ODatabaseComplex<?> && !(current instanceof ODatabaseDocumentTx))
      current = current.getUnderlying();

    if (current != null)
      for (ODocument d : ((ODatabaseDocumentTx) current).browseClass(name, true)) {
        d.validate();
      }
  }

  public OClass getSuperClass() {
    return superClass;
  }

  /**
   * Set the super class.
   * 
   * @param iSuperClass
   *          Super class as OClass instance
   * @return the object itself.
   */
  public OClass setSuperClass(final OClass iSuperClass) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s superclass %s", name, iSuperClass != null ? iSuperClass.getName() : null);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setSuperClassInternal(iSuperClass);
    return this;
  }

  public void setSuperClassInternal(final OClass iSuperClass) {
    final OClassImpl cls = (OClassImpl) iSuperClass;

    if (cls != null)
      cls.addBaseClasses(this);
    else if (superClass != null)
      // REMOVE THE PREVIOUS ONE
      superClass.removeBaseClassInternal(this);

    this.superClass = cls;
  }

  public String getName() {
    return name;
  }

  public OClass setName(final String iName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s name %s", name, iName);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    name = iName;
    return this;
  }

  public void setNameInternal(final String iName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    owner.changeClassName(name, iName);
    name = iName;
  }

  public long getSize() {
    long size = 0;
    for (int clusterId : clusterIds)
      size += getDatabase().getClusterRecordSizeById(clusterId);

    return size;
  }

  public String getShortName() {
    return shortName;
  }

  public OClass setShortName(final String iShortName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s shortname %s", name, iShortName);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setShortNameInternal(iShortName);
    return this;
  }

  public void setShortNameInternal(final String iShortName) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    if (this.shortName != null)
      // UNREGISTER ANY PREVIOUS SHORT NAME
      owner.classes.remove(this.shortName);

    this.shortName = iShortName;

    // REGISTER IT
    owner.classes.put(iShortName.toLowerCase(), this);
  }

  public String getStreamableName() {
    return shortName != null ? shortName : name;
  }

  public Collection<OProperty> declaredProperties() {
    return Collections.unmodifiableCollection(properties.values());
  }

  public Collection<OProperty> properties() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    Collection<OProperty> props = null;

    OClassImpl currentClass = this;

    do {
      if (currentClass.properties != null) {
        if (props == null)
          props = new ArrayList<OProperty>();
        props.addAll(currentClass.properties.values());
      }

      currentClass = (OClassImpl) currentClass.getSuperClass();

    } while (currentClass != null);

    return (Collection<OProperty>) (props != null ? props : Collections.emptyList());
  }

  public Collection<OProperty> getIndexedProperties() {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_READ);

    Collection<OProperty> indexedProps = null;

    OClassImpl currentClass = this;

    do {
      if (currentClass.properties != null) {
        for (OProperty p : currentClass.properties.values())
          if (areIndexed(p.getName())) {
            if (indexedProps == null)
              indexedProps = new ArrayList<OProperty>();
            indexedProps.add(p);
          }
      }

      currentClass = (OClassImpl) currentClass.getSuperClass();

    } while (currentClass != null);

    return (Collection<OProperty>) (indexedProps != null ? indexedProps : Collections.emptyList());
  }

  public OProperty getProperty(final String iPropertyName) {
    OClassImpl currentClass = this;
    OProperty p = null;

    do {
      if (currentClass.properties != null)
        p = currentClass.properties.get(iPropertyName.toLowerCase());

      if (p != null)
        return p;

      currentClass = (OClassImpl) currentClass.getSuperClass();

    } while (currentClass != null);

    return p;
  }

  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return addProperty(iPropertyName, iType, null, null);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    if (iLinkedClass == null)
      throw new OSchemaException("Missing linked class");

    return addProperty(iPropertyName, iType, null, iLinkedClass);
  }

  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    return addProperty(iPropertyName, iType, iLinkedType, null);
  }

  public boolean existsProperty(final String iPropertyName) {
    return properties.containsKey(iPropertyName.toLowerCase());
  }

  public void dropProperty(final String iPropertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final String lowerName = iPropertyName.toLowerCase();

    if (!properties.containsKey(lowerName))
      throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");

    final StringBuilder cmd = new StringBuilder("drop property ");
    // CLASS.PROPERTY NAME
    cmd.append(name);
    cmd.append('.');
    cmd.append(iPropertyName);

    getDatabase().command(new OCommandSQL(cmd.toString())).execute();

    if (existsProperty(iPropertyName))
      properties.remove(lowerName);
  }

  public void dropPropertyInternal(final String iPropertyName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop a property inside a transaction");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_DELETE);

    final OProperty prop = properties.remove(iPropertyName.toLowerCase());

    if (prop == null)
      throw new OSchemaException("Property '" + iPropertyName + "' not found in class " + name + "'");
  }

  protected OProperty addProperty(final String iPropertyName, final OType iType, final OType iLinkedType, final OClass iLinkedClass) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot create a new property inside a transaction");

    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    final String lowerName = iPropertyName.toLowerCase();

    if (properties.containsKey(lowerName))
      throw new OSchemaException("Class " + name + " already has property '" + iPropertyName + "'");

    if (iType == null)
      throw new OSchemaException("Property type not defined.");

    final StringBuilder cmd = new StringBuilder("create property ");
    // CLASS.PROPERTY NAME
    cmd.append(name);
    cmd.append('.');
    cmd.append(iPropertyName);

    // TYPE
    cmd.append(' ');
    cmd.append(iType.name);

    if (iLinkedType != null) {
      // TYPE
      cmd.append(' ');
      cmd.append(iLinkedType.name);

    } else if (iLinkedClass != null) {
      // TYPE
      cmd.append(' ');
      cmd.append(iLinkedClass.getName());
    }

    getDatabase().command(new OCommandSQL(cmd.toString())).execute();

    if (existsProperty(iPropertyName))
      return properties.get(lowerName);
    else
      // ADD IT LOCALLY AVOIDING TO RELOAD THE ENTIRE SCHEMA
      return addPropertyInternal(iPropertyName, iType, iLinkedType, iLinkedClass);
  }

  @Override
  public void fromStream() {
    name = document.field("name");
    if (document.containsField("shortName"))
      shortName = document.field("shortName");
    else
      shortName = null;
    defaultClusterId = (Integer) document.field("defaultClusterId");
    if (document.containsField("strictMode"))
      strictMode = (Boolean) document.field("strictMode");
    else
      strictMode = false;

    if (document.containsField("abstract"))
      abstractClass = (Boolean) document.field("abstract");
    else
      abstractClass = false;

    if (document.field("overSize") != null)
      overSize = (Float) document.field("overSize");
    else
      overSize = 0f;

    final Object cc = document.field("clusterIds");
    if (cc instanceof Collection<?>) {
      final Collection<Integer> coll = document.field("clusterIds");
      clusterIds = new int[coll.size()];
      int i = 0;
      for (final Integer item : coll)
        clusterIds[i++] = item.intValue();
    } else
      clusterIds = (int[]) cc;
    Arrays.sort(clusterIds);

    setPolymorphicClusterIds(clusterIds);

    // READ PROPERTIES
    OPropertyImpl prop;
    Collection<ODocument> storedProperties = document.field("properties");
    if (storedProperties != null)
      for (ODocument p : storedProperties) {
        prop = new OPropertyImpl(this, p);
        prop.fromStream();
        properties.put(prop.getName().toLowerCase(), prop);
      }

    customFields = document.field("customFields", OType.EMBEDDEDMAP);
  }

  @Override
  @OBeforeSerialization
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

    try {
      document.field("name", name);
      document.field("shortName", shortName);
      document.field("defaultClusterId", defaultClusterId);
      document.field("clusterIds", clusterIds);
      document.field("overSize", overSize);
      document.field("strictMode", strictMode);
      document.field("abstract", abstractClass);

      if (properties != null) {
        final Set<ODocument> props = new LinkedHashSet<ODocument>();
        for (final OProperty p : properties.values()) {
          props.add(((OPropertyImpl) p).toStream());
        }
        document.field("properties", props, OType.EMBEDDEDSET);
      }

      document.field("superClass", superClass != null ? superClass.getName() : null);
      document.field("customFields", customFields != null && customFields.size() > 0 ? customFields : null, OType.EMBEDDEDMAP);

    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }

    return document;
  }

  public Class<?> getJavaClass() {
    return javaClass;
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(final int iDefaultClusterId) {
    this.defaultClusterId = iDefaultClusterId;
    setDirty();
  }

  public int[] getClusterIds() {
    return clusterIds;
  }

  public int[] getPolymorphicClusterIds() {
    return polymorphicClusterIds;
  }

  public OClass addClusterId(final int iId) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s addcluster %d", name, iId);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    addClusterIdInternal(iId);
    return this;
  }

  private void addClusterIdToIndexes(int iId) {
    String clusterName = getDatabase().getClusterNameById(iId);
    for (OIndex<?> index : getIndexes()) {
      if (index.getInternal() != null) {
        index.getInternal().addCluster(clusterName);
      }
    }
  }

  public OClass addClusterIdInternal(final int iId) {
    for (int currId : clusterIds)
      if (currId == iId)
        // ALREADY ADDED
        return this;

    clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
    clusterIds[clusterIds.length - 1] = iId;
    Arrays.sort(clusterIds);

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = iId;
    Arrays.sort(polymorphicClusterIds);

    setDirty();
    addClusterIdToIndexes(iId);

    return this;
  }

  public OClass removeClusterId(final int iId) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s removecluster %d", name, iId);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    removeClusterIdInternal(iId);
    return this;
  }

  public OClass removeClusterIdInternal(final int iId) {
    boolean found = false;
    for (int clusterId : clusterIds) {
      if (clusterId == iId) {
        found = true;
        break;
      }
    }

    if (found) {
      final int[] newClusterIds = new int[clusterIds.length - 1];
      for (int i = 0, k = 0; i < clusterIds.length; ++i) {
        if (clusterIds[i] == iId)
          // JUMP IT
          continue;

        newClusterIds[k] = clusterIds[i];
        k++;
      }
      clusterIds = newClusterIds;
    }
    return this;
  }

  public OClass setDirty() {
    document.setDirty();
    if (owner != null)
      owner.setDirty();
    return this;
  }

  public Iterator<OClass> getBaseClasses() {
    if (baseClasses == null || baseClasses.size() == 0)
      return EMPTY_CLASSES;

    return baseClasses.iterator();
  }

  /**
   * Adds a base class to the current one. It adds also the base class cluster ids to the polymorphic cluster ids array.
   * 
   * @param iBaseClass
   *          The base class to add.
   */
  private OClass addBaseClasses(final OClass iBaseClass) {
    if (baseClasses == null)
      baseClasses = new ArrayList<OClass>();

    if (baseClasses.contains(iBaseClass))
      return this;

    baseClasses.add(iBaseClass);

    // ADD CLUSTER IDS OF BASE CLASS TO THIS CLASS AND ALL SUPER-CLASSES
    OClassImpl currentClass = this;
    while (currentClass != null) {
      currentClass.addPolymorphicClusterIds((OClassImpl) iBaseClass);
      currentClass = (OClassImpl) currentClass.getSuperClass();
    }

    return this;
  }

  public OClass removeBaseClassInternal(final OClass baseClass) {
    if (baseClasses == null)
      return this;

    if (baseClasses.remove(baseClass)) {
      OClassImpl currentClass = this;

      while (currentClass != null) {
        currentClass.removePolymorphicClusterIds((OClassImpl) baseClass);
        currentClass = (OClassImpl) currentClass.getSuperClass();
      }
    }

    return this;
  }

  private void removePolymorphicClusterIds(final OClassImpl iBaseClass) {
    for (final int clusterId : iBaseClass.polymorphicClusterIds) {
      final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
      if (index == -1)
        continue;

      if (index < polymorphicClusterIds.length - 1)
        System
            .arraycopy(polymorphicClusterIds, index + 1, polymorphicClusterIds, index, polymorphicClusterIds.length - (index + 1));

      polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);
    }
  }

  public float getOverSize() {
    if (overSize > 0)
      // CUSTOM OVERSIZE SETTED
      return overSize;

    if (superClass != null)
      // RETURN THE OVERSIZE OF THE SUPER CLASS
      return superClass.getOverSize();

    // NO OVERSIZE
    return 0;
  }

  public OClass setOverSize(final float overSize) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s oversize %f", name, overSize);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setOverSizeInternal(overSize);
    return this;
  }

  public void setOverSizeInternal(final float overSize) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.overSize = overSize;
  }

  public float getOverSizeInternal() {
    return overSize;
  }

  public boolean isAbstract() {
    return abstractClass;
  }

  public OClass setAbstract(boolean iAbstract) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s abstract %s", name, iAbstract);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setAbstractInternal(iAbstract);
    return this;
  }

  public void setAbstractInternal(final boolean iAbstract) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);

    if (iAbstract) {
      // SWITCH TO ABSTRACT
      if (defaultClusterId > -1) {
        // CHECK
        final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
        if (count() > 0)
          throw new IllegalStateException("Cannot set the class as abstract because contains records.");

        if (name.toLowerCase().equals(db.getClusterNameById(defaultClusterId))) {
          // DROP THE DEFAULT CLUSTER CALLED WITH THE SAME NAME ONLY IF EMPTY
          if (ODatabaseRecordThreadLocal.INSTANCE.get().getClusterRecordSizeById(defaultClusterId) == 0)
            ODatabaseRecordThreadLocal.INSTANCE.get().dropCluster(defaultClusterId, true);
        }
      }
    } else {
      // SWITCH TO NOT ABSTRACT
      this.defaultClusterId = getDatabase().getDefaultClusterId();
      this.clusterIds[0] = this.defaultClusterId;
    }

    this.abstractClass = iAbstract;
  }

  public boolean isStrictMode() {
    return strictMode;
  }

  public OClass setStrictMode(final boolean iStrict) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    final String cmd = String.format("alter class %s strictmode %s", name, iStrict);
    getDatabase().command(new OCommandSQL(cmd)).execute();
    setStrictModeInternal(iStrict);
    return this;
  }

  public void setStrictModeInternal(final boolean iStrict) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.SCHEMA, ORole.PERMISSION_UPDATE);
    this.strictMode = iStrict;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((owner == null) ? 0 : owner.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    final OClassImpl other = (OClassImpl) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  public int compareTo(final OClass o) {
    return name.compareTo(o.getName());
  }

  public long count() {
    return count(true);
  }

  public long count(final boolean iPolymorphic) {
    if (iPolymorphic)
      return getDatabase().countClusterElements(readableClusters(getDatabase(), polymorphicClusterIds));

    return getDatabase().countClusterElements(readableClusters(getDatabase(), clusterIds));
  }

  public static int[] readableClusters(final ODatabaseRecord iDatabase, final int[] iClusterIds) {
    List<Integer> listOfReadableIds = new ArrayList<Integer>();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        String clusterName = iDatabase.getClusterNameById(clusterId);
        iDatabase.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, clusterName);
        listOfReadableIds.add(clusterId);
      } catch (OSecurityAccessException securityException) {
        // if the cluster is inaccessible it's simply not processed in the list.add
      }
    }
    
    if (all)
      // JUST RETURN INPUT ARRAY (FASTER)
      return iClusterIds;

    int[] readableClusterIds = new int[listOfReadableIds.size()];
    int index = 0;
    for (int clusterId : listOfReadableIds) {
      readableClusterIds[index++] = clusterId;
    }

    return readableClusterIds;
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  /**
   * Truncates all the clusters the class uses.
   * 
   * @throws IOException
   */
  public void truncate() throws IOException {
    getDatabase().checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_UPDATE);

    if (isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME))
      throw new OSecurityException("Class " + getName()
          + " cannot be truncated because has record level security enabled (extends " + OSecurityShared.RESTRICTED_CLASSNAME + ")");

    getDatabase().getStorage().callInLock(new Callable<Object>() {
      public Object call() throws Exception {
        for (int id : clusterIds) {
          final OStorage storage = getDatabase().getStorage();
          storage.getClusterById(id).truncate();
          storage.getLevel2Cache().freeCluster(id);
        }

        for (OIndex<?> index : getClassIndexes()) {
          index.clear();
        }
        return null;
      }
    }, true);
  }

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   * 
   * @param iClassName
   * @return
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final String iClassName) {
    if (iClassName == null)
      return false;

    if (iClassName.equals(name) || iClassName.equals(shortName))
      // SPEEDUP CHECK IF CLASS NAME ARE THE SAME
      return true;

    if (superClass == null)
      return false;

    return isSubClassOf(owner.getClass(iClassName));
  }

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   * 
   * @param iClass
   * @return
   * @see #isSuperClassOf(OClass)
   */
  public boolean isSubClassOf(final OClass iClass) {
    if (iClass == null)
      return false;

    OClass cls = this;
    while (cls != null) {
      if (cls.equals(iClass))
        return true;
      cls = cls.getSuperClass();
    }
    return false;
  }

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   * 
   * @param iClass
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(OClass)
   */
  public boolean isSuperClassOf(final OClass iClass) {
    if( iClass == null )
      return false;
    return iClass.isSubClassOf(this);
  }

  public Object get(final ATTRIBUTES iAttribute) {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
    case NAME:
      return getName();
    case SHORTNAME:
      return getShortName();
    case SUPERCLASS:
      return getSuperClass();
    case OVERSIZE:
      return getOverSize();
    case STRICTMODE:
      return isStrictMode();
    case ABSTRACT:
      return isAbstract();
    case CUSTOM:
      return getCustomInternal();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  public void setInternalAndSave(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    switch (attribute) {
    case NAME:
      setNameInternal(stringValue);
      break;
    case SHORTNAME:
      setShortNameInternal(isNull ? null : stringValue);
      break;
    case SUPERCLASS:
      setSuperClassInternal(isNull ? null : getDatabase().getMetadata().getSchema().getClass(stringValue));
      break;
    case OVERSIZE:
      setOverSizeInternal(Float.parseFloat(stringValue.replace(',', '.')));
      break;
    case STRICTMODE:
      setStrictModeInternal(Boolean.parseBoolean(stringValue));
      break;
    case ABSTRACT:
      setAbstractInternal(Boolean.parseBoolean(stringValue));
      break;
    case ADDCLUSTER: {
      int clId = getClusterId(stringValue);
      if (clId == -1)
        throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be added");
      addClusterIdInternal(clId);
      break;
    }
    case REMOVECLUSTER: {
      int clId = getClusterId(stringValue);
      if (clId == -1)
        throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be removed");
      removeClusterIdInternal(clId);
      break;
    }
    case CUSTOM:
      if (iValue.toString().indexOf("=") == -1) {
        if (iValue.toString().equalsIgnoreCase("clear")) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        final List<String> words = OStringSerializerHelper.smartSplit(iValue.toString(), '=');
        setCustomInternal(words.get(0).trim(), words.get(1).trim());
      }
      break;
    }

    saveInternal();
  }

  protected int getClusterId(final String stringValue) {
    int clId;
    try {
      clId = Integer.parseInt(stringValue);
    } catch (NumberFormatException e) {
      clId = getDatabase().getClusterIdByName(stringValue);
    }
    return clId;
  }

  public OClass set(final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (attribute) {
    case NAME:
      setName(stringValue);
      break;
    case SHORTNAME:
      setShortName(stringValue);
      break;
    case SUPERCLASS:
      setSuperClass(getDatabase().getMetadata().getSchema().getClass(stringValue));
      break;
    case OVERSIZE:
      setOverSize(Float.parseFloat(stringValue));
      break;
    case STRICTMODE:
      setStrictMode(Boolean.parseBoolean(stringValue));
      break;
    case ABSTRACT:
      setAbstract(Boolean.parseBoolean(stringValue));
      break;
    case ADDCLUSTER: {
      int clId = getClusterId(stringValue);
      if (clId == -1)
        throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be added");
      addClusterId(clId);
      break;
    }
    case REMOVECLUSTER:
      int clId = getClusterId(stringValue);
      if (clId == -1)
        throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be added");
      removeClusterId(clId);
      break;
    case CUSTOM:
      if (iValue.toString().indexOf("=") == -1) {
        if (iValue.toString().equalsIgnoreCase("clear")) {
          clearCustom();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        final List<String> words = OStringSerializerHelper.smartSplit(iValue.toString(), '=');
        setCustom(words.get(0).trim(), words.get(1).trim());
      }
      break;
    }
    return this;
  }

  /**
   * Add different cluster id to the "polymorphic cluster ids" array.
   */
  private void addPolymorphicClusterIds(final OClassImpl iBaseClass) {
    boolean found;
    for (int i : iBaseClass.polymorphicClusterIds) {
      found = false;
      for (int k : polymorphicClusterIds) {
        if (i == k) {
          found = true;
          break;
        }
      }

      if (!found) {
        // ADD IT
        polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
        polymorphicClusterIds[polymorphicClusterIds.length - 1] = i;
        Arrays.sort(polymorphicClusterIds);
      }
    }
  }

  public OPropertyImpl addPropertyInternal(final String iName, final OType iType, final OType iLinkedType, final OClass iLinkedClass) {
    if (iName == null || iName.length() == 0)
      throw new OSchemaException("Found property name null");

    final Character wrongCharacter = OSchemaShared.checkNameIfValid(iName);
    if (wrongCharacter != null)
      throw new OSchemaException("Invalid property name found. Character '" + wrongCharacter + "' cannot be used in property name.");

    final String lowerName = iName.toLowerCase();

    if (properties.containsKey(lowerName))
      throw new OSchemaException("Class " + name + " already has property '" + iName + "'");

    final OPropertyImpl prop = new OPropertyImpl(this, iName, iType);

    properties.put(lowerName, prop);

    if (iLinkedType != null)
      prop.setLinkedTypeInternal(iLinkedType);
    else if (iLinkedClass != null)
      prop.setLinkedClassInternal(iLinkedClass);
    return prop;
  }

  public void saveInternal() {
    owner.saveInternal();
  }

  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    return createIndex(iName, iType.name(), fields);
  }

  public OIndex<?> createIndex(final String iName, final String iType, final String... fields) {
    return createIndex(iName, iType, null, fields);
  }

  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
      final String... fields) {
    return createIndex(iName, iType.name(), iProgressListener, fields);
  }

  public OIndex<?> createIndex(final String iName, String iType, final OProgressListener iProgressListener, final String... fields) {
    if (iType == null)
      throw new IllegalArgumentException("Index type is null");

    iType = iType.toUpperCase();

    try {
      final INDEX_TYPE recognizedIdxType = INDEX_TYPE.valueOf(iType);
      if (!recognizedIdxType.isAutomaticIndexable())
        throw new IllegalArgumentException("Index type '" + iType + "' cannot be used as automatic index against properties");
    } catch (IllegalArgumentException e) {
      // IGNORE IT
    }

    if (fields.length == 0) {
      throw new OIndexException("List of fields to index cannot be empty.");
    }

    final Set<String> existingFieldNames = new HashSet<String>();
    OClassImpl currentClass = this;
    do {
      existingFieldNames.addAll(currentClass.properties.keySet());
      currentClass = (OClassImpl) currentClass.getSuperClass();
    } while (currentClass != null);

    for (final String fieldToIndex : fields) {
      final String fieldName = OIndexDefinitionFactory.extractFieldName(fieldToIndex);
      if (!existingFieldNames.contains(fieldName.toLowerCase()))
        throw new OIndexException("Index with name : '" + iName + "' cannot be created on class : '" + name + "' because field: '"
            + fieldName + "' is absent in class definition.");
    }

    final OIndexDefinition indexDefinition = OIndexDefinitionFactory.createIndexDefinition(this, Arrays.asList(fields),
        extractFieldTypes(fields));

    return getDatabase().getMetadata().getIndexManager()
        .createIndex(iName, iType, indexDefinition, polymorphicClusterIds, iProgressListener);
  }

  private List<OType> extractFieldTypes(String[] fieldNames) {
    final List<OType> types = new ArrayList<OType>(fieldNames.length);

    for (String fieldName : fieldNames) {
      types.add(getProperty(OIndexDefinitionFactory.extractFieldName(fieldName).toLowerCase()).getType());
    }
    return types;
  }

  public boolean areIndexed(final String... fields) {
    return areIndexed(Arrays.asList(fields));
  }

  public boolean areIndexed(final Collection<String> fields) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    final boolean currentClassResult = indexManager.areIndexed(name, fields);

    if (superClass != null)
      return currentClassResult || superClass.areIndexed(fields);
    return currentClassResult;
  }

  public Set<OIndex<?>> getInvolvedIndexes(final String... fields) {
    return getInvolvedIndexes(Arrays.asList(fields));
  }

  public Set<OIndex<?>> getInvolvedIndexes(final Collection<String> fields) {
    final Set<OIndex<?>> result = new HashSet<OIndex<?>>(getClassInvolvedIndexes(fields));

    if (superClass != null)
      result.addAll(superClass.getInvolvedIndexes(fields));

    return result;
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final Collection<String> fields) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    return indexManager.getClassInvolvedIndexes(name, fields);
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String... fields) {
    return getClassInvolvedIndexes(Arrays.asList(fields));
  }

  public OIndex<?> getClassIndex(final String iName) {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    return indexManager.getClassIndex(name, iName);
  }

  public Set<OIndex<?>> getClassIndexes() {
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();

    return indexManager.getClassIndexes(name);
  }

  public Set<OIndex<?>> getIndexes() {
    final Set<OIndex<?>> indexes = getClassIndexes();
    if (superClass == null)
      return indexes;

    final Set<OIndex<?>> result = new HashSet<OIndex<?>>(indexes);
    result.addAll(superClass.getIndexes());

    return result;
  }

  private void setPolymorphicClusterIds(final int[] iClusterIds) {
    polymorphicClusterIds = iClusterIds;
    Arrays.sort(polymorphicClusterIds);
  }

  private OClass setClusterIds(final int[] iClusterIds) {
    clusterIds = iClusterIds;
    Arrays.sort(clusterIds);
    return this;
  }
}
