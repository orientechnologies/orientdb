package com.orientechnologies.orient.core.metadata.schema;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.validation.ValidationBinaryComparable;
import com.orientechnologies.orient.core.metadata.schema.validation.ValidationCollectionComparable;
import com.orientechnologies.orient.core.metadata.schema.validation.ValidationMapComparable;
import com.orientechnologies.orient.core.metadata.schema.validation.ValidationStringComparable;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/21/14
 */
public class OImmutableProperty implements OProperty {
  private final String              name;
  private final String              fullName;
  private final OType               type;
  private final String              description;

  // do not make it volatile it is already thread safe.
  private OClass                    linkedClass = null;

  private final String              linkedClassName;

  private final OType               linkedType;
  private final boolean             notNull;
  private final OCollate            collate;
  private final boolean             mandatory;
  private final String              min;
  private final String              max;
  private final String              defaultValue;
  private final String              regexp;
  private final Map<String, String> customProperties;
  private final OClass              owner;
  private final Integer             id;
  private final boolean             readOnly;
  private final Comparable<Object>  minComparable;
  private final Comparable<Object>  maxComparable;

  public OImmutableProperty(OProperty property, OImmutableClass owner) {
    name = property.getName();
    fullName = property.getFullName();
    type = property.getType();
    description = property.getDescription();

    if (property.getLinkedClass() != null)
      linkedClassName = property.getLinkedClass().getName();
    else
      linkedClassName = null;

    linkedType = property.getLinkedType();
    notNull = property.isNotNull();
    collate = property.getCollate();
    mandatory = property.isMandatory();
    min = property.getMin();
    max = property.getMax();
    defaultValue = property.getDefaultValue();
    regexp = property.getRegexp();
    customProperties = new HashMap<String, String>();

    for (String key : property.getCustomKeys())
      customProperties.put(key, property.getCustom(key));

    this.owner = owner;
    id = property.getId();
    readOnly = property.isReadonly();

    if (min != null) {
      if (type.equals(OType.STRING))
        minComparable = new ValidationStringComparable((Integer) OType.convert(min, Integer.class));
      else if (type.equals(OType.BINARY))
        minComparable = new ValidationBinaryComparable((Integer) OType.convert(min, Integer.class));
      else if (type.equals(OType.DATE) || type.equals(OType.BYTE) || type.equals(OType.SHORT) || type.equals(OType.INTEGER)
          || type.equals(OType.LONG) || type.equals(OType.FLOAT) || type.equals(OType.DOUBLE) || type.equals(OType.DECIMAL)
          || type.equals(OType.DATETIME))
        minComparable = (Comparable<Object>) OType.convert(min, type.getDefaultJavaType());
      else if (type.equals(OType.EMBEDDEDLIST) || type.equals(OType.EMBEDDEDSET) || type.equals(OType.LINKLIST)
          || type.equals(OType.LINKSET))
        minComparable = new ValidationCollectionComparable((Integer) OType.convert(min, Integer.class));
      else if (type.equals(OType.EMBEDDEDMAP) || type.equals(OType.LINKMAP))
        minComparable = new ValidationMapComparable((Integer) OType.convert(min, Integer.class));
      else
        minComparable = null;
    } else
      minComparable = null;

    if (max != null) {
      if (type.equals(OType.STRING))
        maxComparable = new ValidationStringComparable((Integer) OType.convert(max, Integer.class));
      else if (type.equals(OType.BINARY))
        maxComparable = new ValidationBinaryComparable((Integer) OType.convert(max, Integer.class));
      else if (type.equals(OType.DATE)) {
        // This is needed because a date is valid in any time range of the day.
        Date maxDate = (Date) OType.convert(max, OType.DATE.getDefaultJavaType());
        Calendar cal = Calendar.getInstance();
        cal.setTime(maxDate);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        maxDate = new Date(cal.getTime().getTime() - 1);
        maxComparable = (Comparable) maxDate;
      } else if (type.equals(OType.BYTE) || type.equals(OType.SHORT) || type.equals(OType.INTEGER) || type.equals(OType.LONG)
          || type.equals(OType.FLOAT) || type.equals(OType.DOUBLE) || type.equals(OType.DECIMAL) || type.equals(OType.DATETIME))
        maxComparable = (Comparable<Object>) OType.convert(max, type.getDefaultJavaType());
      else if (type.equals(OType.EMBEDDEDLIST) || type.equals(OType.EMBEDDEDSET) || type.equals(OType.LINKLIST)
          || type.equals(OType.LINKSET))
        maxComparable = new ValidationCollectionComparable((Integer) OType.convert(max, Integer.class));
      else if (type.equals(OType.EMBEDDEDMAP) || type.equals(OType.LINKMAP))
        maxComparable = new ValidationMapComparable((Integer) OType.convert(max, Integer.class));
      else
        maxComparable = null;
    } else {
      maxComparable = null;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getFullName() {
    return fullName;
  }
  

  @Override
  public OProperty setName(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription() {
    return description;
  }
  
  @Override
  public OProperty setDescription(String iDescription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OType getType() {
    return type;
  }

  @Override
  public OClass getLinkedClass() {
    if (linkedClassName == null)
      return null;

    if (linkedClass != null)
      return linkedClass;

    OSchema schema = ((OImmutableClass) owner).getSchema();
    linkedClass = schema.getClass(linkedClassName);

    return linkedClass;
  }

  @Override
  public OProperty setLinkedClass(OClass oClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OType getLinkedType() {
    return linkedType;
  }

  @Override
  public OProperty setLinkedType(OType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNotNull() {
    return notNull;
  }

  @Override
  public OProperty setNotNull(boolean iNotNull) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCollate getCollate() {
    return collate;
  }

  @Override
  public OProperty setCollate(String iCollateName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty setCollate(OCollate collate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMandatory() {
    return mandatory;
  }

  @Override
  public OProperty setMandatory(boolean mandatory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadonly() {
    return readOnly;
  }

  @Override
  public OProperty setReadonly(boolean iReadonly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMin() {
    return min;
  }

  @Override
  public OProperty setMin(String min) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMax() {
    return max;
  }

  @Override
  public OProperty setMax(String max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public OProperty setDefaultValue(String defaultValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(OClass.INDEX_TYPE iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex<?> createIndex(String iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty dropIndexes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OIndex<?>> getIndexes() {
    return owner.getInvolvedIndexes(name);
  }

  @Override
  public OIndex<?> getIndex() {
    Set<OIndex<?>> indexes = owner.getInvolvedIndexes(name);
    if (indexes != null && !indexes.isEmpty())
      return indexes.iterator().next();
    return null;

  }

  @Override
  public Collection<OIndex<?>> getAllIndexes() {
    final Set<OIndex<?>> indexes = owner.getIndexes();
    final List<OIndex<?>> indexList = new LinkedList<OIndex<?>>();
    for (final OIndex<?> index : indexes) {
      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition.getFields().contains(name))
        indexList.add(index);
    }

    return indexList;
  }

  @Override
  public boolean isIndexed() {
    return owner.areIndexed(name);
  }

  @Override
  public String getRegexp() {
    return regexp;
  }

  @Override
  public OProperty setRegexp(String regexp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OProperty setType(OType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCustom(String iName) {
    return customProperties.get(iName);
  }

  @Override
  public OProperty setCustom(String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(customProperties.keySet());
  }

  @Override
  public OClass getOwnerClass() {
    return owner;
  }

  @Override
  public Object get(ATTRIBUTES attribute) {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (attribute) {
    case LINKEDCLASS:
      return getLinkedClass();
    case LINKEDTYPE:
      return getLinkedType();
    case MIN:
      return getMin();
    case MANDATORY:
      return isMandatory();
    case READONLY:
      return isReadonly();
    case MAX:
      return getMax();
    case DEFAULT:
      return getDefaultValue();
    case NAME:
      return getName();
    case NOTNULL:
      return isNotNull();
    case REGEXP:
      return getRegexp();
    case TYPE:
      return getType();
    case COLLATE:
      return getCollate();
    case DESCRIPTION:
      return getDescription();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public int compareTo(OProperty other) {
    return name.compareTo(other.getName());
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
    if (!super.equals(obj))
      return false;
    if (!OProperty.class.isAssignableFrom(obj.getClass()))
      return false;
    OProperty other = (OProperty) obj;
    if (owner == null) {
      if (other.getOwnerClass() != null)
        return false;
    } else if (!owner.equals(other.getOwnerClass()))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return getName() + " (type=" + getType() + ")";
  }

  public Comparable<Object> getMaxComparable() {
    return maxComparable;
  }

  public Comparable<Object> getMinComparable() {
    return minComparable;
  }
}
