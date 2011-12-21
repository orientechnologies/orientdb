/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.*;

/**
 * Index implementation bound to one schema class property that presents
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDMAP or
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#LINKMAP} property.
 */
public class OPropertyMapIndexDefinition extends OPropertyIndexDefinition implements OIndexDefinitionMultiValue {

  /**
   * Indicates whether Map will be indexed using its keys or values.
   */
  public static enum INDEX_BY {
    KEY,
    VALUE
  }

  private INDEX_BY indexBy = INDEX_BY.KEY;

  public OPropertyMapIndexDefinition()
  {
  }

  public OPropertyMapIndexDefinition(final String iClassName,final String iField,final OType iType,final INDEX_BY indexBy) {
    super(iClassName, iField, iType);
    
    if(indexBy == null)
      throw new NullPointerException("You have to provide way by which map entries should be mapped");
    
    this.indexBy = indexBy;
  }

  @Override
  public Object createValue(List<?> params) {
    if(! (params.get( 0 ) instanceof Map))
      return null;

    final Collection mapParams = extractMapParams( (Map) params.get( 0 ) );
    final List<Object> result = new ArrayList<Object>( mapParams.size() );
    for( final Object mapParam : mapParams ) {
      result.add( createSingleValue( mapParam ) );
    }

    return result;
  }

  @Override
  public Object createValue( Object... params )
  {
    if(! (params[0] instanceof Map))
      return null;

    final Collection mapParams = extractMapParams( (Map) params[0] );

    final List<Object> result = new ArrayList<Object>( mapParams.size() );
    for( final Object mapParam : mapParams ) {
      result.add( createSingleValue( mapParam ) );
    }

    return result;
  }

  public INDEX_BY getIndexBy() {
    return indexBy;
  }

  @Override
  protected void serializeToStream() {
    super.serializeToStream();
    document.field("mapIndexBy", indexBy.toString());
  }

  @Override
  protected void serializeFromStream() {
    super.serializeFromStream();
    indexBy = INDEX_BY.valueOf(document.<String>field("mapIndexBy"));
  }

  private Collection extractMapParams(Map map) {
    if(indexBy == INDEX_BY.KEY)
      return map.keySet();

    return map.values();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    OPropertyMapIndexDefinition that = (OPropertyMapIndexDefinition) o;

    if (indexBy != that.indexBy) return false;

    return true;
  }

  public Object createSingleValue( final Object param )
  {
    return OType.convert( param, keyType.getDefaultJavaType() );
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + indexBy.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OPropertyMapIndexDefinition{" +
            "indexBy=" + indexBy +
            "} " + super.toString();
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType) {
    final StringBuilder ddl = new StringBuilder("create index ");

    ddl.append(indexName).append(" on ");
    ddl.append(className).append(" ( ").append(field);

    if(indexBy == INDEX_BY.KEY)
      ddl.append(" by key");
    else
      ddl.append(" by value");

    ddl.append(" ) ");
    ddl.append(indexType);

    return ddl.toString();
  }
}
