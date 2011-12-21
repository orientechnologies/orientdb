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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Index implementation bound to one schema class property that presents
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDLIST},
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#LINKLIST}, {@link com.orientechnologies.orient.core.metadata.schema.OType#LINKSET} or
 * {@link com.orientechnologies.orient.core.metadata.schema.OType#EMBEDDEDSET}
 * properties.
 */
public class OPropertyListIndexDefinition extends OPropertyIndexDefinition implements OIndexDefinitionMultiValue
{

  public OPropertyListIndexDefinition( final String iClassName, final String iField, final OType iType )
  {
    super( iClassName, iField, iType );
  }

  public OPropertyListIndexDefinition()
  {
  }

  @Override
  public Object createValue( final List<?> params )
  {
    if(!(params.get( 0 ) instanceof Collection))
      return null;

    final Collection multiValueCollection = (Collection) params.get(0);
    final List<Object> values = new ArrayList<Object>( multiValueCollection.size() );
    for( final Object item : multiValueCollection ) {
      values.add( createSingleValue( item ) );
    }
    return values;
  }

  @Override
  public Object createValue( final Object... params )
  {
    if (!( params[0] instanceof Collection ) ) {
      return null;
    }

    final Collection multiValueCollection = (Collection) params[0];
    final List<Object> values = new ArrayList<Object>( multiValueCollection.size() );
    for( final Object item : multiValueCollection ) {
        values.add( createSingleValue( item ) );
    }
    return values;
  }

  public Object createSingleValue( final Object param )
  {
    return OType.convert( param, keyType.getDefaultJavaType() );
  }
}
