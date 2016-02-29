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
package com.orientechnologies.orient.core.collate;

import com.orientechnologies.common.comparator.ODefaultComparator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Case insensitive collate.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OCaseInsensitiveCollate extends ODefaultComparator implements OCollate {
  public static final String NAME = "ci";

  public String getName() {
    return NAME;
  }

  public Object transform(final Object obj) {
    if (obj instanceof String)
      return ((String) obj).toLowerCase();

    if(obj instanceof Set){
      Set result = new HashSet();
      for(Object o:(Set)obj){
        result.add(transform(o));
      }
      return result;
    }

    if(obj instanceof List){
      List result = new ArrayList();
      for(Object o:(List)obj){
        result.add(transform(o));
      }
      return result;
    }
    return obj;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj==null || obj.getClass() != this.getClass())
      return false;

    final OCaseInsensitiveCollate that = (OCaseInsensitiveCollate) obj;

    return getName().equals(that.getName());
  }

  @Override
  public String toString() {
    return "{" + getClass().getSimpleName() + " : name = " + getName() + "}";
  }
}
