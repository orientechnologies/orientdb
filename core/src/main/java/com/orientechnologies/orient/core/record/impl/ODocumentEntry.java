/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;
import java.util.Map;

/**
 * Document entry. Used by ODocument.
 * 
 * @author Emanuele Tagliaferri
 * @since 2.1
 */
public class ODocumentEntry {

  public Object                                          value;
  public Object                                          original;
  public OType                                           type;
  public OProperty                                       property;
  public OSimpleMultiValueChangeListener<Object, Object> changeListener;
  public OMultiValueChangeTimeLine<Object, Object>       timeLine;
  public boolean                                         changed = false;
  public boolean                                         exist   = true;
  public boolean                                         created = false;

  public ODocumentEntry() {

  }

  public boolean isChanged() {
    return changed;
  }
  
  private static OMultiValueChangeEvent.OChangeType isNestedValueChanged(ONestedMultiValueChangeEvent event, Object value, List<Object> ownersTrace, int ownersTraceOffset, Object valueIdentifier){
    if (event.getTimeLine() != null){
      List<OMultiValueChangeEvent<Object, Object>> events = event.getTimeLine().getMultiValueChangeEvents();
      if (events != null){
        for (OMultiValueChangeEvent<Object, Object> nestedEvent : events){
          if (ownersTraceOffset < ownersTrace.size() &&
              nestedEvent.getKey() == ownersTrace.get(ownersTraceOffset) &&
              nestedEvent instanceof ONestedMultiValueChangeEvent){
            ONestedMultiValueChangeEvent ne = (ONestedMultiValueChangeEvent)nestedEvent;
            OMultiValueChangeEvent.OChangeType ret = isNestedValueChanged(ne, value, ownersTrace, ownersTraceOffset + 1, valueIdentifier);
            if (ret != null){
              return ret;
            }
          }
          else{
            if (nestedEvent.getKey().equals(valueIdentifier) && nestedEvent.getValue() == value && ownersTraceOffset == ownersTrace.size()){
              return nestedEvent.getChangeType();
            }
          }
        }
      }
    }
    
    return null;
  }
  
  public static OMultiValueChangeEvent.OChangeType isNestedValueChanged(ODocumentEntry entry, Object value, List<Object> ownersTrace, int ownersTraceOffset, Object valueIdentifier){
    if (entry.timeLine != null){
      List<OMultiValueChangeEvent<Object, Object>> timeline = entry.timeLine.getMultiValueChangeEvents();
      if (timeline != null){
        for (OMultiValueChangeEvent<Object, Object> event : timeline){
          if (ownersTraceOffset < ownersTrace.size() &&
              event.getKey() == ownersTrace.get(ownersTraceOffset) && 
              event instanceof ONestedMultiValueChangeEvent){
            ONestedMultiValueChangeEvent nestedEvent = (ONestedMultiValueChangeEvent)event;
            OMultiValueChangeEvent.OChangeType ret = isNestedValueChanged(nestedEvent, value, ownersTrace, ownersTraceOffset + 1, valueIdentifier);
            if (ret != null){
              return ret;
            }
          }
          else if (event.getKey().equals(valueIdentifier) && event.getValue() == value && ownersTraceOffset == ownersTrace.size()){
            return event.getChangeType();
          }
        }
      }
    }
    
    return null;
  }
  
  private static boolean isInNestedEvent(ONestedMultiValueChangeEvent event, List list){
    if (event.getKey() == list){
      return true;
    }
    if (event.getTimeLine() != null){
      List<OMultiValueChangeEvent<Object, Object>> timeline = event.getTimeLine().getMultiValueChangeEvents();
      for (OMultiValueChangeEvent<Object, Object> nestedEvent : timeline){
        if (!(nestedEvent instanceof ONestedMultiValueChangeEvent)){
          if (nestedEvent.getKey() == list){
            return true;
          }
        }
        else{
          if (isInNestedEvent((ONestedMultiValueChangeEvent)nestedEvent, list)){
            return true;
          }
        }
      }
    }
    return false;
  }
  
  public static boolean isChangedList(List list, ODocumentEntry entry){        
    for (Object element : list){
      if (element instanceof ODocument){
        if (((ODocument)element).isChangedInDepth()){
          return true;
        }
      }
      else if (element instanceof List){        
        if (isChangedList((List)element, entry)){
          return true;
        }
      }      
    }
    
    if (entry.timeLine != null){
      List<OMultiValueChangeEvent<Object, Object>> timeline = entry.timeLine.getMultiValueChangeEvents();
      if (timeline != null){
        for (OMultiValueChangeEvent<Object, Object> event : timeline){
          Object key = event.getKey();
          if (key == list){
            return true;
          }
          if (event instanceof ONestedMultiValueChangeEvent){
            ONestedMultiValueChangeEvent nestedEvent = (ONestedMultiValueChangeEvent)event;
            if (isInNestedEvent(nestedEvent, list)){
              return true;
            }
          }
        }
      }
    }
    
    return false;
  }
  
  private static boolean hasNonExistingInList(List list){
    for (Object element : list){
      if (element instanceof ODocument){
        if (((ODocument)element).hasNonExistingInDepth()){
          return true;
        }
      }
      else if (element instanceof List){
        if (hasNonExistingInList((List)element)){
          return true;
        }
      }
    }
    return false;
  }
  
  public boolean isChangedTree(){
    if (changed && exist){
      return true;
    }
    
    if (value instanceof ODocument){
      ODocument doc  = (ODocument)value;
      return doc.isChangedInDepth();
    }
    
    if (value instanceof List){
      List list = (List)value;
      for (Object element : list){
        if (element instanceof ODocument){
          ODocument doc = (ODocument)element;
          for (Map.Entry<String, ODocumentEntry> field : doc._fields.entrySet()){
            if (field.getValue().isChangedTree()){
              return true;
            }
          }
        }
        else if (element instanceof List){
          if (isChangedList((List)element, this)){
            return true;
          }
        }
      }      
    }
    
    if (timeLine != null){
      List<OMultiValueChangeEvent<Object, Object>> timeline = timeLine.getMultiValueChangeEvents();
      if (timeline != null){
        for (OMultiValueChangeEvent<Object, Object> event : timeline){          
          if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.ADD ||
              event.getChangeType() == OMultiValueChangeEvent.OChangeType.NESTED ||
              event.getChangeType() == OMultiValueChangeEvent.OChangeType.UPDATE){
            return true;
          }
        }
      }
    }
    
    return false;
  }
  
  public boolean hasNonExistingTree(){
    if (!exist){
      return true;
    }
    
    if (value instanceof ODocument){
      ODocument doc  = (ODocument)value;
      for (Map.Entry<String, ODocumentEntry> field : doc._fields.entrySet()){
        if (field.getValue().hasNonExistingTree()){
          return true;
        }
      }
    }
    
    if (value instanceof List){
      List list = (List)value;
      for (Object element : list){
        if (element instanceof ODocument){
          ODocument doc = (ODocument)element;
          for (Map.Entry<String, ODocumentEntry> field : doc._fields.entrySet()){
            if (field.getValue().hasNonExistingTree()){
              return true;
            }
          }
        }
        else if (element instanceof List){
          if (hasNonExistingInList((List)element)){
            return true;
          }
        }
      }
    }        
    
    return false;
  }

  public void setChanged(final boolean changed) {
    this.changed = changed;
  }

  public boolean exist() {
    return exist;
  }

  public void setExist(final boolean exist) {
    this.exist = exist;
  }

  public boolean isCreated() {
    return created;
  }

  public void setCreated(final boolean created) {
    this.created = created;
  }

  protected ODocumentEntry clone() {
    final ODocumentEntry entry = new ODocumentEntry();
    entry.type = type;
    entry.property = property;
    entry.value = value;
    entry.changed = changed;
    entry.created = created;
    entry.exist = exist;
    return entry;
  }

}
