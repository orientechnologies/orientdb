/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 *
 * @author mdjurovi
 */
public class OSBTreeRidBagFactory {
  
  private static OSBTreeRidBagFactory instance = null;
  
  private final int DEFAULT_DELEGATE_VERSION = 0;        
  private final Class<? extends OSBTreeRidBag>[] versionClasses;
  private final ChangeSerializationHelper[] helpers;
  
  private OSBTreeRidBagFactory(){
    versionClasses = new Class[2];
    versionClasses[0] = OSBTreeRidBag_V0.class;
    versionClasses[1] = OSBTreeRidBag_V1.class;
    
    helpers = new ChangeSerializationHelper[2];
    helpers[0] = new ChangeSerializationHelper_V0();
    helpers[1] = new ChangeSerializationHelper_V1();
  }
  
  public static OSBTreeRidBagFactory getInstance(){
    if (instance == null){
      synchronized (OSBTreeRidBagFactory.class){
        if (instance == null){
          instance = new OSBTreeRidBagFactory();
        }
      }
    }
    
    return instance;
  }
  
  public ChangeSerializationHelper getChangeSerializationHelper(){
    return helpers[DEFAULT_DELEGATE_VERSION];
  }
  
  public ChangeSerializationHelper getChangeSerializationHelper(int version){
    return helpers[version];
  }
  
  public OSBTreeRidBag getOSBTreeRidBag(){
    Class<? extends OSBTreeRidBag> classToInstance = versionClasses[DEFAULT_DELEGATE_VERSION];
    return instanceObjectOfClass(classToInstance);
  }
  
  public OSBTreeRidBag getOSBTreeRidBag(int version){
    Class<? extends OSBTreeRidBag> classToInstance = versionClasses[version];
    return instanceObjectOfClass(classToInstance);
  }  
  
  public OSBTreeRidBag getOSBTreeRidBag(OBonsaiCollectionPointer pointer, Map<OIdentifiable, Change> changes){
    Class<? extends OSBTreeRidBag> classToInstance = versionClasses[DEFAULT_DELEGATE_VERSION];
    return instanceObjectOfClass(classToInstance, pointer, changes);
  }
  
  public OSBTreeRidBag getOSBTreeRidBag(int version, OBonsaiCollectionPointer pointer, Map<OIdentifiable, Change> changes){
    Class<? extends OSBTreeRidBag> classToInstance = versionClasses[version];
    return instanceObjectOfClass(classToInstance, pointer, changes);
  }
          
  private OSBTreeRidBag instanceObjectOfClass(Class<? extends OSBTreeRidBag> classToInstance){
    try{
      Constructor<? extends OSBTreeRidBag> constructor = classToInstance.getDeclaredConstructor();
      OSBTreeRidBag ret = constructor.newInstance();
      return ret;
    }
    catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
            InvocationTargetException exc){
      //TODO DO NOT LEVE IT LIKE THIS
      return null;
    }
  }
  
  private OSBTreeRidBag instanceObjectOfClass(Class<? extends OSBTreeRidBag> classToInstance, 
          OBonsaiCollectionPointer pointer, Map<OIdentifiable, Change> changes){
    try{
      Constructor<? extends OSBTreeRidBag> constructor = classToInstance.getDeclaredConstructor(OBonsaiCollectionPointer.class, Map.class);
      OSBTreeRidBag ret = constructor.newInstance(pointer, changes);
      return ret;
    }
    catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
            InvocationTargetException exc){
      //TODO DO NOT LEAVE IT LIKE THIS
      return null;
    }
  }
}
