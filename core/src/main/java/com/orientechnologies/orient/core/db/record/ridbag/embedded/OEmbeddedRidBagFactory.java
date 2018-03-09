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
package com.orientechnologies.orient.core.db.record.ridbag.embedded;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * @author mdjurovi
 */
public class OEmbeddedRidBagFactory {
  
  private static OEmbeddedRidBagFactory instance = null;
  
  private static final int DEFAULT_DELEGATE_VERSION = 0;
  
  private final Class<? extends OEmbeddedRidBag>[] versionClasses;
  
  public static OEmbeddedRidBagFactory getInstance(){
    if (instance == null){
      synchronized (OEmbeddedRidBagFactory.class){
        if (instance == null){
          instance = new OEmbeddedRidBagFactory();
        }
      }
    }
    
    return instance;
  }
  
  private OEmbeddedRidBagFactory(){
    versionClasses = new Class[2];
    versionClasses[0] = OEmbeddedRidBag_V0.class;
    versionClasses[1] = OEmbeddedRidBag_V1.class;
  }
  
  public OEmbeddedRidBag getOEmbeddedRidBag(){
    Class<? extends OEmbeddedRidBag> classToInstance = versionClasses[DEFAULT_DELEGATE_VERSION];
    return instanceObjectOfClass(classToInstance);
  }
  
  public OEmbeddedRidBag getOEmbeddedRidBag(int version){
    Class<? extends OEmbeddedRidBag> classToInstance = versionClasses[version];
    return instanceObjectOfClass(classToInstance);
  }
  
  private OEmbeddedRidBag instanceObjectOfClass(Class<? extends OEmbeddedRidBag> classToInstance){
    try{
      Constructor<? extends OEmbeddedRidBag> constructor = classToInstance.getConstructor();
      OEmbeddedRidBag ret = constructor.newInstance();
      return ret;
    }
    catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
            InvocationTargetException exc){
      //TODO DO NOT LEAVE IT LIKE THIS
      return null;
    }
  }
  
}
