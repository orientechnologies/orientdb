/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.object.entity;

import com.orientechnologies.orient.core.entity.OEntityManagerClassHandler;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author luca.molino
 */
public class OObjectEntityClassHandler extends OEntityManagerClassHandler {

  private static final ConcurrentMap<String, OObjectEntityClassHandler> instances = new ConcurrentHashMap<String, OObjectEntityClassHandler>();

  @Override
  public void registerEntityClass(Class<?> iClass) {
    if (!OObjectEntitySerializer.isToSerialize(iClass) && !iClass.isEnum())
      registerEntityClass(iClass.getSimpleName(), iClass);
  }

  @Override
  public void registerEntityClass(String iClassName, Class<?> iClass) {
    if (!OObjectEntitySerializer.isToSerialize(iClass) && !iClass.isEnum()) {
      OObjectEntitySerializer.registerClass(iClass);
      super.registerEntityClass(iClassName, iClass);
    }
  }

  @Override
  public synchronized void deregisterEntityClass(Class<?> iClass) {
    if (!OObjectEntitySerializer.isToSerialize(iClass) && !iClass.isEnum()) {
      OObjectEntitySerializer.deregisterClass(iClass);
      super.deregisterEntityClass(iClass);
    }
  }

  public static OObjectEntityClassHandler getInstance(String url) {
    OObjectEntityClassHandler classHandler = instances.get(url);
    if (classHandler != null)
      return classHandler;

    classHandler = new OObjectEntityClassHandler();
    OObjectEntityClassHandler oldClassHandler = instances.putIfAbsent(url, classHandler);
    if (oldClassHandler != null)
      classHandler = oldClassHandler;

    return classHandler;
  }

}
