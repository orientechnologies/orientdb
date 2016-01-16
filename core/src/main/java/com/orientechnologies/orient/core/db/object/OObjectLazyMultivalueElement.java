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
package com.orientechnologies.orient.core.db.object;

import java.util.Map;

/**
 * @author luca.molino
 * 
 */
public interface OObjectLazyMultivalueElement<T> {

  public void detach(boolean nonProxiedInstance);

  public void detachAll(boolean nonProxiedInstance, Map<Object, Object> alreadyDetached, Map<Object, Object> lazyObjects);

  public T getNonOrientInstance();

  public Object getUnderlying();
}
