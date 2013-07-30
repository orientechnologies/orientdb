/*
 * Copyright 2009-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.command;

/**
 * Base command processing class.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OCommandProcess<C extends OCommand, T, R> {
  protected final C command;
  protected T       target;

  /**
   * Create the process defining command and target.
   */
  public OCommandProcess(final C iCommand, final T iTarget) {
    command = iCommand;
    target = iTarget;
  }

  public abstract R process();

  public T getTarget() {
    return target;
  }

  @Override
  public String toString() {
    return target.toString();
  }
}
