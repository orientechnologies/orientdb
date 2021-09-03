/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2012 Geomatys.
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
package com.orientechnologies.common.console;

import com.orientechnologies.common.console.annotation.ConsoleCommand;

/**
 * Commun interface for addtitional console commands. Instances of this class are discovered
 * throught serviceLoaders. It should be declared in file :
 * META-INF/services/com.orientechnologies.common.console.OConsoleCommandCollection
 *
 * <p>This interface is empty, all wanted commands are expected to be annoted with {@link
 * ConsoleCommand}.
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OConsoleCommandCollection {

  protected OConsoleApplication context;

  void setContext(OConsoleApplication context) {
    this.context = context;
  }
}
