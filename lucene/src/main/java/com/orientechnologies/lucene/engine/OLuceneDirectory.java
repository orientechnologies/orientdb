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
package com.orientechnologies.lucene.engine;

import org.apache.lucene.store.Directory;

/** @author mdjurovi */
public class OLuceneDirectory {
  private final Directory dir;
  private final String path;

  public OLuceneDirectory(final Directory dir, final String path) {
    this.dir = dir;
    this.path = path;
  }

  public Directory getDirectory() {
    return dir;
  }

  public String getPath() {
    return path;
  }
}
