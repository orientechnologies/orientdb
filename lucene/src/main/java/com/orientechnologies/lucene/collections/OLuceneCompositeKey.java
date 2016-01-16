/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.collections;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OCompositeKey;

import java.util.List;

/**
 * Created by enricorisa on 02/10/14.
 */
public class OLuceneCompositeKey extends OCompositeKey {
  OCommandContext context;

  public OLuceneCompositeKey(List<?> keys) {
    super(keys);
  }

  public OLuceneCompositeKey setContext(OCommandContext context) {
    this.context = context;
    return this;
  }

  public OCommandContext getContext() {
    return context;
  }
}
