/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

package com.orientechnologies.orient.etl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.etl.extract.OExtractor;
import com.orientechnologies.orient.etl.extract.OFileExtractor;
import com.orientechnologies.orient.etl.extract.OLineExtractor;
import com.orientechnologies.orient.etl.loader.OLoader;
import com.orientechnologies.orient.etl.loader.OOrientDocumentLoader;
import com.orientechnologies.orient.etl.transform.OCSVTransformer;
import com.orientechnologies.orient.etl.transform.OCodeTransformer;
import com.orientechnologies.orient.etl.transform.OEdgeTransformer;
import com.orientechnologies.orient.etl.transform.OFieldTransformer;
import com.orientechnologies.orient.etl.transform.OJSONTransformer;
import com.orientechnologies.orient.etl.transform.ONullTransformer;
import com.orientechnologies.orient.etl.transform.OSkipTransformer;
import com.orientechnologies.orient.etl.transform.OTransformer;
import com.orientechnologies.orient.etl.transform.OVertexTransformer;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL component factory. Registers all the ETL components.
 * 
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public class OETLComponentFactory {
  protected final Map<String, Class<? extends OExtractor>>   extractors   = new HashMap<String, Class<? extends OExtractor>>();
  protected final Map<String, Class<? extends OTransformer>> transformers = new HashMap<String, Class<? extends OTransformer>>();
  protected final Map<String, Class<? extends OLoader>>      loaders      = new HashMap<String, Class<? extends OLoader>>();

  public OETLComponentFactory() {
    registerExtractor(OFileExtractor.class);
    registerExtractor(OLineExtractor.class);

    registerTransformer(OFieldTransformer.class);
    registerTransformer(OVertexTransformer.class);
    registerTransformer(OEdgeTransformer.class);
    registerTransformer(OCodeTransformer.class);
    registerTransformer(OJSONTransformer.class);
    registerTransformer(OCSVTransformer.class);
    registerTransformer(OSkipTransformer.class);
    registerTransformer(ONullTransformer.class);

    registerLoader(OOrientDocumentLoader.class);
  }

  public OETLComponentFactory registerExtractor(final Class<? extends OExtractor> iComponent) {
    try {
      extractors.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering extractor: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerTransformer(final Class<? extends OTransformer> iComponent) {
    try {
      transformers.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering transformer: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerLoader(final Class<? extends OLoader> iComponent) {
    try {
      loaders.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering loader: %s", iComponent.getName());
    }
    return this;
  }

  public OExtractor getExtractor(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OExtractor> cls = extractors.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Extractor '" + iName + "' not found");
    return cls.newInstance();
  }

  public OTransformer getTransformer(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OTransformer> cls = transformers.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Transformer '" + iName + "' not found");
    return cls.newInstance();
  }

  public OLoader getLoader(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OLoader> cls = loaders.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Loader '" + iName + "' not found");
    return cls.newInstance();
  }
}
