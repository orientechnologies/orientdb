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
import com.orientechnologies.orient.etl.block.OBlock;
import com.orientechnologies.orient.etl.block.OCodeBlock;
import com.orientechnologies.orient.etl.block.OConsoleBlock;
import com.orientechnologies.orient.etl.block.OLetBlock;
import com.orientechnologies.orient.etl.extractor.*;
import com.orientechnologies.orient.etl.loader.OLoader;
import com.orientechnologies.orient.etl.loader.OOrientDBLoader;
import com.orientechnologies.orient.etl.loader.OOutputLoader;
import com.orientechnologies.orient.etl.source.OContentSource;
import com.orientechnologies.orient.etl.source.OFileSource;
import com.orientechnologies.orient.etl.source.OHttpSource;
import com.orientechnologies.orient.etl.source.OInputSource;
import com.orientechnologies.orient.etl.source.OSource;
import com.orientechnologies.orient.etl.transformer.*;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL component factory. Registers all the ETL components.
 * 
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public class OETLComponentFactory {
  protected final Map<String, Class<? extends OSource>>      sources      = new HashMap<String, Class<? extends OSource>>();
  protected final Map<String, Class<? extends OBlock>>       blocks       = new HashMap<String, Class<? extends OBlock>>();
  protected final Map<String, Class<? extends OExtractor>>   extractors   = new HashMap<String, Class<? extends OExtractor>>();
  protected final Map<String, Class<? extends OTransformer>> transformers = new HashMap<String, Class<? extends OTransformer>>();
  protected final Map<String, Class<? extends OLoader>>      loaders      = new HashMap<String, Class<? extends OLoader>>();

  public OETLComponentFactory() {
    registerSource(OFileSource.class);
    registerSource(OHttpSource.class);
    registerSource(OInputSource.class);
    registerSource(OContentSource.class);

    registerBlock(OCodeBlock.class);
    registerBlock(OLetBlock.class);
    registerBlock(OConsoleBlock.class);

    registerExtractor(OJDBCExtractor.class);
    registerExtractor(ORowExtractor.class);
    registerExtractor(OJsonExtractor.class);
    registerExtractor(OCSVExtractor.class);

    registerTransformer(OBlockTransformer.class);
    registerTransformer(OCodeTransformer.class);
    registerTransformer(OCSVTransformer.class);
    registerTransformer(OCommandTransformer.class);
    registerTransformer(OEdgeTransformer.class);
    registerTransformer(OFieldTransformer.class);
    registerTransformer(OJSONTransformer.class);
    registerTransformer(OLinkTransformer.class);
    registerTransformer(OLogTransformer.class);
    registerTransformer(OMergeTransformer.class);
    registerTransformer(OFlowTransformer.class);
    registerTransformer(OVertexTransformer.class);

    registerLoader(OOrientDBLoader.class);
    registerLoader(OOutputLoader.class);
  }

  public OETLComponentFactory registerSource(final Class<? extends OSource> iComponent) {
    try {
      sources.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering source: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerBlock(final Class<? extends OBlock> iComponent) {
    try {
      blocks.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering block: %s", iComponent.getName());
    }
    return this;
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

  public OBlock getBlock(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OBlock> cls = blocks.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Block '" + iName + "' not found");
    return cls.newInstance();
  }

  public OLoader getLoader(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OLoader> cls = loaders.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Loader '" + iName + "' not found");
    return cls.newInstance();
  }

  public OSource getSource(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OSource> cls = sources.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Source '" + iName + "' not found");
    return cls.newInstance();
  }
}
