/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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
import com.orientechnologies.orient.etl.block.OETLBlock;
import com.orientechnologies.orient.etl.block.OETLCodeBlock;
import com.orientechnologies.orient.etl.block.OETLConsoleBlock;
import com.orientechnologies.orient.etl.block.OETLLetBlock;
import com.orientechnologies.orient.etl.extractor.*;
import com.orientechnologies.orient.etl.loader.OETLLoader;
import com.orientechnologies.orient.etl.loader.OETLOrientDBLoader;
import com.orientechnologies.orient.etl.loader.OETLOutputLoader;
import com.orientechnologies.orient.etl.source.*;
import com.orientechnologies.orient.etl.transformer.*;

import java.util.HashMap;
import java.util.Map;

/**
 * ETL component factory. Registers all the ETL components.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli-at-orientdb.com)
 */
public class OETLComponentFactory {
  protected final Map<String, Class<? extends OETLSource>>      sources      = new HashMap<String, Class<? extends OETLSource>>();
  protected final Map<String, Class<? extends OETLBlock>>       blocks       = new HashMap<String, Class<? extends OETLBlock>>();
  protected final Map<String, Class<? extends OETLExtractor>>   extractors   = new HashMap<String, Class<? extends OETLExtractor>>();
  protected final Map<String, Class<? extends OETLTransformer>> transformers = new HashMap<String, Class<? extends OETLTransformer>>();
  protected final Map<String, Class<? extends OETLLoader>>      loaders      = new HashMap<String, Class<? extends OETLLoader>>();

  public OETLComponentFactory() {
    registerSource(OETLFileSource.class);
    registerSource(OETLHttpSource.class);
    registerSource(OETLInputSource.class);
    registerSource(OETLContentSource.class);

    registerBlock(OETLCodeBlock.class);
    registerBlock(OETLLetBlock.class);
    registerBlock(OETLConsoleBlock.class);

    registerExtractor(OETLJDBCExtractor.class);
    registerExtractor(OETLRowExtractor.class);
    registerExtractor(OETLJsonExtractor.class);
    registerExtractor(OETLXmlExtractor.class);
    registerExtractor(OETLCSVExtractor.class);

    registerTransformer(OETLBlockTransformer.class);
    registerTransformer(OETLCodeTransformer.class);
    registerTransformer(OETLCommandTransformer.class);
    registerTransformer(OETLEdgeTransformer.class);
    registerTransformer(OETLFieldTransformer.class);
    registerTransformer(OETLJSONTransformer.class);
    registerTransformer(OETLLinkTransformer.class);
    registerTransformer(OETLLogTransformer.class);
    registerTransformer(OETLMergeTransformer.class);
    registerTransformer(OETLFlowTransformer.class);
    registerTransformer(OETLVertexTransformer.class);

    registerLoader(OETLOrientDBLoader.class);
    registerLoader(OETLOutputLoader.class);
  }

  public OETLComponentFactory registerSource(final Class<? extends OETLSource> iComponent) {
    try {
      sources.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering source: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerBlock(final Class<? extends OETLBlock> iComponent) {
    try {
      blocks.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering block: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerExtractor(final Class<? extends OETLExtractor> iComponent) {
    try {
      extractors.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering extractor: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerTransformer(final Class<? extends OETLTransformer> iComponent) {
    try {
      transformers.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering transformer: %s", iComponent.getName());
    }
    return this;
  }

  public OETLComponentFactory registerLoader(final Class<? extends OETLLoader> iComponent) {
    try {
      loaders.put(iComponent.newInstance().getName(), iComponent);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on registering loader: %s", iComponent.getName());
    }
    return this;
  }

  public OETLExtractor getExtractor(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OETLExtractor> cls = extractors.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Extractor '" + iName + "' not found");
    return cls.newInstance();
  }

  public OETLTransformer getTransformer(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OETLTransformer> cls = transformers.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Transformer '" + iName + "' not found");
    return cls.newInstance();
  }

  public OETLBlock getBlock(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OETLBlock> cls = blocks.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Block '" + iName + "' not found");
    return cls.newInstance();
  }

  public OETLLoader getLoader(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OETLLoader> cls = loaders.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Loader '" + iName + "' not found");
    return cls.newInstance();
  }

  public OETLSource getSource(final String iName) throws IllegalAccessException, InstantiationException {
    final Class<? extends OETLSource> cls = sources.get(iName);
    if (cls == null)
      throw new IllegalArgumentException("Source '" + iName + "' not found");
    return cls.newInstance();
  }
}
