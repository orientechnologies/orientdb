/*
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
package com.orientechnologies.orient.core.index;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * Utility class to create indexes. New OIndexFactory can be registered
 *
 * <p>
 *
 * <p>In order to be detected, factories must implement the {@link OIndexFactory} interface.
 *
 * <p>
 *
 * <p>In addition to implementing this interface datasources should have a services file:<br>
 * <code>META-INF/services/com.orientechnologies.orient.core.index.OIndexFactory</code>
 *
 * <p>
 *
 * <p>The file should contain a single line which gives the full name of the implementing class.
 *
 * <p>
 *
 * <p>Example:<br>
 * <code>org.mycompany.index.MyIndexFactory</code>
 *
 * @author Johann Sorel (Geomatys)
 */
public final class OIndexes {

  private static Set<OIndexFactory> FACTORIES = null;
  private static final Set<OIndexFactory> DYNAMIC_FACTORIES =
      Collections.synchronizedSet(new HashSet<>());
  private static ClassLoader orientClassLoader = OIndexes.class.getClassLoader();

  private OIndexes() {}

  /**
   * Cache a set of all factories. we do not use the service loader directly since it is not
   * concurrent.
   *
   * @return Set<OIndexFactory>
   */
  private static synchronized Set<OIndexFactory> getFactories() {
    if (FACTORIES == null) {

      final Iterator<OIndexFactory> ite =
          lookupProviderWithOrientClassLoader(OIndexFactory.class, orientClassLoader);

      final Set<OIndexFactory> factories = new HashSet<>();
      while (ite.hasNext()) {
        factories.add(ite.next());
      }
      factories.addAll(DYNAMIC_FACTORIES);
      FACTORIES = Collections.unmodifiableSet(factories);
    }
    return FACTORIES;
  }

  /** @return Iterator of all index factories */
  public static Iterator<OIndexFactory> getAllFactories() {
    return getFactories().iterator();
  }

  /**
   * Iterates on all factories and append all index types.
   *
   * @return Set of all index types.
   */
  private static Set<String> getIndexTypes() {
    final Set<String> types = new HashSet<>();
    final Iterator<OIndexFactory> ite = getAllFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getTypes());
    }
    return types;
  }

  /**
   * Iterates on all factories and append all index engines.
   *
   * @return Set of all index engines.
   */
  public static Set<String> getIndexEngines() {
    final Set<String> engines = new HashSet<>();
    final Iterator<OIndexFactory> ite = getAllFactories();
    while (ite.hasNext()) {
      engines.addAll(ite.next().getAlgorithms());
    }
    return engines;
  }

  public static OIndexFactory getFactory(String indexType, String algorithm) {
    if (algorithm == null) {
      algorithm = chooseDefaultIndexAlgorithm(indexType);
    }

    algorithm = algorithm.toUpperCase(Locale.ENGLISH);
    final Iterator<OIndexFactory> ite = getAllFactories();

    while (ite.hasNext()) {
      final OIndexFactory factory = ite.next();
      if (factory.getTypes().contains(indexType) && factory.getAlgorithms().contains(algorithm)) {
        return factory;
      }
    }

    throw new OIndexException(
        "Index with type " + indexType + " and algorithm " + algorithm + " does not exist.");
  }

  /**
   * @param storage TODO
   * @param indexType index type
   * @return OIndexInternal
   * @throws OConfigurationException if index creation failed
   * @throws OIndexException if index type does not exist
   */
  public static OIndexInternal createIndex(
      OStorage storage,
      String name,
      String indexType,
      String algorithm,
      String valueContainerAlgorithm,
      ODocument metadata,
      int version)
      throws OConfigurationException, OIndexException {
    if (indexType.equalsIgnoreCase(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name())
        || indexType.equalsIgnoreCase(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.name())
        || indexType.equalsIgnoreCase(OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name())) {
      if (!algorithm.equalsIgnoreCase("autosharding")) {
        algorithm = OHashIndexFactory.HASH_INDEX_ALGORITHM;
      }
    }

    return findFactoryByAlgorithmAndType(algorithm, indexType)
        .createIndex(
            name, storage, indexType, algorithm, valueContainerAlgorithm, metadata, version);
  }

  private static OIndexFactory findFactoryByAlgorithmAndType(String algorithm, String indexType) {

    for (OIndexFactory factory : getFactories()) {
      if (indexType == null
          || indexType.isEmpty()
          || (factory.getTypes().contains(indexType))
              && factory.getAlgorithms().contains(algorithm)) {
        return factory;
      }
    }
    throw new OIndexException(
        "Index type "
            + indexType
            + " with engine "
            + algorithm
            + " is not supported. Types are "
            + OCollections.toString(getIndexTypes()));
  }

  public static OBaseIndexEngine createIndexEngine(
      int indexId,
      final String name,
      final String algorithm,
      final String type,
      final OStorage storage,
      final int version,
      boolean multivalue) {

    final OIndexFactory factory = findFactoryByAlgorithmAndType(algorithm, type);

    return factory.createIndexEngine(indexId, algorithm, name, storage, version, multivalue);
  }

  public static String chooseDefaultIndexAlgorithm(String type) {
    String algorithm = null;

    if (OClass.INDEX_TYPE.DICTIONARY.name().equalsIgnoreCase(type)
        || OClass.INDEX_TYPE.FULLTEXT.name().equalsIgnoreCase(type)
        || OClass.INDEX_TYPE.NOTUNIQUE.name().equalsIgnoreCase(type)
        || OClass.INDEX_TYPE.UNIQUE.name().equalsIgnoreCase(type)) {
      algorithm = ODefaultIndexFactory.CELL_BTREE_ALGORITHM;
    } else if (OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name().equalsIgnoreCase(type)
        || OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.name().equalsIgnoreCase(type)
        || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name().equalsIgnoreCase(type)) {
      algorithm = OHashIndexFactory.HASH_INDEX_ALGORITHM;
    }
    return algorithm;
  }

  /**
   * Scans for factory plug-ins on the application class path. This method is needed because the
   * application class path can theoretically change, or additional plug-ins may become available.
   * Rather than re-scanning the classpath on every invocation of the API, the class path is scanned
   * automatically only on the first invocation. Clients can call this method to prompt a re-scan.
   * Thus this method need only be invoked by sophisticated applications which dynamically make new
   * plug-ins available at runtime.
   */
  private static synchronized void scanForPlugins() {
    // clear cache, will cause a rescan on next getFactories call
    FACTORIES = null;
  }

  /** Register at runtime custom factories */
  public static void registerFactory(OIndexFactory factory) {
    DYNAMIC_FACTORIES.add(factory);
    scanForPlugins();
  }

  /** Unregister custom factories */
  public static void unregisterFactory(OIndexFactory factory) {
    DYNAMIC_FACTORIES.remove(factory);
    scanForPlugins();
  }
}
