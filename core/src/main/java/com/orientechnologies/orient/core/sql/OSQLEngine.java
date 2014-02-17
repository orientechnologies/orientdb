/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.OCollateFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OSQLEngine {

  private static List<OSQLFunctionFactory>        FUNCTION_FACTORIES = null;
  private static List<OCommandExecutorSQLFactory> EXECUTOR_FACTORIES = null;
  private static List<OQueryOperatorFactory>      OPERATOR_FACTORIES = null;
  private static List<OCollateFactory>            COLLATE_FACTORIES  = null;
  private static OQueryOperator[]                 SORTED_OPERATORS   = null;

  protected static final OSQLEngine               INSTANCE           = new OSQLEngine();

  private static ClassLoader                      orientClassLoader  = OSQLEngine.class.getClassLoader();

  protected OSQLEngine() {
  }

  public synchronized OQueryOperator[] getRecordOperators() {
    if (SORTED_OPERATORS != null) {
      return SORTED_OPERATORS;
    }

    // sort operators, will happen only very few times since we cache the
    // result
    final Iterator<OQueryOperatorFactory> ite = getOperatorFactories();
    final List<OQueryOperator> operators = new ArrayList<OQueryOperator>();
    while (ite.hasNext()) {
      final OQueryOperatorFactory factory = ite.next();
      operators.addAll(factory.getOperators());
    }

    final List<OQueryOperator> sorted = new ArrayList<OQueryOperator>();
    final Set<Pair> pairs = new LinkedHashSet<Pair>();
    for (final OQueryOperator ca : operators) {
      for (final OQueryOperator cb : operators) {
        if (ca != cb) {
          switch (ca.compare(cb)) {
          case BEFORE:
            pairs.add(new Pair(ca, cb));
            break;
          case AFTER:
            pairs.add(new Pair(cb, ca));
            break;
          }
          switch (cb.compare(ca)) {
          case BEFORE:
            pairs.add(new Pair(cb, ca));
            break;
          case AFTER:
            pairs.add(new Pair(ca, cb));
            break;
          }
        }
      }
    }
    boolean added;
    do {
      added = false;
      scan: for (final Iterator<OQueryOperator> it = operators.iterator(); it.hasNext();) {
        final OQueryOperator candidate = it.next();
        for (final Pair pair : pairs) {
          if (pair.after == candidate) {
            continue scan;
          }
        }
        sorted.add(candidate);
        it.remove();
        for (final Iterator<Pair> itp = pairs.iterator(); itp.hasNext();) {
          if (itp.next().before == candidate) {
            itp.remove();
          }
        }
        added = true;
      }
    } while (added);
    if (!operators.isEmpty()) {
      throw new OException("Unvalid sorting. " + OCollections.toString(pairs));
    }
    SORTED_OPERATORS = sorted.toArray(new OQueryOperator[sorted.size()]);
    return SORTED_OPERATORS;
  }

  public static void registerOperator(final OQueryOperator iOperator) {
    ODynamicSQLElementFactory.OPERATORS.add(iOperator);
    SORTED_OPERATORS = null; // clear cache
  }

  public void registerFunction(final String iName, final OSQLFunction iFunction) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunction);
  }

  public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunctionClass);
  }

  public OSQLFunction getFunction(String iFunctionName) {
    iFunctionName = iFunctionName.toLowerCase(Locale.ENGLISH);

    if (iFunctionName.equalsIgnoreCase("any") || iFunctionName.equalsIgnoreCase("all"))
      // SPECIAL FUNCTIONS
      return null;

    final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
    while (ite.hasNext()) {
      final OSQLFunctionFactory factory = ite.next();
      if (factory.hasFunction(iFunctionName)) {
        return factory.createFunction(iFunctionName);
      }
    }

    throw new OCommandSQLParsingException("No function for name " + iFunctionName + ", available names are : "
        + OCollections.toString(getFunctionNames()));
  }

  public void unregisterFunction(String iName) {
    iName = iName.toLowerCase(Locale.ENGLISH);
    ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
  }

  /**
   * @return Iterator of all function factories
   */
  public static synchronized Iterator<OSQLFunctionFactory> getFunctionFactories() {
    if (FUNCTION_FACTORIES == null) {

      final Iterator<OSQLFunctionFactory> ite = lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, orientClassLoader);

      final List<OSQLFunctionFactory> factories = new ArrayList<OSQLFunctionFactory>();
      while (ite.hasNext()) {
        factories.add(ite.next());
      }
      FUNCTION_FACTORIES = Collections.unmodifiableList(factories);
    }
    return FUNCTION_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all function factories
   */
  public static synchronized Iterator<OCollateFactory> getCollateFactories() {
    if (COLLATE_FACTORIES == null) {

      final Iterator<OCollateFactory> ite = lookupProviderWithOrientClassLoader(OCollateFactory.class, orientClassLoader);

      final List<OCollateFactory> factories = new ArrayList<OCollateFactory>();
      while (ite.hasNext()) {
        factories.add(ite.next());
      }
      COLLATE_FACTORIES = Collections.unmodifiableList(factories);
    }
    return COLLATE_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all operator factories
   */
  public static synchronized Iterator<OQueryOperatorFactory> getOperatorFactories() {
    if (OPERATOR_FACTORIES == null) {

      final Iterator<OQueryOperatorFactory> ite = lookupProviderWithOrientClassLoader(OQueryOperatorFactory.class,
          orientClassLoader);

      final List<OQueryOperatorFactory> factories = new ArrayList<OQueryOperatorFactory>();
      while (ite.hasNext()) {
        factories.add(ite.next());
      }
      OPERATOR_FACTORIES = Collections.unmodifiableList(factories);
    }
    return OPERATOR_FACTORIES.iterator();
  }

  /**
   * @return Iterator of all command factories
   */
  public static synchronized Iterator<OCommandExecutorSQLFactory> getCommandFactories() {
    if (EXECUTOR_FACTORIES == null) {

      final Iterator<OCommandExecutorSQLFactory> ite = lookupProviderWithOrientClassLoader(OCommandExecutorSQLFactory.class,
          orientClassLoader);
      final List<OCommandExecutorSQLFactory> factories = new ArrayList<OCommandExecutorSQLFactory>();
      while (ite.hasNext()) {
        try {
          factories.add(ite.next());
        } catch (Exception e) {
          OLogManager.instance().warn(null, "Cannot load OCommandExecutorSQLFactory instance from service registry", e);
        }
      }

      EXECUTOR_FACTORIES = Collections.unmodifiableList(factories);

    }
    return EXECUTOR_FACTORIES.iterator();
  }

  /**
   * Iterates on all factories and append all function names.
   * 
   * @return Set of all function names.
   */
  public static Set<String> getFunctionNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getFunctionNames());
    }
    return types;
  }

  /**
   * Iterates on all factories and append all collate names.
   * 
   * @return Set of all colate names.
   */
  public static Set<String> getCollateNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OCollateFactory> ite = getCollateFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getNames());
    }
    return types;
  }

  /**
   * Iterates on all factories and append all command names.
   * 
   * @return Set of all command names.
   */
  public static Set<String> getCommandNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getCommandNames());
    }
    return types;
  }

  /**
   * Scans for factory plug-ins on the application class path. This method is needed because the application class path can
   * theoretically change, or additional plug-ins may become available. Rather than re-scanning the classpath on every invocation of
   * the API, the class path is scanned automatically only on the first invocation. Clients can call this method to prompt a
   * re-scan. Thus this method need only be invoked by sophisticated applications which dynamically make new plug-ins available at
   * runtime.
   */
  public static synchronized void scanForPlugins() {
    // clear cache, will cause a rescan on next getFunctionFactories call
    FUNCTION_FACTORIES = null;
  }

  public OCommandExecutorSQLAbstract getCommand(final String candidate) {
    final Set<String> names = getCommandNames();
    String commandName = candidate;
    boolean found = names.contains(commandName);
    int pos = -1;
    while (!found) {
      pos = OStringSerializerHelper.getLowerIndexOf(candidate, pos + 1, " ", "\n", "\r");
      if (pos > -1) {
        commandName = candidate.substring(0, pos);
        found = names.contains(commandName);
      } else {
        break;
      }
    }

    if (found) {
      final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
      while (ite.hasNext()) {
        final OCommandExecutorSQLFactory factory = ite.next();
        if (factory.getCommandNames().contains(commandName)) {
          return factory.createCommand(commandName);
        }
      }
    }

    return null;
  }

  public OSQLFilter parseCondition(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    return new OSQLFilter(iText, iContext, iFilterKeyword);
  }

  public OSQLTarget parseTarget(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
    return new OSQLTarget(iText, iContext, iFilterKeyword);
  }

  public static Object foreachRecord(final OCallable<Object, OIdentifiable> iCallable, final Object iCurrent,
      final OCommandContext iContext) {
    if (iCurrent == null)
      return null;

    if (iContext != null && !iContext.checkTimeout())
      return null;

    if (OMultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final OMultiCollectionIterator<Object> result = new OMultiCollectionIterator<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(iCurrent)) {
        if (iContext != null && !iContext.checkTimeout())
          return null;

        if (OMultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : OMultiValue.getMultiValueIterable(o)) {
            result.add(iCallable.call((OIdentifiable) inner));
          }
        } else
          result.add(iCallable.call((OIdentifiable) o));
      }
      return result;
    } else if (iCurrent instanceof OIdentifiable)
      return iCallable.call((OIdentifiable) iCurrent);

    return null;
  }

  public static OSQLEngine getInstance() {
    return INSTANCE;
  }

  /**
   * internal use only, to sort operators.
   */
  private static final class Pair {

    final OQueryOperator before;
    final OQueryOperator after;

    public Pair(final OQueryOperator before, final OQueryOperator after) {
      this.before = before;
      this.after = after;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj instanceof Pair) {
        final Pair that = (Pair) obj;
        return before == that.before && after == that.after;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(before) + 31 * System.identityHashCode(after);
    }

    @Override
    public String toString() {
      return before + " > " + after;
    }

  }

  public Set<ORID> parseRIDTarget(final ODatabaseRecord database, final String iTarget) {
    final Set<ORID> ids;
    if (iTarget.startsWith("(")) {
      // SUB-QUERY
      final List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>(iTarget.substring(1, iTarget.length() - 1)));
      if (result == null || result.isEmpty())
        ids = Collections.emptySet();
      else {
        ids = new HashSet<ORID>((int) (result.size() * 1.3));
        for (OIdentifiable aResult : result)
          ids.add(aResult.getIdentity());
      }
    } else if (iTarget.startsWith("[")) {
      // COLLECTION OF RIDS
      final String[] idsAsStrings = iTarget.substring(1, iTarget.length() - 1).split(",");
      ids = new HashSet<ORID>((int) (idsAsStrings.length * 1.3));
      for (String idsAsString : idsAsStrings)
        ids.add(new ORecordId(idsAsString));
    } else
      // SINGLE RID
      ids = Collections.<ORID> singleton(new ORecordId(iTarget));
    return ids;
  }

  public static OCollate getCollate(final String name) {
    for (Iterator<OCollateFactory> iter = getCollateFactories(); iter.hasNext();) {
      OCollateFactory f = iter.next();
      final OCollate c = f.getCollate(name);
      if (c != null)
        return c;
    }
    return null;
  }
}
