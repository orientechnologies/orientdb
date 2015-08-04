/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.OCollateFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import java.util.*;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

public class OSQLEngine {

  protected static final OSQLEngine               INSTANCE           = new OSQLEngine();
  private static List<OSQLFunctionFactory>        FUNCTION_FACTORIES = null;
  private static List<OSQLMethodFactory>          METHOD_FACTORIES   = null;
  private static List<OCommandExecutorSQLFactory> EXECUTOR_FACTORIES = null;
  private static List<OQueryOperatorFactory>      OPERATOR_FACTORIES = null;
  private static List<OCollateFactory>            COLLATE_FACTORIES  = null;
  private static OQueryOperator[]                 SORTED_OPERATORS   = null;
  private static ClassLoader                      orientClassLoader  = OSQLEngine.class.getClassLoader();

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

  protected OSQLEngine() {
  }

  public static void registerOperator(final OQueryOperator iOperator) {
    ODynamicSQLElementFactory.OPERATORS.add(iOperator);
    SORTED_OPERATORS = null; // clear cache
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

  public static synchronized Iterator<OSQLMethodFactory> getMethodFactories() {
    if (METHOD_FACTORIES == null) {

      final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, orientClassLoader);

      final List<OSQLMethodFactory> factories = new ArrayList<OSQLMethodFactory>();
      while (ite.hasNext()) {
        factories.add(ite.next());
      }
      METHOD_FACTORIES = Collections.unmodifiableList(factories);
    }
    return METHOD_FACTORIES.iterator();
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

  public static Set<String> getMethodNames() {
    final Set<String> types = new HashSet<String>();
    final Iterator<OSQLMethodFactory> ite = getMethodFactories();
    while (ite.hasNext()) {
      types.addAll(ite.next().getMethodNames());
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

  public static Object foreachRecord(final OCallable<Object, OIdentifiable> iCallable, final Object iCurrent,
      final OCommandContext iContext) {
    if (iCurrent == null)
      return null;

    if( !OCommandExecutorAbstract.checkInterruption(iContext) )
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

  public static OCollate getCollate(final String name) {
    for (Iterator<OCollateFactory> iter = getCollateFactories(); iter.hasNext();) {
      OCollateFactory f = iter.next();
      final OCollate c = f.getCollate(name);
      if (c != null)
        return c;
    }
    return null;
  }

  public static OSQLMethod getMethod(String iMethodName) {
    iMethodName = iMethodName.toLowerCase(Locale.ENGLISH);

    final Iterator<OSQLMethodFactory> ite = getMethodFactories();
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      if (factory.hasMethod(iMethodName)) {
        return factory.createMethod(iMethodName);
      }
    }

    return null;
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

    throw new OCommandSQLParsingException("No function with name '" + iFunctionName + "', available names are : "
        + OCollections.toString(getFunctionNames()));
  }

  public void unregisterFunction(String iName) {
    iName = iName.toLowerCase(Locale.ENGLISH);
    ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
  }

  public OCommandExecutorSQLAbstract getCommand(String candidate) {
    candidate = candidate.trim();
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

  public Set<OIdentifiable> parseRIDTarget(final ODatabaseDocument database, String iTarget, final OCommandContext iContext,
      Map<Object, Object> iArgs) {
    final Set<OIdentifiable> ids;
    if (iTarget.startsWith("(")) {
      // SUB-QUERY
      final OSQLSynchQuery<Object> query = new OSQLSynchQuery<Object>(iTarget.substring(1, iTarget.length() - 1));
      query.setContext(iContext);

      final List<OIdentifiable> result = database.query(query, iArgs);
      if (result == null || result.isEmpty())
        ids = Collections.emptySet();
      else {
        ids = new HashSet<OIdentifiable>((int) (result.size() * 1.3));
        for (OIdentifiable aResult : result)
          ids.add(aResult.getIdentity());
      }
    } else if (iTarget.startsWith("[")) {
      // COLLECTION OF RIDS
      final String[] idsAsStrings = iTarget.substring(1, iTarget.length() - 1).split(",");
      ids = new HashSet<OIdentifiable>((int) (idsAsStrings.length * 1.3));
      for (String idsAsString : idsAsStrings) {
        if (idsAsString.startsWith("$")) {
          Object r = iContext.getVariable(idsAsString);
          if (r instanceof OIdentifiable)
            ids.add((OIdentifiable) r);
          else
            OMultiValue.add(ids, r);
        } else
          ids.add(new ORecordId(idsAsString));
      }
    } else {
      // SINGLE RID
      if (iTarget.startsWith("$")) {
        Object r = iContext.getVariable(iTarget);
        if (r instanceof OIdentifiable)
          ids = Collections.<OIdentifiable> singleton((OIdentifiable) r);
        else
          ids = (Set<OIdentifiable>) OMultiValue.add(new HashSet<OIdentifiable>(OMultiValue.getSize(r)), r);

      } else
        ids = Collections.<OIdentifiable> singleton(new ORecordId(iTarget));

    }
    return ids;
  }
}
