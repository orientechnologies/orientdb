/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.OCollateFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OUpdatableResult;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import com.orientechnologies.orient.core.sql.parser.OAdminStatement;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OOrBlock;
import com.orientechnologies.orient.core.sql.parser.OSecurityResourceSegment;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.sql.parser.OrientSql;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.sql.parser.TokenMgrError;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public class OSQLEngine {
  private static final OLogger logger = OLogManager.instance().logger(OSQLEngine.class);
  protected static final OSQLEngine INSTANCE = new OSQLEngine();
  private static volatile List<OSQLFunctionFactory> FUNCTION_FACTORIES = null;
  private static List<OSQLMethodFactory> METHOD_FACTORIES = null;
  private static List<OQueryOperatorFactory> OPERATOR_FACTORIES = null;
  private static List<OCollateFactory> COLLATE_FACTORIES = null;
  private static ClassLoader orientClassLoader = OSQLEngine.class.getClassLoader();

  public static OStatement parse(String query, ODatabaseDocumentInternal db) {
    return OStatementCache.get(query, db);
  }

  public static OAdminStatement parseAdminStatement(String statement, OrientDBInternal db) {
    try {
      OrientSql osql = new OrientSql(statement);
      OAdminStatement result = osql.parseAdminStatement();
      return result;
    } catch (ParseException e) {
      throwParsingException(e, statement);
    } catch (TokenMgrError e2) {
      throwParsingException(e2, statement);
    }
    return null;
  }

  protected static void throwParsingException(ParseException e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

  protected static void throwParsingException(TokenMgrError e, String statement) {
    throw new OCommandSQLParsingException(e, statement);
  }

  public static List<OStatement> parseScript(String script, ODatabaseDocumentInternal db) {
    try {
      OrientSql osql = new OrientSql(script);
      return osql.parseScript();
    } catch (ParseException e) {
      throw OException.wrapException(new OCommandSQLParsingException(e, ""), e);
    }
  }

  public static List<OStatement> parseScript(InputStream script, ODatabaseDocumentInternal db) {
    try {
      final OrientSql osql = new OrientSql(script);
      List<OStatement> result = osql.parseScript();
      return result;
    } catch (ParseException e) {
      throw OException.wrapException(new OCommandSQLParsingException(e, ""), e);
    }
  }

  public static OOrBlock parsePredicate(String predicate) throws OCommandSQLParsingException {
    try {
      final OrientSql osql = new OrientSql(predicate);
      OOrBlock result = osql.OrBlock();
      return result;
    } catch (ParseException e) {
      throw OException.wrapException(new OCommandSQLParsingException(e, ""), e);
    }
  }

  public static Optional<OOrBlock> maybeParsePredicate(String predicate)
      throws OCommandSQLParsingException {
    try {
      final OrientSql osql = new OrientSql(predicate);
      OOrBlock result = osql.OrBlock();
      return Optional.of(result);
    } catch (ParseException e) {
      return Optional.empty();
    }
  }

  public static Object eval(String expression, Object target, OCommandContext ctx) {
    Optional<OOrBlock> predicate = maybeParsePredicate(expression);
    if (predicate.isPresent()) {
      return predicate.get().evaluate(target, ctx);
    } else {
      return parseExpression(expression).execute(new OUpdatableResult((OElement) target), ctx);
    }
  }

  public static OExpression parseExpression(String predicate) throws OCommandSQLParsingException {
    try {
      final OrientSql osql = new OrientSql(predicate);
      OExpression result = osql.Expression();
      return result;
    } catch (ParseException e) {
      throw OException.wrapException(new OCommandSQLParsingException(e, ""), e);
    }
  }

  public static OSecurityResourceSegment parseSecurityResource(String exp) {
    try {
      final OrientSql osql = new OrientSql(exp);
      OSecurityResourceSegment result = osql.SecurityResourceSegment();
      return result;
    } catch (ParseException e) {
      throw OException.wrapException(new OCommandSQLParsingException(e, ""), e);
    }
  }

  protected OSQLEngine() {}

  /** @return Iterator of all function factories */
  public static Iterator<OSQLFunctionFactory> getFunctionFactories() {
    if (FUNCTION_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (FUNCTION_FACTORIES == null) {
          final Iterator<OSQLFunctionFactory> ite =
              lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, orientClassLoader);

          final List<OSQLFunctionFactory> factories = new ArrayList<OSQLFunctionFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          FUNCTION_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return FUNCTION_FACTORIES.iterator();
  }

  public static Iterator<OSQLMethodFactory> getMethodFactories() {
    if (METHOD_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (METHOD_FACTORIES == null) {

          final Iterator<OSQLMethodFactory> ite =
              lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, orientClassLoader);

          final List<OSQLMethodFactory> factories = new ArrayList<OSQLMethodFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          METHOD_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return METHOD_FACTORIES.iterator();
  }

  /** @return Iterator of all function factories */
  public static Iterator<OCollateFactory> getCollateFactories() {
    if (COLLATE_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (COLLATE_FACTORIES == null) {

          final Iterator<OCollateFactory> ite =
              lookupProviderWithOrientClassLoader(OCollateFactory.class, orientClassLoader);

          final List<OCollateFactory> factories = new ArrayList<OCollateFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          COLLATE_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return COLLATE_FACTORIES.iterator();
  }

  /** @return Iterator of all operator factories */
  public static Iterator<OQueryOperatorFactory> getOperatorFactories() {
    if (OPERATOR_FACTORIES == null) {
      synchronized (INSTANCE) {
        if (OPERATOR_FACTORIES == null) {

          final Iterator<OQueryOperatorFactory> ite =
              lookupProviderWithOrientClassLoader(OQueryOperatorFactory.class, orientClassLoader);

          final List<OQueryOperatorFactory> factories = new ArrayList<OQueryOperatorFactory>();
          while (ite.hasNext()) {
            factories.add(ite.next());
          }
          OPERATOR_FACTORIES = Collections.unmodifiableList(factories);
        }
      }
    }
    return OPERATOR_FACTORIES.iterator();
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
   * Scans for factory plug-ins on the application class path. This method is needed because the
   * application class path can theoretically change, or additional plug-ins may become available.
   * Rather than re-scanning the classpath on every invocation of the API, the class path is scanned
   * automatically only on the first invocation. Clients can call this method to prompt a re-scan.
   * Thus this method need only be invoked by sophisticated applications which dynamically make new
   * plug-ins available at runtime.
   */
  public static void scanForPlugins() {
    // clear cache, will cause a rescan on next getFunctionFactories call
    FUNCTION_FACTORIES = null;
  }

  public static Object foreachRecord(
      final OCallable<Object, OIdentifiable> iCallable,
      Object iCurrent,
      final OCommandContext iContext) {
    if (iCurrent == null) return null;

    if (OExecutionThreadLocal.isInterruptCurrentOperation())
      throw new OCommandInterruptedException("The command has been interrupted");

    if (iCurrent instanceof Iterable && !(iCurrent instanceof OIdentifiable)) {
      iCurrent = ((Iterable) iCurrent).iterator();
    }
    if (OMultiValue.isMultiValue(iCurrent) || iCurrent instanceof Iterator) {
      final OMultiCollectionIterator<Object> result = new OMultiCollectionIterator<Object>();
      for (Object o : OMultiValue.getMultiValueIterable(iCurrent, false)) {
        if (iContext != null && !iContext.checkTimeout()) return null;

        if (OMultiValue.isMultiValue(o) || o instanceof Iterator) {
          for (Object inner : OMultiValue.getMultiValueIterable(o, false)) {
            result.add(iCallable.call((OIdentifiable) inner));
          }
        } else if (o instanceof OIdentifiable) {
          result.add(iCallable.call((OIdentifiable) o));
        } else if (o instanceof OResult) {
          return iCallable.call(((OResult) o).toElement());
        }
      }
      return result;
    } else if (iCurrent instanceof OIdentifiable) {
      return iCallable.call((OIdentifiable) iCurrent);
    } else if (iCurrent instanceof OResult) {
      return iCallable.call(((OResult) iCurrent).toElement());
    }

    return null;
  }

  public static OSQLEngine getInstance() {
    return INSTANCE;
  }

  public static OCollate getCollate(final String name) {
    if (name == null) {
      return null;
    }
    for (Iterator<OCollateFactory> iter = getCollateFactories(); iter.hasNext(); ) {
      OCollateFactory f = iter.next();
      final OCollate c = f.getCollate(name.toLowerCase());
      if (c != null) return c;
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

  public void registerFunction(final String iName, final OSQLFunction iFunction) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunction);
  }

  public void registerFunction(
      final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
    ODynamicSQLElementFactory.FUNCTIONS.put(iName.toLowerCase(Locale.ENGLISH), iFunctionClass);
  }

  public OSQLFunction getFunction(String iFunctionName) {
    OSQLFunction function = getFunctionIfExists(iFunctionName);
    if (function != null) {
      return function;
    }
    throw new OCommandSQLParsingException(
        "No function with name '"
            + iFunctionName
            + "', available names are : "
            + OCollections.toString(getFunctionNames()));
  }

  public OSQLFunction getFunctionIfExists(String iFunctionName) {
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
    return null;
  }

  public void unregisterFunction(String iName) {
    iName = iName.toLowerCase(Locale.ENGLISH);
    ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
  }
}
