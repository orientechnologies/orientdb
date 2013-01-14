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
package com.orientechnologies.orient.core.sql.filter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import java.util.Iterator;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import java.util.Collection;

/**
 * Represents an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OSQLFilterItemAbstract implements OSQLFilterItem {
    
  private static ClassLoader orientClassLoader = OSQLFilterItemAbstract.class.getClassLoader();
        
  protected List<OPair<OSQLMethod, Object[]>> operationsChain = null;

  public OSQLFilterItemAbstract(final OBaseParser iQueryToParse, final String iText) {
    final List<String> parts = OStringSerializerHelper.smartSplit(iText, '.');

    setRoot(iQueryToParse, parts.get(0));

    if (parts.size() > 1) {
      operationsChain = new ArrayList<OPair<OSQLMethod, Object[]>>();

      // GET ALL SPECIAL OPERATIONS
      for (int i = 1; i < parts.size(); ++i) {
        String part = parts.get(i);
        String partUpperCase = part.toLowerCase(Locale.ENGLISH);

        if (part.indexOf('(') > -1) {
          boolean operatorFound = false;
          for (OSQLMethod op : getAllMethods())
            if (partUpperCase.startsWith(op.getName() + "(")) {
              // OPERATOR MATCH
              final Object[] arguments;

              if (op.getMaxParams() > 0) {
                arguments = OStringSerializerHelper.getParameters(part).toArray();
                if (arguments.length < op.getMinParams() || arguments.length > op.getMaxParams())
                  throw new OQueryParsingException(iQueryToParse.parserText, "Syntax error: field operator '" + op.getName()
                      + "' needs "
                      + (op.getMinParams() == op.getMaxParams() ? op.getMinParams() : op.getMinParams() + "-" + op.getMaxParams())
                      + " argument(s) while has been received " + arguments.length, 0);
              } else {
                arguments = null;
              }

              // SPECIAL OPERATION FOUND: ADD IT IN TO THE CHAIN
              operationsChain.add(new OPair<OSQLMethod, Object[]>(op, arguments));
              operatorFound = true;
              break;
            }

          if (!operatorFound)
            // ERROR: OPERATOR NOT FOUND OR MISPELLED
            throw new OQueryParsingException(iQueryToParse.parserText,
                "Syntax error: field operator not recognized between the supported ones: "
                    + Arrays.toString(getAllMethodNames()), 0);
        } else {
          operationsChain.add(new OPair<OSQLMethod, Object[]>(getMethod(OSQLMethodField.NAME), new Object[]{part}));
        }
      }
    }
  }

  public abstract String getRoot();

  protected abstract void setRoot(OBaseParser iQueryToParse, final String iRoot);

  public Object transformValue(final OIdentifiable iRecord, Object ioResult) {
    if (ioResult != null && operationsChain != null) {
      // APPLY OPERATIONS FOLLOWING THE STACK ORDER
      OSQLMethod operator = null;

      try {
        for (OPair<OSQLMethod,Object[]> op : operationsChain) {
          operator = op.getKey();
          ioResult = operator.execute(iRecord, ioResult, op.getValue());
        }
      } catch (ParseException e) {
        OLogManager.instance().exception("Error on conversion of value '%s' using field operator %s", e,
            OCommandExecutionException.class, ioResult, operator.getName());
      }
    }

    return ioResult;
  }

  public boolean hasChainOperators() {
    return operationsChain != null;
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    final String root = getRoot();
    if (root != null)
      buffer.append(root);
    if (operationsChain != null) {
      for (OPair<OSQLMethod, Object[]> op : operationsChain) {
        buffer.append('.');
        buffer.append(op.getKey());
        if (op.getValue() != null) {
          final Object[] values = op.getValue();
          buffer.append('(');
          int i = 0;
          for (Object v : values) {
            if (i++ > 0)
              buffer.append(',');
            buffer.append(v);
          }
          buffer.append(')');
        }
      }
    }
    return buffer.toString();
  }
  
  
  private static Collection<OSQLMethod> getAllMethods(){
      final List<OSQLMethod> methods = new ArrayList<OSQLMethod>();
      final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class,orientClassLoader);
      while(ite.hasNext()){
          final OSQLMethodFactory factory = ite.next();
          for(String name : factory.getMethodNames()){
              methods.add(factory.createMethod(name));
          }
      }
      return methods;
  }
  
  private static String[] getAllMethodNames(){
      final List<String> methods = new ArrayList<String>();
      final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class,orientClassLoader);
      while(ite.hasNext()){
          final OSQLMethodFactory factory = ite.next();
          methods.addAll(factory.getMethodNames());
      }
      return methods.toArray(new String[methods.size()]);
  }
  
  private static OSQLMethod getMethod(String name){
      name = name.toLowerCase();
      final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class,orientClassLoader);
      while(ite.hasNext()){
          final OSQLMethodFactory factory = ite.next();
          if(factory.getMethodNames().contains(name)){
              return factory.createMethod(name);
          }
      }
      return null;
  }
  
}
