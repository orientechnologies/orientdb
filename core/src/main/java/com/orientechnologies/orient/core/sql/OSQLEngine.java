/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.exception.OException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import javax.imageio.spi.ServiceRegistry;

import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorFactory;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class OSQLEngine {

    
    private static Set<OSQLFunctionFactory> FUNCTION_FACTORIES = null;
    private static Set<OCommandExecutorSQLFactory> EXECUTOR_FACTORIES = null;
    private static Set<OQueryOperatorFactory> OPERATOR_FACTORIES = null;
    private static OQueryOperator[] SORTED_OPERATORS = null;
    
	protected static final OSQLEngine																		INSTANCE						= new OSQLEngine();

	protected OSQLEngine() {}

	public synchronized OQueryOperator[] getRecordOperators() {
        if(SORTED_OPERATORS != null){
            return SORTED_OPERATORS;
        }
        
        //sort operators, will happen only very few times since we cache the result
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
                        case BEFORE: pairs.add(new Pair(ca, cb)); break;
                        case AFTER:  pairs.add(new Pair(cb, ca)); break;
                    }
                    switch (cb.compare(ca)) {
                        case BEFORE: pairs.add(new Pair(cb, ca)); break;
                        case AFTER:  pairs.add(new Pair(ca, cb)); break;
                    }
                }
            }
        }
        boolean added;
        do {
            added = false;
scan:       for (final Iterator<OQueryOperator> it=operators.iterator(); it.hasNext();) {
                final OQueryOperator candidate = it.next();
                for (final Pair pair : pairs) {
                    if (pair.after == candidate) {
                        continue scan;
                    }
                }
                sorted.add(candidate);
                it.remove();
                for (final Iterator<Pair> itp=pairs.iterator(); itp.hasNext();) {
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
                SORTED_OPERATORS = null; //clear cache
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		ODynamicSQLElementFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunction);
	}

	public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
		ODynamicSQLElementFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunctionClass);
	}

	public OSQLFunction getFunction(String iFunctionName) {
		iFunctionName = iFunctionName.toUpperCase(Locale.ENGLISH);

		final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
		while (ite.hasNext()) {
			final OSQLFunctionFactory factory = ite.next();
			if (factory.getFunctionNames().contains(iFunctionName)) {
				return factory.createFunction(iFunctionName);
			}
		}

		throw new OCommandSQLParsingException("No function for name " + iFunctionName + ", available names are : "
				+ OCollections.toString(getFunctionNames()));
	}

	public void unregisterFunction(String iName) {
		iName = iName.toUpperCase(Locale.ENGLISH);
		ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
	}

	/**
	 * @return Iterator of all function factories
	 */
	public static synchronized Iterator<OSQLFunctionFactory> getFunctionFactories() {
		if (FUNCTION_FACTORIES == null) {
			final Iterator<OSQLFunctionFactory> ite = ServiceRegistry.lookupProviders(OSQLFunctionFactory.class);
			final Set<OSQLFunctionFactory> factories = new HashSet<OSQLFunctionFactory>();
			while (ite.hasNext()) {
				factories.add(ite.next());
			}
			FUNCTION_FACTORIES = Collections.unmodifiableSet(factories);
		}
		return FUNCTION_FACTORIES.iterator();
	}
        
        /**
	 * @return Iterator of all operator factories
	 */
	public static synchronized Iterator<OQueryOperatorFactory> getOperatorFactories() {
		if (OPERATOR_FACTORIES == null) {
			final Iterator<OQueryOperatorFactory> ite = ServiceRegistry.lookupProviders(OQueryOperatorFactory.class);
			final Set<OQueryOperatorFactory> factories = new HashSet<OQueryOperatorFactory>();
			while (ite.hasNext()) {
				factories.add(ite.next());
			}
			OPERATOR_FACTORIES = Collections.unmodifiableSet(factories);
		}
		return OPERATOR_FACTORIES.iterator();
	}
                
        /**
	 * @return Iterator of all command factories
	 */
	public static synchronized Iterator<OCommandExecutorSQLFactory> getCommandFactories() {
		if (EXECUTOR_FACTORIES == null) {
			final Iterator<OCommandExecutorSQLFactory> ite = ServiceRegistry.lookupProviders(OCommandExecutorSQLFactory.class);
			final Set<OCommandExecutorSQLFactory> factories = new HashSet<OCommandExecutorSQLFactory>();
			while (ite.hasNext()) {
				factories.add(ite.next());
			}
			EXECUTOR_FACTORIES = Collections.unmodifiableSet(factories);
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
                while(!found){
                    pos = candidate.indexOf(' ', pos + 1);
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

	public OSQLFilter parseFromWhereCondition(final String iText, final OCommandContext iContext) {
		return new OSQLFilter(iText, iContext);
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
            return before +" > "+ after;
        }
        
    }
    
}
