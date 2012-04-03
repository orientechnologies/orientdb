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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorAnd;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorBetween;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsAll;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsKey;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsText;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsValue;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorInstanceof;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIs;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorLike;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMatches;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNot;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNotEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorOr;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorTraverse;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorDivide;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMinus;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMod;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorMultiply;
import com.orientechnologies.orient.core.sql.operator.math.OQueryOperatorPlus;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.imageio.spi.ServiceRegistry;

public class OSQLEngine {
    
    private static Set<OSQLFunctionFactory> FUNCTION_FACTORIES = null;
    
    protected Map<String, Class<? extends OCommandExecutorSQLAbstract>>	commands							= new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();

	// WARNING: ORDER IS IMPORTANT TO AVOID SUB-STRING LIKE "IS" and AND "INSTANCEOF": INSTANCEOF MUST BE PLACED BEFORE! AND ALSO FOR
	// PERFORMANCE (MOST USED BEFORE)
	public static OQueryOperator[]																			RECORD_OPERATORS			= { new OQueryOperatorEquals(),
			new OQueryOperatorAnd(), new OQueryOperatorOr(), new OQueryOperatorNotEquals(), new OQueryOperatorNot(),
			new OQueryOperatorMinorEquals(), new OQueryOperatorMinor(), new OQueryOperatorMajorEquals(), new OQueryOperatorContainsAll(),
			new OQueryOperatorMajor(), new OQueryOperatorLike(), new OQueryOperatorMatches(), new OQueryOperatorInstanceof(),
			new OQueryOperatorIs(), new OQueryOperatorIn(), new OQueryOperatorContainsKey(), new OQueryOperatorContainsValue(),
			new OQueryOperatorContainsText(), new OQueryOperatorContains(), new OQueryOperatorContainsText(),
			new OQueryOperatorTraverse(), new OQueryOperatorBetween(), new OQueryOperatorPlus(), new OQueryOperatorMinus(),
			new OQueryOperatorMultiply(), new OQueryOperatorDivide(), new OQueryOperatorMod()		};

	protected static final OSQLEngine																					INSTANCE							= new OSQLEngine();

	protected OSQLEngine() {
		// COMMANDS
		commands.put(OCommandExecutorSQLAlterDatabase.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterDatabase.KEYWORD_DATABASE,
				OCommandExecutorSQLAlterDatabase.class);
		commands.put(OCommandExecutorSQLSelect.KEYWORD_SELECT, OCommandExecutorSQLSelect.class);
		commands.put(OCommandExecutorSQLTraverse.KEYWORD_TRAVERSE, OCommandExecutorSQLTraverse.class);
		commands.put(OCommandExecutorSQLInsert.KEYWORD_INSERT, OCommandExecutorSQLInsert.class);
		commands.put(OCommandExecutorSQLUpdate.KEYWORD_UPDATE, OCommandExecutorSQLUpdate.class);
		commands.put(OCommandExecutorSQLDelete.KEYWORD_DELETE, OCommandExecutorSQLDelete.class);
		commands.put(OCommandExecutorSQLGrant.KEYWORD_GRANT, OCommandExecutorSQLGrant.class);
		commands.put(OCommandExecutorSQLRevoke.KEYWORD_REVOKE, OCommandExecutorSQLRevoke.class);
		commands.put(OCommandExecutorSQLCreateLink.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateLink.KEYWORD_LINK,
				OCommandExecutorSQLCreateLink.class);
		commands.put(OCommandExecutorSQLCreateIndex.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateIndex.KEYWORD_INDEX,
				OCommandExecutorSQLCreateIndex.class);
		commands.put(OCommandExecutorSQLDropIndex.KEYWORD_DROP + " " + OCommandExecutorSQLDropIndex.KEYWORD_INDEX,
				OCommandExecutorSQLDropIndex.class);
		commands.put(OCommandExecutorSQLRebuildIndex.KEYWORD_REBUILD + " " + OCommandExecutorSQLRebuildIndex.KEYWORD_INDEX,
				OCommandExecutorSQLRebuildIndex.class);
		commands.put(OCommandExecutorSQLCreateClass.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateClass.KEYWORD_CLASS,
				OCommandExecutorSQLCreateClass.class);
		commands.put(OCommandExecutorSQLAlterClass.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterClass.KEYWORD_CLASS,
				OCommandExecutorSQLAlterClass.class);
		commands.put(OCommandExecutorSQLCreateProperty.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLCreateProperty.class);
		commands.put(OCommandExecutorSQLAlterProperty.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLAlterProperty.class);
		commands.put(OCommandExecutorSQLDropClass.KEYWORD_DROP + " " + OCommandExecutorSQLDropClass.KEYWORD_CLASS,
				OCommandExecutorSQLDropClass.class);
		commands.put(OCommandExecutorSQLDropProperty.KEYWORD_DROP + " " + OCommandExecutorSQLDropProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLDropProperty.class);
		commands.put(OCommandExecutorSQLFindReferences.KEYWORD_FIND + " " + OCommandExecutorSQLFindReferences.KEYWORD_REFERENCES,
				OCommandExecutorSQLFindReferences.class);
		commands.put(OCommandExecutorSQLTruncateClass.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateClass.KEYWORD_CLASS,
				OCommandExecutorSQLTruncateClass.class);
		commands.put(OCommandExecutorSQLTruncateCluster.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateCluster.KEYWORD_CLUSTER,
				OCommandExecutorSQLTruncateCluster.class);
		commands.put(OCommandExecutorSQLTruncateRecord.KEYWORD_TRUNCATE + " " + OCommandExecutorSQLTruncateRecord.KEYWORD_RECORD,
				OCommandExecutorSQLTruncateRecord.class);
		commands.put(OCommandExecutorSQLAlterCluster.KEYWORD_ALTER + " " + OCommandExecutorSQLAlterCluster.KEYWORD_CLUSTER,
				OCommandExecutorSQLAlterCluster.class);

	}

	public OQueryOperator[] getRecordOperators() {
		return RECORD_OPERATORS;
	}

	public static void registerOperator(final OQueryOperator iOperator) {
		final OQueryOperator[] ops = new OQueryOperator[RECORD_OPERATORS.length + 1];
		System.arraycopy(RECORD_OPERATORS, 0, ops, 0, RECORD_OPERATORS.length);
		RECORD_OPERATORS = ops;
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		ODynamicFunctionFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunction);
	}

	public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
		ODynamicFunctionFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunctionClass);
	}

	public OSQLFunction getFunction(String iFunctionName) {
        iFunctionName = iFunctionName.toUpperCase(Locale.ENGLISH);
        
		final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
        while(ite.hasNext()){
            final OSQLFunctionFactory factory = ite.next();
            if(factory.getNames().contains(iFunctionName)){
                return factory.createFunction(iFunctionName);
            }
        }
        
        throw new OCommandSQLParsingException("No function for name "+iFunctionName+
                ", available names are : "+OCollections.toString(getFunctionNames()));
	}

	public void unregisterFunction(String iName) {
		iName = iName.toUpperCase(Locale.ENGLISH);
        ODynamicFunctionFactory.FUNCTIONS.remove(iName);
	}
    
    /**
     * @return Iterator of all function factories
     */
    public static synchronized Iterator<OSQLFunctionFactory> getFunctionFactories(){
        if(FUNCTION_FACTORIES == null){
            final Iterator<OSQLFunctionFactory> ite = ServiceRegistry.lookupProviders(OSQLFunctionFactory.class);
            final Set<OSQLFunctionFactory> factories = new HashSet<OSQLFunctionFactory>();
            while(ite.hasNext()){
                factories.add(ite.next());
            }
            FUNCTION_FACTORIES = Collections.unmodifiableSet(factories);
        }
        return FUNCTION_FACTORIES.iterator();
    }
    
    /**
     * Iterates on all factories and append all function names.
     * 
     * @return Set of all function names.
     */
    public static Set<String> getFunctionNames(){
        final Set<String> types = new HashSet<String>();
        final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
        while(ite.hasNext()){
            types.addAll(ite.next().getNames());
        }
        return types;
    }
            
    /**
     * Scans for factory plug-ins on the application class path. This method is
     * needed because the application class path can theoretically change, or
     * additional plug-ins may become available. Rather than re-scanning the
     * classpath on every invocation of the API, the class path is scanned
     * automatically only on the first invocation. Clients can call this method
     * to prompt a re-scan. Thus this method need only be invoked by
     * sophisticated applications which dynamically make new plug-ins available
     * at runtime.
     */
    public static synchronized void scanForPlugins() {
        //clear cache, will cause a rescan on next getFunctionFactories call
        FUNCTION_FACTORIES = null; 
    }
    
	public OCommandExecutorSQLAbstract getCommand(final String iText) {
		int pos = -1;
		Class<? extends OCommandExecutorSQLAbstract> commandClass = null;
		while (commandClass == null) {
			pos = iText.indexOf(' ', pos + 1);
			if (pos > -1) {
				String piece = iText.substring(0, pos);
				commandClass = commands.get(piece);
			} else
				break;
		}

		if (commandClass != null)
			try {
				return commandClass.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of command " + commandClass
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}

		return null;
	}

	public OSQLFilter parseFromWhereCondition(final String iText, final OCommandContext iContext) {
		return new OSQLFilter(iText, iContext);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}
    
    public static class ODynamicFunctionFactory implements OSQLFunctionFactory{

        private static final Map<String,Object> FUNCTIONS = new ConcurrentHashMap<String, Object>();
        
        public Set<String> getNames() {
            return FUNCTIONS.keySet();
        }

        public OSQLFunction createFunction(String name) throws OCommandExecutionException {
            final Object obj = FUNCTIONS.get(name);

            if(obj == null){
                throw new OCommandExecutionException("Unknowned function name :" + name);
            }

            if(obj instanceof OSQLFunction){
                return (OSQLFunction) obj;
            }else{
                //it's a class
                final Class clazz = (Class) obj;
                try {
                    return (OSQLFunction) clazz.newInstance();
                } catch (Exception e) {
                    throw new OCommandExecutionException("Error in creation of function " + name
                            + "(). Probably there is not an empty constructor or the constructor generates errors", e);
                }
            }
        }
        
    }
    
}
