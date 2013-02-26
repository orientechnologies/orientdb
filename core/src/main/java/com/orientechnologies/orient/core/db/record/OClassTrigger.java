/** 
Author : henryzhao81@gmail.com
Feb 19, 2013

Create a class OTriggered which contains 8 additional class attribuites, which link to OFunction
- beforeCreate
- afterCreate
- beforeRead
- afterRead
- beforeUpdate
- afterUpdate
- beforeDelete
- afterDelete

*/

package com.orientechnologies.orient.core.db.record;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptDocumentDatabaseWrapper;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OClassTrigger extends ODocumentHookAbstract {
	public static final String CLASSNAME = "OTriggered";
	
	//Record Level Trigger (property name)
	public static final String PROP_BEFORE_CREATE  = "beforeCreate"; 
	public static final String PROP_AFTER_CREATE   = "afterCreate";
	public static final String PROP_BEFORE_READ    = "beforeRead";
	public static final String PROP_AFTER_READ     = "afterRead";
	public static final String PROP_BEFORE_UPDATE  = "beforeUpdate";
	public static final String PROP_AFTER_UPDATE   = "afterUpdate";
	public static final String PROP_BEFORE_DELETE  = "beforeDelete";
	public static final String PROP_AFTER_DELETE   = "afterDelete";
	
	//Class Level Trigger (class custom attribute)
	public static final String ONBEFORE_CREATED    = "onBeforeCreated";
	public static final String ONAFTER_CREATED     = "onAfterCreated";
	public static final String ONBEFORE_READ       = "onBeforeRead";
	public static final String ONAFTER_READ        = "onAfterRead";
	public static final String ONBEFORE_UPDATED    = "onBeforeUpdated";
	public static final String ONAFTER_UPDATED     = "onAfterUpdated";
	public static final String ONBEFORE_DELETE     = "onBeforeDelete";
	public static final String ONAFTER_DELETE      = "onAfterDelete";
	
	public OClassTrigger() {
	}

	@Override
	public RESULT onRecordBeforeCreate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_CREATE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_CREATED, PROP_BEFORE_CREATE);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterCreate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_CREATE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_CREATED, PROP_AFTER_CREATE);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeRead(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_READ);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_READ, PROP_BEFORE_READ);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterRead(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_READ);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_READ, PROP_AFTER_READ);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_UPDATE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_UPDATED, PROP_BEFORE_UPDATE);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterUpdate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_UPDATE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_UPDATED, PROP_AFTER_UPDATE);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeDelete(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_DELETE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_DELETE, PROP_BEFORE_DELETE);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterDelete(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_DELETE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_DELETE, PROP_AFTER_DELETE);
		this.executeFunction(iDocument, func);
	}
	
	private OFunction checkClzAttribute(final ODocument iDocument, String attr, String prop) {
		OClass clz = iDocument.getSchemaClass();
		if(clz != null && clz.isSubClassOf(CLASSNAME)) {
			OFunction func = null;
			//OClass superClz = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(CLASSNAME);
			String fieldName = ((OClassImpl) clz).getCustom(attr);
			if(fieldName != null && fieldName.length() > 0) {
				func = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction(fieldName);
				if(func == null) { //check rid
					ODocument funcDoc = ODatabaseRecordThreadLocal.INSTANCE.get().load(new ORecordId(fieldName));
					if(funcDoc != null) {
						func = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction((String)funcDoc.field("name"));
					}
				}
			} else{
				ODocument funcDoc = iDocument.field(prop);
				if(funcDoc != null) {
					func = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction((String)funcDoc.field("name"));
				}
			}
			return func;
		}
		return null;
	}
	
	public RESULT onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && ODatabaseRecordThreadLocal.INSTANCE.get().getStatus() != STATUS.OPEN)
			return RESULT.RECORD_NOT_CHANGED;

		if (!(iRecord instanceof ODocument))
			return RESULT.RECORD_NOT_CHANGED;

		final ODocument document = (ODocument) iRecord;
		if(document.getSchemaClass() != null && document.getSchemaClass().isSubClassOf(CLASSNAME))
			return super.onTrigger(iType, iRecord);
		return RESULT.RECORD_NOT_CHANGED;
	}
	
	private RESULT executeFunction(final ODocument iDocument, final OFunction func) {
		if(func == null)
			return RESULT.RECORD_NOT_CHANGED;
//		String funcName = funcDoc.field("name");
//		if(funcName == null || funcName.length() == 0)
//			return RESULT.RECORD_NOT_CHANGED;
		
	    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
	    if (db != null && !(db instanceof ODatabaseRecordTx))
	      db = db.getUnderlying();
	    //final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(funcName);
	    final OScriptManager scriptManager = Orient.instance().getScriptManager();
	    final ScriptEngine scriptEngine = scriptManager.getEngine(func.getLanguage());
	    //final ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("javascript");	
	    final Bindings binding = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
	    //final Bindings binding = scriptEngine.createBindings();  

        for (OScriptInjection i : scriptManager.getInjections())
            i.bind(binding);
	    binding.put("doc", iDocument);
	    if(db != null)
	    	binding.put("db", new OScriptDocumentDatabaseWrapper((ODatabaseRecordTx) db));
	    //scriptEngine.setBindings(binding, ScriptContext.ENGINE_SCOPE);

	    boolean isSuccess = false;
	    try {
	    	if(func.getLanguage() == null)
	    		throw new OConfigurationException("Database function '" + func.getName() + "' has no language");
	    	final String funcStr = scriptManager.getFunctionDefinition(func);
	    	if(funcStr != null) {
	    		try{
	    			scriptEngine.eval(funcStr);
	    		} catch(ScriptException e) {
	    			scriptManager.getErrorMessage(e, funcStr);
	    		}
	    	}
	    	if (scriptEngine instanceof Invocable) {
	    		final Invocable invocableEngine = (Invocable) scriptEngine;
	    		Object[] EMPTY = new Object[0];
	    		isSuccess = (Boolean)invocableEngine.invokeFunction(func.getName(), EMPTY);
	    		//invocableEngine.invokeFunction(funcName, pargs);
	    	}
	    }  catch (ScriptException e) {
	        throw new OCommandScriptException("Error on execution of the script", func.getName(), e.getColumnNumber(), e);
	    } catch (NoSuchMethodException e) {
	      throw new OCommandScriptException("Error on execution of the script", func.getName(), 0, e);
	    } catch (OCommandScriptException e) {
	      // PASS THROUGH
	      throw e;

	    } finally {
	      scriptManager.unbind(binding);
	    }
	    if(isSuccess) {
	    	return RESULT.RECORD_CHANGED;
	    }
	    return RESULT.RECORD_NOT_CHANGED;
	} 
}
