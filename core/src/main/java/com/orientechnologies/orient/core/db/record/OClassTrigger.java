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
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OClassTrigger extends ODocumentHookAbstract {
  public static final String CLASSNAME = "OTriggered";
	
	public static final String PROP_BEFORE_CREATE = "beforeCreate"; 
	public static final String PROP_AFTER_CREATE = "afterCreate";
	public static final String PROP_BEFORE_READ = "beforeRead";
	public static final String PROP_AFTER_READ = "afterRead";
	public static final String PROP_BEFORE_UPDATE = "beforeUpdate";
	public static final String PROP_AFTER_UPDATE = "afterUpdate";
	public static final String PROP_BEFORE_DELETE = "beforeDelete";
	public static final String PROP_AFTER_DELETE = "afterDelete";
    
	public OClassTrigger() {
	}

	@Override
	public RESULT onRecordBeforeCreate(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_BEFORE_CREATE);
		return this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public void onRecordAfterCreate(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_AFTER_CREATE);
		this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public RESULT onRecordBeforeRead(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_BEFORE_READ);
		return this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public void onRecordAfterRead(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_AFTER_READ);
		this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_BEFORE_UPDATE);
		return this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public void onRecordAfterUpdate(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_AFTER_UPDATE);
		this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public RESULT onRecordBeforeDelete(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_BEFORE_DELETE);
		return this.executeFunction(iDocument, funcDoc);
	}

	@Override
	public void onRecordAfterDelete(final ODocument iDocument) {
		ODocument funcDoc = iDocument.field(PROP_AFTER_DELETE);
		this.executeFunction(iDocument, funcDoc);
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
	
	private RESULT executeFunction(final ODocument iDocument, final ODocument funcDoc) {
		if(funcDoc == null)
			return RESULT.RECORD_NOT_CHANGED;
		String funcName = funcDoc.field("name");
		if(funcName == null || funcName.length() == 0)
			return RESULT.RECORD_NOT_CHANGED;
		
	    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
	    if (db != null && !(db instanceof ODatabaseRecordTx))
	      db = db.getUnderlying();
	    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(funcName);
	    final OScriptManager scriptManager = Orient.instance().getScriptManager();
	    final ScriptEngine scriptEngine = scriptManager.getEngine(f.getLanguage());
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
	    	if(f.getLanguage() == null)
	    		throw new OConfigurationException("Database function '" + funcName + "' has no language");
	    	final String funcStr = scriptManager.getFunctionDefinition(f);
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
	    		isSuccess = (Boolean)invocableEngine.invokeFunction(funcName, EMPTY);
	    		//invocableEngine.invokeFunction(funcName, pargs);
	    	}
	    }  catch (ScriptException e) {
	        throw new OCommandScriptException("Error on execution of the script", funcName, e.getColumnNumber(), e);
	    } catch (NoSuchMethodException e) {
	      throw new OCommandScriptException("Error on execution of the script", funcName, 0, e);
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
