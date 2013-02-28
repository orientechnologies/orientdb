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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptDocumentDatabaseWrapper;
import com.orientechnologies.orient.core.command.script.OScriptInjection;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class OClassTrigger extends ODocumentHookAbstract {
	public static final String CLASSNAME = "OTriggered";
	
	//Class Level Trigger (class custom attribute)
	public static final String ONBEFORE_CREATED    = "onBeforeCreate";
	public static final String ONAFTER_CREATED     = "onAfterCreate";
	public static final String ONBEFORE_READ       = "onBeforeRead";
	public static final String ONAFTER_READ        = "onAfterRead";
	public static final String ONBEFORE_UPDATED    = "onBeforeUpdate";
	public static final String ONAFTER_UPDATED     = "onAfterUpdate";
	public static final String ONBEFORE_DELETE     = "onBeforeDelete";
	public static final String ONAFTER_DELETE      = "onAfterDelete";
	
	//Record Level Trigger (property name)
	public static final String PROP_BEFORE_CREATE  = ONBEFORE_CREATED; 
	public static final String PROP_AFTER_CREATE   = ONAFTER_CREATED;
	public static final String PROP_BEFORE_READ    = ONBEFORE_READ;
	public static final String PROP_AFTER_READ     = ONAFTER_READ;
	public static final String PROP_BEFORE_UPDATE  = ONBEFORE_UPDATED;
	public static final String PROP_AFTER_UPDATE   = ONAFTER_UPDATED;
	public static final String PROP_BEFORE_DELETE  = ONBEFORE_DELETE;
	public static final String PROP_AFTER_DELETE   = ONAFTER_DELETE;
	
	public OClassTrigger() {
	}

	@Override
	public RESULT onRecordBeforeCreate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_CREATE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_CREATED);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterCreate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_CREATE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_CREATED);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeRead(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_READ);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_READ);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterRead(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_READ);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_READ);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_UPDATE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_UPDATED);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterUpdate(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_UPDATE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_UPDATED);
		this.executeFunction(iDocument, func);
	}

	@Override
	public RESULT onRecordBeforeDelete(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_BEFORE_DELETE);
		OFunction func = this.checkClzAttribute(iDocument, ONBEFORE_DELETE);
		return this.executeFunction(iDocument, func);
	}

	@Override
	public void onRecordAfterDelete(final ODocument iDocument) {
		//ODocument funcDoc = iDocument.field(PROP_AFTER_DELETE);
		OFunction func = this.checkClzAttribute(iDocument, ONAFTER_DELETE);
		this.executeFunction(iDocument, func);
	}
	
	private OFunction checkClzAttribute(final ODocument iDocument, String attr) {
		OClass clz = iDocument.getSchemaClass();
		if(clz != null && clz.isSubClassOf(CLASSNAME)) {
			OFunction func = null;
			String fieldName = ((OClassImpl) clz).getCustom(attr);
			OClass superClz = clz.getSuperClass();
			while(fieldName == null || fieldName.length() == 0 ) {
				if(superClz == null || superClz.getName().equals(CLASSNAME))
					break;
				fieldName = ((OClassImpl) superClz).getCustom(attr);
				superClz = superClz.getSuperClass();
			}
			if(fieldName != null && fieldName.length() > 0) {
				func = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction(fieldName);
				if(func == null) { //check if it is rid	
					if (OStringSerializerHelper.contains(fieldName, ORID.SEPARATOR)) {
						try{
							ODocument funcDoc = ODatabaseRecordThreadLocal.INSTANCE.get().load(new ORecordId(fieldName));
							if(funcDoc != null) {
								func = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction((String)funcDoc.field("name"));
							}
						}catch(Exception ex) {
							OLogManager.instance().error(this, "illegal record id : ", ex.getMessage());
						}
					}
				}
			} else{
				ODocument funcDoc = iDocument.field(attr);
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

	    String result = null;
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
	    		result = (String)invocableEngine.invokeFunction(func.getName(), EMPTY);
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
	    if(result == null) {
	    	return RESULT.RECORD_NOT_CHANGED;
	    }
	    return RESULT.valueOf(result);//result;
	} 
}
