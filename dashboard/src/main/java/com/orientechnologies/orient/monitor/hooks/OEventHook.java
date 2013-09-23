package com.orientechnologies.orient.monitor.hooks;

import java.util.List;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OEventHook extends ORecordHookAbstract {

	@Override
	public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onRecordAfterCreate(ORecord<?> iiRecord) {

		ODocument doc = (ODocument) iiRecord;

		
		final List<ODocument> triggers = doc.getDatabase().query(new OSQLSynchQuery<Object>("select from Event where clazz = '" + doc.getClassName()+"'" ));
		
		for (ODocument oDocument : triggers) {
			
			ODocument condition = oDocument.field("condition");
			if(condition!=null){
				OFunction docFunction = new OFunction(condition);
				Object ret =  docFunction.execute(doc);
				System.out.println(ret);
			}
		}
		
	}
}
