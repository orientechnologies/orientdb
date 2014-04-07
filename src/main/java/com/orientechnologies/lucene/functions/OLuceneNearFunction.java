package com.orientechnologies.lucene.functions;

import java.util.*;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionMultiValueAbstract;

/**
 * Created by enricorisa on 02/04/14.
 */
public class OLuceneNearFunction extends OSQLFunctionMultiValueAbstract<Set<Object>> {

  public static final String NAME = "near";

  public OLuceneNearFunction() {
    super(NAME, 5, 6);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    String clazz = (String) iParams[0];
    String latField = (String) iParams[1];
    String lngField = (String) iParams[2];
    ODatabaseRecord databaseRecord = ODatabaseRecordThreadLocal.INSTANCE.get();
    Set<OIndex<?>> indexes = databaseRecord.getMetadata().getSchema().getClass(clazz).getInvolvedIndexes(latField, lngField);
    for (OIndex i : indexes) {
      if (OClass.INDEX_TYPE.SPATIAL.toString().equals(i.getInternal().getType())) {
        List<Object> params = new ArrayList<Object>();
        params.add(iParams[3]);
        params.add(iParams[4]);
        double distance = iParams.length > 5 ? ((Number) iParams[5]).doubleValue() : 0;
        Object ret = i.get(new OSpatialCompositeKey(params).setMaxDistance(distance));
        if (ret instanceof Collection) {
          if (context == null)
            context = new HashSet<Object>();
          context.addAll((Collection<?>) ret);
        }
        return ret;
      }
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "near(<class>,<field-x>,<field-y>,<x-value>,<y-value>[,<maxDistance>])";
  }
}
