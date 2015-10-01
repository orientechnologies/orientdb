package com.orientechnologies.orient.core.sql.sequence;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.annotation.OExposedMethod;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/1/2015
 */
public class OSQLSequenceItem implements OSQLFilterItem {
    public static final String PREFIX = "#";
    public static final String DEFAULT_METHOD_NAME = "current".toLowerCase();

    private final String sequenceName;
    private final Method method;

    public OSQLSequenceItem(OBaseParser iCommand, String iWord) {
      if (!iWord.startsWith(PREFIX)) {
        throw new IllegalArgumentException("Sequence name must starts with #");
      }

      int separator = iWord.indexOf('.');
      if (separator == -1) {
        this.sequenceName = iWord.substring(1);
        this.method = getSequenceMethod(DEFAULT_METHOD_NAME);
      } else {
        this.sequenceName = iWord.substring(1, separator);
        this.method = getSequenceMethod(iWord.substring(separator + 1).toLowerCase());
      }
    }

    private Method getSequenceMethod(String methodName) {
      if (methodName.endsWith("()")) {
        methodName = methodName.substring(0, methodName.length() - 2);
      }

      try {
        Method method = OSequence.class.getMethod(methodName);
        if (!method.isAnnotationPresent(OExposedMethod.class)) {
          throw new NoSuchMethodException();
        }
        return method;
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Function '" + methodName + "' on sequence does not exists");
      }
    }

    @Override
    public Object getValue(OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContetx) {
      final ODatabaseDocumentInternal db = getDatabase();
      final OSequenceLibrary sequenceLibrary = db.getMetadata().getSequenceLibrary();

      OSequence sequence = sequenceLibrary.getSequence(this.sequenceName);
      if (sequence == null) {
        return null;
      }

      try {
        return this.method.invoke(sequence);
      } catch (IllegalAccessException e) {
      throw OException.wrapException(new OCommandExecutionException("Failed executing method '" + this.method.getName()
          + "' on sequence '" + this.sequenceName + "'"), e);
      } catch (InvocationTargetException e) {
      throw OException.wrapException(new OCommandExecutionException("Failed executing method '" + this.method.getName()
          + "' on sequence '" + this.sequenceName + "'"), e);
      }
    }

    private static ODatabaseDocumentInternal getDatabase() {
        return ODatabaseRecordThreadLocal.INSTANCE.get();
    }
}
