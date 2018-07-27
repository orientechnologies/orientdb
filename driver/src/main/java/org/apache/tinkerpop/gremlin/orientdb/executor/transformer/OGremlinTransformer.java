package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;

import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Enrico Risa on 14/06/2018.
 */
public class OGremlinTransformer implements OScriptTransformer {

  OScriptTransformer transformer;

  public OGremlinTransformer(OScriptTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  public OResultSet toResultSet(Object value) {
    return this.transformer.toResultSet(value);
  }

  @Override
  public OResult toResult(Object value) {

    if (value instanceof Iterable) {
      Spliterator spliterator = ((Iterable) value).spliterator();
      Object collect = StreamSupport.stream(spliterator, false).map((e) -> {
        if (e instanceof OrientElement) {
          return this.transformer.toResult(e);
        } else {
          return e;
        }
      }).collect(Collectors.toList());

      return this.transformer.toResult(collect);
    }else {
      return this.transformer.toResult(value);
    }

  }

  @Override
  public boolean doesHandleResult(Object value) {
    return this.transformer.doesHandleResult(value);
  }

  @Override
  public void registerResultTransformer(Class clazz, OResultTransformer resultTransformer) {
    this.transformer.registerResultTransformer(clazz, resultTransformer);
  }

  @Override
  public void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer) {
    this.transformer.registerResultSetTransformer(clazz,transformer);
  }
}
