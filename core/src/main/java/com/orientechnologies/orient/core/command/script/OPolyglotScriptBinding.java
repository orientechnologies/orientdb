package com.orientechnologies.orient.core.command.script;

import java.util.*;
import javax.script.Bindings;
import org.graalvm.polyglot.Value;

/**
 * Wraps a GraalVM value as a JSR223 Bindings for compatibility.
 *
 * @author Luca Garulli
 */
public class OPolyglotScriptBinding implements Bindings {
  private final Value context;

  public OPolyglotScriptBinding(final Value value) {
    this.context = value;
  }

  @Override
  public Object put(String name, Object value) {
    final Value old = context.getMember(name);
    context.putMember(name, value);
    return old;
  }

  @Override
  public void putAll(Map<? extends String, ?> toMerge) {
    for (Entry<? extends String, ?> entry : toMerge.entrySet())
      context.putMember(entry.getKey(), entry.getValue());
  }

  @Override
  public void clear() {
    for (String name : context.getMemberKeys()) context.removeMember(name);
  }

  @Override
  public Set<String> keySet() {
    return context.getMemberKeys();
  }

  @Override
  public Collection<Object> values() {
    List<Object> result = new ArrayList<>();
    for (String name : context.getMemberKeys()) result.add(context.getMember(name));
    return result;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return null;
  }

  @Override
  public int size() {
    return context.getMemberKeys().size();
  }

  @Override
  public boolean isEmpty() {
    return !context.hasMembers();
  }

  @Override
  public boolean containsKey(final Object key) {
    return context.hasMember(key.toString());
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public Object get(Object key) {
    return context.getMember(key.toString());
  }

  @Override
  public Object remove(Object key) {
    final Value old = context.getMember(key.toString());
    context.removeMember(key.toString());
    return old;
  }
}
