package com.orientechnologies.orient.core.command.script;

import java.util.Map;

import sun.org.mozilla.javascript.internal.Context;
import sun.org.mozilla.javascript.internal.Scriptable;

@SuppressWarnings("restriction")
public class OJSMap extends sun.org.mozilla.javascript.internal.NativeObject {
  private static final long   serialVersionUID = 1L;

  private Map<Object, Object> map;

  public OJSMap() {
  }

  public OJSMap(Map<Object, Object> iArgs) {
    map = iArgs;
    for (Entry<Object, Object> a : iArgs.entrySet())
      defineProperty(a.getKey().toString(), a.getValue(), sun.org.mozilla.javascript.internal.NativeObject.CONST);
  }

  @Override
  public Object get(String name, sun.org.mozilla.javascript.internal.Scriptable start) {
    if ("size".equals(name))
      return new sun.org.mozilla.javascript.internal.Function() {

        @Override
        public Object call(sun.org.mozilla.javascript.internal.Context cx, sun.org.mozilla.javascript.internal.Scriptable scope,
            sun.org.mozilla.javascript.internal.Scriptable thisObj, Object[] args) {
          return map.size();
        }

        @Override
        public void delete(String arg0) {
        }

        @Override
        public void delete(int arg0) {

        }

        @Override
        public Object get(String arg0, Scriptable arg1) {

          return null;
        }

        @Override
        public Object get(int arg0, Scriptable arg1) {

          return null;
        }

        @Override
        public String getClassName() {

          return null;
        }

        @Override
        public Object getDefaultValue(Class<?> arg0) {

          return null;
        }

        @Override
        public Object[] getIds() {

          return null;
        }

        @Override
        public Scriptable getParentScope() {

          return null;
        }

        @Override
        public Scriptable getPrototype() {

          return null;
        }

        @Override
        public boolean has(String arg0, Scriptable arg1) {

          return false;
        }

        @Override
        public boolean has(int arg0, Scriptable arg1) {

          return false;
        }

        @Override
        public boolean hasInstance(Scriptable arg0) {

          return false;
        }

        @Override
        public void put(String arg0, Scriptable arg1, Object arg2) {

        }

        @Override
        public void put(int arg0, Scriptable arg1, Object arg2) {

        }

        @Override
        public void setParentScope(Scriptable arg0) {

        }

        @Override
        public void setPrototype(Scriptable arg0) {

        }

        @Override
        public Scriptable construct(Context arg0, Scriptable arg1, Object[] arg2) {

          return null;
        }

      };
    return super.get(name, start);
  }

  @Override
  public Object getDefaultValue(Class<?> arg0) {
    return map.toString();
  }
}
