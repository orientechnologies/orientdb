package com.orientechnologies.common.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Reflection utility methods that keep caches to speed up subsequent reflection calls.
 *
 * @author Wouter de Vaal
 */
public final class OReflectionUtils {

    private static Map<Class<?>, Field[]> declaredFieldsCache = new HashMap<Class<?>, Field[]>();
    private static Map<Class<?>, Annotation[]> declaredAnnotationsCache = new HashMap<Class<?>, Annotation[]>();
    private static Map<Class<?>, Class<?>> enclosingClassCache = new HashMap<Class<?>, Class<?>>();

    private OReflectionUtils() {
    }

    /**
     * Retrieves and returns all declared {@link Field}s from the given class.
     *
     * @param clazz The class type.
     * @return All declared {@link Field}s for the object instance.
     */
    public static Field[] getDeclaredFields(Class<?> clazz) {
        //fields won't change during the jvm lifetime and so they are cached
        Field[] fields = declaredFieldsCache.get(clazz);
        if (fields == null) {
            fields = clazz.getDeclaredFields();
            declaredFieldsCache.put(clazz, fields);
        }
        return fields;
    }

    /**
     * Retrieves and returns all declared {@link Field}s from the given class.
     *
     * @param clazz The class type.
     * @return All declared {@link Field}s for the object instance.
     */
    public static Annotation[] getDeclaredAnnotations(Class<?> clazz) {
        //annotations won't change during the jvm lifetime and so they are cached
        Annotation[] annotations = declaredAnnotationsCache.get(clazz);
        if (annotations == null) {
            annotations = clazz.getDeclaredAnnotations();
            declaredAnnotationsCache.put(clazz, annotations);
        }
        return annotations;
    }

    public static Class<?> getEnclosingClass(Class<?> clazz) {
        Class<?> enclosingClass = enclosingClassCache.get(clazz);
        if (enclosingClass == null) {
            enclosingClass = clazz.getEnclosingClass();
            enclosingClassCache.put(clazz, enclosingClass);
        }
        return enclosingClass;
    }

}
