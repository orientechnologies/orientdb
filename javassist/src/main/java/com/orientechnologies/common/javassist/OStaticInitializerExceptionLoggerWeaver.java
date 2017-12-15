package com.orientechnologies.common.javassist;

import de.icongmbh.oss.maven.plugin.javassist.ClassTransformer;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.NotFoundException;

/**
 * Adds loggers about {@link RuntimeException}s and {@link Error}s to if they happen in static initializer.
 * Also if we detect {@link Error} happens inside of JVM we switch all storages to read-only mode.
 */
public class OStaticInitializerExceptionLoggerWeaver extends ClassTransformer {
  @Override
  protected boolean shouldTransform(CtClass candidateClass) throws Exception {
    final String className = candidateClass.getName();

    if (className.equals("com.orientechnologies.common.log.OLogManager")) {
      return false;
    }
    return className.startsWith("com.orientechnologies") && !className.startsWith("com.orientechnologies.common.javassist");
  }

  @Override
  protected void applyTransformations(CtClass ctClass) throws Exception {
    final CtConstructor staticInit = ctClass.getClassInitializer();
    final ClassPool classPool = ctClass.getClassPool();
    if (staticInit != null) {
      try {
        getLogger().debug("Wave static init of " + ctClass.getName());

        final CtClass runtimeType = classPool.get("java.lang.RuntimeException");
        final CtClass errorType = classPool.get("java.lang.Error");

        staticInit.addCatch("{"
            + "com.orientechnologies.common.log.OLogManager.instance().errorNoDb(null, \"Error in static initializer\", $e, new String[0]);"
            + "throw $e;" + "}", runtimeType);
        staticInit.addCatch("{"
            + "com.orientechnologies.common.log.OLogManager.instance().errorNoDb(null, \"Error in static initializer\", $e, new String[0]);"
            + "throw $e;" + "}", errorType);
      } catch (NotFoundException | CannotCompileException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
