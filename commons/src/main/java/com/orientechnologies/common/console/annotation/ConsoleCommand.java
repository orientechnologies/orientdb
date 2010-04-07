package com.orientechnologies.common.console.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConsoleCommand {
	String[] aliases() default {};

	String description() default "";

	boolean splitInWords() default true;
}
