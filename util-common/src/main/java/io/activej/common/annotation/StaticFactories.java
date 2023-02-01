package io.activej.common.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Annotates classes that consist of static factories for some interface or class
 */
@Target(TYPE)
@Retention(SOURCE)
public @interface StaticFactories {
	Class<?> value();
}
