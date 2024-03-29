package io.activej.common.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Annotates API types that are explicitly exposed for serialization,
 * introspection or other reasons
 */
@Target(TYPE)
@Retention(SOURCE)
public @interface ExposedInternals {
	String reason() default "";
}
