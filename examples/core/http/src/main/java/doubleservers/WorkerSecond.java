package doubleservers;

import io.activej.inject.annotation.ScopeAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@ScopeAnnotation
@Target(METHOD)
@Retention(RUNTIME)
public @interface WorkerSecond {
}
