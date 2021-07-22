package adder;

import io.activej.inject.annotation.QualifierAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@QualifierAnnotation
@Target({PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface ServerId {
}
