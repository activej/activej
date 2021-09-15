/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.inject.annotation;

import io.activej.inject.Key;
import io.activej.inject.Qualifiers;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a marker meta-annotation that declares an annotation as the one that
 * can be used as a {@link Key key} qualifier.
 * <p>
 * Creating a custom stateless qualifier annotation is as easy as creating your own annotation with no parameters
 * and annotating it with {@link QualifierAnnotation}. That way, an actual class of your stateless qualifier annotation
 * will be used as a qualifier.
 * <p>
 * If you want to create a stateful annotation, you should also annotate it with {@link QualifierAnnotation}.
 * Additionally, you need to get an instance of it with compatible equals method (you can use {@link Qualifiers}.NamedImpl class as an example)
 * After that, you can use your annotation in our DSL and then make keys programmatically with created qualifier instances.
 */
@Target(ANNOTATION_TYPE)
@Retention(RUNTIME)
public @interface QualifierAnnotation {
}
