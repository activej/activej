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

import io.activej.inject.util.ReflectionUtils;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is part of the injection DSL, it allows you to generate bindings
 * using object constructors or static factory methods.
 * By default, there is an implicit binding present, that can generate missing bindings for injectable classes.
 * <p>
 * This annotation can be put on the class itself - its default constructor is used for binding generation and must exist,
 * on class constructor that will be used, or on factory method (static method with return type of that class).
 * <p>
 * When a binding is generated, class methods and fields are scanned for the {@link Inject} annotations and added as the binding dependencies -
 * on instance creation fields will be <i>injected</i> and methods will be called with their parameters <i>injected</i> and return ignored.
 * Name annotations on fields and method parameters will be considered.
 *
 * @see ReflectionUtils#generateImplicitBinding
 */
@Target({FIELD, CONSTRUCTOR, METHOD, TYPE})
@Retention(RUNTIME)
public @interface Inject {
}
