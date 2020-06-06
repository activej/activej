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
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.ModuleBuilder;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is part of the provider method DSL, it allows you to build bindings and even a subset of
 * {@link io.activej.inject.binding.BindingGenerator generators} using methods declared in your modules.
 * <p>
 * Method return type and method {@link QualifierAnnotation qualifier annotation} form a {@link Key key}
 * that the resulting binding is bound to, its parameter types and their {@link QualifierAnnotation qualifier annotations} form
 * binding dependencies and its body forms the factory for the binding.
 * <p>
 * Note that provider methods are called using reflection, so if you need the best performance
 * for some frequently-entered scopes consider using less declarative but reflection-free
 * {@link ModuleBuilder#bind(Key)}  binding DSL}
 *
 * @see AbstractModule
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Provides {
}
