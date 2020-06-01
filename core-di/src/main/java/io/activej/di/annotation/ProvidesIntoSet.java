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

package io.activej.di.annotation;

import io.activej.di.binding.Multibinder;
import io.activej.di.module.AbstractModule;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a built-in shortcut for {@link Provides provider method} that provides its result as a singleton set
 * and adds a {@link Multibinder#toSet() set multibinder} for the provided set key to the module.
 *
 * @see AbstractModule
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface ProvidesIntoSet {
}
