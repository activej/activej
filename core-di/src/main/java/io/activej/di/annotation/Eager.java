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

import io.activej.di.Injector;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A binding marked as eager would be called for an instance of its object immediately upon injector creation.
 * Next calls if {@link Injector#getInstance getInstance} would only retrieve the cached instance.
 * <p>
 * Bindings cannot be both eager and {@link Transient transient} at the same time.
 */
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Eager {
}
