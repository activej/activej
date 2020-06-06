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

package io.activej.inject;

/**
 * This is a function which can inject instances into {@link io.activej.inject.annotation.Inject}
 * fields and methods of some <b>already existing object</b>.
 * This is so-called 'post-injections' since such injections are not part of object creation.
 * <p>
 * It has a {@link io.activej.inject.module.DefaultModule default generator} and
 * can only be obtained by depending on it and then requesting it from the {@link Injector injector}.
 */
public interface InstanceInjector<T> {
	Key<T> key();

	void injectInto(T existingInstance);
}
