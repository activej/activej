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

package io.activej.trigger;

import io.activej.inject.Key;

import java.util.function.Function;

@SuppressWarnings("unused")
public interface TriggersModuleSettings {
	TriggersModuleSettings withNaming(Function<Key<?>, String> keyToString);

	<T> TriggersModuleSettings with(Class<T> type, Severity severity, String name, Function<T, TriggerResult> triggerFunction);

	<T> TriggersModuleSettings with(Key<T> key, Severity severity, String name, Function<T, TriggerResult> triggerFunction);
}
