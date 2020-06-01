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

import java.util.function.Supplier;

public final class Trigger {
	private final Severity severity;
	private final String component;
	private final String name;
	private final Supplier<TriggerResult> triggerFunction;

	Trigger(Severity severity, String component, String name, Supplier<TriggerResult> triggerFunction) {
		this.severity = severity;
		this.component = component;
		this.name = name;
		this.triggerFunction = triggerFunction;
	}

	public static Trigger of(Severity severity, String component, String name, Supplier<TriggerResult> triggerFunction) {
		return new Trigger(severity, component, name, triggerFunction);
	}

	public Severity getSeverity() {
		return severity;
	}

	public String getComponent() {
		return component;
	}

	public String getName() {
		return name;
	}

	public Supplier<TriggerResult> getTriggerFunction() {
		return triggerFunction;
	}

	@Override
	public String toString() {
		return severity + " : " + component + " : " + name;
	}
}
