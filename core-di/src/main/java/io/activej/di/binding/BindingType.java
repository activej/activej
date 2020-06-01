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

package io.activej.di.binding;

/**
 * Bindings can be transient, eager, or none of the previous.
 * This enum represents that.
 */
public enum BindingType {
	/**
	 * Such binding has no special properties and behaves like a lazy singleton, this is the default
	 */
	REGULAR,
	/**
	 * Such binding has no cache slot and each time <code>getInstance</code> is called, a new instance of the object is created
	 */
	TRANSIENT,
	/**
	 * Such binding behaves like eager singleton - instance is created and placed in the cache at the moment of injector creation
	 */
	EAGER
}
