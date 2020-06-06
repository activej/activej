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

package io.activej.inject.util;

/**
 * These are just set of functional interfaces to be used by the DSL
 */
public final class Constructors {

	@FunctionalInterface
	public interface Constructor0<R> {
		R create();
	}

	@FunctionalInterface
	public interface Constructor1<P1, R> {
		R create(P1 arg1);
	}

	@FunctionalInterface
	public interface Constructor2<P1, P2, R> {
		R create(P1 arg1, P2 arg2);
	}

	@FunctionalInterface
	public interface Constructor3<P1, P2, P3, R> {
		R create(P1 arg1, P2 arg2, P3 arg3);
	}

	@FunctionalInterface
	public interface Constructor4<P1, P2, P3, P4, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4);
	}

	@FunctionalInterface
	public interface Constructor5<P1, P2, P3, P4, P5, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4, P5 arg5);
	}

	@FunctionalInterface
	public interface Constructor6<P1, P2, P3, P4, P5, P6, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4, P5 arg5, P6 arg6);
	}

	@FunctionalInterface
	public interface ConstructorN<R> {
		R create(Object... args);
	}
}
