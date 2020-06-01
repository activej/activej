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

package io.activej.datastream.processor;

import io.activej.datastream.StreamDataAcceptor;

public abstract class AbstractTransducer<I, O, A> implements Transducer<I, O, A> {
	@Override
	public A onStarted(StreamDataAcceptor<O> output) {
		return null;
	}

	@Override
	public void onEndOfStream(StreamDataAcceptor<O> output, A accumulator) {
	}

	@Override
	public boolean isOneToMany() {
		return false;
	}
}
