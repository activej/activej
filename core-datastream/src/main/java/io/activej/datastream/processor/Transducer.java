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

import java.util.function.Function;
import java.util.function.Predicate;

public interface Transducer<I, O, A> {
    A onStarted(StreamDataAcceptor<O> output);

    void onItem(StreamDataAcceptor<O> output, I item, A accumulator);

    void onEndOfStream(StreamDataAcceptor<O> output, A accumulator);

	boolean isOneToMany();

    static <T> Transducer<T, T, Void> filter(Predicate<? super T> predicate) {
        return new AbstractTransducer<T, T, Void>() {
            @Override
            public void onItem(StreamDataAcceptor<T> output, T item, Void accumulator) {
                if (predicate.test(item)) {
                    output.accept(item);
                }
            }
        };
    }

    static <I, O> Transducer<I, O, Void> mapper(Function<? super I, ? extends O> fn) {
        return new AbstractTransducer<I, O, Void>() {
            @Override
            public void onItem(StreamDataAcceptor<O> output, I item, Void accumulator) {
				output.accept(fn.apply(item));
            }
        };
    }

}
