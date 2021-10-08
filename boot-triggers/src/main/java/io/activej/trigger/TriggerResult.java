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

import io.activej.common.initializer.WithInitializer;
import io.activej.jmx.stats.ExceptionStats;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkState;
import static io.activej.jmx.stats.MBeanFormat.formatExceptionMultiline;

public final class TriggerResult implements WithInitializer<TriggerResult> {
	private static final TriggerResult NONE = new TriggerResult(0, null, null);

	private final long timestamp;
	private final Throwable throwable;
	private final Object value;
	private final int count;

	TriggerResult(long timestamp, @Nullable Throwable e, @Nullable Object value, int count) {
		this.timestamp = timestamp;
		this.throwable = e;
		this.value = value;
		this.count = count;
	}

	TriggerResult(long timestamp, @Nullable Throwable e, @Nullable Object context) {
		this(timestamp, e, context, 1);
	}

	public static TriggerResult none() {
		return NONE;
	}

	public static TriggerResult create() {
		return new TriggerResult(0L, null, null);
	}

	public static TriggerResult create(long timestamp, Throwable e, int count) {
		return new TriggerResult(timestamp, e, null, count);
	}

	public static TriggerResult create(long timestamp, @Nullable Throwable e, @Nullable Object value) {
		return new TriggerResult(timestamp, e, value);
	}

	public static TriggerResult create(long timestamp, Throwable e, Object value, int count) {
		return new TriggerResult(timestamp, e, value, count);
	}

	public static TriggerResult create(Instant instant, @Nullable Throwable e, @Nullable Object value) {
		return create(instant.toEpochMilli(), e, value);
	}

	public static TriggerResult create(Instant instant, Throwable e, Object value, int count) {
		return create(instant.toEpochMilli(), e, value, count);
	}

	public static TriggerResult ofTimestamp(long timestamp) {
		return timestamp != 0L ?
				new TriggerResult(timestamp, null, null) : NONE;
	}

	public static TriggerResult ofTimestamp(long timestamp, boolean condition) {
		return timestamp != 0L && condition ?
				new TriggerResult(timestamp, null, null) : NONE;
	}

	public static TriggerResult ofInstant(@Nullable Instant instant) {
		return instant != null ?
				create(instant, null, null) : NONE;
	}

	public static TriggerResult ofInstant(Instant instant, boolean condition) {
		return instant != null && condition ?
				create(instant, null, null) : NONE;
	}

	public static TriggerResult ofError(Throwable e) {
		return e != null ?
				new TriggerResult(0L, e, null) : NONE;
	}

	public static TriggerResult ofError(Throwable e, long timestamp) {
		return e != null ?
				new TriggerResult(timestamp, e, null) : NONE;
	}

	public static TriggerResult ofError(Throwable e, Instant instant) {
		return e != null ?
				create(instant.toEpochMilli(), e, null) : NONE;
	}

	public static TriggerResult ofError(ExceptionStats exceptionStats) {
		Throwable lastException = exceptionStats.getLastException();
		return lastException != null ?
				create(exceptionStats.getLastTime() != null ?
								exceptionStats.getLastTime().toEpochMilli() :
								0,
						lastException, exceptionStats.getTotal()) :
				NONE;
	}

	public static TriggerResult ofValue(Object value) {
		return value != null ?
				new TriggerResult(0L, null, value) : NONE;
	}

	public static <T> TriggerResult ofValue(T value, Predicate<T> predicate) {
		return value != null && predicate.test(value) ?
				new TriggerResult(0L, null, value) : NONE;
	}

	public static <T> TriggerResult ofValue(T value, boolean condition) {
		return value != null && condition ?
				new TriggerResult(0L, null, value) : NONE;
	}

	public static <T> TriggerResult ofValue(Supplier<T> supplier, boolean condition) {
		return condition ? ofValue(supplier.get()) : NONE;
	}

	public TriggerResult withValue(Object value) {
		return isPresent() ? new TriggerResult(timestamp, throwable, value) : NONE;
	}

	public TriggerResult withValue(Supplier<?> value) {
		return isPresent() ? new TriggerResult(timestamp, throwable, value.get()) : NONE;
	}

	public TriggerResult withCount(int count) {
		return isPresent() ? new TriggerResult(timestamp, throwable, value, count) : NONE;
	}

	public TriggerResult withCount(IntSupplier count) {
		return isPresent() ? new TriggerResult(timestamp, throwable, value, count.getAsInt()) : NONE;
	}

	public TriggerResult when(boolean condition) {
		return isPresent() && condition ? this : NONE;
	}

	public TriggerResult when(BooleanSupplier conditionSupplier) {
		return isPresent() && conditionSupplier.getAsBoolean() ? this : NONE;
	}

	@SuppressWarnings("unchecked")
	public <T> TriggerResult whenValue(Predicate<T> valuePredicate) {
		return isPresent() && hasValue() && valuePredicate.test((T) value) ? this : NONE;
	}

	public boolean isPresent() {
		return this != NONE;
	}

	public boolean hasTimestamp() {
		return timestamp != 0L;
	}

	public boolean hasThrowable() {
		return throwable != null;
	}

	public boolean hasValue() {
		return value != null;
	}

	public long getTimestamp() {
		checkState(isPresent(), "Trigger is not present");
		return timestamp;
	}

	public @Nullable Instant getInstant() {
		checkState(isPresent(), "Trigger is not present");
		return hasTimestamp() ? Instant.ofEpochMilli(timestamp) : null;
	}

	public @Nullable Throwable getThrowable() {
		checkState(isPresent(), "Trigger is not present");
		return throwable;
	}

	public @Nullable Object getValue() {
		checkState(isPresent(), "Trigger is not present");
		return value;
	}

	public int getCount() {
		checkState(isPresent(), "Trigger is not present");
		return count;
	}

	@Override
	public String toString() {
		return "@" + Instant.ofEpochMilli(timestamp) +
				(count != 1 ? " #" + count : "") +
				(value != null ? " : " + value : "") +
				(throwable != null ? "\n" + formatExceptionMultiline(throwable) : "");
	}
}
