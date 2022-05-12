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

package io.activej.ot;

import java.util.List;

public class TransformResult<D> {
	public enum ConflictResolution {LEFT, RIGHT}

	public final ConflictResolution resolution;
	public final List<D> left;
	public final List<D> right;

	private TransformResult(ConflictResolution resolution, List<D> left, List<D> right) {
		this.resolution = resolution;
		this.left = left;
		this.right = right;
	}

	public static <D> TransformResult<D> conflict(ConflictResolution conflict) {
		return new TransformResult<>(conflict, null, null);
	}

	public static <D> TransformResult<D> empty() {
		return new TransformResult<>(null, List.of(), List.of());
	}

	@SuppressWarnings("unchecked")
	public static <D> TransformResult<D> of(List<? extends D> left, List<? extends D> right) {
		return new TransformResult<>(null, (List<D>) left, (List<D>) right);
	}

	@SuppressWarnings("unchecked")
	public static <D> TransformResult<D> of(ConflictResolution conflict, List<? extends D> left, List<? extends D> right) {
		return new TransformResult<>(conflict, (List<D>) left, (List<D>) right);
	}

	public static <D> TransformResult<D> of(D left, D right) {
		return of(List.of(left), List.of(right));
	}

	public static <D> TransformResult<D> left(D left) {
		return of(List.of(left), List.of());
	}

	public static <D> TransformResult<D> left(List<? extends D> left) {
		return of(left, List.of());
	}

	public static <D> TransformResult<D> right(D right) {
		return of(List.of(), List.of(right));
	}

	public static <D> TransformResult<D> right(List<? extends D> right) {
		return of(List.of(), right);
	}

	public boolean hasConflict() {
		return resolution != null;
	}

	@Override
	public String toString() {
		return "{" +
				"left=" + left +
				", right=" + right +
				'}';
	}
}
