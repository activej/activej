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

package io.activej.ot.system;

import io.activej.common.initializer.WithInitializer;
import io.activej.ot.TransformResult;
import io.activej.ot.TransformResult.ConflictResolution;
import io.activej.ot.exception.TransformException;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.activej.common.Utils.concat;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public final class OTSystemImpl<D> implements OTSystem<D>, WithInitializer<OTSystemImpl<D>> {
	@FunctionalInterface
	public interface TransformFunction<OP, L extends OP, R extends OP> {
		TransformResult<? extends OP> transform(L left, R right) throws TransformException;
	}

	@FunctionalInterface
	public interface SquashFunction<OP, OP1 extends OP, OP2 extends OP> {
		@Nullable OP trySquash(OP1 op1, OP2 op2);
	}

	@FunctionalInterface
	public interface InvertFunction<OP, OP2 extends OP> {
		List<? extends OP> invert(OP2 op);
	}

	@FunctionalInterface
	public interface EmptyPredicate<OP> {
		boolean isEmpty(OP op);
	}

	private record KeyPair<O>(Class<? extends O> left, Class<? extends O> right) {}

	private final Map<KeyPair<D>, TransformFunction<D, ?, ?>> transformers = new HashMap<>();
	private final Map<KeyPair<D>, SquashFunction<D, ?, ?>> squashers = new HashMap<>();
	private final Map<Class<? extends D>, InvertFunction<D, ? extends D>> inverters = new HashMap<>();
	private final Map<Class<? extends D>, EmptyPredicate<? extends D>> emptyPredicates = new HashMap<>();

	private OTSystemImpl() {
	}

	public static <O> OTSystemImpl<O> create() {
		return new OTSystemImpl<>();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <L extends D, R extends D> OTSystemImpl<D> withTransformFunction(Class<? super L> leftType, Class<? super R> rightType,
			TransformFunction<D, L, R> transformer) {
		transformers.put(new KeyPair(leftType, rightType), transformer);
		if (leftType != rightType) {
			transformers.put(new KeyPair(rightType, leftType), (TransformFunction<D, R, L>) (left, right) -> {
				TransformResult<? extends D> transform = transformer.transform(right, left);
				if (transform.hasConflict()) {
					if (transform.resolution == ConflictResolution.LEFT)
						return TransformResult.conflict(ConflictResolution.RIGHT);
					if (transform.resolution == ConflictResolution.RIGHT)
						return TransformResult.conflict(ConflictResolution.LEFT);
					return transform;
				}
				return TransformResult.of(transform.right, transform.left);
			});
		}
		return this;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <O1 extends D, O2 extends D> OTSystemImpl<D> withSquashFunction(Class<? super O1> opType1, Class<? super O2> opType2,
			SquashFunction<D, O1, O2> squashFunction) {
		squashers.put(new KeyPair(opType1, opType2), squashFunction);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <O extends D> OTSystemImpl<D> withInvertFunction(Class<? super O> opType, InvertFunction<D, O> inverter) {
		inverters.put((Class<D>) opType, inverter);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <O extends D> OTSystemImpl<D> withEmptyPredicate(Class<? super O> opType, EmptyPredicate<O> emptyChecker) {
		emptyPredicates.put((Class<D>) opType, emptyChecker);
		return this;
	}

	@SuppressWarnings({"SimplifiableIfStatement", "unchecked"})
	@Override
	public boolean isEmpty(D op) {
		if (emptyPredicates.isEmpty())
			return false;
		EmptyPredicate<D> emptyChecker = (EmptyPredicate<D>) emptyPredicates.get(op.getClass());
		if (emptyChecker == null)
			return false;
		return emptyChecker.isEmpty(op);
	}

	@Override
	public TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws TransformException {
		TransformResult<D> transform = doTransform(leftDiffs, rightDiffs);
		if (!transform.hasConflict())
			return transform;
		return resolveConflicts(leftDiffs, rightDiffs, transform);
	}

	private TransformResult<D> resolveConflicts(List<? extends D> leftDiffs, List<? extends D> rightDiffs, TransformResult<D> transform) {
		if (transform.resolution == ConflictResolution.LEFT) {
			return TransformResult.of(transform.resolution,
					List.of(),
					squash(concat(invert(rightDiffs).stream(), leftDiffs.stream()).collect(toList())));
		}
		if (transform.resolution == ConflictResolution.RIGHT) {
			return TransformResult.of(transform.resolution,
					squash(concat(invert(leftDiffs).stream(), rightDiffs.stream()).collect(toList())),
					List.of());
		}
		throw new AssertionError();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public TransformResult<D> doTransform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws TransformException {
		if (leftDiffs.isEmpty() && rightDiffs.isEmpty())
			return TransformResult.empty();
		if (leftDiffs.isEmpty()) {
			return TransformResult.left(rightDiffs);
		}
		if (rightDiffs.isEmpty()) {
			return TransformResult.right(leftDiffs);
		}
		if (leftDiffs.size() == 1) {
			D left = leftDiffs.get(0);
			D right = rightDiffs.get(0);
			KeyPair<D> key = new KeyPair(left.getClass(), right.getClass());
			TransformFunction<D, D, D> transformer = (TransformFunction<D, D, D>) transformers.get(key);
			TransformResult<D> transform1 = (TransformResult<D>) transformer.transform(left, right);
			if (transform1.hasConflict()) return transform1;
			TransformResult<D> transform2 = doTransform(transform1.right, rightDiffs.subList(1, rightDiffs.size()));
			if (transform2.hasConflict()) return transform2;
			return TransformResult.of(concat(transform1.left, transform2.left), transform2.right);
		}
		TransformResult<D> transform1 = doTransform(leftDiffs.subList(0, 1), rightDiffs);
		if (transform1.hasConflict()) return transform1;
		TransformResult<D> transform2 = doTransform(leftDiffs.subList(1, leftDiffs.size()), transform1.left);
		if (transform2.hasConflict()) return transform2;
		return TransformResult.of(transform2.left, concat(transform1.right, transform2.right));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public List<D> squash(List<? extends D> ops) {
		if (squashers.isEmpty())
			return (List<D>) ops;
		List<D> result = new ArrayList<>();
		Iterator<D> it = ((List<D>) ops).iterator();
		if (!it.hasNext())
			return List.of();
		D cur = it.next();
		while (it.hasNext()) {
			D next = it.next();
			KeyPair<D> key = new KeyPair(cur.getClass(), next.getClass());
			SquashFunction<D, D, D> squashFunction = (SquashFunction<D, D, D>) squashers.get(key);
			D squashed = squashFunction == null ? null : squashFunction.trySquash(cur, next);
			if (squashed != null) {
				cur = squashed;
			} else {
				if (!isEmpty(cur)) {
					result.add(cur);
				}
				cur = next;
			}
		}
		if (!isEmpty(cur)) {
			result.add(cur);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <O extends D> List<D> invert(List<O> ops) {
		int size = ops.size();
		List<D> result = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			O op = ops.get(size - i - 1);
			InvertFunction<D, O> inverter = (InvertFunction<D, O>) inverters.get(op.getClass());
			List<? extends D> inverted = inverter.invert(op);
			result.addAll(inverted);
		}
		return result;
	}
}
