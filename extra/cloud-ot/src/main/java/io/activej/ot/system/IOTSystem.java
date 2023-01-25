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

import io.activej.common.annotation.ComponentInterface;
import io.activej.ot.TransformResult;
import io.activej.ot.exception.TransformException;

import java.util.List;

@ComponentInterface
public interface IOTSystem<D> {

	TransformResult<D> transform(List<? extends D> leftDiffs, List<? extends D> rightDiffs) throws TransformException;

	default TransformResult<D> transform(D leftDiff, D rightDiff) throws TransformException {
		return transform(List.of(leftDiff), List.of(rightDiff));
	}

	List<D> squash(List<? extends D> ops);

	boolean isEmpty(D op);

	<O extends D> List<D> invert(List<O> ops);
}
