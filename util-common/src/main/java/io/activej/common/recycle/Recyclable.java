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

package io.activej.common.recycle;

/**
 * Since some objects (mainly {@link io.activej.bytebuf.ByteBuf ByteBufs}) have
 * the notion of ownership and recyclability, a way to recognize them is needed.
 * This interface marks objects that need to be recycled at the end of their lifetime,
 * so that some generic abstraction (such as CSP) could try to recycle some object that it consumes.
 */
@SuppressWarnings("JavadocReference")
public interface Recyclable {
	/**
	 * Free some resource that this object possesses, e.g. return itself to the pool, etc.
	 */
	void recycle();
}
