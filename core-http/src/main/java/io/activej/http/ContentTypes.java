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

package io.activej.http;

import static io.activej.http.HttpCharset.US_ASCII;
import static io.activej.http.HttpCharset.UTF_8;
import static io.activej.http.MediaTypes.*;

/**
 * This is a collection of most well-known {@link ContentType} token references as Java constants.
 */
public final class ContentTypes {
	public static final ContentType ANY_TEXT_UTF_8 = register(ANY_TEXT, UTF_8);
	public static final ContentType PLAIN_TEXT_UTF_8 = register(PLAIN_TEXT, UTF_8);
	public static final ContentType JSON_UTF_8 = register(JSON, UTF_8);
	public static final ContentType HTML_UTF_8 = register(HTML, UTF_8);
	public static final ContentType CSS_UTF_8 = register(CSS, UTF_8);
	public static final ContentType PLAIN_TEXT_ASCII = register(PLAIN_TEXT, US_ASCII);

	static ContentType lookup(MediaType mime, HttpCharset charset) {
		if (mime == JSON && charset == UTF_8) {
			return JSON_UTF_8;
		}
		return new ContentType(mime, charset);
	}

	static ContentType register(MediaType mime, HttpCharset charset) {
		return new ContentType(mime, charset);
	}
}

