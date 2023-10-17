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

package io.activej.cube.http;

import io.activej.common.exception.MalformedDataException;
import io.activej.cube.CubeQuery;
import io.activej.cube.ICubeReporting;
import io.activej.cube.QueryResult;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.exception.CubeException;
import io.activej.http.HttpRequest;
import io.activej.http.HttpUtils;
import io.activej.http.IHttpClient;
import io.activej.json.JsonCodec;
import io.activej.promise.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.cube.http.Utils.*;
import static io.activej.json.JsonUtils.fromJsonBytes;
import static io.activej.json.JsonUtils.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpClientCubeReporting implements ICubeReporting {
	private static final Logger logger = LoggerFactory.getLogger(HttpClientCubeReporting.class);

	private final String url;
	private final IHttpClient httpClient;
	private final JsonCodec<QueryResult> queryResultJsonCodec;
	private final JsonCodec<AggregationPredicate> aggregationPredicateCodec;

	private HttpClientCubeReporting(IHttpClient httpClient, String url,
		JsonCodec<QueryResult> queryResultJsonCodec, JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		this.url = url.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.queryResultJsonCodec = queryResultJsonCodec;
		this.aggregationPredicateCodec = aggregationPredicateCodec;
	}

	public static HttpClientCubeReporting create(
		IHttpClient httpClient,
		String cubeServletUrl,
		JsonCodec<QueryResult> queryResultJsonCodec,
		JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		return new HttpClientCubeReporting(httpClient, cubeServletUrl, queryResultJsonCodec, aggregationPredicateCodec);
	}

	public static HttpClientCubeReporting create(
		IHttpClient httpClient,
		URI cubeServletUrl,
		JsonCodec<QueryResult> queryResultJsonCodec,
		JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		return create(httpClient, cubeServletUrl.toString(), queryResultJsonCodec, aggregationPredicateCodec);
	}

	@Override
	public Promise<QueryResult> query(CubeQuery query) {
		return httpClient.request(buildRequest(query))
			.mapException(e -> new CubeException("HTTP request failed", e))
			.then(response -> response.loadBody()
				.mapException(e -> new CubeException("HTTP request failed", e))
				.map(body -> {
					try {
						if (response.getCode() != 200) {
							throw new CubeException(
								"CubeHTTP query failed. Response code: " + response.getCode() +
								" Body: " + body.getString(UTF_8));
						}
						return fromJsonBytes(queryResultJsonCodec, body.getArray());
					} catch (MalformedDataException e) {
						throw new CubeException("Cube HTTP query failed. Invalid data received", e);
					}
				})
				.whenComplete(toLogger(logger, "query", query)));
	}

	private HttpRequest buildRequest(CubeQuery query) {
		Map<String, String> urlParams = new LinkedHashMap<>();

		urlParams.put(ATTRIBUTES_PARAM, String.join(",", query.getAttributes()));
		urlParams.put(MEASURES_PARAM, String.join(",", query.getMeasures()));
		urlParams.put(WHERE_PARAM, toJson(aggregationPredicateCodec, query.getWhere()));
		urlParams.put(SORT_PARAM, formatOrderings(query.getOrderings()));
		urlParams.put(HAVING_PARAM, toJson(aggregationPredicateCodec, query.getHaving()));
		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());
		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());
		urlParams.put(REPORT_TYPE_PARAM, query.getReportType().toString().toLowerCase());
		String url = this.url + "/" + "?" + HttpUtils.renderQueryString(urlParams);

		return HttpRequest.get(url).build();
	}
}
