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

import io.activej.bytebuf.ByteBuf;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.Stopwatch;
import io.activej.cube.AsyncCube;
import io.activej.cube.CubeQuery;
import io.activej.cube.exception.QueryException;
import io.activej.http.*;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.Utils.not;
import static io.activej.cube.Utils.fromJson;
import static io.activej.cube.Utils.toJsonBuf;
import static io.activej.cube.http.Utils.*;
import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.http.HttpMethod.GET;
import static java.util.stream.Collectors.toList;

public final class ReportingServiceServlet extends AsyncServletWithStats implements WithInitializer<ReportingServiceServlet> {
	private static final Logger logger = LoggerFactory.getLogger(ReportingServiceServlet.class);

	private final AsyncCube cube;
	private QueryResultCodec queryResultCodec;
	private AggregationPredicateCodec aggregationPredicateCodec;

	private DefiningClassLoader classLoader = DefiningClassLoader.create();

	private ReportingServiceServlet(Reactor reactor, AsyncCube cube) {
		super(reactor);
		this.cube = cube;
	}

	public static ReportingServiceServlet create(Reactor reactor, AsyncCube cube) {
		return new ReportingServiceServlet(reactor, cube);
	}

	public static RoutingServlet createRootServlet(Reactor reactor, AsyncCube cube) {
		return createRootServlet(
				ReportingServiceServlet.create(reactor, cube));
	}

	public static RoutingServlet createRootServlet(ReportingServiceServlet reportingServiceServlet) {
		return RoutingServlet.create()
				.map(GET, "/", reportingServiceServlet);
	}

	public ReportingServiceServlet withClassLoader(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
		return this;
	}

	private AggregationPredicateCodec getAggregationPredicateCodec() {
		if (aggregationPredicateCodec == null) {
			aggregationPredicateCodec = AggregationPredicateCodec.create(cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return aggregationPredicateCodec;
	}

	private QueryResultCodec getQueryResultCodec() {
		if (queryResultCodec == null) {
			queryResultCodec = QueryResultCodec.create(classLoader, cube.getAttributeTypes(), cube.getMeasureTypes());
		}
		return queryResultCodec;
	}

	@Override
	public Promise<HttpResponse> doServe(HttpRequest httpRequest) {
		logger.info("Received request: {}", httpRequest);
		try {
			Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			CubeQuery cubeQuery = parseQuery(httpRequest);
			return cube.query(cubeQuery)
					.map(queryResult -> {
						Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
						ByteBuf jsonBuf = toJsonBuf(getQueryResultCodec(), queryResult);
						HttpResponse httpResponse = createResponse(jsonBuf);
						logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
								cubeQuery, totalTimeStopwatch, resultProcessingStopwatch);
						return httpResponse;
					});
		} catch (QueryException e) {
			logger.error("Query exception: " + httpRequest, e);
			return Promise.of(createErrorResponse(e.getMessage()));
		} catch (MalformedDataException e) {
			logger.error("Parse exception: " + httpRequest, e);
			return Promise.of(createErrorResponse(e.getMessage()));
		}
	}

	private static HttpResponse createResponse(ByteBuf body) {
		HttpResponse response = HttpResponse.ok200();
		response.addHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.JSON, StandardCharsets.UTF_8)));
		response.setBody(body);
		response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static HttpResponse createErrorResponse(String body) {
		HttpResponse response = HttpResponse.ofCode(400);
		response.addHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT, StandardCharsets.UTF_8)));
		response.setBody(wrapUtf8(body));
		response.addHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return response;
	}

	private static final Pattern SPLITTER = Pattern.compile(",");

	private static List<String> split(String input) {
		return SPLITTER.splitAsStream(input)
				.map(String::trim)
				.filter(not(String::isEmpty))
				.collect(toList());
	}

	public CubeQuery parseQuery(HttpRequest request) throws MalformedDataException {
		CubeQuery query = CubeQuery.create();

		String parameter;
		parameter = request.getQueryParameter(ATTRIBUTES_PARAM);
		if (parameter != null)
			query.withAttributes(split(parameter));

		parameter = request.getQueryParameter(MEASURES_PARAM);
		if (parameter != null)
			query.withMeasures(split(parameter));

		parameter = request.getQueryParameter(WHERE_PARAM);
		if (parameter != null)
			query.withWhere(fromJson(getAggregationPredicateCodec(), parameter));

		parameter = request.getQueryParameter(SORT_PARAM);
		if (parameter != null)
			query.withOrderings(parseOrderings(parameter));

		parameter = request.getQueryParameter(HAVING_PARAM);
		if (parameter != null)
			query.withHaving(fromJson(getAggregationPredicateCodec(), parameter));

		parameter = request.getQueryParameter(LIMIT_PARAM);
		if (parameter != null)
			query.withLimit(parseNonNegativeInteger(parameter));

		parameter = request.getQueryParameter(OFFSET_PARAM);
		if (parameter != null)
			query.withOffset(parseNonNegativeInteger(parameter));

		parameter = request.getQueryParameter(REPORT_TYPE_PARAM);
		if (parameter != null)
			query.withReportType(parseReportType(parameter));

		return query;
	}

}
