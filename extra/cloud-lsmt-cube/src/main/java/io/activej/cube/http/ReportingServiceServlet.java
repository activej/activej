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
import io.activej.common.exception.MalformedDataException;
import io.activej.common.time.Stopwatch;
import io.activej.cube.CubeQuery;
import io.activej.cube.ICubeReporting;
import io.activej.cube.QueryResult;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.exception.QueryException;
import io.activej.http.*;
import io.activej.json.JsonCodec;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.Utils.not;
import static io.activej.cube.http.Utils.*;
import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.http.HttpMethod.GET;
import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJsonBytes;
import static java.util.stream.Collectors.toList;

public final class ReportingServiceServlet extends ServletWithStats {
	private static final Logger logger = LoggerFactory.getLogger(ReportingServiceServlet.class);

	private final ICubeReporting cubeReporting;
	private final JsonCodec<QueryResult> queryResultCodec;
	private final JsonCodec<AggregationPredicate> aggregationPredicateCodec;

	private ReportingServiceServlet(Reactor reactor, ICubeReporting cubeReporting,
		JsonCodec<QueryResult> queryResultCodec, JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		super(reactor);
		this.cubeReporting = cubeReporting;
		this.queryResultCodec = queryResultCodec;
		this.aggregationPredicateCodec = aggregationPredicateCodec;
	}

	public static ReportingServiceServlet create(
		Reactor reactor,
		ICubeReporting cubeReporting,
		JsonCodec<QueryResult> queryResultCodec,
		JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		return new ReportingServiceServlet(reactor, cubeReporting, queryResultCodec, aggregationPredicateCodec);
	}

	public static RoutingServlet createRootServlet(
		Reactor reactor,
		ICubeReporting cubeReporting,
		JsonCodec<QueryResult> queryResultCodec,
		JsonCodec<AggregationPredicate> aggregationPredicateCodec
	) {
		return createRootServlet(
			ReportingServiceServlet.create(reactor, cubeReporting, queryResultCodec, aggregationPredicateCodec));
	}

	public static RoutingServlet createRootServlet(ReportingServiceServlet reportingServiceServlet) {
		return RoutingServlet.builder(reportingServiceServlet.reactor)
			.with(GET, "/", reportingServiceServlet)
			.build();
	}

	@Override
	public Promise<HttpResponse> doServe(HttpRequest httpRequest) {
		logger.info("Received request: {}", httpRequest);
		try {
			Stopwatch totalTimeStopwatch = Stopwatch.createStarted();
			CubeQuery cubeQuery = parseQuery(httpRequest);
			return cubeReporting.query(cubeQuery)
				.then(queryResult -> {
					Stopwatch resultProcessingStopwatch = Stopwatch.createStarted();
					ByteBuf jsonBuf = ByteBuf.wrapForReading(toJsonBytes(queryResultCodec, queryResult));
					Promise<HttpResponse> httpResponse = createResponse(jsonBuf);
					logger.info("Processed request {} ({}) [totalTime={}, jsonConstruction={}]", httpRequest,
						cubeQuery, totalTimeStopwatch, resultProcessingStopwatch);
					return httpResponse;
				});
		} catch (QueryException e) {
			logger.warn("Query exception: {}", httpRequest, e);
			return createErrorResponse(e.getMessage());
		} catch (MalformedDataException e) {
			logger.warn("Parse exception: {}", httpRequest, e);
			return createErrorResponse(e.getMessage());
		}
	}

	private static Promise<HttpResponse> createResponse(ByteBuf body) {
		return HttpResponse.ok200()
			.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.JSON, StandardCharsets.UTF_8)))
			.withBody(body)
			.withHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
			.toPromise();
	}

	private static Promise<HttpResponse> createErrorResponse(String body) {
		return HttpResponse.ofCode(400)
			.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT, StandardCharsets.UTF_8)))
			.withBody(wrapUtf8(body))
			.withHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
			.toPromise();
	}

	private static final Pattern SPLITTER = Pattern.compile(",");

	private static List<String> split(String input) {
		return SPLITTER.splitAsStream(input)
			.map(String::trim)
			.filter(not(String::isEmpty))
			.collect(toList());
	}

	public CubeQuery parseQuery(HttpRequest request) throws MalformedDataException {
		CubeQuery.Builder queryBuilder = CubeQuery.builder();

		String parameter;
		parameter = request.getQueryParameter(ATTRIBUTES_PARAM);
		if (parameter != null)
			queryBuilder.withAttributes(split(parameter));

		parameter = request.getQueryParameter(MEASURES_PARAM);
		if (parameter != null)
			queryBuilder.withMeasures(split(parameter));

		parameter = request.getQueryParameter(WHERE_PARAM);
		if (parameter != null)
			queryBuilder.withWhere(fromJson(aggregationPredicateCodec, parameter));

		parameter = request.getQueryParameter(SORT_PARAM);
		if (parameter != null)
			queryBuilder.withOrderings(parseOrderings(parameter));

		parameter = request.getQueryParameter(HAVING_PARAM);
		if (parameter != null)
			queryBuilder.withHaving(fromJson(aggregationPredicateCodec, parameter));

		parameter = request.getQueryParameter(LIMIT_PARAM);
		if (parameter != null)
			queryBuilder.withLimit(parseNonNegativeInteger(parameter));

		parameter = request.getQueryParameter(OFFSET_PARAM);
		if (parameter != null)
			queryBuilder.withOffset(parseNonNegativeInteger(parameter));

		parameter = request.getQueryParameter(REPORT_TYPE_PARAM);
		if (parameter != null)
			queryBuilder.withReportType(parseReportType(parameter));

		return queryBuilder.build();
	}

}
