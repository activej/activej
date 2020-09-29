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

package io.activej.cube;

import io.activej.common.record.Record;
import io.activej.common.record.RecordScheme;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.activej.common.collection.CollectionUtils.toLimitedString;

public final class QueryResult {
	private final RecordScheme recordScheme;
	private final List<String> attributes;
	private final List<String> measures;
	private final List<String> sortedBy;

	private final List<Record> records;
	private final Record totals;
	private final int totalCount;

	private final Map<String, Object> filterAttributes;
	private final ReportType reportType;

	private QueryResult(RecordScheme recordScheme, List<Record> records, Record totals, int totalCount,
			List<String> attributes, List<String> measures, List<String> sortedBy,
			Map<String, Object> filterAttributes, ReportType reportType) {
		this.recordScheme = recordScheme;
		this.records = records;
		this.totals = totals;
		this.totalCount = totalCount;
		this.attributes = attributes;
		this.measures = measures;
		this.sortedBy = sortedBy;
		this.filterAttributes = filterAttributes;
		this.reportType = reportType;
	}

	public static QueryResult create(RecordScheme recordScheme, List<String> attributes, List<String> measures, List<String> sortedBy, List<Record> records, Record totals, int totalCount,
			Map<String, Object> filterAttributes, ReportType reportType) {
		return new QueryResult(recordScheme, records, totals, totalCount, attributes, measures, sortedBy,
				filterAttributes, reportType);
	}

	public static QueryResult createForMetadata(RecordScheme recordScheme, List<String> attributes,
			List<String> measures) {
		return create(recordScheme, attributes, measures, Collections.emptyList(), Collections.emptyList(), Record.create(recordScheme), 0,
				Collections.emptyMap(), ReportType.METADATA);
	}

	public static QueryResult createForData(RecordScheme recordScheme, List<Record> records, List<String> attributes,
			List<String> measures, List<String> sortedBy,
			Map<String, Object> filterAttributes) {
		return create(recordScheme, attributes, measures, sortedBy, records, Record.create(recordScheme), 0,
				filterAttributes, ReportType.DATA);
	}

	public static QueryResult createForDataWithTotals(RecordScheme recordScheme, List<Record> records, Record totals,
			int totalCount, List<String> attributes, List<String> measures,
			List<String> sortedBy, Map<String, Object> filterAttributes) {
		return create(recordScheme, attributes, measures, sortedBy, records, totals, totalCount, filterAttributes,
				ReportType.DATA_WITH_TOTALS);
	}

	public RecordScheme getRecordScheme() {
		return recordScheme;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public List<Record> getRecords() {
		return records;
	}

	public Record getTotals() {
		return totals;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public Map<String, Object> getFilterAttributes() {
		return filterAttributes;
	}

	public List<String> getSortedBy() {
		return sortedBy;
	}

	public ReportType getReportType() {
		return reportType;
	}

	@Override
	public String toString() {
		return "QueryResult{" +
				"attributes=" + attributes +
				", measures=" + measures +
				", records=" + toLimitedString(records, 5) +
				", totals=" + totals +
				", count=" + totalCount +
				", sortedBy=" + sortedBy +
				'}';
	}
}
