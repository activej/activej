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

import io.activej.aggregation.AggregationPredicates;
import io.activej.aggregation.PredicateDef;
import io.activej.common.builder.AbstractBuilder;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class CubeQuery {
	private Set<String> attributes = new LinkedHashSet<>();
	private Set<String> measures = new LinkedHashSet<>();
	private PredicateDef where = AggregationPredicates.alwaysTrue();
	private PredicateDef having = AggregationPredicates.alwaysTrue();
	private Integer limit = null;
	private Integer offset = null;
	private List<Ordering> orderings = new ArrayList<>();

	private ReportType reportType = ReportType.DATA_WITH_TOTALS;

	private CubeQuery() {}

	public static Builder builder() {
		return new CubeQuery().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeQuery> {
		private Builder() {}

		public Builder withMeasures(List<String> measures) {
			checkNotBuilt(this);
			CubeQuery.this.measures = new LinkedHashSet<>(measures);
			return this;
		}

		public Builder withMeasures(String... measures) {
			checkNotBuilt(this);
			return withMeasures(List.of(measures));
		}

		public Builder withAttributes(List<String> attributes) {
			checkNotBuilt(this);
			CubeQuery.this.attributes = new LinkedHashSet<>(attributes);
			return this;
		}

		public Builder withAttributes(String... attributes) {
			checkNotBuilt(this);
			return withAttributes(List.of(attributes));
		}

		public Builder withWhere(PredicateDef where) {
			checkNotBuilt(this);
			CubeQuery.this.where = where;
			return this;
		}

		public Builder withHaving(PredicateDef having) {
			checkNotBuilt(this);
			CubeQuery.this.having = having;
			return this;
		}

		public Builder withOrderings(List<Ordering> orderings) {
			checkNotBuilt(this);
			CubeQuery.this.orderings = orderings;
			return this;
		}

		public Builder withOrderingAsc(String field) {
			checkNotBuilt(this);
			CubeQuery.this.orderings.add(Ordering.asc(field));
			return this;
		}

		public Builder withOrderingDesc(String field) {
			checkNotBuilt(this);
			CubeQuery.this.orderings.add(Ordering.desc(field));
			return this;
		}

		public Builder withOrderings(Ordering... orderings) {
			checkNotBuilt(this);
			return withOrderings(List.of(orderings));
		}

		@SuppressWarnings("UnusedReturnValue")
		public Builder withLimit(Integer limit) {
			checkNotBuilt(this);
			CubeQuery.this.limit = limit;
			return this;
		}

		@SuppressWarnings("UnusedReturnValue")
		public Builder withOffset(Integer offset) {
			checkNotBuilt(this);
			CubeQuery.this.offset = offset;
			return this;
		}

		public Builder withReportType(ReportType reportType) {
			checkNotBuilt(this);
			CubeQuery.this.reportType = reportType;
			return this;
		}

		@Override
		protected CubeQuery doBuild() {
			return CubeQuery.this;
		}
	}

	// region getters
	public Set<String> getAttributes() {
		return attributes;
	}

	public Set<String> getMeasures() {
		return measures;
	}

	public PredicateDef getWhere() {
		return where;
	}

	public List<Ordering> getOrderings() {
		return orderings;
	}

	public PredicateDef getHaving() {
		return having;
	}

	public Integer getLimit() {
		return limit;
	}

	public Integer getOffset() {
		return offset;
	}

	public ReportType getReportType() {
		return reportType;
	}

	// endregion

	// region helper classes

	/**
	 * Represents a query result ordering. Contains a propertyName name and ordering (ascending or descending).
	 */
	public static final class Ordering {
		private final String field;
		private final boolean desc;

		private Ordering(String field, boolean desc) {
			this.field = field;
			this.desc = desc;
		}

		public static Ordering asc(String field) {
			return new Ordering(field, false);
		}

		public static Ordering desc(String field) {
			return new Ordering(field, true);
		}

		public String getField() {
			return field;
		}

		public boolean isAsc() {
			return !isDesc();
		}

		public boolean isDesc() {
			return desc;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Ordering that = (Ordering) o;
			return desc == that.desc && field.equals(that.field);
		}

		@Override
		public int hashCode() {
			int result = field.hashCode();
			result = 31 * result + (desc ? 1 : 0);
			return result;
		}

		@Override
		public String toString() {
			return field + " " + (desc ? "desc" : "asc");
		}
	}

	// endregion

	@Override
	public String toString() {
		return "CubeQuery{" +
				"attributes=" + attributes +
				", measures=" + measures +
				", where=" + where +
				", having=" + having +
				", limit=" + limit +
				", offset=" + offset +
				", orderings=" + orderings +
				", reportType=" + reportType + '}';
	}
}
