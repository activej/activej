package io.activej.cube.linear;

import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;

public final class CubeSqlNaming {
	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "revisionTable", "cube_revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "positionTable", "cube_position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "chunkTable", "cube_chunk");

	public static final String BACKUP_REVISION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupTable", "cube_revision_backup");
	public static final String BACKUP_POSITION_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupPositionTable", "cube_position_backup");
	public static final String BACKUP_CHUNK_TABLE = ApplicationSettings.getString(CubeBackupController.class, "backupChunkTable", "cube_chunk_backup");

	public static final CubeSqlNaming DEFAULT_SQL_NAMING = create();

	private String tableRevision = REVISION_TABLE;
	private String tablePosition = POSITION_TABLE;
	private String tableChunk = CHUNK_TABLE;

	private String tableRevisionBackup = BACKUP_REVISION_TABLE;
	private String tablePositionBackup = BACKUP_POSITION_TABLE;
	private String tableChunkBackup = BACKUP_CHUNK_TABLE;

	private String backupBy = "null";

	private CubeSqlNaming() {
	}

	public static CubeSqlNaming create() {
		return builder().build();
	}

	public static Builder builder() {
		return new CubeSqlNaming().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeSqlNaming> {
		private Builder() {}

		public Builder withTableNames(String tableRevision, String tablePosition, String tableChunk) {
			checkNotBuilt(this);
			CubeSqlNaming.this.tableRevision = tableRevision;
			CubeSqlNaming.this.tablePosition = tablePosition;
			CubeSqlNaming.this.tableChunk = tableChunk;
			return this;
		}

		public Builder withBackupTableNames(String tableRevisionBackup, String tablePositionBackup, String tableChunkBackup) {
			checkNotBuilt(this);
			CubeSqlNaming.this.tableRevisionBackup = tableRevisionBackup;
			CubeSqlNaming.this.tablePositionBackup = tablePositionBackup;
			CubeSqlNaming.this.tableChunkBackup = tableChunkBackup;
			return this;
		}

		public Builder withBackupBy(String backupBy) {
			checkNotBuilt(this);
			CubeSqlNaming.this.backupBy = backupBy;
			return this;
		}

		@Override
		protected CubeSqlNaming doBuild() {
			return CubeSqlNaming.this;
		}
	}

	public String sql(String sql) {
		return sql
			.replace("{revision}", escape(tableRevision))
			.replace("{position}", escape(tablePosition))
			.replace("{chunk}", escape(tableChunk))
			.replace("{backup}", escape(tableRevisionBackup))
			.replace("{backup_position}", escape(tablePositionBackup))
			.replace("{backup_chunk}", escape(tableChunkBackup))
			.replace("{backup_by}", backupBy == null ? "null" : ('\'' + backupBy + '\''));
	}

	public static String escape(String name) {
		return '`' + name + '`';
	}

}
