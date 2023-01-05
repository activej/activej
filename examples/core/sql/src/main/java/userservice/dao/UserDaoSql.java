package userservice.dao;

import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link AsyncUserDao} which uses generic SQL commands for operation
 */
public final class UserDaoSql implements AsyncUserDao {
	private final DataSource dataSource;
	private final Executor executor;

	public UserDaoSql(DataSource dataSource, Executor executor) {
		this.dataSource = dataSource;
		this.executor = executor;
	}

	@Override
	public Promise<@Nullable User> get(long id) {
		return Promise.ofBlocking(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						"SELECT first_name, last_name FROM user WHERE id=?")) {
					statement.setLong(1, id);
					try (ResultSet resultSet = statement.executeQuery()) {
						if (!resultSet.next()) {
							return null;
						}

						String firstName = resultSet.getString(1);
						String lastName = resultSet.getString(2);
						return new User(firstName, lastName);
					}
				}
			}
		});
	}

	@Override
	public Promise<Map<Long, User>> getAll() {
		return Promise.ofBlocking(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						"SELECT * FROM user")) {
					try (ResultSet resultSet = statement.executeQuery()) {
						Map<Long, User> result = new LinkedHashMap<>();

						while (resultSet.next()) {
							long id = resultSet.getLong(1);
							String firstName = resultSet.getString(2);
							String lastName = resultSet.getString(3);

							result.put(id, new User(firstName, lastName));
						}
						return result;
					}
				}
			}
		});
	}

	@Override
	public Promise<Void> addUser(User user) {
		return Promise.ofBlocking(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						"INSERT INTO user(first_name, last_name) VALUES(?, ?)")) {

					statement.setString(1, user.firstName());
					statement.setString(2, user.lastName());

					statement.executeUpdate();
				}
			}
		});
	}

	@Override
	public Promise<Boolean> updateUser(long id, User newUser) {
		return Promise.ofBlocking(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						"UPDATE user SET first_name=?, last_name=? WHERE id=?")) {

					statement.setString(1, newUser.firstName());
					statement.setString(2, newUser.lastName());
					statement.setLong(3, id);

					return statement.executeUpdate() != 0;
				}
			}
		});
	}

	@Override
	public Promise<Boolean> deleteUser(long id) {
		return Promise.ofBlocking(executor, () -> {
			try (Connection connection = dataSource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(
						"DELETE FROM user WHERE id=?")) {

					statement.setLong(1, id);

					return statement.executeUpdate() != 0;
				}
			}
		});
	}
}
