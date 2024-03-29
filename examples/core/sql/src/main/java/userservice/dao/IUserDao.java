package userservice.dao;

import io.activej.common.annotation.ComponentInterface;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Basic DAO (Data Access Object) class that provides User CRUD-operations
 */
@ComponentInterface
public interface IUserDao {
	Promise<@Nullable User> get(long id);

	Promise<Map<Long, User>> getAll();

	Promise<Void> addUser(User user);

	Promise<Boolean> updateUser(long id, User newUser);

	Promise<Boolean> deleteUser(long id);
}
