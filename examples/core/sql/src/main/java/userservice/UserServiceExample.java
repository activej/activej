package userservice;

import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.service.ServiceGraphModule;
import userservice.dao.User;
import userservice.dao.AsyncUserDao;
import userservice.dao.UserDaoSql;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * This example demonstrates how to work with DAO objects using ActiveJ.
 * Basic CRUD operations are performed on 'user' SQL table.
 * <p>
 * By default, in-memory H2 database is used for the example
 * <p>
 * If you wish, you can swap {@link H2Module} with {@link MySqlModule}
 */
public class UserServiceExample extends Launcher {

	@Inject
	Reactor reactor;

	@Inject
	AsyncUserDao userDao;

	@Provides
	Reactor eventloop() {
		return Eventloop.create();
	}

	@Provides
	Executor executor() {
		return Executors.newCachedThreadPool();
	}

	@Provides
	AsyncUserDao userDao(DataSource dataSource, Executor executor) {
		return new UserDaoSql(dataSource, executor);
	}

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				new H2Module()
		);
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		System.out.println();
		CompletableFuture<?> future = reactor.submit(() ->
				Promises.all(
						userDao.addUser(new User("Kory", "Holloway")),
						userDao.addUser(new User("Abraham", "Ventura")),
						userDao.addUser(new User("Emile", "Weiss")),
						userDao.addUser(new User("Torin", "Browne")))
						.then(() -> userDao.getAll())
						.whenResult(UserServiceExample::printUsers)
						.then(() -> {
							System.out.println("Updating user with ID 2...");
							return userDao.updateUser(2, new User("George", "Dawson"));
						})
						.whenResult(updated -> System.out.println("Updated: " + updated))
						.then(() -> {
							System.out.println("Retrieving user with ID 2...");
							return userDao.get(2);
						})
						.whenResult(users -> System.out.println("User with ID 2: " + users))
						.then(() -> {
							System.out.println("Deleting user with ID 1...");
							return userDao.deleteUser(1);
						})
						.then(deleted -> {
							System.out.println("Deleted: " + deleted);
							return userDao.getAll();
						})
						.whenResult(UserServiceExample::printUsers)
		);

		future.get();
		System.out.println();
	}

	private static void printUsers(Map<Long, User> users) {
		System.out.println("All users: ");
		users.forEach((id, user) ->
				System.out.printf("\tID %d: %s %s%n", id, user.firstName(), user.lastName()));
	}

	public static void main(String[] args) throws Exception {
		new UserServiceExample().launch(args);
	}
}
