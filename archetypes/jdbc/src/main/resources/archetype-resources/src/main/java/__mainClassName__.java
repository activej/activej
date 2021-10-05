package ${groupId};

import io.activej.inject.annotation.Inject;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class ${mainClassName} extends Launcher {

    @Inject
    DataSource dataSource;

    @Override
    protected void run() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 'Hello, world!'");
        ) {
            resultSet.next();
            System.out.println(resultSet.getString(1));
        }
    }

    @Override
    protected Module getModule() {
        return DataSourceModule.create();
    }

    public static void main(String[] args) throws Exception {
        new ${mainClassName}().launch(args);
    }
}
