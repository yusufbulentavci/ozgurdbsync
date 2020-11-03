package ozgurdbsync;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestBase {
	protected ConArgs testCon() {
		ConnectProps ca = new ConnectProps();
		ca.url = "jdbc:hsqldb:mem:employees";
		ca.user = "vinod";
		ca.password = "vinod";

		ConArgs c = new ConArgs();
		c.props = ca;
		return c;
	}

	@Before
	public void init() throws Exception {
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
//		destroy();
		// initialize database
		initDatabase();
	}

	protected void initDatabase() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@After
	public void destroy() throws SQLException, ClassNotFoundException, IOException {
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.executeUpdate("DROP TABLE if exists employee");
			statement.executeUpdate("DROP TABLE if exists employee2");
			statement.executeUpdate("DROP TABLE if exists employee3");
			connection.commit();
		}
	}
	

	/**
	 * Create a connection
	 * 
	 * @return connection object
	 * @throws SQLException
	 */
	protected Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:hsqldb:mem:employees", "vinod", "vinod");
	}

}
