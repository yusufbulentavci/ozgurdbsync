package ozgurdbsync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

public class DataTest extends TestBase{


	@Test
	public void conTest() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";
		Con con=new Con(ca);
		con.initMeta();
		con.getData();
		Assert.assertEquals(3, con.data.size());

	}

	

	@Test
	public void conTest2() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE2";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();
		Assert.assertEquals(3, con.data.size());

	}




	/**
	 * Database initialization for testing i.e.
	 * <ul>
	 * <li>Creating Table</li>
	 * <li>Inserting record</li>
	 * </ul>
	 * 
	 * @throws SQLException
	 */
	protected void initDatabase() throws Exception {
		destroy();
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY (id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO employee VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO employee VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO employee VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee2 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY(id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO employee2 VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO employee2 VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO employee2 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee3 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, tel VARCHAR(10), PRIMARY KEY (id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO employee3 VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com', '9041')");
			statement.executeUpdate("INSERT INTO employee3 VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com', '9042')");
			statement.executeUpdate("INSERT INTO employee3 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com', '9043')");
			connection.commit();
		}
	
	}


}	


