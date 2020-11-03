package ozgurdbsync;

import static org.junit.Assert.*;

import java.sql.*;
import org.junit.*;

import ozgurdbsync.Con;
import ozgurdbsync.ConArgs;

import java.io.IOException;

public class MetaTest extends TestBase{

	@Test
	public void conTest() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		Assert.assertNull(con.compareSchema(con));

	}

	@Test
	public void conPrimary() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";

		ConArgs ca2 = testCon();
		// HSQLDB table names are in uppercase
		ca2.table="EMPLOYEE2";


		Con con=new Con(ca);
		Con con2=new Con(ca2);
		con.initMeta();
		con2.initMeta();
		Assert.assertNotNull(con.compareSchema(con2));

	}


	@Test
	public void conColumns() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";

		ConArgs ca2 = testCon();
		// HSQLDB table names are in uppercase
		ca2.table="EMPLOYEE3";


		Con con=new Con(ca);
		Con con2=new Con(ca2);
		con.initMeta();
		con2.initMeta();
		Assert.assertNotNull(con.compareSchema(con2));

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
	protected void initDatabase() throws SQLException {
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
					+ "email VARCHAR(50) NOT NULL)");
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


