package ozgurdbsync;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateTest extends TestBase{


	@Test
	public void conSameOnSameTable() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.schema="SEMA";
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();
		List<PkeyValue> ret=con.diffUpdate(con);
		
		Assert.assertEquals(0, ret.size());

	}

	@Test
	public void conSameOnDifferentTables() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.schema="SEMA";
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();

		ConArgs ca2 = testCon();
		// HSQLDB table names are in uppercase
		ca2.schema="SEMA";
		ca2.table="EMPLOYEE2";

		Con con2=new Con(ca2);
		con2.initMeta();
		con2.getData();

		List<PkeyValue> ret=con.diffUpdate(con2);
		

		Assert.assertEquals(0, ret.size());
	
	}
	
	@Test
	public void conDifferent() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.schema="SEMA";
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();

		ConArgs ca2 = testCon();
		// HSQLDB table names are in uppercase
		ca2.schema="SEMA";
		ca2.table="EMPLOYEE3";

		Con con2=new Con(ca2);
		con2.initMeta();
		con2.getData();

		List<PkeyValue> ret=con.diffUpdate(con2);
		
		Assert.assertEquals(1, ret.size());
		Assert.assertTrue(ret.contains(new PkeyValue(1002)));
		
		String update = con2.toSqlUpdate(new PkeyValue(1002));
		
		System.out.println(update);
		
		Assert.assertEquals("update  SEMA.EMPLOYEE3 set NAME='XXX' , EMAIL='dhwani@javacodegeeks.com' where ID=1002;", update);
		
	}

	@After
	public void destroy() throws SQLException, ClassNotFoundException, IOException {
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.executeUpdate("DROP TABLE if exists sema.employee");
			statement.executeUpdate("DROP TABLE if exists sema.employee2");
			statement.executeUpdate("DROP TABLE if exists sema.employee3");
			statement.executeUpdate("DROP schema if exists sema");
			connection.commit();
		}
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
			statement.execute("CREATE SCHEMA sema");
			statement.execute("CREATE TABLE sema.employee (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY (id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO sema.employee VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee VALUES (1005,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE sema.employee2 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY(id))");
			connection.commit();

			statement.executeUpdate("INSERT INTO sema.employee2 VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee2 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			statement.executeUpdate(
					"INSERT INTO sema.employee2 VALUES (1004,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE sema.employee3 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY (id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO sema.employee3 VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee3 VALUES (1002,'XXX', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO sema.employee3 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			connection.commit();
		}
	
	}

}	


