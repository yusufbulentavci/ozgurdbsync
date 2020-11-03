package ozgurdbsync;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DelTest extends TestBase{


	@Test
	public void conSame() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();
		List<PkeyValue> ret=con.diffDel(con);
		
		Assert.assertEquals(0, ret.size());

	}

	@Test
	public void conDiff() throws Exception{
		ConArgs ca = testCon();
		// HSQLDB table names are in uppercase
		ca.table="EMPLOYEE";

		Con con=new Con(ca);
		con.initMeta();
		con.getData();
		
		ConArgs ca2 = testCon();
		// HSQLDB table names are in uppercase
		ca2.table="EMPLOYEE2";

		Con con2=new Con(ca2);
		con2.initMeta();
		con2.getData();

		List<PkeyValue> ret=con.diffDel(con2);
		
		Assert.assertEquals(2, ret.size());
		Assert.assertTrue(ret.contains(new PkeyValue(1001)));
		Assert.assertTrue(ret.contains(new PkeyValue(1005)));
		
		String del1001=con.toSqlDelete(new PkeyValue(1001));
		Assert.assertEquals("delete from  EMPLOYEE where ID=1001;", del1001);
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
			statement.executeUpdate("INSERT INTO employee VALUES (1005,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee2 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, PRIMARY KEY(id))");
			connection.commit();

			statement.executeUpdate("INSERT INTO employee2 VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
			statement.executeUpdate("INSERT INTO employee2 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
			statement.executeUpdate(
					"INSERT INTO employee2 VALUES (1004,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
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


