package ozgurdbsync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

import org.junit.Test;


public class DependencyTest extends TestBase{
//
//
//	@Test
//	public void conPrimary() throws Exception{
//		ConArgs ca = testCon();
//		// HSQLDB table names are in uppercase
//		ca.table="EMPLOYEE";
//
//		ConArgs ca2 = testCon();
//		// HSQLDB table names are in uppercase
//		ca2.table="EMPLOYEE2";
//
//
//		Con con=new Con(ca);
//		Con con2=new Con(ca2);
//		con.initMeta();
//		con2.initMeta();
//		Assert.assertNull(con.compareSchema(con2));
//
//	}
	
	@Test
	public void orderTest() throws Exception{
		Main.main(new String[] {"/home/rompg/workspace/ozgurdbsync/src/test/resources/dependency-test.ini"});
		String result = Main.ordered.stream()
			      .map(n -> String.valueOf(n))
			      .collect(Collectors.joining("-", "{", "}"));
		 System.out.println(result);
	
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
			statement.execute("CREATE TABLE silk (id INT NOT NULL, PRIMARY KEY (id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO silk VALUES (1)");
			statement.executeUpdate(
					"INSERT INTO silk VALUES (2)");
			statement.executeUpdate(
					"INSERT INTO silk VALUES (3)");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE ilk (id INT NOT NULL, silk int, PRIMARY KEY (id), FOREIGN KEY(silk) REFERENCES silk(id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO ilk VALUES (1,1)");
			statement.executeUpdate(
					"INSERT INTO ilk VALUES (2,2)");
			statement.executeUpdate(
					"INSERT INTO ilk VALUES (3,2)");
			connection.commit();
		}
		
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE numera (id INT NOT NULL, ilk int,  PRIMARY KEY (id), "
					+ "FOREIGN KEY(ilk) REFERENCES ilk(id)"
					+ ")");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO numera VALUES (1,1)");
			statement.executeUpdate(
					"INSERT INTO numera VALUES (2,2)");
			statement.executeUpdate(
					"INSERT INTO numera VALUES (3,2)");
			connection.commit();
		}
		
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, otherid INT, PRIMARY KEY (id), FOREIGN KEY(otherid) REFERENCES numera(id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO employee VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com', 1)");
			statement.executeUpdate("INSERT INTO employee VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com', 1)");
			statement.executeUpdate("INSERT INTO employee VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com', 3)");
			connection.commit();
		}
		try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
			statement.execute("CREATE TABLE employee2 (id INT NOT NULL, name VARCHAR(50) NOT NULL,"
					+ "email VARCHAR(50) NOT NULL, otherid INT, PRIMARY KEY (id), FOREIGN KEY(otherid) REFERENCES numera(id))");
			connection.commit();
			statement.executeUpdate(
					"INSERT INTO employee2 VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com', 1)");
			statement.executeUpdate("INSERT INTO employee2 VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com', 1)");
			statement.executeUpdate("INSERT INTO employee2 VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com', 2)");
			connection.commit();
		}
		
	
	}

}	


