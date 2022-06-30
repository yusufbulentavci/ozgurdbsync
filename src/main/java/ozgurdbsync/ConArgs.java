package ozgurdbsync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConArgs{

	ConnectProps props;

	String schema;
	String table;
	
	String toFullTable() {
		if(schema==null)
			return table;
		return schema+"."+table;
	}

	public Connection getCon() throws SQLException {
		return DriverManager.getConnection(props.url, props.user, props.password);
	}
	
	public List<TableProps> getTableProps() {
		List<TableProps> tableProps = new ArrayList<>();
		try(Connection con=getCon()){
			DatabaseMetaData md = con.getMetaData();
//			ResultSet sl = md.getSchemas();
//			while(sl.next()) {
//				String schema=sl.getString(1);
//				String catalog=sl.getString(2);
//				String def=sl.getString(3);
//				
//				System.out.println(schema);
//				System.out.println(catalog);
//				System.out.println(def);
//				System.out.println(".");
//			}
			String[] types = {"TABLE"};
            ResultSet rs = md.getTables(null, null, "%", types);
            while (rs.next()) {
//            	System.out.println(rs.getString("TABLE_CAT"));
//                System.out.println(rs.getString("TABLE_SCHEM"));
//                System.out.println(rs.getString("TABLE_NAME"));
//                System.out.println(".");
                tableProps.add(new TableProps(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME")));
            }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableProps;
	}
	
	boolean isPostgres() {
		return props.isPostgres();
	}
}
