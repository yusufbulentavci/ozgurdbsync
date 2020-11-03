package ozgurdbsync;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
}
