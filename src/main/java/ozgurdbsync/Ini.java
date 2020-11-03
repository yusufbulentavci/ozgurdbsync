package ozgurdbsync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class Ini {
	private IniFile file;

	String sqlEngine;
	ConnectProps srcProps;
	ConnectProps destProps;
	List<TableProps> tableProps=new ArrayList<>();
	public Ini(String fn) throws IOException {
		this.file=new IniFile(fn);
		this.sqlEngine=file.getString("general", "sqlEngine", "hsqldb");
		
		this.srcProps=getConnectProps("source");
		this.destProps=getConnectProps("destination");
	
		for(int i=1; i< 1000; i++) {
			String section="table-"+i;
			if(!file.containsSection(section))
				break;
			tableProps.add(getTableProps(section));
		}
	}

	private TableProps getTableProps(String section) {
		String schema=file.getString(section, "schema", null);
		String table = file.getString(section, "table", null);
		if(table==null)
			throw new RuntimeException("Table name shouldnt be null in ini file. Check section:"+section);
		return new TableProps(schema, table);
	}

	private ConnectProps getConnectProps(String section) {
		ConnectProps props = new ConnectProps();
		props.url=file.getString(section, "url", null);
		props.user=file.getString(section, "user", null);
		props.password=file.getString(section, "password", null);
		return props;
	}

}
