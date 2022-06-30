package ozgurdbsync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class Ini {
	private IniFile file;

	String sqlEngine;
	String allTables;
	ConnectProps srcProps;
	ConnectProps destProps;
	List<TableProps> tableProps;

//	public Integer dataCompareMaxRow=100000;

	public Integer dataCompareDiskSizeTotalInMB;

	public Ini(String fn) throws IOException {
		this.file=new IniFile(fn);
		this.sqlEngine=file.getString("general", "sqlEngine", "hsqldb");
		this.allTables=file.getString("general", "allTables", "no");
		this.dataCompareDiskSizeTotalInMB=file.getInt("general", "dataCompareDiskSizeTotalInMB", 1024);
		
		this.srcProps=getConnectProps("source");
		this.destProps=getConnectProps("destination");
	
		if(allTables.equalsIgnoreCase("yes")) {
			ConArgs source = new ConArgs();
			source.props=srcProps;
			tableProps=source.getTableProps();
		}else {
			tableProps=new ArrayList<>();
			for(int i=1; i< 1000; i++) {
				String section="table-"+i;
				if(!file.containsSection(section))
					break;
				tableProps.add(getTableProps(section));
			}
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
