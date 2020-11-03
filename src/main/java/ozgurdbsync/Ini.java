package ozgurdbsync;

import java.io.IOException;



public class Ini {
	private IniFile file;

	String sqlEngine;
	ConnectProps srcProps;
	ConnectProps destProps;
	public Ini(String fn) throws IOException {
		this.file=new IniFile(fn);
		this.sqlEngine=file.getString("general", "sqlEngine", "hsqldb");
		
		this.srcProps=getConnectProps("source");
		this.destProps=getConnectProps("destination");
	}

	private ConnectProps getConnectProps(String section) {
		ConnectProps props = new ConnectProps();
		props.url=file.getString(section, "url", null);
		props.user=file.getString(section, "user", null);
		props.password=file.getString(section, "password", null);
		return props;
	}

}
