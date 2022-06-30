package ozgurdbsync;

public class ConnectProps {
	public String url;
	public String user;
	public String password;

	public ConnectProps() {
	}
	
	public boolean isPostgres() {
		return url.indexOf("postgres")>=0;
	}
}