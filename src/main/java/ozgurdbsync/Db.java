package ozgurdbsync;

import java.sql.*;
import java.util.*;


public class Db{
	final ConArgs srcArgs;
	final ConArgs destArgs;

	final Con src;
	final Con dest;


	public Db(ConArgs srcArgs, ConArgs destArgs) throws SQLException{
		this.srcArgs=srcArgs;
		this.destArgs=destArgs;

		this.src=new Con(srcArgs);
		this.dest=new Con(destArgs);
	}
}

