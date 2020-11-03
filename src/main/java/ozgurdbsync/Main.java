package ozgurdbsync;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class Main {

	public static void main(String[] args) throws IOException, SQLException {
		if(args.length!=1)
			throw new RuntimeException("Usage: java -cp ./ozgurdbsynch.jar ozgurdbsync/Main path.ini.file");
		
		Ini ini=new Ini(args[0]);
		
		
		
		System.out.println("--BEGIN");
		for(int i=0; i<ini.tableProps.size(); i++) {
			TableProps tp=ini.tableProps.get(i);
			ConArgs source=new ConArgs();
			source.props=ini.srcProps;
			source.schema=tp.schema;
			source.table=tp.table;

			System.out.println("--Schema comparison table:"+source.toFullTable());
			ConArgs dest=new ConArgs();
			dest.props=ini.destProps;
			dest.schema=tp.schema;
			dest.table=tp.table;
			
			
			Con s=new Con(source);
			s.initMeta();
			Con d=new Con(dest);
			d.initMeta();
			s.compareSchema(d);
			System.out.println("--Schema comparison success");
			
			s.getData();
			d.getData();
			
			System.out.println("--Deletes");
			List<PkeyValue> ret=s.diffDel(d);
			for (PkeyValue pkeyValue : ret) {
				String delStr=s.toSqlDelete(pkeyValue);
				System.out.println(delStr);
			}
			System.out.println("--Updates");
			ret=s.diffUpdate(d);
			for (PkeyValue pkeyValue : ret) {
				String updateStr=d.toSqlUpdate(pkeyValue);
				System.out.println(updateStr);
			}
			
			System.out.println("--Inserts");
			ret=s.diffInsert(d);
			for (PkeyValue pkeyValue : ret) {
				String insertStr=d.toSqlInsert(pkeyValue);
				System.out.println(insertStr);
			}
			
			System.out.println("--Diff data success");
			
			
			s.disconnect();
			d.disconnect();
		}
		
		System.out.println("--END SUCCESS");
		
		
	}

}
