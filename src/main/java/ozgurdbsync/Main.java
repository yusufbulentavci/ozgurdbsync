package ozgurdbsync;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Main {

//	public static List<String> ordered = new ArrayList<>();

	public static void main(String[] args) throws IOException, SQLException {
		if (args.length != 1)
			throw new RuntimeException("Usage: java -cp ./ozgurdbsynch.jar ozgurdbsync/Main path.ini.file");

		Ini ini = new Ini(args[0]);

		Map<String, TableProps> tableProps = new HashMap<String, TableProps>();

		Map<String, List<String>> deps = new HashMap<String, List<String>>();
		for (int i = 0; i < ini.tableProps.size(); i++) {
			TableProps tp = ini.tableProps.get(i);
			ConArgs source = new ConArgs();
			source.props = ini.srcProps;
			source.schema = tp.schema;
			source.table = tp.table;
			String tn = source.toFullTable();
			tableProps.put(tn, tp);

			Con s = new Con(source);
			s.initMeta();
			List<String> d = s.getDepends();
			deps.put(tn, d);
		}

		for (Entry<String, List<String>> e : deps.entrySet()) {
			System.out.println("-- "+e.getKey() + "->" + e.getValue().toString());
		}

//		boolean ready = false;
//		for (int i = 0; i < 10000; i++) {
//			if (ordered.size() == deps.size()) {
//				ready = true;
//				break;
//			}
//			for (Entry<String, List<String>> e : deps.entrySet()) {
//				String tn = e.getKey();
//				if (ordered.contains(tn)) {
//					continue;
//				}
//				List<String> dps = e.getValue();
//				if (dps.size() == 0) {
//					ordered.add(tn);
//					continue;
//				}
//				boolean notReady = false;
//				for (String string : dps) {
//					if (deps.containsKey(string))
//						continue;
//					if (ordered.contains(string)) {
//						continue;
//					}
//					notReady = true;
//				}
//				if (notReady)
//					continue;
//				ordered.add(tn);
//			}
//		}

		System.out.println("--BEGIN");
		Set<String> processed = new HashSet<>();
		boolean success=false;
		for (int i = 0; i < 1000; i++) {
			if (processed.size() == tableProps.size()) {
				success=true;
				break;
			}

			for (Entry<String, TableProps> tpe : tableProps.entrySet()) {
				String tpname = tpe.getKey();
//				System.out.println(tpname);
				if (processed.contains(tpname)) {
					continue;
				}
				List<String> weneed = deps.get(tpname);
				boolean notFound = false;
				if (weneed != null) {
					for (String str : weneed) {
						if (!processed.contains(str)) {
							notFound = true;
							break;
						}
					}
				}
				if (notFound)
					continue;

				processed.add(tpname);

				TableProps tp = tpe.getValue();

				ConArgs source = new ConArgs();
				source.props = ini.srcProps;
				source.schema = tp.schema;
				source.table = tp.table;

				System.out.println("--Schema comparison table:" + source.toFullTable());
				ConArgs dest = new ConArgs();
				dest.props = ini.destProps;
				dest.schema = tp.schema;
				dest.table = tp.table;

				Con s = new Con(source);
				s.initMeta();
				Con d = new Con(dest);
				d.initMeta();
				String cs = s.compareSchema(d);
				if (cs != null) {
					System.out.println(cs);
					System.exit(-1);
				}
				System.out.println("--Schema comparison success");

				s.getData();
				d.getData();

				System.out.println("--Deletes");
				List<PkeyValue> ret = s.diffDel(d);
				for (PkeyValue pkeyValue : ret) {
					String delStr = s.toSqlDelete(pkeyValue);
					System.out.println(delStr);
				}
				System.out.println("--Updates");
				ret = s.diffUpdate(d);
				for (PkeyValue pkeyValue : ret) {
					String updateStr = d.toSqlUpdate(pkeyValue);
					System.out.println(updateStr);
				}

				System.out.println("--Inserts");
				ret = s.diffInsert(d);
				for (PkeyValue pkeyValue : ret) {
					String insertStr = d.toSqlInsert(pkeyValue);
					System.out.println(insertStr);
				}

				System.out.println("--Diff data success");

				s.disconnect();
				d.disconnect();
			}
		}
		if(success) {
			System.out.println("--END SUCCESS");
		}else {
			System.out.println("--FAILED; CYCLIC DEPENDENCY");
		}


	}

}
