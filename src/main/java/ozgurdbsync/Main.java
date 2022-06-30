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
//		
//		if (args.length != 1)
//			throw new RuntimeException("Usage: java -cp ./ozgurdbsynch.jar ozgurdbsync/Main path.ini.file");
//
//		Ini ini = new Ini(args[0]);
		Ini ini = new Ini("/home/ybavci/workspace/ozgurdbsync/src/test/resources/altin_test.ini");

		Map<String, TableProps> tableProps = new HashMap<String, TableProps>();

		Map<String, List<String>> deps = new HashMap<String, List<String>>();
		for (int i = 0; i < ini.tableProps.size(); i++) {
			TableProps tp = ini.tableProps.get(i);
			ConArgs source = new ConArgs();
			source.props = ini.srcProps;
			source.schema = tp.schema;
			source.table = tp.table;
//			System.out.println("--" + source.toFullTable());
			String tn = source.toFullTable();
			tableProps.put(tn, tp);

//			System.out.println("--"+source.toFullTable());
			Con s = new Con(source);
			s.initMeta();
			List<String> d = s.getDepends();
			deps.put(tn, d);
		}

//		for (Entry<String, List<String>> e : deps.entrySet()) {
//			System.out.println("-- "+e.getKey() + "->" + e.getValue().toString());
//		}

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
		boolean success = false;
		boolean ignoreDependency = false;
		for (int i = 0; i < 11000; i++) {
//			System.out.print(".");
			if (processed.size() == tableProps.size()) {
				success = true;
				break;
			}

			for (Entry<String, TableProps> tpe : tableProps.entrySet()) {
				String tpname = tpe.getKey();
				if (processed.contains(tpname)) {
//					System.out.println("-- already processed; continue");
					continue;
				}

				if (!ignoreDependency) {
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
				}
				processed.add(tpname);

				System.out.println("--------------------------------------------------");
				System.out.println("--" + tpname);

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

				System.out.println("--Estimated Source row size:" + s.getRowSize());
				System.out.println("--Estimated Destination row size:" + d.getRowSize());
//				

				System.out.println("--Source disk size(MB):" + s.getOnDiskSize());
				System.out.println("--Destination disk size(MB):" + d.getOnDiskSize());

				System.out.println("--Schema comparison begin");
				String cs = s.compareSchema(d);
				if (cs != null) {
					System.out.println(cs);
					continue;
				}
				System.out.println("--Schema comparison success");
				if (!(s.hasPrimKeys() && d.hasPrimKeys())) {
					System.out.println("--No primary keys:" + source.toFullTable() + " Bypassing");
					continue;
				}
				System.out.println("--Compare row counts");
				System.out.println("--Source row count:" + s.getRowCount());
				System.out.println("--Destination row count:" + d.getRowCount());

//				if (s.getRowCount() == null || d.getRowCount() == null) {
//					System.out.println("--Table row count couldnt be fetched; Bypassing this table");
//					continue;
//				}
//
//				if (s.getRowCount() > ini.dataCompareMaxRow || d.getRowCount() > ini.dataCompareMaxRow) {
//					System.out.println("--Source or destination table row count over limit ; Bypassing this table");
//					continue;
//				}

				double inMB = s.getOnDiskSize() + d.getOnDiskSize();
				System.out.println("--Total disk size need:" + inMB);
				if (inMB > ini.dataCompareDiskSizeLimitInMB) {
					System.out.println("--Memory need over limit ; Bypassing this table");
					continue;
				}

				System.out.println("--Getting data");
				s.getData();
				d.getData();
				System.out.println("--Getting data; done");

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

				System.out.println("--------------------------------------------------");
//				s.disconnect();
//				d.disconnect();
			}
			if (i == 9998) {
				System.out.println("----------------------------------------------------");
				System.out.println("----------------------------------------------------");
				System.out.println("--We will ignore dependency then");
				System.out.println("--FAILED; CYCLIC DEPENDENCY");
				System.out.println("--These are tables left...");
				ignoreDependency = true;
				for (String string : tableProps.keySet()) {
					if (!processed.contains(string)) {
						System.out.println("--" + string);
					}
				}
			}
		}
		if (success) {
			System.out.println("--END SUCCESS");
		} else {
			System.out.println("--END FAILED");

		}

	}

}
