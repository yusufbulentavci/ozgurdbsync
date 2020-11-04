package ozgurdbsync;

import java.sql.*;
import java.util.*;

public class Con {
	final ConArgs args;
	final Connection db;
	List<String> primKeys = new ArrayList<>();
	Map<String, ColumnInfo> columns = new TreeMap<>();
	List<ColumnInfo> orderedColumns = new ArrayList<>();
	Map<PkeyValue, List<Object>> data = new HashMap<PkeyValue, List<Object>>();

	public Con(ConArgs a) throws SQLException {
		this.args = a;
		db = a.getCon();
	}

	public void initMeta() throws SQLException {
		DatabaseMetaData meta;
		meta = db.getMetaData();
		ResultSet columns = meta.getColumns(null, args.schema, args.table, null);
		int ind = 0;
		while (columns.next()) {
			String columnName = columns.getString("COLUMN_NAME");
			String datatype = columns.getString("DATA_TYPE");
			String columnsize = columns.getString("COLUMN_SIZE");
			String decimaldigits = columns.getString("DECIMAL_DIGITS");
			boolean isNullable = columns.getString("IS_NULLABLE").equals("YES");
			boolean is_autoIncrment = columns.getString("IS_AUTOINCREMENT").equals("YES");
			// Printing results
//			String str = datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---"
//					+ is_autoIncrment;
			ColumnInfo ci = new ColumnInfo(ind++, columnName, Integer.parseInt(datatype), is_autoIncrment, isNullable);
			this.columns.put(columnName, ci);
			this.orderedColumns.add(ci);

		}
		columns.close();

		ResultSet prims = meta.getPrimaryKeys(null, args.schema, args.table);
		while (prims.next()) {
			String cn = prims.getString("COLUMN_NAME");
			primKeys.add(cn);
		}
		prims.close();
	}

	public String compareSchema(Con other) {

		String t = columns.toString();
		String o = other.columns.toString();

		if (!t.equals(o)) {
			System.err.println("Columns not match:" + t + "<->" + o);
			System.exit(-1);
		}

		t = String.join(",", primKeys);
		o = String.join(",", other.primKeys);
		if (!t.equals(o)) {
			System.err.println("Primary keys not match:" + t + "<->" + o);
			System.exit(-2);
		}

		return null;
	}

	public void getData() throws SQLException {
		String query = "select * from ";
		if (this.args.schema != null) {
			query = query + args.schema + ".";
		}
		query += args.table;

		PreparedStatement ps = db.prepareStatement(query);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			PkeyValue pv = new PkeyValue();
			for (String pk : primKeys) {
				Object pvp = rs.getObject(pk);
				pv.add(pvp);
			}

			String pri = null;
			List<Object> row = new ArrayList<Object>();
			for (int i = 0; i < orderedColumns.size(); i++) {
				ColumnInfo ci = orderedColumns.get(i);
				Object o = rs.getObject(ci.name);
				row.add(o);
			}
			data.put(pv, row);
		}

	}

	public List<PkeyValue> diffDel(Con con) {
		List<PkeyValue> ret = new ArrayList<>();
		String pri = primKeys.get(0);
		for (PkeyValue okey : data.keySet()) {
			if (con.data.containsKey(okey)) {
				continue;
			}
			ret.add(okey);
		}

		return ret;
	}

	public List<PkeyValue> diffInsert(Con con) {
		List<PkeyValue> ret = new ArrayList<>();
		String pri = primKeys.get(0);
		for (PkeyValue dkey : con.data.keySet()) {
			if (data.containsKey(dkey)) {
				continue;
			}
			ret.add(dkey);
		}

		return ret;
	}

	public List<PkeyValue> diffUpdate(Con con) {
		List<PkeyValue> ret = new ArrayList<>();
		for (PkeyValue dkey : con.data.keySet()) {
			if (data.containsKey(dkey)) {
				if (isDiffent(this.orderedColumns, this.data.get(dkey), con.orderedColumns, con.data.get(dkey)))
					ret.add(dkey);
			}
		}

		return ret;
	}

	public boolean isDiffent(List<ColumnInfo> oc, List<Object> odata, List<ColumnInfo> dc, List<Object> ddata) {
		// this is not an update
		if (odata == null || ddata == null)
			return false;

		for (int i = 0; i < oc.size(); i++) {
			ColumnInfo of = oc.get(i);
			Object od = odata.get(i);
			for (int j = 0; j < dc.size(); j++) {
				ColumnInfo df = dc.get(j);
				if (df.name.equals(of.name)) {
					Object dd = ddata.get(j);
					if (!java.util.Objects.equals(od, dd)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public String toSqlInsert(PkeyValue pkv) {
		List<Object> d = data.get(pkv);
		if (d == null)
			throw new RuntimeException("Unexpected state");

		StringBuilder cls = new StringBuilder();
		StringBuilder vls = new StringBuilder();
		cls.append("insert into ");
		cls.append(args.toFullTable());
		cls.append("(");
		for (int i = 0; i < orderedColumns.size(); i++) {
			ColumnInfo ci = orderedColumns.get(i);
			if (i != 0) {
				cls.append(",");
				vls.append(",");
			}
			cls.append(ci.name);
			Object o = d.get(i);
			if (o == null) {
				vls.append("null");
			} else {
				vls.append(ci.toSqlValue(o));
			}
		}
		cls.append(") values(");
		cls.append(vls);
		cls.append(");");

		return cls.toString();
	}

	public String toSqlUpdate(PkeyValue pkv) {
		List<Object> d = data.get(pkv);
		if (d == null)
			throw new RuntimeException("Unexpected state");

		StringBuilder cls = new StringBuilder();
		StringBuilder vls = new StringBuilder();
		cls.append("update  ");
		cls.append(args.toFullTable());
		cls.append(" set ");
		boolean andW = false;
		boolean virS = false;
		for (int i = 0; i < orderedColumns.size(); i++) {
			ColumnInfo ci = orderedColumns.get(i);
			Object o = d.get(i);
			if (primKeys.contains(ci.name)) {
				if (andW) {
					vls.append(" and ");
				} else {
					andW = true;
				}
				vls.append(ci.name);
				if (o == null) {
					vls.append("is null");
				} else {
					vls.append("=");
					vls.append(ci.toSqlValue(o));
				}

			} else {
				if (virS) {
					cls.append(" , ");
				} else {
					virS = true;
				}
				cls.append(ci.name);
				cls.append("=");
				if (o == null) {
					cls.append("null");
				} else {
					cls.append(ci.toSqlValue(o));
				}
			}

		}
		cls.append(" where ");
		cls.append(vls);
		cls.append(";");

		return cls.toString();
	}

	public String toSqlDelete(PkeyValue pkv) {
		List<Object> d = data.get(pkv);
		if (d == null)
			throw new RuntimeException("Unexpected state");

		StringBuilder cls = new StringBuilder();
		cls.append("delete from  ");
		cls.append(args.toFullTable());
		cls.append(" where ");
		boolean andW = false;
		for (int i = 0; i < orderedColumns.size(); i++) {
			ColumnInfo ci = orderedColumns.get(i);
			Object o = d.get(i);
			if (primKeys.contains(ci.name)) {
				if (andW) {
					cls.append(" and ");
				} else {
					andW = true;
				}
				cls.append(ci.name);
				if (o == null) {
					cls.append("is null");
				} else {
					cls.append("=");
					cls.append(ci.toSqlValue(o));
				}

			}

		}
		cls.append(";");

		return cls.toString();
	}

	public void disconnect() {
		try {
			if (db != null)
				db.close();
		} catch (Exception e) {
		}

	}

}
