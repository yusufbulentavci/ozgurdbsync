package ozgurdbsync;

import java.util.ArrayList;
import java.util.Collection;

public class PkeyValue extends ArrayList<Object>{

	public PkeyValue() {
		super();
	}

	public PkeyValue(Collection<? extends Object> c) {
		super(c);
	}

//	public PkeyValue(int initialCapacity) {
//		super(initialCapacity);
//	}
	
	public PkeyValue(Object... vals) {
		super();
		for (Object object : vals) {
			add(object);
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

}
