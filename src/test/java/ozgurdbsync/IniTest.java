package ozgurdbsync;

import org.junit.Ignore;
import org.junit.Test;

import junit.framework.Assert;

@Ignore
public class IniTest {


	@Test
	public void testParse() throws Exception{
		Ini ini=new Ini("/home/ybavci/workspace/ozgurdbsync/src/test/resources/load-test.ini");
		Assert.assertEquals("hsqldb", ini.sqlEngine);
	}

	
	@Test
	public void testTables() throws Exception{
		Ini ini=new Ini("/home/ybavci/workspace/ozgurdbsync/src/test/resources/load-test.ini");
		Assert.assertEquals(1, ini.tableProps.size());
	}

}	


