package ozgurdbsync;

import org.junit.Test;

import junit.framework.Assert;

public class IniTest {


	@Test
	public void testParse() throws Exception{
		Ini ini=new Ini("/home/rompg/workspace/pgdatasynctr/src/test/resources/load-test.ini");
		Assert.assertEquals("hsqldb", ini.sqlEngine);
	}

}	


