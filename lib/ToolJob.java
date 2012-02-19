package lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

public abstract class ToolJob implements Tool{

	private Configuration conf;
	@Override
	public void setConf(Configuration conf) {
		this.conf=conf;

	}

	@Override
	public Configuration getConf() {
		return conf;
	}


}