import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cluster.CentroidRunJob;

import preData.CreatePoint;
import preData.UniqueNidJob;

public class MainDriver {

	private Map<String,Class<?>> classMap = new HashMap<String,Class<?>>();

	public MainDriver() {

		add("uniqueNid", UniqueNidJob.class);
		add("createVector", CreatePoint.class);
		add("centroid", CentroidRunJob.class);
	}

	public static void main (String[] args) throws Exception {
		MainDriver driver = new MainDriver();
		String className = args[0];
		if (args[0].equalsIgnoreCase("--help") || args[0].equalsIgnoreCase("-h")) {
			driver.printHelp();
			System.exit(0);
		}
		if (!driver.find(className)) {
			System.out.println("do not have this class");
			System.exit(0);
		}
		else {
			String[] newArgs = new String[args.length-1];
			for (int i=1; i<args.length; i++) {
				newArgs[i-1] = args[i];
			}
			ToolRunner.run((Tool) driver.getClass(className).newInstance(), newArgs);

		}
	}
	public void add(String className,Class<?> cls) {
		classMap.put(className, cls);

	}

	public Class<?> getClass(String className) {
		return classMap.get(className);
	}
	public void printHelp() {
		for (Map.Entry<String, Class<?>> entry: classMap.entrySet()) {
			System.out.println(entry.getKey());
		}

	}

	public boolean find (String key) {
		if (classMap.containsKey(key))
			return true;
		else
			return false;
	}



}