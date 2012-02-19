package my;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import util.TextToSequence;

public class Driver {
	
private Map<String,Class<?>> classMap = new HashMap<String,Class<?>>();
	
	public void add(String className,Class<?> cls) {
		classMap.put(className, cls);
		
	}
	
	public boolean find (String key) {
		if (classMap.containsKey(key))
			return true;
		else
			return false;
	}

	public void run(String className, String[] args) {
		Class<?>[] parameterTypes = new Class<?>[] {String[].class};
		try {
			classMap.get(className).getMethod("main", parameterTypes).invoke(null, new Object[]{args});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		Driver runDriver = new Driver();
		runDriver.add("TextToSequence", TextToSequence.class);
		
		String className = args[0];
		if (!runDriver.find(className)) {
			System.out.println("do not have this class");
			System.exit(0);
		}
		else {
			String[] newArgs = new String[args.length-1];
			for (int i=1; i<args.length; i++) {
				newArgs[i-1] = args[i];
			}
			runDriver.run(className, newArgs);
		}

	}

}
