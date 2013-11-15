

public class Sum implements Reduce {
	public String execute(String s1, String s2) {
		Integer i = Integer.parseInt(s1);
		Integer j = Integer.parseInt(s2);
		
		return Integer.toString(i+j);
	}
}
