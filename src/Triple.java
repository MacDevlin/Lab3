
public class Triple implements IFunction {
	public String execute(String o) {
		Integer i = Integer.parseInt(o);
		i *= 3;
		return Integer.toString(i);
	}
}
