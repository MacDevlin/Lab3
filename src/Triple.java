
public class Triple implements IFunction {
	public void execute(String o) {
		Integer i = Integer.parseInt(o);
		i *= 3;
		o = Integer.toString(i);
	}
}
