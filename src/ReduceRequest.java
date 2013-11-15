import java.util.ArrayList;
import java.util.List;


public class ReduceRequest {
	String[] results;
	int resultNum;
	Reduce func;
	int caller;
	public ReduceRequest(int caller, int rep, Reduce func) {
		this.caller = caller;
		results = new String[rep];
		resultNum=0;
		this.func = func;
	}
}
