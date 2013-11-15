import java.util.ArrayList;
import java.util.List;


public class ReduceRequest {
	String[] results;
	int resultNum;
	Reduce func;
	int caller;
	public int id;
	public ReduceRequest(int caller, int id, int rep, Reduce func) {
		this.caller = caller;
		this.id = id;
		results = new String[rep];
		resultNum=0;
		this.func = func;
	}
}
