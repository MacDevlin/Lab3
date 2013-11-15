
public class MapRequest {
	public Connection requester;
	public String filename;
	public int startRec;
	public int recNum;
	public byte[] funcData;
	public MapRequest(Connection requester, String filename, int start, int num, byte[] funcData) {
		this.requester = requester;
		this.filename = filename;
		this.startRec = start;
		this.recNum = num;
		this.funcData = funcData;
	}
}
