
public class MapRequest {
	public Connection requester;
	public String filename;
	public int startRec;
	public int recNum;
	public byte[] funcData;
	public boolean isMap;
	public MapRequest(boolean isMap, Connection requester, String filename, int start, int num, byte[] funcData) {
		this.isMap = isMap;
		this.requester = requester;
		this.filename = filename;
		this.startRec = start;
		this.recNum = num;
		this.funcData = funcData;
	}
}
