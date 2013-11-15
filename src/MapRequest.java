
public class MapRequest {
	public Connection requester;
	public int id;
	public String filename;
	public int startRec;
	public int recNum;
	public byte[] funcData;
	public boolean isMap;
	public MapRequest(boolean isMap, Connection requester, int id, String filename, int start, int num, byte[] funcData) {
		this.isMap = isMap;
		this.requester = requester;
		this.id = id;
		this.filename = filename;
		this.startRec = start;
		this.recNum = num;
		this.funcData = funcData;
	}
}
