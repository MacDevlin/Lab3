
public class MapRequest {
	public Connection requester;
	public int id;
	public String filename;
	public int startRec;
	public int recNum;
	public byte[] funcData;
	public boolean isMap;
	public int completeParts;
	public int totalParts;
	public MapRequest(boolean isMap, Connection requester, int id, String filename, int start, int num, int totalParts, byte[] funcData) {
		this.isMap = isMap;
		this.requester = requester;
		this.id = id;
		this.filename = filename;
		this.startRec = start;
		this.recNum = num;
		this.funcData = funcData;
		this.totalParts = totalParts;
		this.completeParts = 0;
	}
}
