import java.util.ArrayList;
import java.util.List;


public class FileSystem implements Runnable{

	List<File> files;
	String ip;
	int port;
	boolean isMaster;
	CommunicationServer cServer;
	public FileSystem(String ip, int port, CommunicationServer cServer, boolean isMaster) {
		files = new ArrayList<File>();
		this.ip = ip;
		this.port = port;
		this.cServer=cServer;
		this.isMaster = isMaster;
	}
	
	public File createFile(String filename) {
		int id = files.size();
		File f = new File(filename, ip, port, id);
		files.add(f);
		return f;
	}
	
	public File makeLocal(File f) {
		return null;
	}
	
	public void run() {
		if(isMaster) {
			//wait for all participants, then initialize all files
		}
	}
}
