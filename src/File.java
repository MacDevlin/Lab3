import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;


public class File implements Serializable {
	String ip;
	int port;
	int localFileId;
	String filename;
	DataInputStream data;
	public File(String filename, String ip, int port, int id) {
		this.localFileId = id;
		this.port = port;
		this.ip = ip;
		this.filename = filename;
	}
	
	public void openFile() throws FileNotFoundException {
		if(data == null) {
			data = new DataInputStream(new FileInputStream(filename));
		}
	}
	
	public String getNextLine(int lineNum) throws IOException {
		if(data == null) {
			openFile();
		}
		return data.readLine();
		
	}
	
}
