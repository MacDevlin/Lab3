import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;


public class DistFile implements Serializable {
	String filename;
	int part;
	public int numBytes;
	DataInputStream data;
	public String path;
	public DistFile(String path, String filename, int part, List<String> lines, int startLine, int numLines) throws FileNotFoundException {
		this.part = part;
		this.path = path;
		this.filename = filename + "_" + part;
		File f = new File(this.path + this.filename);
		PrintStream fo = new PrintStream(f);
		numBytes = 0;
		for(int l = startLine; l<startLine+numLines; l++) {
			fo.println(lines.get(l));
			numBytes += lines.get(l).getBytes().length + 1;//this might fail on windows.
		}
		fo.close();
		
	}
	
	//returns file size in bytes
	public int getSize() {
		return numBytes;
	}
	
	public void openFile() throws FileNotFoundException {
		if(data == null) {
			data = new DataInputStream(new FileInputStream(path+filename));
		}
	}
	//asumes no changes to file size ever
	public byte[] getBytes() throws IOException {
		openFile();
		byte[] b = new byte[numBytes];
		data.readFully(b);
		return b;
	}
	
	public String getNextLine(int lineNum) throws IOException {
		if(data == null) {
			openFile();
		}
		return data.readLine();
		
	}
	
}
