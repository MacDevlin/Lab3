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
	DataInputStream data;
	
	public DistFile(String filename, int part, List<String> lines, int startLine, int numLines) throws FileNotFoundException {
		this.part = part;
		this.filename = "/tmp/" + filename + "_" + part;
		File f = new File(this.filename);
		PrintStream fo = new PrintStream(f);
		for(int l = startLine; l<startLine+numLines; l++) {
			fo.println(lines.get(l));
		}
		fo.close();
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
