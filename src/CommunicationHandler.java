import java.io.IOException;


public class CommunicationHandler implements Runnable {

	public CommunicationServer cServer;
	public volatile boolean running;
	public CommunicationHandler(CommunicationServer cServer) {
		this.cServer = cServer;
		running=false;
	}
	
	public void processRequest(String request) {
		if(request.startsWith("MAP")) {
			System.out.println("MAP REQUEST");
			
		}
		else if(request.startsWith("REDUCE")) {
			System.out.println("REDUCE REQUEST");
		}
	}
	
	public void run() {
		running = true;
		
		while(running) {
			int c = cServer.hasData();
			if(c!=-1) {
				//handle communication with connection c
				Connection con = cServer.connections.get(c);
				String line;
				try {
					line = con.getLine();
				} catch (IOException e) {
					continue; //this is weird.
				}
				processRequest(line);
			}
		}
	}
}
