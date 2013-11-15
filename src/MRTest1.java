import java.io.IOException;

public class MRTest1 implements Runnable {
	public FourFortyMapReduce MR;
	public String filename;
	public int startRec;
	public int numRecs;

	public MRTest1(FourFortyMapReduce MR, String[] args) {
		this.MR = MR;
		this.filename = args[0];
		startRec = Integer.parseInt(args[1]);
		numRecs = Integer.parseInt(args[2]);
	}

	public void run() {
		System.out.println("Running MRTest1");

		// map triple across a.txt 0 -> 5
		Map triple = (Map) new Triple();
		Reduce sum = (Reduce) new Sum();
		try {
			MR.map(filename, startRec, numRecs, triple);
			int answer = Integer.parseInt(MR.reduce(filename, startRec,numRecs, sum));
			System.out.println("Sum of the digits from " + startRec + " to " + (startRec + numRecs - 1) + ": " + answer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

	}
}
