import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.Date;

public class QueryRequestRunnable implements Runnable,Config{
	private Writer out;
	private long sessionNextTimeStamp = 0L;
	private long totalStaleness =0L;
	private long count = 0L;
	private long sessionStartTime = 0L;

	public QueryRequestRunnable() {
		sessionStartTime = System.currentTimeMillis();
		String stalenessFileName = "staleness_" + dateFormat.format(new Date());
	
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stalenessFileName, true), "UTF-8"));
			
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void run() {
		sessionNextTimeStamp = System.currentTimeMillis()+60000;
		while(true) {
			count++;
			try {
				long staleness = System.currentTimeMillis() - Client.freshness.getTime();
				totalStaleness += staleness;
				long avgStaleness = totalStaleness/ count;
				out.append((System.currentTimeMillis() - sessionStartTime) +","+avgStaleness+","+ staleness+"\n");
				if(System.currentTimeMillis()>sessionNextTimeStamp) {
					sessionNextTimeStamp +=60000;
					out.flush();
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
					//have a file to record staleness
					//buffer the staleness into a list, flush the list periodically to the on-disk file
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
