import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class CoordinatorRunnable implements Runnable, Config {
	private Connection conn = null;
	private PreparedStatement stmt = null;
	private ResultSet rs = null;
	public long maxSeqID = 0L;
	private BlockingQueue<Task> queue = null;
	private long sessionEndTime = 0L;
	private String tableNames = null;
	private long sleepDuration = 0L;
	private int threadID = 0;

	public CoordinatorRunnable(int threadID, BlockingQueue<Task> queue,long sleepDuration, long sessionEndTime) {
		this.threadID = threadID;
		this.queue = queue;
		this.sessionEndTime = sessionEndTime;
		this.sleepDuration = sleepDuration;
		try {
			conn = Client.getConnection();
			String query = "select max(event_id) from audit.logged_actions where " + "table_name in("
					+ tables.get(System.getProperty("tables")) + ")";
			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			rs.next();
			maxSeqID = rs.getLong(1);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}


	}

	@Override
	public void run() {
		System.out.println("Coordinator : "+ threadID + " Starts");
		while (sessionEndTime > System.currentTimeMillis()) {
			try {
				Thread.sleep(sleepDuration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			try {
				rs = stmt.executeQuery();
				rs.next();
				long tmpMaxSeqID = rs.getLong(1);
				if (tmpMaxSeqID > maxSeqID) {
					queue.put(new Task(maxSeqID, tmpMaxSeqID));
					maxSeqID = tmpMaxSeqID;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
		System.out.println("Coordinator : "+ threadID + " Ends");
	}



}
