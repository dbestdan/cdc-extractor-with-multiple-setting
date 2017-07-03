import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerRunnable implements Runnable, Config {
	private BlockingQueue<Task> queue = null;
	private Writer out = null;
	private int threadID = 0;
	private Object lock = null;
	private long sessionEndTime = 0L;

	public WorkerRunnable(int threadID, BlockingQueue<Task> queue, long sessionEndTime, Object lock) {
		this.queue = queue;
		this.sessionEndTime = sessionEndTime;
		this.lock = lock;
		String fileName = "chunk" + threadID;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}

		this.threadID = threadID;
	}

	@Override
	public void run() {
		Connection conn = Client.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ResultSet commitTimeResultSet = null;
		try {
			String query = "select *, pg_xact_commit_timestamp(transaction_id::text::xid) from audit.logged_actions "
					+ "where event_id > ? and event_id <= ? " + "and table_name in ("
					+ tables.get(System.getProperty("tables")) + ")";
			stmt = conn.prepareStatement(query);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {
			System.out.println("Worker : "+ threadID + " Starts");
			while (sessionEndTime> System.currentTimeMillis()) {
				Task task = queue.take();

				stmt.setLong(1, task.getMinSeqID());
				stmt.setLong(2, task.getMaxSeqID());
				rs = stmt.executeQuery();

				while (rs.next()) {

					// write to a file
					writeLocalFile(rs);

					// get transaction commit timestamp from transaction id
					Timestamp t = rs.getTimestamp(18);

					// if commit timestamp is latest then update uptodate time
					if (Client.freshness == null || Client.freshness.before(t)) {
						Client.freshness = t;
						synchronized (lock) {
							lock.notifyAll();
						}
					}
				}

			}
			System.out.println("Worker : "+ threadID + " Ends");

		} catch (InterruptedException | SQLException e) {
			e.printStackTrace();

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				out.close();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void writeLocalFile(ResultSet rs) {

		try {
			StringBuffer sb = new StringBuffer();
			for (int i = 1; i < 18; i++) {
				sb.append(rs.getString(i) + "|");
			}
			sb.append("\n");
			out.append(sb.toString());
			out.flush();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

}
