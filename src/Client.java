import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Client {

	public static Timestamp freshness = null;
	public static void main(String[] args){
		freshness = new Timestamp(System.currentTimeMillis());
		

		BlockingQueue<Task> queue = new ArrayBlockingQueue<Task>(10000);
		
		final Object lock = new Object();
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// this is just to check the staleness probably we don't even need it. 
		QueryRequestRunnable queryRequestRunnable = new QueryRequestRunnable();
		new Thread(queryRequestRunnable).start();
		
		
		int numberOfExperiment = Integer.parseInt(prop.getProperty("no_of_experiment"));
		
		for(int i=1; i<=numberOfExperiment; i++) {
			String experiment = prop.getProperty("experiment_"+i);
			String experimentParameter[] = experiment.split("_");
			long coordinatorSleepDuration = Long.parseLong(experimentParameter[1]);
			int threadSize = Integer.parseInt(experimentParameter[3]);
			long runDuration = Long.parseLong(experimentParameter[5])*60000L;
			long sessionEndTime = System.currentTimeMillis()+runDuration;
			
			
			
			ArrayList<Thread> threads = new ArrayList<Thread>();
			
			
//			create a coordinator thread
			CoordinatorRunnable coordinator = new CoordinatorRunnable(i,queue, coordinatorSleepDuration, sessionEndTime);
			threads.add(new Thread(coordinator));
			
			System.out.println("Running experiment: "+ experiment +
								", Coordinator Sleep Duration: " +	coordinatorSleepDuration +
								", Thread size: " + threadSize +
								", Run Duration: " + runDuration);

			
//			create a number of worker thread to read updates
			for(int j=0; j<threadSize; j++){
				threads.add(new Thread(new WorkerRunnable(i*j, queue, sessionEndTime, lock)));
			}
			
			for(int j=0; j<threads.size(); j++){
				threads.get(j).start();
			}
			
			try {
				Thread.sleep(runDuration);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}		

		System.out.println("program ends");
		
		System.exit(0);
		
		

		
		
		
	}
	
	public static Connection getConnection(){
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String user = prop.getProperty("user");
		String password = prop.getProperty("password");
		String url = prop.getProperty("url");
		
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url, connectionProps);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
}
