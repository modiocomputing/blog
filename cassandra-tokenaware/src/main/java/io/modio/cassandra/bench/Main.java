package io.modio.cassandra.bench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;


public class Main {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	private static final String QUERY = 
		"INSERT INTO tuples (client, sensor, time, value) "
		+ "VALUES (?, ?, ?, ?)";
	
	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().
			addContactPoint("cassandra1").
			addContactPoint("cassandra2").
			addContactPoint("cassandra3").
			addContactPoint("cassandra4").
			build();
		cluster.getConfiguration().getQueryOptions().setConsistencyLevel(
			ConsistencyLevel.ONE);
		Session session = cluster.connect("bench");
		
		PreparedStatement prepared = session.prepare(QUERY);

		//new SyncSingleExecutor(session, prepared).execute();
		new SyncTokenAwareBatchExecutor(session, prepared).execute();
		new AsyncSingleExecutor(session, prepared).execute();
		new SyncBatchExecutor(session, prepared).execute();
		
		session.close();
		cluster.close();
	}
}
