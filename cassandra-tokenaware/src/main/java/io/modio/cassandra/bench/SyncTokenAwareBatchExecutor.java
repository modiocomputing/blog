package io.modio.cassandra.bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;

public class SyncTokenAwareBatchExecutor extends AbstractExecutor {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(
		SyncTokenAwareBatchExecutor.class);

	private static final int PROCESS_BATCH = 32000;

	private static final int RF = 3;
	
	private List<Statement> statements;
	
	private List<List<Statement>> splitByRoute(Cluster cluster, 
		List<Statement> batch) {
		if (batch == null || batch.size() == 0) {
			return null;
		}
		
		Map<Set<Host>,List<Statement>> batches = new HashMap<>();
		for (Statement statement:batch) {
			Set<Host> hosts = new HashSet<>();
			int replicas = 0;
			
			Iterator<Host> it = cluster.getConfiguration().getPolicies().
				getLoadBalancingPolicy().newQueryPlan(
				statement.getKeyspace(), statement);
			if (it != null && it.hasNext()) {
				while (it.hasNext() && replicas < RF) {
					hosts.add(it.next());
					replicas++;
				}
			}
			
			List<Statement> hostBatch = batches.get(hosts);
			if (hostBatch == null) {
				hostBatch = new ArrayList<>();
				batches.put(hosts, hostBatch);
			}
			hostBatch.add(statement);
		}
		
		return new ArrayList<>(batches.values());
	}

	private void executeSplit(List<Statement> list) {
		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
		batch.addAll(list);
		
		session.execute(batch);
	}
	
	private void executeBatch(List<Statement> batch) {
		List<List<Statement>> splits = splitByRoute(session.getCluster(), 
			batch);
//		LOG.info(String.format("%s statements split in %s batches", 
//			statements.size(), splits.size()));
			
		for (List<Statement> list:splits) {
			executeSplit(list);
		}
	}
	
	@Override
	protected void begin() {
		statements = new ArrayList<Statement>();
	}

	@Override
	protected void doExecute(Statement s) {
		statements.add(s);
		
		if (statements.size() < PROCESS_BATCH) {
			return;
		}
		
		executeBatch(statements);	
		statements.clear();
	}

	@Override
	protected void end() {
		if (statements.size() != 0) {
			executeBatch(statements);
		}
		
		statements = null;
	}

	public SyncTokenAwareBatchExecutor(Session session,
			PreparedStatement prepared) {
		super("sync-token-aware-batch", session, prepared);
	}
}
