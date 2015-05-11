package io.modio.cassandra.bench;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class SyncBatchExecutor extends AbstractExecutor {
	private static final int BATCH = 8000;

	private BatchStatement batch;

	@Override
	protected void begin() {
		batch = new BatchStatement(Type.UNLOGGED);
	}

	@Override
	protected void doExecute(Statement s) {
		batch.add(s);

		if (batch.size() < BATCH) {
			return;
		}
		
		session.execute(batch);
		batch = new BatchStatement(Type.UNLOGGED);
	}

	@Override
	protected void end() {
		if (batch.size() != 0) {
			session.execute(batch);
		}
		
		batch = null;
	}

	public SyncBatchExecutor(Session session, PreparedStatement prepared) {
		super("sync-batch", session, prepared);
	}

}
