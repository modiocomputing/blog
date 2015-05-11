package io.modio.cassandra.bench;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class SyncSingleExecutor extends AbstractExecutor {
	@Override
	protected void begin() {
	}

	@Override
	protected void doExecute(Statement s) {
		session.execute(s);
	}

	@Override
	protected void end() {
	}

	public SyncSingleExecutor(Session session, PreparedStatement prepared) {
		super("sync-single", session, prepared);
	}
}
