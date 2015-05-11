package io.modio.cassandra.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class AsyncSingleExecutor extends AbstractExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(
		AsyncSingleExecutor.class);

	private static final int BATCH = 8000;
	
	private static final long WAIT = 10000;
	
	private List<ResultSetFuture> tasks;

	private void waitResultSet(ResultSetFuture future) {
		try {
			future.getUninterruptibly(WAIT, TimeUnit.MILLISECONDS);
		} catch(TimeoutException e) {
			LOG.error("Timed out waiting for statement to complete.");
		}
	}
	
	
	@Override
	protected void begin() {
		tasks = new ArrayList<>();
	}

	@Override
	protected void doExecute(Statement s) {
		ResultSetFuture future = session.executeAsync(s);
		tasks.add(future);
		if (tasks.size() < BATCH) {
			return;
		}
		
		for (ResultSetFuture f:tasks) {
			waitResultSet(f);
		}
		tasks.clear();
	}

	@Override
	protected void end() {
		if (tasks.size() != 0) {
			for (ResultSetFuture f:tasks) {
				waitResultSet(f);
			}			
		}
		
		tasks = null;
	}

	public AsyncSingleExecutor(Session session,	PreparedStatement prepared) {
		super("async-single", session, prepared);
		// TODO Auto-generated constructor stub
	}
}
