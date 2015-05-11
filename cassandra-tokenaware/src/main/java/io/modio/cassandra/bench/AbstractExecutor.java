package io.modio.cassandra.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public abstract class AbstractExecutor {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	private static final int LINES = 1000000;

	private static final int FETCH_BATCH = 10000;
	
	private static final int MARK = 5000;

	protected Random rnd = new Random();
	
	protected String name;
	
	protected Session session;
	
	protected PreparedStatement prepared;
	
	protected List<Tuple> getTuples(int fetchBatch) {
		List<Tuple> tuples = new ArrayList<>();
		List<Tuple> duplicates = new ArrayList<>();
		
		float percent = 0.80f;
		
		for (int i = 0; i < fetchBatch*(1-percent); ++i) {
			Tuple tuple = new Tuple();
			tuple.client = rnd.nextInt();
			tuple.sensor = rnd.nextInt();
			tuple.time = rnd.nextLong() % 2000000000000l;
			tuple.value = rnd.nextInt();
			
			tuples.add(tuple);
		}
		
		int size = tuples.size();
		for (int i = 0; i < fetchBatch*percent; ++i) {
			int pos = rnd.nextInt(size);
			Tuple tuple = tuples.get(pos);
			
			Tuple duplicate = new Tuple();
			duplicate.client = tuple.client;
			duplicate.sensor = tuple.sensor;
			duplicate.time = rnd.nextLong() % 2000000000000l;
			duplicate.value = rnd.nextInt();
			
			duplicates.add(duplicate);
		}
		
		tuples.addAll(duplicates);
		return tuples;
	}
	
	protected Statement getStatement(Tuple tuple) {
		BoundStatement s = prepared.bind(tuple.client, tuple.sensor, 
			tuple.time, tuple.value);
		
		return s;
	}

	protected abstract void begin();
	
	protected abstract void doExecute(Statement s);
	
	protected abstract void end();
	
	public AbstractExecutor(String name, Session session, 
		PreparedStatement prepared) {
		super();
		this.name = name;
		this.session = session;
		this.prepared = prepared;
	}

	public void execute() {
		Long begin = System.currentTimeMillis();
		
		begin();
		
		for (int i = 0; i < LINES;) {
			List<Tuple> tuples = getTuples(FETCH_BATCH);
			for (Tuple tuple:tuples) {
				Statement s = getStatement(tuple);
				doExecute(s);

				i++;
				if (i % MARK == 0) {
					LOG.info(i+"");
				}
			}
		}
		
		end();
		
		Long end = System.currentTimeMillis();
		
		LOG.info(String.format("%s: Latency: %s ms", name, (end-begin)));
	}
}
