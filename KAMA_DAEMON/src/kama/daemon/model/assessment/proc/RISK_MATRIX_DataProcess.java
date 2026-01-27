package kama.daemon.model.assessment.proc;

import java.io.File;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

public class RISK_MATRIX_DataProcess extends DataProcessor {
	
	private static final String DATAFILE_PREFIX = "risk_matrix";

	public RISK_MATRIX_DataProcess(final DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void processDataInternal(DatabaseManager dbManager, File file,
			ProcessorInfo processorInfo) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
}
