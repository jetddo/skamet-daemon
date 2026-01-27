package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.google.gson.Gson;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

public class NcToAmo_DataProcess extends DataProcessor {
	
	private static final String DATAFILE_PREFIX = "nc_to_amo";
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	String insertQuery = "INSERT INTO AAMI.NC_TO_AMO_FCST (SEQ, FCST_DATE, SLUG, STN_ID, METAR, TAF, RPT, WEATHER_CANDIDATE_1, WEATHER_CANDIDATE_2, WEATHER_CANDIDATE_3, DANGER_CANDIDATE_1, DANGER_CANDIDATE_2, DANGER_CANDIDATE_3) "+
			 "VALUES (AAMI.NC_TO_AMO_FCST_SEQ.NEXTVAL, TO_DATE(''{0}'', ''YYYYMMDD''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'')";

	
	public NcToAmo_DataProcess(final DaemonSettings settings) {
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
	
	private void parseJsonFile(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start NC_TO_AMO Parser :::::");
		
		try {

            dbManager.setAutoCommit(false);
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsoluteFile()), "UTF-8"));
	   
            Gson gson = new Gson();
          
            NcToAmoFcst[] fcstList = gson.fromJson(reader, NcToAmoFcst[].class);
            
            for(int i=0 ; i<fcstList.length ; i++) {
            	
            	NcToAmoFcst fcst = fcstList[i];
            	
            	int weatherSize = fcst.getWeather_candidate().size();
            	int dangerSize = fcst.getDanger_candidate().size();
            	
                String query = MessageFormat.format(insertQuery, new Object[]{
        				
                		fcst.getDate(),
                		fcst.getSlug(),
                		fcst.getStn_id().toUpperCase(),
    					fcst.getMetar(),
    					fcst.getTaf(),
    					null/*fcst.getRpt()*/,
    					weatherSize > 0 ? fcst.getWeather_candidate().get(0).getText() : null,
						weatherSize > 1 ? fcst.getWeather_candidate().get(1).getText() : null,
						weatherSize > 2 ? fcst.getWeather_candidate().get(2).getText() : null,
						dangerSize > 0 ? fcst.getDanger_candidate().get(0).getText() : null,	
						dangerSize > 1 ? fcst.getDanger_candidate().get(1).getText() : null,
						dangerSize > 2 ? fcst.getDanger_candidate().get(2).getText() : null,
    			}).replaceAll("'null'", "null");
                
                dbManager.executeUpdate(query);
            }
            
	    	dbManager.commit();	
	    	
		} catch (Exception e) {
			e.printStackTrace();
			dbManager.rollback();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End NC_TO_AMO Parser :::::");
	}
	

	@Override
	protected void processDataInternal(DatabaseManager dbManager, File file,
			ProcessorInfo processorInfo) throws Exception {
			
		this.parseJsonFile(dbManager, file, processorInfo);  	
		
	}
	
	private class NcToAmoFcst {
		
		private String date;
		
		private String slug;
		
		private String stn_id;
		
		private String metar;
		
		private String cloud;
		
		private String taf;
		
		private String rpt;
		
		private List<Candidate> weather_candidate;
		
		private List<Candidate> danger_candidate;
		
		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public String getSlug() {
			return slug;
		}

		public void setSlug(String slug) {
			this.slug = slug;
		}

		public String getStn_id() {
			return stn_id;
		}

		public void setStn_id(String stn_id) {
			this.stn_id = stn_id;
		}

		public String getMetar() {
			return metar;
		}

		public void setMetar(String metar) {
			this.metar = metar;
		}

		public String getCloud() {
			return cloud;
		}

		public void setCloud(String cloud) {
			this.cloud = cloud;
		}

		public String getTaf() {
			return taf;
		}

		public void setTaf(String taf) {
			this.taf = taf;
		}

		public String getRpt() {
			return rpt;
		}

		public void setRpt(String rpt) {
			this.rpt = rpt;
		}

		public List<Candidate> getWeather_candidate() {
			return weather_candidate;
		}

		public void setWeather_candidate(List<Candidate> weather_candidate) {
			this.weather_candidate = weather_candidate;
		}

		public List<Candidate> getDanger_candidate() {
			return danger_candidate;
		}

		public void setDanger_candidate(List<Candidate> danger_candidate) {
			this.danger_candidate = danger_candidate;
		}

		@Override
		public String toString() {
			return "ModelReport [date=" + date + ", slug=" + slug + ", stn_id=" + stn_id + ", metar=" + metar
					+ ", cloud=" + cloud + ", taf=" + taf + ", rpt=" + rpt + ", weather_candidate=" + weather_candidate
					+ ", danger_candidate=" + danger_candidate + "]";
		}

		private class Candidate {
			
			private String text;			
			private String rank;
			public String getText() {
				return text;
			}
			public void setText(String text) {
				this.text = text;
			}
			public String getRank() {
				return rank;
			}
			public void setRank(String rank) {
				this.rank = rank;
			}
			@Override
			public String toString() {
				return "Candidate [text=" + text + ", rank=" + rank + "]";
			}
		}
	}
}
