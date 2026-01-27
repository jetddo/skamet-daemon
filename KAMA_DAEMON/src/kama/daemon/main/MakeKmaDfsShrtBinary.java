package kama.daemon.main;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;


public class MakeKmaDfsShrtBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private final String insertKmaDfsShrtProcInfo = 
			
			" INSERT INTO AAMI.KMA_DFS_PROC_INFO(ISSUED_DT, FCST_DT, PROC_TM, DFS_TYPE) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), SYSDATE, ''SHRT'') "; 
	
	private final String GATHER_KMA_DFS_SHRT_API_URL = "http://api.kma.go.kr/cgi-bin/url/nph-dfs_shrt_grd?tmfc={issuedTm}&tmef={fcstTm}&vars={element}";
	
//	private final String GATHER_KMA_DFS_SHRT_API_URL = "http://10.2.10.8/TEST/dfs_shrt_csv/{issuedTm}_{fcstTm}_{element}.csv";
	
	private final String[] KMA_DFS_SHRT_ELEMENTS = {"PCP", "SNO", "POP", "REH", "SKY", "PTY", "TMN", "TMP", "TMX", "VEC", "WSD"};
	
//	private final String[] KMA_DFS_SHRT_ELEMENTS = {"PCP"};
	
	private final int KMA_DFS_SHRT_FCST_COUNT = 72;
	
	private final int KMA_DFS_SHRT_GRID_COUNT = 37697;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeKmaDfsShrtBinary.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeKmaDfsShrtBinary.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
				  								  : this.config.getString("global.storePath.unix");
		
		try {
			
			// 현재 시간을 기준으로 동네예보의 발표시각과 예보시각을 계산한다. 시간은 모두 KST 기준이다.
			// 발표시각은 3시간 간격이며 예보시각은 발표시각 이후 +1 시간부터 1시간 간격으로 72시간까지이다.
			Date currentTm = new Date();
//			currentTm = sdf.parse("2025101409"); // 테스트 고정값
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(currentTm);
			
			// 발표시각 계산
			// 발표시각은 현재 시간에서 가장 가까운 3시간 단위 시각에서 1시간을 뺀 시각이다.
			// 발표가 항상 최신으로 유지되지 않을수도 있으므로 3시간을 더 뺀다
			// 현재 시간이 KST 01시 라면 전일 20시가 된다.			
			
			int hour = cal.get(Calendar.HOUR_OF_DAY);
			cal.add(Calendar.HOUR_OF_DAY, -(hour % 3) - 4);			
			
			Date issuedTm = cal.getTime();		
			
			String issuedTmStr = sdf.format(issuedTm);
			
			System.out.println("\t-> KMA DFS Issued Time: " + issuedTmStr);
			
			for (int fcstHour = 1; fcstHour <= KMA_DFS_SHRT_FCST_COUNT; fcstHour++) {

				cal.setTime(issuedTm);
				cal.add(Calendar.HOUR_OF_DAY, fcstHour);

				Date fcstTm = cal.getTime();

				String fcstTmStr = sdf.format(fcstTm);

				for (String element : KMA_DFS_SHRT_ELEMENTS) {

					String apiUrl = GATHER_KMA_DFS_SHRT_API_URL.replace("{issuedTm}", issuedTmStr)
							.replace("{fcstTm}", fcstTmStr).replace("{element}", element);
					
					Float[] kmaDfsShrtData = this.gatherKmaDfsShrtData(apiUrl);
					
					if (kmaDfsShrtData == null) {
						System.out.println("\t-> Error: Failed to gather KMA DFS Shrt data for element " + element
								+ ", issuedTm " + issuedTmStr + ", fcstTm " + fcstTmStr);
						continue;
					}
					
					String savePath = storePath + File.separator + "KMA_DFS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd/HH").format(issuedTm); 
					
					String binaryFileName = "KMA_DFS_SHRT_" + element + "_" + issuedTmStr + "_" + fcstTmStr + ".bin";
					
					if(!this.createDfsShrtBinaryFile(savePath, binaryFileName, kmaDfsShrtData)) {
						System.out.println("\t-> Error: Failed to create KMA DFS SHRT binary file for element " + element
								+ ", issuedTm " + issuedTmStr + ", fcstTm " + fcstTmStr);
					}
					
					String query = MessageFormat.format(this.insertKmaDfsShrtProcInfo, new Object[]{
						issuedTmStr, fcstTmStr	
					});
					
					this.dbManager.executeUpdate(query, false);
					this.dbManager.commit();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private boolean createDfsShrtBinaryFile(String savePath, String binaryFileName, Float[] kmaDfsShrtData) throws Exception {
		
		if(!new File(savePath).exists()) {
			new File(savePath).mkdirs();
		}
		
		System.out.println("\t->[createDfsShrtBinaryFile] Write Binary [" + savePath + File.separator + binaryFileName + "]");
		
		BufferedOutputStream dos = new BufferedOutputStream(new FileOutputStream(savePath + File.separator + binaryFileName));
		
		for(int i=0 ; i<kmaDfsShrtData.length ; i++) {						
			dos.write(ByteBuffer.allocate(4).putFloat(kmaDfsShrtData[i]).array());
		}
		
		dos.close();
		
		return true;
	}
	
	private Float[] gatherKmaDfsShrtData(String apiUrl) throws Exception {
			
		System.out.println("\t->[gatherKmaDfsShrtData] Gather URL: " + apiUrl);
		
		HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
		conn.setReadTimeout(20000);
		conn.setRequestMethod("GET");
		
		OutputStream os = conn.getOutputStream();
		OutputStreamWriter writer = new OutputStreamWriter(os);
			
		writer.close();
		
		os.close();
		
		Float[] kmaDfsShrtData = null;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		String allData = "";
		
		String line = null;
		
		// 일단 전체를 다 읽어서 하나의 문자열로 만든다.
		while((line = br.readLine()) != null) {			
			allData += line + "\n";
		}
		
		// 그리고 나서 쉼표나 공백을 기준으로 토큰화 한다.
		
		// 정상 그리드 숫자에 항상 같다는 보장이 없으므로 정상적인 토큰 갯수까지만 읽는다
		
		// allData 의 빈칸을 모두 제거한 후 쉼표로 토큰화 한다.
		
		String[] tokens = allData.replaceAll("\\s+", "").split(",");		
		
		if(tokens.length < KMA_DFS_SHRT_GRID_COUNT) {
			System.out.println("\t->[gatherKmaDfsShrtData] Error: Insufficient KMA DFS SHRT data tokens. Expected " + KMA_DFS_SHRT_GRID_COUNT + ", but got " + tokens.length);			
			return null;
		}
		
		try {
			
			kmaDfsShrtData = new Float[KMA_DFS_SHRT_GRID_COUNT];
			
			for (int i = 0; i < KMA_DFS_SHRT_GRID_COUNT; i++) {
				kmaDfsShrtData[i] = Float.parseFloat(tokens[i]);
			}
			
		} catch (Exception e) {
			System.out.println("\t->[gatherKmaDfsShrtData] Error: Failed to parse KMA DFS data tokens -> " + e);
			return null;
		}
		
		System.out.println("\t->[gatherKmaDfsShrtData] Total tokens: " + kmaDfsShrtData.length);
		
		br.close();
		conn.disconnect();
		
		return kmaDfsShrtData;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		MakeKmaDfsShrtBinary maker = new MakeKmaDfsShrtBinary();
		maker.process();
	}
	
}
