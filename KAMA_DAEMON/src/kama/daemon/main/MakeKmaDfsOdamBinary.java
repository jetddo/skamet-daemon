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


public class MakeKmaDfsOdamBinary {
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private final String insertKmaDfsOdamProcInfo = 
			
			" INSERT INTO AAMI.KMA_DFS_PROC_INFO(ISSUED_DT, FCST_DT, PROC_TM, DFS_TYPE) VALUES " + 
			" (TO_DATE(''{0}'', ''YYYYMMDDHH24MI''), TO_DATE(''{1}'', ''YYYYMMDDHH24MI''), SYSDATE, ''ODAM'') "; 
	
	private final String GATHER_KMA_DFS_ODAM_API_URL = "http://api.kma.go.kr/cgi-bin/url/nph-dfs_odam_grd?tmfc={obsTm}&vars={element}";
	//private final String GATHER_KMA_DFS_ODAM_API_URL = "http://api.kma.go.kr/cgi-bin/url/nph-dfs_odam_grd?tmfc={obsTm}&vars={element}";
	
	private final String[] KMA_DFS_ODAM_ELEMENTS = {"RN1", "REH", "T1H", "VEC", "WSD", "PTY"};
	
	private final int KMA_DFS_ODAM_GRID_COUNT = 37697;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeKmaDfsOdamBinary.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		return true;
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeKmaDfsOdamBinary.process -> initialize failed");
			return;
		}
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
				  								  : this.config.getString("global.storePath.unix");
		
		try {
			
			// 현재 시간을 기준으로 동네예보의 발표시각과 예보시각을 계산한다. 시간은 모두 KST 기준이다.
			// 발표시각은 3시간 간격이며 예보시각은 발표시각 이후 +1 시간부터 1시간 간격으로 72시간까지이다.
			Date currentTm = new Date();
			//currentTm = sdf.parse("202510140917"); // 테스트 고정값
			
			Calendar cal = new GregorianCalendar();
			cal.setTime(currentTm);
			
			// 실황시각 계산
			// 실황은 10분단위로 이루어진다
			// 서버 시간을 고려하여 10분을 더 빼준다
			
			cal.add(Calendar.MINUTE, -cal.get(Calendar.MINUTE)%10-10);			
			
			Date obsTm = cal.getTime();		
			
			String obsTmStr = sdf.format(obsTm);
			
			System.out.println("\t-> KMA DFS Odam Time: " + obsTmStr);

			for (String element : KMA_DFS_ODAM_ELEMENTS) {

				String apiUrl = GATHER_KMA_DFS_ODAM_API_URL.replace("{obsTm}", obsTmStr).replace("{element}", element);
				
				Float[] kmaDfsOdamData = this.gatherKmaDfsOdamData(apiUrl);
				
				if (kmaDfsOdamData == null) {
					System.out.println("\t-> Error: Failed to gather KMA DFS Odam data for element " + element + ", obsTm " + obsTmStr);
					continue;
				}
				
				String savePath = storePath + File.separator + "KMA_DFS_BIN" + File.separator + new SimpleDateFormat("yyyy/MM/dd/HH").format(obsTm); 
				
				String binaryFileName = "KMA_DFS_ODAM_" + element + "_" + obsTmStr + ".bin";
				
				if(!this.createDfsOdamBinaryFile(savePath, binaryFileName, kmaDfsOdamData)) {
					System.out.println("\t-> Error: Failed to create KMA DFS ODAM binary file for element " + element + ", obsTm " + obsTmStr);
				}
				
				String query = MessageFormat.format(this.insertKmaDfsOdamProcInfo, new Object[]{
					obsTmStr, obsTmStr
				});
				
				this.dbManager.executeUpdate(query, false);
				this.dbManager.commit();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.dbManager.commit();
		
		this.destroy();
	}
	
	private boolean createDfsOdamBinaryFile(String savePath, String binaryFileName, Float[] kmaDfsOdamData) throws Exception {
		
		if(!new File(savePath).exists()) {
			new File(savePath).mkdirs();
		}
		
		System.out.println("\t->[createDfsOdamBinaryFile] Write Binary [" + savePath + File.separator + binaryFileName + "]");
		
		BufferedOutputStream dos = new BufferedOutputStream(new FileOutputStream(savePath + File.separator + binaryFileName));
		
		for(int i=0 ; i<kmaDfsOdamData.length ; i++) {						
			dos.write(ByteBuffer.allocate(4).putFloat(kmaDfsOdamData[i]).array());
		}
		
		dos.close();
		
		return true;
	}
	
	private Float[] gatherKmaDfsOdamData(String apiUrl) throws Exception {
			
		System.out.println("\t->[gatherKmaDfsOdamData] Gather URL: " + apiUrl);
		
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
		
		Float[] kmaDfsOdamData = null;
		
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
		
		if(tokens.length < KMA_DFS_ODAM_GRID_COUNT) {
			System.out.println("\t->[gatherKmaDfsOdamData] Error: Insufficient KMA DFS ODAM data tokens. Expected " + KMA_DFS_ODAM_GRID_COUNT + ", but got " + tokens.length);			
			return null;
		}
		
		try {
			
			kmaDfsOdamData = new Float[KMA_DFS_ODAM_GRID_COUNT];
			
			for (int i = 0; i < KMA_DFS_ODAM_GRID_COUNT; i++) {
				kmaDfsOdamData[i] = Float.parseFloat(tokens[i]);
			}
			
		} catch (Exception e) {
			System.out.println("\t->[gatherKmaDfsOdamData] Error: Failed to parse KMA DFS data tokens -> " + e);
			return null;
		}
		
		System.out.println("\t->[gatherKmaDfsOdamData] Total tokens: " + kmaDfsOdamData.length);
		
		br.close();
		conn.disconnect();
		
		return kmaDfsOdamData;
	}
	
	private void destroy() {
		
		this.dbManager.safeClose();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		MakeKmaDfsOdamBinary maker = new MakeKmaDfsOdamBinary();
		maker.process();
	}
	
}
