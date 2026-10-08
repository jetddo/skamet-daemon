package kama.daemon.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class CalculateEvalWintemAnalysis {

	// 기본값은 상세 로그 OFF이다. -Dwintem.eval.detailLog=true로 켤 수 있다.
	private boolean evalDetailLogEnabled = Boolean.getBoolean("wintem.eval.detailLog");

	// 실행 중에도 상세 계산 결과의 출력 여부를 바꿀 수 있다.
	public void setEvalDetailLogEnabled(boolean enabled) {

		this.evalDetailLogEnabled = enabled;

	}

	private Configuration config;

	private DatabaseManager dbManager;

	private final String DATA_STORE_PATH = "//172.26.56.124/data_store";

	private final String GDPS_NE57_ANAL_PATH = DATA_STORE_PATH + "/GDPS_NE57_ANAL";
	private final String GDPS_NE57_ANAL_PREFIX = "GDPS_NE57";

	private final String WINTEM_XML_PATH = DATA_STORE_PATH + "/KIM_RDPS_WINTEM";
	private final String WINTEM_XML_PREFIX = "WINTEM_KIM_RDPS_NOR";

	private final int MAX_FCST_HOUR = 36;
	private final int WINTEM_XML_FILE_COUNT = 23;

	private final String EVAL_WINTEM_RESULT_TABLE = "AAMI.EVAL_WINTEM_RESULT";

	// DB 저장용 모델과 도메인은 확장 시 이 상수에서 변경한다.
	private final String EVAL_WINTEM_MODEL = "KIM RDPS";
	private final String EVAL_WINTEM_DOMAIN = "NORMAL";

	private boolean initialize() {

		Configurations configs = new Configurations();

		try {

			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));

			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);

		} catch (ConfigurationException e ) {

			logEval("ERROR", "INITIALIZE failed | reason=%s", e.toString());

			if(this.dbManager != null) {

				this.dbManager.safeClose();

			}

			return false;

		}

		return true;

	}

	private void destroy() {

		if(this.dbManager != null) {

			this.dbManager.safeClose();

		}

	}

	public void process() {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		if(this.initialize()) {
			
			logEval("INFO", "::: Start CalculateEvalWintemAnalysis :::");

			try {

				Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

				Date currentTm = new Date();
				//Date currentTm = sdf.parse("2026100700");

				// 하루단위로 실행하고 00,06,12,18시 단위로 데이터를 처리한다.
				cal.setTime(new Date(currentTm.getTime()));
				cal.set(Calendar.HOUR_OF_DAY, 0);

				Date startTm = cal.getTime();

				cal.setTime(new Date(currentTm.getTime()));
				cal.set(Calendar.HOUR_OF_DAY, 18);

				Date endTm = cal.getTime();

				Date tm = new Date(startTm.getTime());

				while(tm.getTime() <= endTm.getTime()) {

					calculateEvalWintemAnalysis(tm);

					cal.setTime(tm);
					cal.add(Calendar.HOUR_OF_DAY, 6);
					tm = cal.getTime();

				}
				
				logEval("INFO", "::: End CalculateEvalWintemAnalysis :::");

			} catch (Exception e) {

				logEval("ERROR", "PROCESS failed | reason=%s", e.toString());

				if(this.evalDetailLogEnabled) {

					e.printStackTrace(System.err);

				}

			}

		}

		this.destroy();

	}

	private List<Map<String, Object>> getEvalWintemAnalysisPairList(final Date issuedTm) {

		List<Map<String, Object>> evalWintemAnalysisPairList = new ArrayList<Map<String, Object>>();

		final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH");
		sdf2.setTimeZone(TimeZone.getTimeZone("UTC"));

		String gdpsNe57AnalPath = GDPS_NE57_ANAL_PATH + "/" + sdf2.format(issuedTm);

		File gdpsNe57AnalDir = new File(gdpsNe57AnalPath);

		File[] gdpsNe57AnalFileList = gdpsNe57AnalDir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {

				String regex = GDPS_NE57_ANAL_PREFIX +"_" + sdf.format(issuedTm) + "_[0-9]{10}_h[0-9]{3}.csv";

				if (name.matches(regex)) {

					int hour = Integer.parseInt(name.substring(name.length() - 7, name.length() - 4));

					if (hour > MAX_FCST_HOUR) {

						return false;

					}

					return true;

				}

				return false;

			}

		});

		if(gdpsNe57AnalFileList == null || gdpsNe57AnalFileList.length != MAX_FCST_HOUR+1) {

			logEval("WARN", "\t자료 누락: issuedTm=%s UTC, CSV=%d/%d, path=%s",
					sdf.format(issuedTm), gdpsNe57AnalFileList == null ? 0 : gdpsNe57AnalFileList.length,
					MAX_FCST_HOUR+1, gdpsNe57AnalPath);
			return evalWintemAnalysisPairList;

		}

		// 예보시간 순서대로 처리하여 같은 발표시각 아래의 로그 순서를 일정하게 유지한다.
		Arrays.sort(gdpsNe57AnalFileList, new Comparator<File>() {

			@Override
			public int compare(File left, File right) {

				return left.getName().compareTo(right.getName());

			}

		});

		String wintemXmlPath = WINTEM_XML_PATH + "/" + sdf2.format(issuedTm);

		for (int i = 0; i < gdpsNe57AnalFileList.length; i++) {

			Map<String, Object> evalWintemAnalysisPair = new HashMap<String, Object>();

			String gdpsNe57AnalFileName = gdpsNe57AnalFileList[i].getName();

			final int hour = Integer.parseInt(gdpsNe57AnalFileName.substring(gdpsNe57AnalFileName.length() - 7, gdpsNe57AnalFileName.length() - 4));

			File wintemXmlDir = new File(wintemXmlPath);

			File[] wintemXmlFileList = wintemXmlDir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {

					//WINTEM_KIM_RDPS_NOR_FL060_00H_202610050000.xml

					String regex = WINTEM_XML_PREFIX +"_FL[0-9]{3}_" + String.format("%02d", hour) + "H_" + sdf.format(issuedTm) + "00.xml";

					if (name.matches(regex)) {

						return true;

					}

					return false;

				}

			});

			if (wintemXmlFileList == null || wintemXmlFileList.length != WINTEM_XML_FILE_COUNT) {

				logEval("WARN", "\tH+%02d 제외: XML=%d/%d", hour, wintemXmlFileList == null ? 0 : wintemXmlFileList.length, WINTEM_XML_FILE_COUNT);
				continue;

			}

			evalWintemAnalysisPair.put("issuedTm", issuedTm);
			evalWintemAnalysisPair.put("fcstHour", hour);
			evalWintemAnalysisPair.put("gdpsNe57AnalFile", gdpsNe57AnalFileList[i]);
			evalWintemAnalysisPair.put("wintemXmlFileList", wintemXmlFileList);
			evalWintemAnalysisPairList.add(evalWintemAnalysisPair);

		}

		return evalWintemAnalysisPairList;

	}

	private void calculateEvalWintemAnalysis(Date issuedTm) {

		// 처리 시작 시각과 발표시각 로그를 준비한다.
		long evaluationStarted = System.nanoTime();
		SimpleDateFormat issuedLogFormat = new SimpleDateFormat("yyyyMMddHH", Locale.ROOT);
		issuedLogFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		String issuedLogText = issuedLogFormat.format(issuedTm);
		logEval("INFO", "issuedTm=%s UTC", issuedLogText);

		// CSV 한 개와 XML 23개가 대응하는 파일 묶음을 가져온다.
		List<Map<String, Object>> evalWintemAnalysisPairList =
				this.getEvalWintemAnalysisPairList(issuedTm);
		int pairTotal = evalWintemAnalysisPairList.size();

		if (pairTotal == 0) {

			logEval("WARN", "\t계산 대상 없음");
			return;

		}


		int successCount = 0, failureCount = 0, resultRowCount = 0;

		// 파일 묶음별로 자료를 읽고 고도별 평균 편향을 계산한다.
		for (Map<String, Object> evalWintemAnalysisPair : evalWintemAnalysisPairList) {


			try {

				if (evalWintemAnalysisPair == null
						|| !(evalWintemAnalysisPair.get("issuedTm") instanceof Date)
						|| !(evalWintemAnalysisPair.get("fcstHour") instanceof Number)
						|| !(evalWintemAnalysisPair.get("gdpsNe57AnalFile") instanceof File)
						|| !(evalWintemAnalysisPair.get("wintemXmlFileList") instanceof File[])) {

					throw new IllegalArgumentException("Invalid evalWintemAnalysisPair");

				}

				// 파일 묶음에 들어 있는 발표시각, 예보시간, 파일 목록을 꺼낸다.
				Date pairIssuedTm = (Date) evalWintemAnalysisPair.get("issuedTm");
				double hourValue = ((Number) evalWintemAnalysisPair.get("fcstHour")).doubleValue();

				if (!Double.isFinite(hourValue) || hourValue < 0 || hourValue > MAX_FCST_HOUR
						|| hourValue != Math.floor(hourValue)) {

					throw new IllegalArgumentException("fcstHour must be an integer from 0 to 36");

				}

				int fcstHour = (int) hourValue;
				File csvFile = (File) evalWintemAnalysisPair.get("gdpsNe57AnalFile");
				File[] xmlFiles = ((File[]) evalWintemAnalysisPair.get("wintemXmlFileList")).clone();

				if (!csvFile.isFile() || xmlFiles.length != WINTEM_XML_FILE_COUNT) {

					throw new IllegalArgumentException("CSV file and exactly 23 XML files are required");

				}

				// 발표시각과 예보시간으로 예측시각을 구하고 CSV 파일명과 비교한다.
				TimeZone utc = TimeZone.getTimeZone("UTC");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH", Locale.ROOT);
				sdf.setTimeZone(utc);
				sdf.setLenient(false);
				String issuedText = sdf.format(pairIssuedTm);
				Date expectedIssued = sdf.parse(issuedText);

				if (expectedIssued.getTime() != pairIssuedTm.getTime()) {

					throw new IllegalArgumentException("pairIssuedTm must be on the exact UTC hour");

				}

				Calendar calendar = Calendar.getInstance(utc);
				calendar.setTime(expectedIssued);
				calendar.add(Calendar.HOUR_OF_DAY, fcstHour);
				Date fcstTm = calendar.getTime();
				String fcstText = sdf.format(fcstTm);
				Matcher csvName = Pattern.compile(
						"GDPS_NE57_(\\d{10})_(\\d{10})_h(\\d{3})\\.csv").matcher(csvFile.getName());

				if (!csvName.matches() || !issuedText.equals(csvName.group(1))
						|| !fcstText.equals(csvName.group(2))
						|| fcstHour != Integer.parseInt(csvName.group(3))) {

					throw new IllegalArgumentException("CSV issued/fcst time mismatch: " + csvFile);

				}

				// 모델 높이 배열: 인덱스 0부터 시작하며 단위는 hPa이다.
				int[] levels = {

					1000, 975, 950, 925, 900, 875, 850, 800, 750, 700, 650, 600,
					550, 500, 450, 400, 350, 300, 250, 200, 150, 100, 70, 50

				};

				int[] flightLevels = {

					10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 120,
					140, 160, 180, 210, 240, 270, 300, 340, 390, 440, 520

				};

				// 인덱스가 하나면 해당 값을 사용하고, 두 개면 두 값을 단순 평균한다.
				int[][] heightIndexes = {

					{1},      // FL010: 975 hPa
					{2, 3},   // FL020: 950, 925 hPa
					{3},      // FL025: 925 hPa
					{4},      // FL030: 900 hPa
					{5},      // FL040: 875 hPa
					{6},      // FL050: 850 hPa
					{7},      // FL060: 800 hPa
					{7, 8},   // FL070: 800, 750 hPa
					{8},      // FL080: 750 hPa
					{8, 9},   // FL090: 750, 700 hPa
					{9},      // FL100: 700 hPa
					{10},     // FL120: 650 hPa
					{11},     // FL140: 600 hPa
					{12},     // FL160: 550 hPa
					{13},     // FL180: 500 hPa
					{14},     // FL210: 450 hPa
					{15},     // FL240: 400 hPa
					{16},     // FL270: 350 hPa
					{17},     // FL300: 300 hPa
					{18},     // FL340: 250 hPa
					{19},     // FL390: 200 hPa
					{20},     // FL440: 150 hPa
					{21}      // FL520: 100 hPa

				};

				Map<Integer, int[]> indexesByFl = new HashMap<Integer, int[]>();

				for (int i = 0; i < flightLevels.length; i++) {

					indexesByFl.put(flightLevels[i], heightIndexes[i]);

				}

				// CSV 행 순서와 관계없이 격자번호와 기압면을 기준으로 자료를 저장한다.
				Map<Integer, double[]> coordinates = new LinkedHashMap<Integer, double[]>();
				Map<Integer, Map<Integer, double[]>> data = new HashMap<Integer, Map<Integer, double[]>>();
				final double coordinateTolerance = 0.000001;
				final double msToKnot = 3600.0 / 1852.0;
				int csvRowCount = 0;

				// CSV를 한 번만 읽어 격자별·기압면별 자료를 메모리에 저장한다.
				try (BufferedReader reader = Files.newBufferedReader(csvFile.toPath(), StandardCharsets.UTF_8)) {

					String header = reader.readLine();

					if (header == null) {

						throw new IllegalArgumentException("Empty CSV: " + csvFile);

					}

					String[] columns = header.replace("\uFEFF", "").split(",", -1);
					Map<String, Integer> columnIndex = new HashMap<String, Integer>();

					for (int i = 0; i < columns.length; i++) {

						if (columnIndex.put(columns[i].trim(), i) != null) {

							throw new IllegalArgumentException("Duplicate CSV column: " + columns[i]);

						}

					}

					String[] required = {"stn_lat","stn_lon","grid_no","level","temp","u","v"};
					int[] positions = new int[required.length];

					for (int i = 0; i < required.length; i++) {

						Integer position = columnIndex.get(required[i]);

						if (position == null) {

							throw new IllegalArgumentException("Missing CSV column: " + required[i]);

						}

						positions[i] = position;

					}

					String line;
					int lineNumber = 1;

					while ((line = reader.readLine()) != null) {

						lineNumber++;

						if (line.trim().isEmpty()) {

							continue;

						}

						// 제공된 CSV는 따옴표 없이 숫자와 쉼표로 구성되어 있다.
						String[] fields = line.split(",", -1);

						if (fields.length != columns.length) {

							throw new IllegalArgumentException("Wrong CSV field count at line " + lineNumber);

						}

						double[] values = new double[required.length];

						for (int i = 0; i < values.length; i++) {

							values[i] = Double.parseDouble(fields[positions[i]].trim());

							if (!Double.isFinite(values[i])) {

								throw new IllegalArgumentException("Non-finite CSV value at line " + lineNumber);

							}

						}

						if (values[2] < 1 || values[2] > Integer.MAX_VALUE || values[2] != Math.floor(values[2])
								|| values[3] != Math.floor(values[3])) {

							throw new IllegalArgumentException("Invalid grid_no/level at line " + lineNumber);

						}

						int gridNo = (int) values[2];
						int level = (int) values[3];
						double[] coordinate = coordinates.get(gridNo);

						if (coordinate == null) {

							coordinates.put(gridNo, new double[] {values[0], values[1]});
							data.put(gridNo, new HashMap<Integer, double[]>());

						} else if (Math.abs(coordinate[0]-values[0]) > coordinateTolerance
								|| Math.abs(coordinate[1]-values[1]) > coordinateTolerance) {

							throw new IllegalArgumentException("Coordinates differ within grid " + gridNo);

						}

						// CSV 온도를 K에서 섭씨로 변환하여 저장한다.
						double levelTemp = values[4] - 273.15;

						// 기압면마다 u, v로 풍속과 풍향을 먼저 계산하여 메모리에 저장한다.
						double levelWs = Math.hypot(values[5], values[6]) * msToKnot;
						double levelWd = levelWs == 0 ? 0
								: (Math.toDegrees(Math.atan2(-values[5], -values[6])) + 360.0) % 360.0;

						// 저장 배열: 온도(℃), 풍속(kt), 풍향(degree).
						if (data.get(gridNo).put(level, new double[] {levelTemp, levelWs, levelWd}) != null) {

							throw new IllegalArgumentException("Duplicate CSV grid/level: " + gridNo + "/" + level);

						}

						csvRowCount++;

					}

				}

				if (coordinates.isEmpty()) {

					throw new IllegalArgumentException("CSV has no grid data");

				}

				List<Integer> gridOrder = new ArrayList<Integer>(coordinates.keySet());

				// 서로 다른 격자번호가 같은 좌표를 갖는지 확인한다.
				for (int i = 0; i < gridOrder.size(); i++) {

					double[] a = coordinates.get(gridOrder.get(i));

					for (int j = i+1; j < gridOrder.size(); j++) {

						double[] b = coordinates.get(gridOrder.get(j));

						if (Math.abs(a[0]-b[0]) <= coordinateTolerance
								&& Math.abs(a[1]-b[1]) <= coordinateTolerance) {

							throw new IllegalArgumentException("Duplicate CSV station coordinates");

						}

					}

				}

				// XML을 읽을 파서를 준비한다. 외부 파일이나 외부 주소는 읽지 않는다.
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
				factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
				factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
				factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
				factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
				factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
				factory.setXIncludeAware(false);
				factory.setExpandEntityReferences(false);
				DocumentBuilder builder = factory.newDocumentBuilder();
				Arrays.sort(xmlFiles, new Comparator<File>() {

					public int compare(File a, File b) {

						return a.getName().compareTo(b.getName());

					}

				});

				List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
				Set<Integer> seenFl = new HashSet<Integer>();
				String pairModel = null;
				String pairDomain = null;

				// XML 23개를 모두 메모리에 읽은 후, 메모리에 있는 자료로 계산한다.
				List<Document> xmlDataList = new ArrayList<Document>();

				for (File xmlFile : xmlFiles) {

					if (!xmlFile.isFile()) {

						throw new IllegalArgumentException("Missing XML: " + xmlFile);

					}

					xmlDataList.add(builder.parse(xmlFile));

				}

				// 각 XML의 고도에 해당하는 분석장 자료로 격자별 차이를 계산한다.
				for (int xmlIndex = 0; xmlIndex < xmlFiles.length; xmlIndex++) {

					File xmlFile = xmlFiles[xmlIndex];
					Matcher xmlName = Pattern.compile(
							"WINTEM_(.+)_([^_]+)_FL(\\d{3})_(\\d{2})H_(\\d{10})00\\.xml")
							.matcher(xmlFile.getName());

					if (!xmlName.matches() || !issuedText.equals(xmlName.group(5))
							|| fcstHour != Integer.parseInt(xmlName.group(4))) {

						throw new IllegalArgumentException("XML filename mismatch: " + xmlFile);

					}

					Element root = xmlDataList.get(xmlIndex).getDocumentElement();
					int fl = Integer.parseInt(xmlName.group(3));
					int[] indexes = indexesByFl.get(fl);

					if (!"wintem".equals(root.getTagName()) || indexes == null || !seenFl.add(fl)
							|| !issuedText.equals(root.getAttribute("issued_dt"))
							|| !fcstText.equals(root.getAttribute("fcst_dt"))
							|| !"utc".equalsIgnoreCase(root.getAttribute("timezone"))
							|| !("FL"+xmlName.group(3)).equals(root.getAttribute("height"))
							|| !"knot".equalsIgnoreCase(root.getAttribute("ws_unit"))
							|| !"degree".equalsIgnoreCase(root.getAttribute("wd_unit"))
							|| !"\u2103".equals(root.getAttribute("temp_unit"))) {

						throw new IllegalArgumentException("XML time/height/unit metadata mismatch: " + xmlFile);

					}

					String model = root.getAttribute("model").toUpperCase(Locale.ROOT);
					String domain = xmlName.group(2).toUpperCase(Locale.ROOT);

					if (!model.equals(xmlName.group(1).toUpperCase(Locale.ROOT))
							|| (pairModel != null && (!pairModel.equals(model) || !pairDomain.equals(domain)))) {

						throw new IllegalArgumentException("XML model/domain mismatch: " + xmlFile);

					}

					pairModel = model;
					pairDomain = domain;
					NodeList grids = root.getElementsByTagName("grid");

					if (grids.getLength() != coordinates.size()) {

						throw new IllegalArgumentException("XML/CSV grid count mismatch: " + xmlFile);

					}

					double tempSum = 0, wsSum = 0, wdSum = 0;
					int sampleCount = 0, wdSampleCount = 0;
					Set<Integer> matchedGrids = new HashSet<Integer>();

					for (int i = 0; i < grids.getLength(); i++) {

						Element grid = (Element) grids.item(i);
						double lat = Double.parseDouble(grid.getAttribute("lat"));
						double lon = Double.parseDouble(grid.getAttribute("lon"));

						if (!Double.isFinite(lat) || !Double.isFinite(lon)) {

							throw new IllegalArgumentException("Invalid XML coordinates: " + xmlFile);

						}

						// 동일한 순서의 좌표를 먼저 비교하고, 다르면 좌표로 격자를 찾는다.
						Integer gridNo = gridOrder.get(i);
						double[] coordinate = coordinates.get(gridNo);

						if (Math.abs(lat-coordinate[0]) > coordinateTolerance
								|| Math.abs(lon-coordinate[1]) > coordinateTolerance) {

							gridNo = null;

							for (Integer candidate : gridOrder) {

								double[] c = coordinates.get(candidate);

								if (Math.abs(lat-c[0]) <= coordinateTolerance
										&& Math.abs(lon-c[1]) <= coordinateTolerance) {

									gridNo = candidate;
									break;

								}

							}

						}

						if (gridNo == null || !matchedGrids.add(gridNo)) {

							throw new IllegalArgumentException("Unmatched/duplicate XML grid: " + lat + "," + lon);

						}

						// 현재 FL에 대응하는 기압면의 온도, 풍속, 풍향을 가져온다.
						double analysisTemp = 0, analysisWs = 0, analysisWd = 0;
						boolean analysisWdValid = true;

						for (int index : indexes) {

							double[] row = data.get(gridNo).get(levels[index]);

							if (row == null) {

								throw new IllegalArgumentException("Missing CSV grid/level: " + gridNo+"/"+levels[index]);

							}

							analysisTemp += row[0];
							analysisWs += row[1];
							analysisWd += row[2];

							// 무풍 기압면이 포함되면 해당 격자는 풍향 편향 평균에서 제외한다.
							if (row[1] == 0) {

								analysisWdValid = false;

							}

						}

						// 기압면별로 계산된 값을 단순 평균한다. u, v를 평균하지 않는다.
						// 풍향도 지정한 방식대로 두 기압면의 각도를 더해 나눈다.
						analysisTemp /= indexes.length;
						analysisWs /= indexes.length;
						analysisWd /= indexes.length;

						// 메모리에 읽어 둔 XML에서 예측장의 온도, 풍속, 풍향을 꺼낸다.
						double[] forecast = new double[3];
						String[] tags = {"temp", "ws", "wd"};

						for (int j = 0; j < tags.length; j++) {

							NodeList nodes = grid.getElementsByTagName(tags[j]);

							if (nodes.getLength() != 1) {

								throw new IllegalArgumentException("Missing/duplicate XML value: " + tags[j]);

							}

							forecast[j] = Double.parseDouble(nodes.item(0).getTextContent().trim());

							if (!Double.isFinite(forecast[j])) {

								throw new IllegalArgumentException("Non-finite XML value: " + xmlFile);

							}

						}

						if (forecast[1] < 0 || forecast[2] < 0 || forecast[2] > 360) {

							throw new IllegalArgumentException("Invalid XML wind speed/direction: " + xmlFile);

						}

						// 편향은 예측장 값에서 분석장 값을 뺀 값으로 누적한다.
						tempSum += forecast[0] - analysisTemp;
						wsSum += forecast[1] - analysisWs;
						sampleCount++;

						// 풍향 차이는 -180도 이상 180도 미만으로 맞추며, 무풍 격자는 제외한다.
						if (forecast[1] > 0 && analysisWs > 0 && analysisWdValid) {

							double wdDifference = ((forecast[2] - analysisWd + 540.0) % 360.0) - 180.0;
							wdSum += wdDifference;
							wdSampleCount++;

						}

					}

					// 격자별 차이의 평균을 DB 칼럼명 기준으로 구성한다.
					Map<String, Object> result = new LinkedHashMap<String, Object>();
					result.put("ISSUED_TM", new Date(expectedIssued.getTime()));
					result.put("FCST_TM", new Date(fcstTm.getTime()));
					result.put("DOMAIN", EVAL_WINTEM_DOMAIN);
					result.put("MODEL", EVAL_WINTEM_MODEL);

					// FL_HEIGHT는 FL 번호이다. 예: FL010은 10으로 저장한다.
					result.put("FL_HEIGHT", fl);
					result.put("TEMP_BAIS_AVG", tempSum / sampleCount);
					result.put("WS_BIAS_AVG", wsSum / sampleCount);
					result.put("WD_BIAS_AVG", wdSampleCount == 0 ? null : Double.valueOf(wdSum / wdSampleCount));

					// 아래 표본 수는 확인용이다. DB에 해당 칼럼이 없으면 저장 대상에서 제외한다.
					result.put("SAMPLE_COUNT", sampleCount);
					result.put("WD_SAMPLE_COUNT", wdSampleCount);
					results.add(result);

				}

				// 고도별 결과 23건을 모두 저장하고 커밋한 후에만 성공으로 처리한다.
				this.saveEvalWintemAnalysisResults(results);

				// DB 저장까지 완료한 결과를 원래 파일 묶음에 넣는다.
				evalWintemAnalysisPair.put("evalResultList", results);
				evalWintemAnalysisPair.remove("evalError");

				successCount++;
				resultRowCount += results.size();

				// 기본 로그에는 계산 및 DB 저장 성공 여부만 예보시간별로 표시한다.
				logEval("INFO", "\tH+%02d 계산·DB 저장 성공 (%d개 고도)", fcstHour, results.size());

				// 상세 옵션을 켰을 때만 고도별 편향과 표본 수를 한 단계 더 들여써 출력한다.
				if(this.evalDetailLogEnabled) {

					for (Map<String, Object> row : results) {

						Object direction = row.get("WD_BIAS_AVG");
						String wdText = direction == null ? "N/A"
								: String.format(Locale.ROOT, "%+.4f", ((Number) direction).doubleValue());
						logEval("INFO", "\t\tFL%03d 저장 성공: TEMP=%+.4f ℃, WS=%+.4f kt, WD=%s deg, N=%d, WD_N=%d",
								((Number)row.get("FL_HEIGHT")).intValue(),
								((Number)row.get("TEMP_BAIS_AVG")).doubleValue(),
								((Number)row.get("WS_BIAS_AVG")).doubleValue(), wdText,
								((Number)row.get("SAMPLE_COUNT")).intValue(),
								((Number)row.get("WD_SAMPLE_COUNT")).intValue());

					}

				}

			} catch (Exception e) {

				// 실패한 파일 묶음에는 결과를 남기지 않으며, 오류는 상세 옵션과 관계없이 출력한다.
				if(evalWintemAnalysisPair != null) {

					evalWintemAnalysisPair.remove("evalResultList");
					evalWintemAnalysisPair.put("evalError", e.getMessage());

				}

				failureCount++;
				Object failedHour = evalWintemAnalysisPair == null ? "N/A" : evalWintemAnalysisPair.get("fcstHour");
				logEval("ERROR", "\t계산·DB 저장 실패: fcstHour=%s, reason=%s", failedHour,
						e.getClass().getSimpleName()+": "+e.getMessage());

				if(this.evalDetailLogEnabled) {

					e.printStackTrace(System.err);

				}

				// 롤백까지 실패하면 다음 파일 묶음을 저장하지 않고 전체 처리를 중단한다.
				if(e instanceof EvalDatabaseRollbackException) {

					throw new IllegalStateException("DB 롤백 실패로 전체 처리를 중단합니다.", e);

				}

			}

		}

		// 실제 저장한 파일 묶음의 성공·실패·누락 개수를 요약한다.
		int skippedCount = Math.max(0, MAX_FCST_HOUR+1-pairTotal);
		String status = failureCount == 0 && skippedCount == 0 ? "전체 계산·DB 저장 성공"
				: successCount == 0 ? "계산·DB 저장 실패" : "일부 계산·DB 저장 완료";
		logEval(failureCount > 0 ? "ERROR" : skippedCount > 0 ? "WARN" : "INFO",
				"\t%s: 성공=%d, 실패=%d, 누락=%d, 고도=%d, 소요=%.3fs",
				status, successCount, failureCount, skippedCount, resultRowCount,
				(System.nanoTime()-evaluationStarted)/1_000_000_000.0);

	}

	private void saveEvalWintemAnalysisResults(List<Map<String, Object>> results) throws Exception {

		if(this.dbManager == null) {

			throw new IllegalStateException("DatabaseManager가 초기화되지 않았습니다.");

		}

		if(results == null || results.size() != WINTEM_XML_FILE_COUNT) {

			throw new IllegalArgumentException("DB 저장에는 고도별 결과 23건이 필요합니다.");

		}

		// 모든 SQL을 먼저 구성하여 값 검증이 끝난 뒤에 DB 작업을 시작한다.
		List<String> queryList = new ArrayList<String>();

		for(Map<String, Object> result : results) {

			queryList.add(this.buildEvalWintemMergeQuery(result));

		}

		try {

			// SQL 오류는 executeUpdate() 내부의 기존 로그 처리를 사용한다.
			for(String query : queryList) {

				this.dbManager.executeUpdate(query);

			}

			// initialize()의 setAutoCommit(false)를 사용하여 파일 묶음별로 커밋한다.
			this.dbManager.commit();

		} catch (Exception e) {

			try {

				// 호출자에게 전달된 오류가 있으면 현재 파일 묶음의 트랜잭션을 롤백한다.
				this.dbManager.rollback();

			} catch (Exception rollbackError) {

				EvalDatabaseRollbackException failure = new EvalDatabaseRollbackException(
						"DB 롤백 실패: " + rollbackError.getMessage(), e);
				failure.addSuppressed(rollbackError);
				throw failure;

			}

			throw e;

		}

	}

	private String buildEvalWintemMergeQuery(Map<String, Object> result) {

		if(result == null || !(result.get("ISSUED_TM") instanceof Date)
				|| !(result.get("FCST_TM") instanceof Date)
				|| !(result.get("FL_HEIGHT") instanceof Integer)) {

			throw new IllegalArgumentException("DB 저장 결과의 시각 또는 고도가 잘못되었습니다.");

		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		String issuedText = sdf.format((Date)result.get("ISSUED_TM"));
		String fcstText = sdf.format((Date)result.get("FCST_TM"));
		String model = this.getEvalSqlText(result.get("MODEL"));
		String domain = this.getEvalSqlText(result.get("DOMAIN"));
		String flHeight = Integer.toString((Integer)result.get("FL_HEIGHT"));

		// TEMP_BAIS_AVG는 전달받은 실제 DB 칼럼명이다. BAIS 철자를 그대로 사용한다.
		String tempBiasAvg = this.getEvalSqlNumber(result.get("TEMP_BAIS_AVG"));
		String wsBiasAvg = this.getEvalSqlNumber(result.get("WS_BIAS_AVG"));
		String wdBiasAvg = this.getEvalSqlNumber(result.get("WD_BIAS_AVG"));

		// 같은 발표·예측시각, 모델, 도메인, 고도는 갱신하고 새 결과는 추가한다.
		// SAMPLE_COUNT와 WD_SAMPLE_COUNT는 확인용이므로 저장 칼럼에서 제외한다.
		String query = "MERGE INTO " + EVAL_WINTEM_RESULT_TABLE + " T " +
				"USING (SELECT " +
				"TO_DATE('" + issuedText + "', 'YYYYMMDDHH24MISS') AS ISSUED_TM, " +
				"TO_DATE('" + fcstText + "', 'YYYYMMDDHH24MISS') AS FCST_TM, " +
				model + " AS MODEL, " + domain + " AS DOMAIN, " + flHeight + " AS FL_HEIGHT, " +
				tempBiasAvg + " AS TEMP_BAIS_AVG, " + wsBiasAvg + " AS WS_BIAS_AVG, " +
				wdBiasAvg + " AS WD_BIAS_AVG FROM DUAL) S " +
				"ON (T.ISSUED_TM = S.ISSUED_TM AND T.FCST_TM = S.FCST_TM " +
				"AND T.MODEL = S.MODEL AND T.DOMAIN = S.DOMAIN AND T.FL_HEIGHT = S.FL_HEIGHT) " +
				"WHEN MATCHED THEN UPDATE SET " +
				"T.TEMP_BAIS_AVG = S.TEMP_BAIS_AVG, T.WS_BIAS_AVG = S.WS_BIAS_AVG, " +
				"T.WD_BIAS_AVG = S.WD_BIAS_AVG " +
				"WHEN NOT MATCHED THEN INSERT " +
				"(ISSUED_TM, FCST_TM, MODEL, DOMAIN, FL_HEIGHT, TEMP_BAIS_AVG, WS_BIAS_AVG, WD_BIAS_AVG) " +
				"VALUES (S.ISSUED_TM, S.FCST_TM, S.MODEL, S.DOMAIN, S.FL_HEIGHT, " +
				"S.TEMP_BAIS_AVG, S.WS_BIAS_AVG, S.WD_BIAS_AVG)";

		return query;

	}

	private String getEvalSqlText(Object value) {

		if(!(value instanceof String) || ((String)value).isEmpty()) {

			throw new IllegalArgumentException("DB 저장 결과의 모델 또는 도메인이 비어 있습니다.");

		}

		return "'" + ((String)value).replace("'", "''") + "'";

	}

	private String getEvalSqlNumber(Object value) {

		if(value == null) {

			return "NULL";

		}

		if(!(value instanceof Number) || !Double.isFinite(((Number)value).doubleValue())) {

			throw new IllegalArgumentException("DB 저장 결과에 유효하지 않은 편향 값이 있습니다.");

		}

		// 지역 설정에 따른 쉼표나 지수 표기 없이 숫자 리터럴을 구성한다.
		return java.math.BigDecimal.valueOf(((Number)value).doubleValue()).toPlainString();

	}

	private static class EvalDatabaseRollbackException extends Exception {

		private static final long serialVersionUID = 1L;

		private EvalDatabaseRollbackException(String message, Throwable cause) {

			super(message, cause);

		}

	}

	/** 로그 출력 시각은 실행 환경의 시간대이며, 자료의 발표·예측시각은 UTC이다. */
	private void logEval(String level, String format, Object... args) {

		String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT)
				.format(new Date());
		String message = String.format(Locale.ROOT, format, args);
		java.io.PrintStream stream = "ERROR".equals(level) ? System.err : System.out;
		stream.printf(Locale.ROOT, "%s [%-5s] [WINTEM-EVAL] %s%n", timestamp, level, message);

	}

	public static void main(String[] args) {

		CalculateEvalWintemAnalysis anal = new CalculateEvalWintemAnalysis();
		anal.process();

	}

}
