package kama.daemon.model.prediction.adopt.DFS;

public enum DFS_Type {

	// 3시간기온
	T3H(-50),

	// 일 최저기온
	TMN(-50),

	// 일 최고기온
	TMX(-50),

	// 풍속(동서성분)
	UUU(-100),

	// 풍속(남북성분)
	VVV(-100),

	// 풍향
	VEC(-1),

	// 풍속
	WSD(-1),

	// 하늘상태
	SKY(-1),

	// 강수형태
	PTY(-1),

	// 강수확률
	POP(-1),

	// 6시간 강수량
	R06(-1),

	// 6시간 신적설
	S06(-1),

	// 습도
	REH(-1),

	// 파고
	WAV(-1);

	private final float missingValue;

	DFS_Type(final float missingValue) {
		this.missingValue = missingValue;
	}

	public float getMissingValue() {
		return this.missingValue;
	}

}
