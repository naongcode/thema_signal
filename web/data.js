// Thema Signal - 데이터 로드 및 계산 모듈

// 전역 데이터 저장소
let DATA = {
    stocks: {},      // 종목 기본정보
    themes: [],      // 테마 목록
    prices: {},      // 가격 데이터 (월별 통합)
    market: {},      // 시장 데이터
    financial: {},   // 재무 데이터
    baseDate: null,  // 기준일
    loaded: false
};

// 계산된 테마 데이터
let CALCULATED_THEMES = [];

// 데이터 로드
async function loadAllData() {
    try {
        console.log('데이터 로드 시작...');

        // 병렬로 모든 데이터 로드
        const [stocks, themes, market, financial] = await Promise.all([
            fetch('data/stocks.json').then(r => r.json()),
            fetch('data/themes.json').then(r => r.json()),
            fetch('data/market.json').then(r => r.json()),
            fetch('data/financial.json').then(r => r.json())
        ]);

        DATA.stocks = stocks;
        DATA.themes = themes.themes;
        DATA.market = market.data;
        DATA.financial = financial.data;
        DATA.baseDate = market.date;

        // 가격 데이터 로드 (최근 3개월)
        const priceMonths = getRecentMonths(3);
        const pricePromises = priceMonths.map(month =>
            fetch(`data/prices/${month}.json`)
                .then(r => r.ok ? r.json() : {})
                .catch(() => ({}))
        );
        const priceDataArray = await Promise.all(pricePromises);

        // 가격 데이터 병합
        DATA.prices = {};
        priceDataArray.forEach(monthData => {
            for (const [code, dates] of Object.entries(monthData)) {
                if (!DATA.prices[code]) DATA.prices[code] = {};
                Object.assign(DATA.prices[code], dates);
            }
        });

        DATA.loaded = true;
        console.log(`데이터 로드 완료: ${Object.keys(DATA.stocks).length}개 종목, ${DATA.themes.length}개 테마`);

        // 테마 지표 계산
        calculateAllThemeMetrics();

        return true;
    } catch (error) {
        console.error('데이터 로드 실패:', error);
        return false;
    }
}

// 최근 N개월 목록 반환
function getRecentMonths(n) {
    const months = [];
    const now = new Date();
    for (let i = 0; i < n; i++) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
    }
    return months;
}

// 종목의 N일전 종가 가져오기
function getClosePrice(code, daysAgo = 0) {
    const priceData = DATA.prices[code];
    if (!priceData) return null;

    const dates = Object.keys(priceData).sort().reverse();
    if (dates.length <= daysAgo) return null;

    return priceData[dates[daysAgo]]?.close || null;
}

// 종목의 N주 수익률 계산
function calcReturn(code, weeks) {
    const tradingDays = weeks * 5; // 주당 약 5거래일
    const currentPrice = getClosePrice(code, 0);
    const pastPrice = getClosePrice(code, tradingDays);

    if (!currentPrice || !pastPrice || pastPrice === 0) return null;
    return ((currentPrice - pastPrice) / pastPrice) * 100;
}

// 종목의 최근 1주 평균 거래대금
function calcAvgVolume(code, days = 5) {
    const priceData = DATA.prices[code];
    if (!priceData) return 0;

    const dates = Object.keys(priceData).sort().reverse().slice(0, days);
    if (dates.length === 0) return 0;

    const total = dates.reduce((sum, date) => sum + (priceData[date]?.value || 0), 0);
    return total / dates.length;
}

// 테마 수익률 계산 (상위 3~5개 평균)
function calcThemeReturn(theme, weeks) {
    const returns = theme.stocks
        .map(code => calcReturn(code, weeks))
        .filter(r => r !== null)
        .sort((a, b) => b - a);

    if (returns.length === 0) return 0;

    // 상위 3~5개 평균 (종목 수에 따라 조정)
    const topCount = Math.min(Math.max(3, Math.floor(returns.length / 2)), 5);
    const topReturns = returns.slice(0, topCount);
    return topReturns.reduce((a, b) => a + b, 0) / topReturns.length;
}

// 확산도 계산 (threshold 이상 상승한 종목 비율)
function calcSpread(theme, weeks, threshold) {
    const returns = theme.stocks
        .map(code => calcReturn(code, weeks))
        .filter(r => r !== null);

    if (returns.length === 0) return 0;

    const aboveThreshold = returns.filter(r => r >= threshold).length;
    return Math.round((aboveThreshold / returns.length) * 100);
}

// 대장주 찾기 (수익률 기준)
function findLeader(theme, weeks) {
    let maxReturn = -Infinity;
    let leader = null;

    for (const code of theme.stocks) {
        const ret = calcReturn(code, weeks);
        if (ret !== null && ret > maxReturn) {
            maxReturn = ret;
            leader = code;
        }
    }
    return leader;
}

// 거래대금 대장주 찾기
function findVolumeLeader(theme) {
    let maxVolume = 0;
    let leader = null;

    for (const code of theme.stocks) {
        const vol = calcAvgVolume(code);
        if (vol > maxVolume) {
            maxVolume = vol;
            leader = code;
        }
    }
    return leader;
}

// 단계 결정
function determineStage(return3w, return6w, spread3w, spread6w) {
    // 기획서 기준:
    // 0단계(주목): 1~2개 종목만 상승
    // 1단계(초기): 확산도 0~20%, 수익률 상승
    // 2단계(확산): 확산도 20~50%, 수익률 상승
    // 3단계(과열): 확산도 50%+, 수익률 고점

    const maxSpread = Math.max(spread3w, spread6w);

    if (maxSpread >= 50) return { stage: '3단계', label: '과열' };
    if (maxSpread >= 20) return { stage: '2단계', label: '확산' };
    if (return3w >= 10 || return6w >= 15) return { stage: '1단계', label: '초기' };
    if (return3w >= 5 || return6w >= 8) return { stage: '0단계', label: '주목' };

    // 하락 추세 판단
    if (return3w < 0 && spread3w < 10) {
        if (return6w < 0) return { stage: '소멸', label: '소멸' };
        return { stage: '정리', label: '정리' };
    }

    return { stage: '0단계', label: '주목' };
}

// 모든 테마 지표 계산
function calculateAllThemeMetrics() {
    console.log('테마 지표 계산 시작...');

    const themeMetrics = DATA.themes.map(theme => {
        // 기간별 수익률
        const return_3w = calcThemeReturn(theme, 3);
        const return_6w = calcThemeReturn(theme, 6);
        const return_9w = calcThemeReturn(theme, 9);

        // 확산도
        const spread_3w = calcSpread(theme, 3, 10);  // 3주 10% 이상
        const spread_6w = calcSpread(theme, 6, 15);  // 6주 15% 이상

        // 대장주
        const leader_3w = findLeader(theme, 3);
        const leader_6w = findLeader(theme, 6);
        const leader_9w = findLeader(theme, 9);
        const leader_volume = findVolumeLeader(theme);

        // 단계
        const { stage, label } = determineStage(return_3w, return_6w, spread_3w, spread_6w);

        // 종목별 지표
        const stockMetrics = {};
        for (const code of theme.stocks) {
            stockMetrics[code] = {
                return_3w: calcReturn(code, 3) || 0,
                return_6w: calcReturn(code, 6) || 0,
                return_9w: calcReturn(code, 9) || 0,
                avg_volume_1w: calcAvgVolume(code)
            };
        }

        return {
            id: theme.id,
            name: theme.name,
            stocks: theme.stocks,
            metrics: {
                return_3w,
                return_6w,
                return_9w,
                spread_3w,
                spread_6w,
                rank_3w: 0,  // 나중에 계산
                rank_6w: 0,
                rank_9w: 0,
                stage,
                stageLabel: label,
                leader_3w,
                leader_6w,
                leader_9w,
                leader_volume
            },
            stockMetrics,
            history: []  // 히스토리는 별도 저장 필요
        };
    });

    // 순위 계산
    const sortBy3w = [...themeMetrics].sort((a, b) => b.metrics.return_3w - a.metrics.return_3w);
    const sortBy6w = [...themeMetrics].sort((a, b) => b.metrics.return_6w - a.metrics.return_6w);
    const sortBy9w = [...themeMetrics].sort((a, b) => b.metrics.return_9w - a.metrics.return_9w);

    sortBy3w.forEach((t, i) => t.metrics.rank_3w = i + 1);
    sortBy6w.forEach((t, i) => t.metrics.rank_6w = i + 1);
    sortBy9w.forEach((t, i) => t.metrics.rank_9w = i + 1);

    CALCULATED_THEMES = themeMetrics;
    console.log(`테마 지표 계산 완료: ${CALCULATED_THEMES.length}개 테마`);
}

// 기간별 정렬된 테마 목록 반환
function getThemesByPeriod(period) {
    const rankKey = `rank_${period}`;
    return [...CALCULATED_THEMES].sort((a, b) => a.metrics[rankKey] - b.metrics[rankKey]);
}

// 종목 정보 가져오기
function getStock(code) {
    const stock = DATA.stocks[code];
    if (!stock) return { code, name: code, market: 'UNKNOWN' };
    return { code, ...stock };
}

// 거래대금 포맷팅
function formatVolume(volume) {
    if (volume >= 1000000000000) {
        return (volume / 1000000000000).toFixed(1) + '조';
    } else if (volume >= 100000000) {
        return Math.round(volume / 100000000) + '억';
    }
    return volume.toLocaleString();
}

// 기준일 포맷팅
function getBaseDate() {
    return DATA.baseDate || new Date().toISOString().split('T')[0];
}
