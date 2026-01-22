// Thema Signal - 메인 애플리케이션
let currentPeriod = '3w';

// 초기화
document.addEventListener('DOMContentLoaded', async () => {
    showLoading(true);
    const success = await loadAllData();
    showLoading(false);

    if (success) {
        renderThemeRanking();
        updateBaseDate();
    } else {
        showError('데이터 로드에 실패했습니다. 페이지를 새로고침 해주세요.');
    }

    setupEventListeners();
});

// 로딩 표시
function showLoading(show) {
    const container = document.getElementById('themeRanking');
    if (show) {
        container.innerHTML = '<div class="loading-message">데이터 로딩 중...</div>';
    }
}

// 에러 표시
function showError(message) {
    const container = document.getElementById('themeRanking');
    container.innerHTML = `<div class="error-message">${message}</div>`;
}

// 기준일 업데이트
function updateBaseDate() {
    const footer = document.querySelector('.footer p:last-child');
    if (footer) {
        footer.textContent = `데이터 기준: ${getBaseDate()} 장 마감 기준`;
    }
}

// 이벤트 리스너 설정
function setupEventListeners() {
    // 기간 탭 클릭
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', (e) => {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            e.target.classList.add('active');
            currentPeriod = e.target.dataset.period;
            renderThemeRanking();
        });
    });

    // 모달 닫기
    document.getElementById('modalClose').addEventListener('click', closeModal);
    document.getElementById('themeModal').addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) {
            closeModal();
        }
    });

    // ESC 키로 모달 닫기
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeModal();
        }
    });
}

// 테마 순위 렌더링
function renderThemeRanking() {
    const themes = getThemesByPeriod(currentPeriod);
    const container = document.getElementById('themeRanking');

    if (themes.length === 0) {
        container.innerHTML = '<div class="empty-message">표시할 테마가 없습니다.</div>';
        return;
    }

    container.innerHTML = themes.map((theme, index) => {
        const rank = theme.metrics[`rank_${currentPeriod}`];
        const returnVal = theme.metrics[`return_${currentPeriod}`];
        const spread = Math.max(theme.metrics.spread_3w, theme.metrics.spread_6w);

        // 순위 변화 표시
        const rankChanges = getRankChanges(theme);

        // 단계별 스타일
        const stageClass = getStageClass(theme.metrics.stage);
        const isOverheated = theme.metrics.stage === '3단계';
        const isSettling = theme.metrics.stage === '정리' || theme.metrics.stage === '소멸';

        // 종목별 수익률 기준 TOP 3 계산
        const sortedStocks = Object.entries(theme.stockMetrics)
            .map(([stockCode, metrics]) => ({
                stockCode,
                stock: getStock(stockCode),
                returnVal: metrics[`return_${currentPeriod}`] || 0
            }))
            .sort((a, b) => b.returnVal - a.returnVal)
            .slice(0, 3);

        const top3Html = sortedStocks.map((item, idx) => `
            <div class="top-stock-item">
                <span class="top-stock-rank rank-${idx + 1}">${idx === 0 ? '👑' : idx + 1}</span>
                <span class="top-stock-name">${item.stock.name}</span>
                <span class="top-stock-return ${item.returnVal >= 0 ? 'positive' : 'negative'}">
                    ${item.returnVal >= 0 ? '+' : ''}${item.returnVal.toFixed(1)}%
                </span>
            </div>
        `).join('');

        return `
            <div class="theme-card ${stageClass}" data-theme-id="${theme.id}">
                <div class="theme-header">
                    <div class="theme-rank">${rank}위</div>
                    <div class="theme-name">${theme.name}</div>
                    <div class="theme-return ${returnVal >= 0 ? 'positive' : 'negative'}">
                        ${returnVal >= 0 ? '+' : ''}${returnVal.toFixed(1)}%
                    </div>
                </div>

                <div class="theme-badges">
                    <span class="badge stage-badge ${stageClass}">${theme.metrics.stage}</span>
                    <span class="badge spread-badge ${isOverheated ? 'warning' : ''}">
                        확산 ${spread}%${isOverheated ? '⚠️' : ''}
                    </span>
                    ${isSettling ? '<span class="badge settle-badge">📉</span>' : ''}
                </div>

                <div class="theme-ranks">
                    <span class="rank-item ${currentPeriod === '3w' ? 'active' : ''}">
                        3주 ${theme.metrics.rank_3w}위${rankChanges.trend_3w}
                    </span>
                    <span class="rank-item ${currentPeriod === '6w' ? 'active' : ''}">
                        6주 ${theme.metrics.rank_6w}위${rankChanges.trend_6w}
                    </span>
                    <span class="rank-item ${currentPeriod === '9w' ? 'active' : ''}">
                        9주 ${theme.metrics.rank_9w}위${rankChanges.trend_9w}
                    </span>
                </div>

                <div class="theme-top-stocks">
                    ${top3Html}
                </div>
            </div>
        `;
    }).join('');

    // 카드 클릭 이벤트
    container.querySelectorAll('.theme-card').forEach(card => {
        card.addEventListener('click', () => {
            const themeId = card.dataset.themeId;
            openThemeDetail(themeId);
        });
    });
}

// 순위 변화 계산
function getRankChanges(theme) {
    const { rank_3w, rank_6w, rank_9w } = theme.metrics;

    return {
        trend_3w: rank_3w < rank_6w ? '↑' : rank_3w > rank_6w ? '↓' : '',
        trend_6w: rank_6w < rank_9w ? '↑' : rank_6w > rank_9w ? '↓' : '',
        trend_9w: ''
    };
}

// 단계별 클래스
function getStageClass(stage) {
    switch (stage) {
        case '0단계': return 'stage-0';
        case '1단계': return 'stage-1';
        case '2단계': return 'stage-2';
        case '3단계': return 'stage-3';
        case '정리': return 'stage-settle';
        case '소멸': return 'stage-extinct';
        default: return '';
    }
}

// 테마 상세 모달 열기
function openThemeDetail(themeId) {
    const theme = CALCULATED_THEMES.find(t => t.id === themeId);
    if (!theme) return;

    const modal = document.getElementById('themeModal');
    const detailContainer = document.getElementById('themeDetail');

    // 종목별 수익률 정렬
    const sortedStocks = Object.entries(theme.stockMetrics)
        .map(([stockCode, metrics]) => ({
            stockCode,
            stock: getStock(stockCode),
            metrics
        }))
        .sort((a, b) => b.metrics[`return_${currentPeriod}`] - a.metrics[`return_${currentPeriod}`]);

    // 대장주 정보 안전하게 가져오기
    const leader3w = theme.metrics.leader_3w;
    const leader6w = theme.metrics.leader_6w;
    const leader9w = theme.metrics.leader_9w;
    const leaderVolume = theme.metrics.leader_volume;

    const leader3wMetrics = theme.stockMetrics[leader3w] || { return_3w: 0 };
    const leader6wMetrics = theme.stockMetrics[leader6w] || { return_6w: 0 };
    const leader9wMetrics = theme.stockMetrics[leader9w] || { return_9w: 0 };
    const leaderVolumeMetrics = theme.stockMetrics[leaderVolume] || { avg_volume_1w: 0 };

    detailContainer.innerHTML = `
        <div class="detail-header">
            <div class="detail-stage">
                <span class="badge">${theme.metrics.stage} ${theme.metrics.stageLabel}</span>
            </div>
            <h2>${theme.name} 테마</h2>
            <div class="detail-return ${theme.metrics[`return_${currentPeriod}`] >= 0 ? 'positive' : 'negative'}">
                ${currentPeriod === '3w' ? '3주' : currentPeriod === '6w' ? '6주' : '9주'} 수익률:
                ${theme.metrics[`return_${currentPeriod}`] >= 0 ? '+' : ''}${theme.metrics[`return_${currentPeriod}`].toFixed(1)}%
            </div>
        </div>

        <div class="detail-body">
            <div class="detail-grid">
                <div class="detail-card">
                    <div class="detail-spread">
                        <h4>📊 확산도</h4>
                        <div class="spread-bars">
                            <div class="spread-item">
                                <span>3주</span>
                                <div class="spread-bar">
                                    <div class="spread-fill" style="width: ${Math.min(theme.metrics.spread_3w, 100)}%"></div>
                                </div>
                                <span>${theme.metrics.spread_3w}%</span>
                            </div>
                            <div class="spread-item">
                                <span>6주</span>
                                <div class="spread-bar">
                                    <div class="spread-fill" style="width: ${Math.min(theme.metrics.spread_6w, 100)}%"></div>
                                </div>
                                <span>${theme.metrics.spread_6w}%</span>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="detail-card">
                    <div class="detail-info">
                        <h4>📋 테마 정보</h4>
                        <p>종목 수: ${theme.stocks.length}개</p>
                        <p>3주 수익률: ${theme.metrics.return_3w.toFixed(1)}%</p>
                        <p>6주 수익률: ${theme.metrics.return_6w.toFixed(1)}%</p>
                        <p>9주 수익률: ${theme.metrics.return_9w.toFixed(1)}%</p>
                    </div>
                </div>
            </div>

            <div class="detail-card">
                <div class="detail-leaders">
                    <h4>👑 기간별 대장주</h4>
                    <div class="leader-grid">
                        <div class="leader-item">
                            <span class="period">3주 대장</span>
                            <span class="name">${getStock(leader3w).name}</span>
                            <span class="return ${leader3wMetrics.return_3w >= 0 ? 'positive' : 'negative'}">
                                ${leader3wMetrics.return_3w >= 0 ? '+' : ''}${leader3wMetrics.return_3w.toFixed(1)}%
                            </span>
                        </div>
                        <div class="leader-item">
                            <span class="period">6주 대장</span>
                            <span class="name">${getStock(leader6w).name}</span>
                            <span class="return ${leader6wMetrics.return_6w >= 0 ? 'positive' : 'negative'}">
                                ${leader6wMetrics.return_6w >= 0 ? '+' : ''}${leader6wMetrics.return_6w.toFixed(1)}%
                            </span>
                        </div>
                        <div class="leader-item">
                            <span class="period">9주 대장</span>
                            <span class="name">${getStock(leader9w).name}</span>
                            <span class="return ${leader9wMetrics.return_9w >= 0 ? 'positive' : 'negative'}">
                                ${leader9wMetrics.return_9w >= 0 ? '+' : ''}${leader9wMetrics.return_9w.toFixed(1)}%
                            </span>
                        </div>
                        <div class="leader-item volume">
                            <span class="period">거래대금 1위</span>
                            <span class="name">${getStock(leaderVolume).name}</span>
                            <span class="volume-val">${formatVolume(leaderVolumeMetrics.avg_volume_1w)}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="detail-card full-width">
                <div class="detail-stocks">
                    <h4>📈 종목별 현황 (${currentPeriod === '3w' ? '3주' : currentPeriod === '6w' ? '6주' : '9주'} 기준 정렬)</h4>
                    <table class="stock-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>종목명</th>
                                <th>3주</th>
                                <th>6주</th>
                                <th>9주</th>
                                <th>거래대금(1주)</th>
                                <th>시총</th>
                                <th>매출액</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${sortedStocks.map((item, idx) => {
                                const marketData = DATA.market[item.stockCode] || {};
                                const financialData = DATA.financial[item.stockCode] || {};
                                return `
                                <tr class="${theme.metrics[`leader_${currentPeriod}`] === item.stockCode ? 'leader-row' : ''}">
                                    <td>${idx + 1}</td>
                                    <td>
                                        <span class="stock-name">${item.stock.name}</span>
                                        <span class="stock-code">${item.stockCode}</span>
                                    </td>
                                    <td class="${item.metrics.return_3w >= 10 ? 'highlight' : ''} ${item.metrics.return_3w >= 0 ? 'positive' : 'negative'}">
                                        ${item.metrics.return_3w >= 0 ? '+' : ''}${item.metrics.return_3w.toFixed(1)}%
                                    </td>
                                    <td class="${item.metrics.return_6w >= 15 ? 'highlight' : ''} ${item.metrics.return_6w >= 0 ? 'positive' : 'negative'}">
                                        ${item.metrics.return_6w >= 0 ? '+' : ''}${item.metrics.return_6w.toFixed(1)}%
                                    </td>
                                    <td class="${item.metrics.return_9w >= 0 ? 'positive' : 'negative'}">
                                        ${item.metrics.return_9w >= 0 ? '+' : ''}${item.metrics.return_9w.toFixed(1)}%
                                    </td>
                                    <td>${formatVolume(item.metrics.avg_volume_1w)}</td>
                                    <td>${formatVolume(marketData.market_cap || 0)}</td>
                                    <td>${formatVolume(financialData.revenue || 0)}</td>
                                </tr>
                            `}).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;

    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
}

// 모달 닫기
function closeModal() {
    const modal = document.getElementById('themeModal');
    modal.classList.remove('active');
    document.body.style.overflow = '';
}
