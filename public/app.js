const matchesEl = document.querySelector("#matches");
const matchesMetaEl = document.querySelector("#matchesMeta");
const summaryEl = document.querySelector("#summary");
const strategyEl = document.querySelector("#strategy");
const ticketsEl = document.querySelector("#tickets");
const notesEl = document.querySelector("#notes");
const historySummaryEl = document.querySelector("#historySummary");
const historyListEl = document.querySelector("#historyList");
const learningPanelEl = document.querySelector("#learningPanel");

const bankrollEl = document.querySelector("#bankroll");
const startDateEl = document.querySelector("#startDate");
const endDateEl = document.querySelector("#endDate");
const focusCountryEl = document.querySelector("#focusCountry");
const maxMatchesEl = document.querySelector("#maxMatches");
const riskEl = document.querySelector("#risk");
const riskLabelEl = document.querySelector("#riskLabel");
const teamQueryEl = document.querySelector("#teamQuery");
const datePresetButtons = document.querySelectorAll(".calendar-presets [data-range-days]");
const apiBaseUrlEl = document.querySelector("#apiBaseUrl");
const saveApiBaseBtn = document.querySelector("#saveApiBaseBtn");

const loadMatchesBtn = document.querySelector("#loadMatchesBtn");
const generateBtn = document.querySelector("#generateBtn");
const searchTeamBtn = document.querySelector("#searchTeamBtn");
const refreshHistoryBtn = document.querySelector("#refreshHistoryBtn");
const trainingMetaEl = document.querySelector("#trainingMeta");

const configuredApiBaseUrl = String(window.PREDICT_CONFIG?.API_BASE_URL || "").trim().replace(/\/$/, "");
const storedApiBaseUrl = String(window.localStorage?.getItem("predict.apiBaseUrl") || "").trim().replace(/\/$/, "");
const API_BASE_URL = storedApiBaseUrl || configuredApiBaseUrl;
const runningOnGithubPages = typeof window !== "undefined" && window.location?.hostname.endsWith("github.io");
const canUseSameOriginApi = !runningOnGithubPages;
const hasApiEndpoint = Boolean(API_BASE_URL) || canUseSameOriginApi;

function apiUrl(path) {
  if (!API_BASE_URL) {
    return path;
  }
  return `${API_BASE_URL}${path}`;
}

function apiSetupHint() {
  if (API_BASE_URL) {
    return `API configurata: ${API_BASE_URL}`;
  }
  return "Configura public/config.js con API_BASE_URL per usare GitHub Pages.";
}

function normalizeApiBaseInput(value) {
  return String(value || "").trim().replace(/\/$/, "");
}

function currentFocusCountry() {
  return String(focusCountryEl?.value || "Italia").trim() || "Italia";
}

function euro(value) {
  return new Intl.NumberFormat("it-IT", {
    style: "currency",
    currency: "EUR"
  }).format(value);
}

function percent(value) {
  return `${Math.round(value * 100)}%`;
}

function riskProfileLabel(riskDecimal) {
  if (riskDecimal <= 0.2) {
    return "Molto prudente";
  }
  if (riskDecimal <= 0.4) {
    return "Prudente";
  }
  if (riskDecimal <= 0.6) {
    return "Bilanciato";
  }
  if (riskDecimal <= 0.8) {
    return "Spinto";
  }
  return "Aggressivo";
}

function parseLocalizedNumber(value, fallback) {
  const normalized = String(value).replace(",", ".").trim();
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function currentRiskDecimal() {
  const sliderValue = parseLocalizedNumber(riskEl.value, 45);
  return sliderValue / 100;
}

function updateRiskLabel() {
  const riskDecimal = currentRiskDecimal();
  riskLabelEl.textContent = `${Math.round(riskDecimal * 100)}% · ${riskProfileLabel(riskDecimal)}`;
}

function formatIsoDateLocal(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDaysIso(isoDate, days) {
  const parsed = new Date(`${String(isoDate)}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return isoDate;
  }
  parsed.setDate(parsed.getDate() + Number(days || 0));
  return formatIsoDateLocal(parsed);
}

function daysBetweenInclusive(startIso, endIso) {
  const start = new Date(`${String(startIso)}T00:00:00`);
  const end = new Date(`${String(endIso)}T00:00:00`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return 7;
  }
  const ms = end.getTime() - start.getTime();
  const day = 24 * 60 * 60 * 1000;
  return Math.floor(ms / day) + 1;
}

function normalizeDateWindowFromInputs() {
  const todayIso = formatIsoDateLocal(new Date());
  const start = startDateEl?.value || todayIso;
  let end = endDateEl?.value || addDaysIso(start, 6);

  if (String(end) < String(start)) {
    end = start;
    if (endDateEl) {
      endDateEl.value = end;
    }
  }

  const rawDays = daysBetweenInclusive(start, end);
  const timeRangeDays = Math.max(1, Math.min(14, rawDays));
  const normalizedEnd = addDaysIso(start, timeRangeDays - 1);

  if (endDateEl && endDateEl.value !== normalizedEnd) {
    endDateEl.value = normalizedEnd;
  }

  return {
    startDate: start,
    endDate: normalizedEnd,
    timeRangeDays
  };
}

function updatePresetActiveState(days) {
  for (const btn of datePresetButtons) {
    const btnDays = parseLocalizedNumber(btn.dataset.rangeDays, 0);
    const active = Number(btnDays) === Number(days);
    btn.classList.toggle("is-active", active);
    btn.setAttribute("aria-pressed", active ? "true" : "false");
  }
}

function ensureDateWindowDefaults() {
  if (!startDateEl || !endDateEl) {
    return;
  }
  if (!startDateEl.value) {
    startDateEl.value = formatIsoDateLocal(new Date());
  }
  if (!endDateEl.value) {
    endDateEl.value = addDaysIso(startDateEl.value, 6);
  }
  normalizeDateWindowFromInputs();
}

function renderMatches(matches, meta = {}) {
  if (!matches.length) {
    matchesMetaEl.textContent = "";
    matchesEl.innerHTML = "<p>Nessuna partita trovata.</p>";
    return;
  }

  const teamInfo = meta.teamQuery ? ` · Filtro squadra: ${meta.teamQuery}` : "";
  const rangeInfo = meta.startDate && meta.endDate
    ? ` · Finestra: ${meta.startDate} → ${meta.endDate}`
    : meta.startDate
      ? ` · Dal: ${meta.startDate}`
      : "";
  matchesMetaEl.textContent = `Fonte: ${meta.sourceType || "n/d"} · Range: ${meta.timeRangeDays || 7} giorni${rangeInfo}${teamInfo}`;

  matchesEl.innerHTML = matches
    .slice(0, 20)
    .map(
      (match) => `
      <article class="item">
        <h3>${match.match}</h3>
        <p>Pick: <strong>${match.pick}</strong> · Backup: <strong>${match.backupPick}</strong></p>
        ${(match.marketCandidates || []).length >= 2
          ? `<p>Piano 2 mercati: <strong>${match.marketCandidates[0].market}</strong> · <strong>${match.marketCandidates[1].market}</strong></p>`
          : ""}
        ${match.safetyScore ? `<p>Indice sicurezza: <strong>${Math.round(match.safetyScore * 100)}%</strong></p>` : ""}
        <p>Data: ${match.matchDate || "ND"} · Giornata: ${match.matchday || "ND"}</p>
        <p>Campionato: ${match.tournament || "ND"} · Paese: ${match.country || "ND"}</p>
        <p>Slot: ${match.kickoffSlot || "ND"} ${match.kickoff ? `· ${match.kickoff}` : ""}</p>
        <p>Fonte match: ${match.source || "n/d"}</p>
        ${match.selectionReason ? `<p>Criterio AI: ${match.selectionReason}</p>` : ""}
        ${(match.marketCandidates || []).length
          ? `<p>Alternative: ${(match.marketCandidates || [])
              .slice(0, 3)
              .map((candidate) => `${candidate.market} (${Math.round(candidate.confidence * 100)}%)`)
              .join(" · ")}</p>`
          : ""}
        ${match.pastStats
          ? `<p>Storico 5 gare · Home PPG: ${match.pastStats.homePpg} · Away PPG: ${match.pastStats.awayPpg}</p>`
          : ""}
        <p>Quota: ${match.odd ?? "n/d"} · Confidenza: ${Math.round((match.confidence || 0) * 100)}%</p>
      </article>
    `
    )
    .join("");
}

function renderSystem(payload) {
  const { system, llm } = payload;
  const { summary, tickets, notes, strategy } = system;
  const training = payload.aiTraining;

  summaryEl.innerHTML = `
    <div class="success-meter">
      <span>Probabilità riuscita sistema</span>
      <strong>${percent(summary.successProbability || 0)}</strong>
      <div class="meter-track">
        <div class="meter-fill" style="width:${Math.round((summary.successProbability || 0) * 100)}%"></div>
      </div>
    </div>
    <div class="summary-chip-row">
      <span class="chip">Formato: ${summary.systemFormat || "ND"}</span>
      <span class="chip">Giornata: ${summary.alignment?.matchday || "ND"}</span>
      <span class="chip">Slot: ${summary.alignment?.slot || "ND"}</span>
      <span class="chip">Edge atteso: ${euro(summary.expectedEdge || 0)}</span>
    </div>
    <div class="summary-grid">
      <div><span>Ticket copertura</span><strong>${summary.coverageTickets}</strong></div>
      <div><span>Combinazioni base</span><strong>${summary.combinationCount}</strong></div>
      <div><span>Payout stimato min</span><strong>${euro(summary.payoutMin)}</strong></div>
      <div><span>Payout stimato max</span><strong>${euro(summary.payoutMax)}</strong></div>
      <div><span>Ritorno atteso</span><strong>${euro(summary.expectedPortfolio)}</strong></div>
    </div>
    ${summary.budgetPlan
      ? `<div class="summary-grid">
          <div><span>Budget totale</span><strong>${euro(summary.budgetPlan.bankroll || 0)}</strong></div>
          <div><span>Budget investito</span><strong>${euro(summary.budgetPlan.investableBudget || 0)}</strong></div>
          <div><span>Riserva sicurezza</span><strong>${euro(summary.budgetPlan.reserveStake || 0)}</strong></div>
          <div><span>Puntata minima ticket</span><strong>${euro(summary.budgetPlan.minStakeUnit || 1)}</strong></div>
          <div><span>Ticket attivi</span><strong>${summary.budgetPlan.activeTickets || 0}</strong></div>
          <div><span>Precision mode</span><strong>${summary.budgetPlan.precisionMode ? "ON" : "OFF"}</strong></div>
        </div>`
      : ""}
  `;

  ticketsEl.innerHTML = tickets
    .map(
      (ticket) => `
      <article class="item">
        <h3>${ticket.type}</h3>
        <p>Stake: <strong>${euro(ticket.stake)}</strong> · Quota totale: <strong>${ticket.odd}</strong> · Formato: <strong>${ticket.format || "ND"}</strong></p>
        <p>Probabilità ticket: <strong>${percent(ticket.probability)}</strong> · EV ratio: <strong>${ticket.evRatio ?? "n/d"}</strong> · Lordo se vince: <strong>${euro(ticket.grossIfWin)}</strong></p>
        ${ticket.format === "liquidita" ? `<p><strong>Significato:</strong> è budget tenuto fermo per sicurezza, non è una giocata.</p>` : ""}
        <ul>
          ${ticket.events
            .map(
              (event) =>
                `<li>${event.match}: <strong>${event.pick}</strong> (${event.role})</li>`
            )
            .join("")}
        </ul>
      </article>
    `
    )
    .join("");

  strategyEl.innerHTML = strategy
    ? `
      <article class="card strategy-box">
        <h3>Strategia AI (${strategy.profile})</h3>
        <ul>
          ${(strategy.criteria || []).map((item) => `<li>${item}</li>`).join("")}
        </ul>
        <p>Probabilità sistema: <strong>${percent(strategy.successProbability || 0)}</strong> · Edge stimato: <strong>${euro(strategy.expectedEdge || 0)}</strong></p>
        <p>Training AI: <strong>${training?.updatedAt ? "attivo" : "base"}</strong>${training?.updatedAt ? ` · Aggiornato: ${training.updatedAt.slice(0, 16).replace("T", " ")}` : ""}</p>
        <h4>Scelte partita</h4>
        <ul>
          ${(strategy.matchDecisions || [])
            .map(
              (decision) =>
                `<li><strong>${decision.match}</strong>: ${decision.selectedMarket}${decision.secondaryMarket ? ` + ${decision.secondaryMarket}` : ""} (${Math.round((decision.safetyScore || 0) * 100)}%) — ${decision.reason}</li>`
            )
            .join("")}
        </ul>
      </article>
    `
    : "";

  const llmNotes = llm?.adjustments?.length
    ? `<h3>Rifinitura AI</h3><ul>${llm.adjustments
        .map((item) => `<li>${item}</li>`)
        .join("")}</ul><p>${llm.riskComment || ""}</p>`
    : "";

  notesEl.innerHTML = `
    <h3>Note</h3>
    <ul>
      ${notes.map((note) => `<li>${note}</li>`).join("")}
    </ul>
    ${llmNotes}
  `;
}

function renderPredictionHistory(payload) {
  const summary = payload?.summary || {};
  const items = payload?.items || [];
  const learning = payload?.learning || {};
  const learningReport = payload?.learningReport || {};

  if (learningPanelEl) {
    const overallAcc =
      typeof learningReport?.overall?.accuracy === "number"
        ? `${Math.round(learningReport.overall.accuracy * 100)}%`
        : "n/d";
    const trackedMarkets = Number(learning?.trackedMarkets || 0);
    const topMarkets = Array.isArray(learningReport?.topMarkets)
      ? learningReport.topMarkets.slice(0, 4)
      : [];
    const weakMarkets = Array.isArray(learningReport?.weakMarkets)
      ? learningReport.weakMarkets.slice(0, 4)
      : [];

    learningPanelEl.innerHTML = `
      <article class="card strategy-box">
        <h3>Memoria AI e accuratezza</h3>
        <div class="summary-grid">
          <div><span>Campione valutato</span><strong>${learningReport?.sampleSize || 0}</strong></div>
          <div><span>Accuratezza reale</span><strong>${overallAcc}</strong></div>
          <div><span>Mercati tracciati</span><strong>${trackedMarkets}</strong></div>
          <div><span>Aggiornata</span><strong>${learning?.updatedAt ? String(learning.updatedAt).slice(0, 16).replace("T", " ") : "n/d"}</strong></div>
        </div>
        ${topMarkets.length
          ? `<p><strong>Mercati migliori:</strong> ${topMarkets
              .map((row) => `${row.market} (${Math.round((row.accuracy || 0) * 100)}% su ${row.total})`)
              .join(" · ")}</p>`
          : "<p>Mercati migliori: n/d</p>"}
        ${weakMarkets.length
          ? `<p><strong>Mercati da migliorare:</strong> ${weakMarkets
              .map((row) => `${row.market} (${Math.round((row.accuracy || 0) * 100)}% su ${row.total})`)
              .join(" · ")}</p>`
          : "<p>Mercati da migliorare: n/d</p>"}
      </article>
    `;
  }

  if (historySummaryEl) {
    const accuracyText =
      typeof summary.accuracy === "number"
        ? `${Math.round(summary.accuracy * 100)}%`
        : "n/d";
    historySummaryEl.innerHTML = `
      <div class="summary-grid">
        <div><span>Eventi tracciati</span><strong>${summary.total || 0}</strong></div>
        <div><span>Valutati</span><strong>${summary.decided || 0}</strong></div>
        <div><span>Corretti</span><strong>${summary.wins || 0}</strong></div>
        <div><span>Errati</span><strong>${summary.losses || 0}</strong></div>
        <div><span>In attesa</span><strong>${summary.pending || 0}</strong></div>
        <div><span>Accuratezza</span><strong>${accuracyText}</strong></div>
      </div>
    `;
  }

  if (!historyListEl) {
    return;
  }

  if (!items.length) {
    historyListEl.innerHTML = "<p>Nessuna predizione storica disponibile.</p>";
    return;
  }

  historyListEl.innerHTML = items
    .map((item) => {
      const statusLabel =
        item.status === "win"
          ? "✅ Corretta"
          : item.status === "loss"
            ? "❌ Errata"
            : "⏳ In attesa";

      return `
      <article class="item">
        <h3>${item.match}</h3>
        <p>Data match: ${item.matchDate || "ND"} · Esito: <strong>${statusLabel}</strong> ${item.finalScore ? `· Score: ${item.finalScore}` : ""}</p>
        <p>Mercato principale: <strong>${item.mainMarket}</strong> (quota ${item.mainOdd ?? "n/d"})</p>
        <p>Mercato secondario: <strong>${item.secondaryMarket || "n/d"}</strong> ${item.secondaryOdd ? `(quota ${item.secondaryOdd})` : ""}</p>
        <p>Confidenza: ${Math.round((item.confidence || 0) * 100)}% · Sicurezza: ${Math.round((item.safetyScore || 0) * 100)}%</p>
      </article>
    `;
    })
    .join("");
}

async function loadPredictionHistory(forceRefresh = false) {
  if (!hasApiEndpoint) {
    if (historyListEl) {
      historyListEl.innerHTML = `<p class="error">Storico non disponibile in modalità statica. ${apiSetupHint()}</p>`;
    }
    return;
  }

  if (refreshHistoryBtn) {
    refreshHistoryBtn.disabled = true;
    refreshHistoryBtn.textContent = forceRefresh ? "Aggiornamento..." : "Caricamento...";
  }

  if (historyListEl) {
    historyListEl.innerHTML = forceRefresh
      ? "<p>Verifica storico in corso...</p>"
      : "<p>Caricamento storico...</p>";
  }

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), forceRefresh ? 45000 : 12000);
    const response = await fetch(
      apiUrl(`/api/predictions/history?limit=80&refresh=${forceRefresh ? "1" : "0"}`),
      { signal: controller.signal }
    );
    clearTimeout(timeoutId);
    if (!response.ok) {
      throw new Error("Errore nel recupero dello storico predizioni.");
    }
    const payload = await response.json();
    renderPredictionHistory(payload);
  } catch (error) {
    if (historyListEl) {
      const message =
        error?.name === "AbortError"
          ? "Storico troppo lento da recuperare adesso, riprova tra poco."
          : error.message;
      historyListEl.innerHTML = `<p class="error">${message} ${apiSetupHint()}</p>`;
    }
  } finally {
    if (refreshHistoryBtn) {
      refreshHistoryBtn.disabled = false;
      refreshHistoryBtn.textContent = "Aggiorna storico";
    }
  }
}

async function loadMatches() {
  const dateWindow = normalizeDateWindowFromInputs();

  if (!hasApiEndpoint) {
    if (trainingMetaEl) {
      trainingMetaEl.textContent = "Modalità GitHub Pages: interfaccia attiva, API non configurata.";
    }
    matchesMetaEl.textContent = `Range selezionato: ${dateWindow.startDate} → ${dateWindow.endDate} (${dateWindow.timeRangeDays} giorni)`;
    matchesEl.innerHTML = `<p class="error">Partite non caricabili finché non imposti API_BASE_URL. ${apiSetupHint()}</p>`;
    return;
  }

  matchesEl.innerHTML = "<p>Caricamento...</p>";
  try {
    const params = new URLSearchParams({
      risk: String(currentRiskDecimal()),
      maxMatches: String(parseLocalizedNumber(maxMatchesEl.value, 10)),
      timeRangeDays: String(dateWindow.timeRangeDays),
      focusCountry: currentFocusCountry(),
      startDate: dateWindow.startDate,
      endDate: dateWindow.endDate,
      teamQuery: (teamQueryEl?.value || "").trim()
    });
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000);
    const response = await fetch(apiUrl(`/api/matches?${params.toString()}`), {
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      throw new Error("Errore nel recupero delle partite.");
    }
    const payload = await response.json();
    const training = payload.aiTraining;
    if (trainingMetaEl) {
      trainingMetaEl.textContent = training?.updatedAt
        ? `AI calibrata su risultati recenti (${training.sampleSize || 0} partite campione).`
        : "AI in modalità base (calibrazione non ancora disponibile).";
    }
    renderMatches(payload.matches || [], {
      sourceType: payload.sourceType,
      timeRangeDays: payload.timeRangeDays,
      startDate: payload.startDate,
      endDate: payload.endDate,
      teamQuery: payload.teamQuery
    });
  } catch (error) {
    if (trainingMetaEl) {
      trainingMetaEl.textContent = "";
    }
    matchesMetaEl.textContent = "";
    const message =
      error?.name === "AbortError"
        ? "Recupero partite troppo lento, riprova tra pochi secondi."
        : error.message;
    matchesEl.innerHTML = `<p class="error">${message} ${apiSetupHint()}</p>`;
  }
}

async function generateSystem() {
  if (!hasApiEndpoint) {
    summaryEl.innerHTML = `<p class="error">Generazione sistema non disponibile senza backend API. ${apiSetupHint()}</p>`;
    strategyEl.innerHTML = "";
    ticketsEl.innerHTML = "";
    notesEl.innerHTML = "";
    return;
  }

  summaryEl.innerHTML = "<p>Calcolo sistema...</p>";
  strategyEl.innerHTML = "";
  ticketsEl.innerHTML = "";
  notesEl.innerHTML = "";

  try {
    const dateWindow = normalizeDateWindowFromInputs();
    const response = await fetch(apiUrl("/api/predict"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        bankroll: parseLocalizedNumber(bankrollEl.value, 100),
        maxMatches: parseLocalizedNumber(maxMatchesEl.value, 5),
        risk: currentRiskDecimal(),
        timeRangeDays: dateWindow.timeRangeDays,
        focusCountry: currentFocusCountry(),
        startDate: dateWindow.startDate,
        endDate: dateWindow.endDate,
        teamQuery: (teamQueryEl?.value || "").trim()
      })
    });

    if (!response.ok) {
      throw new Error("Errore nella generazione del sistema.");
    }

    const payload = await response.json();
    renderSystem(payload);
    loadPredictionHistory();
  } catch (error) {
    summaryEl.innerHTML = `<p class="error">${error.message} ${apiSetupHint()}</p>`;
  }
}

loadMatchesBtn.addEventListener("click", loadMatches);
generateBtn.addEventListener("click", generateSystem);
searchTeamBtn.addEventListener("click", loadMatches);
refreshHistoryBtn?.addEventListener("click", () => loadPredictionHistory(true));

riskEl.addEventListener("input", updateRiskLabel);
riskEl.addEventListener("change", loadMatches);
maxMatchesEl.addEventListener("change", loadMatches);
focusCountryEl?.addEventListener("change", loadMatches);

for (const btn of datePresetButtons) {
  btn.addEventListener("click", () => {
    const days = parseLocalizedNumber(btn.dataset.rangeDays, 7);
    const start = startDateEl?.value || formatIsoDateLocal(new Date());
    if (endDateEl) {
      endDateEl.value = addDaysIso(start, Math.max(1, Math.min(14, days)) - 1);
    }
    updatePresetActiveState(Math.max(1, Math.min(14, days)));
    loadMatches();
  });
}

endDateEl?.addEventListener("change", () => {
  const windowRange = normalizeDateWindowFromInputs();
  updatePresetActiveState(windowRange.timeRangeDays);
  loadMatches();
});

startDateEl.addEventListener("change", () => {
  const windowRange = normalizeDateWindowFromInputs();
  updatePresetActiveState(windowRange.timeRangeDays);
  loadMatches();
});

updateRiskLabel();
ensureDateWindowDefaults();
updatePresetActiveState(normalizeDateWindowFromInputs().timeRangeDays);

if (apiBaseUrlEl) {
  apiBaseUrlEl.value = API_BASE_URL;
}

saveApiBaseBtn?.addEventListener("click", () => {
  const nextValue = normalizeApiBaseInput(apiBaseUrlEl?.value || "");
  if (nextValue) {
    window.localStorage.setItem("predict.apiBaseUrl", nextValue);
  } else {
    window.localStorage.removeItem("predict.apiBaseUrl");
  }
  window.location.reload();
});

loadMatches();
loadPredictionHistory(false);
