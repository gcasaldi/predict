const matchesEl = document.querySelector("#matches");
const matchesMetaEl = document.querySelector("#matchesMeta");
const summaryEl = document.querySelector("#summary");
const strategyEl = document.querySelector("#strategy");
const ticketsEl = document.querySelector("#tickets");
const notesEl = document.querySelector("#notes");

const bankrollEl = document.querySelector("#bankroll");
const maxMatchesEl = document.querySelector("#maxMatches");
const riskEl = document.querySelector("#risk");
const riskLabelEl = document.querySelector("#riskLabel");

const loadMatchesBtn = document.querySelector("#loadMatchesBtn");
const generateBtn = document.querySelector("#generateBtn");
const trainingMetaEl = document.querySelector("#trainingMeta");

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

function renderMatches(matches, meta = {}) {
  if (!matches.length) {
    matchesMetaEl.textContent = "";
    matchesEl.innerHTML = "<p>Nessuna partita trovata.</p>";
    return;
  }

  matchesMetaEl.textContent = `Fonte: ${meta.sourceType || "n/d"} · Range: ${meta.timeRangeDays || 7} giorni`;

  matchesEl.innerHTML = matches
    .slice(0, 20)
    .map(
      (match) => `
      <article class="item">
        <h3>${match.match}</h3>
        <p>Pick: <strong>${match.pick}</strong> · Backup: <strong>${match.backupPick}</strong></p>
        ${match.safetyScore ? `<p>Indice sicurezza: <strong>${Math.round(match.safetyScore * 100)}%</strong></p>` : ""}
        <p>Data: ${match.matchDate || "ND"} · Giornata: ${match.matchday || "ND"}</p>
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
                `<li><strong>${decision.match}</strong>: ${decision.selectedMarket} (${Math.round((decision.safetyScore || 0) * 100)}%) — ${decision.reason}</li>`
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

async function loadMatches() {
  matchesEl.innerHTML = "<p>Caricamento...</p>";
  try {
    const params = new URLSearchParams({
      risk: String(currentRiskDecimal()),
      maxMatches: String(parseLocalizedNumber(maxMatchesEl.value, 10)),
      timeRangeDays: "7"
    });
    const response = await fetch(`/api/matches?${params.toString()}`);
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
      timeRangeDays: payload.timeRangeDays
    });
  } catch (error) {
    if (trainingMetaEl) {
      trainingMetaEl.textContent = "";
    }
    matchesMetaEl.textContent = "";
    matchesEl.innerHTML = `<p class="error">${error.message}</p>`;
  }
}

async function generateSystem() {
  summaryEl.innerHTML = "<p>Calcolo sistema...</p>";
  strategyEl.innerHTML = "";
  ticketsEl.innerHTML = "";
  notesEl.innerHTML = "";

  try {
    const response = await fetch("/api/predict", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        bankroll: parseLocalizedNumber(bankrollEl.value, 100),
        maxMatches: parseLocalizedNumber(maxMatchesEl.value, 5),
        risk: currentRiskDecimal(),
        timeRangeDays: 7
      })
    });

    if (!response.ok) {
      throw new Error("Errore nella generazione del sistema.");
    }

    const payload = await response.json();
    renderSystem(payload);
  } catch (error) {
    summaryEl.innerHTML = `<p class="error">${error.message}</p>`;
  }
}

loadMatchesBtn.addEventListener("click", loadMatches);
generateBtn.addEventListener("click", generateSystem);
riskEl.addEventListener("input", updateRiskLabel);
riskEl.addEventListener("change", loadMatches);
maxMatchesEl.addEventListener("change", loadMatches);

updateRiskLabel();
loadMatches();
