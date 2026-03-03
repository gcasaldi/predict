const matchesEl = document.querySelector("#matches");
const summaryEl = document.querySelector("#summary");
const ticketsEl = document.querySelector("#tickets");
const notesEl = document.querySelector("#notes");

const bankrollEl = document.querySelector("#bankroll");
const maxMatchesEl = document.querySelector("#maxMatches");
const riskEl = document.querySelector("#risk");

const loadMatchesBtn = document.querySelector("#loadMatchesBtn");
const generateBtn = document.querySelector("#generateBtn");

function euro(value) {
  return new Intl.NumberFormat("it-IT", {
    style: "currency",
    currency: "EUR"
  }).format(value);
}

function renderMatches(matches) {
  if (!matches.length) {
    matchesEl.innerHTML = "<p>Nessuna partita trovata.</p>";
    return;
  }

  matchesEl.innerHTML = matches
    .slice(0, 20)
    .map(
      (match) => `
      <article class="item">
        <h3>${match.match}</h3>
        <p>Pick: <strong>${match.pick}</strong> · Backup: <strong>${match.backupPick}</strong></p>
        <p>Quota: ${match.odd ?? "n/d"} · Confidenza: ${Math.round((match.confidence || 0) * 100)}%</p>
      </article>
    `
    )
    .join("");
}

function renderSystem(payload) {
  const { system, llm } = payload;
  const { summary, tickets, notes } = system;

  summaryEl.innerHTML = `
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
        <p>Stake: <strong>${euro(ticket.stake)}</strong> · Quota totale: <strong>${ticket.odd}</strong></p>
        <p>Probabilità stimata: <strong>${Math.round(ticket.probability * 100)}%</strong> · Lordo se vince: <strong>${euro(ticket.grossIfWin)}</strong></p>
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
    const response = await fetch("/api/matches");
    if (!response.ok) {
      throw new Error("Errore nel recupero delle partite.");
    }
    const payload = await response.json();
    renderMatches(payload.matches || []);
  } catch (error) {
    matchesEl.innerHTML = `<p class="error">${error.message}</p>`;
  }
}

async function generateSystem() {
  summaryEl.innerHTML = "<p>Calcolo sistema...</p>";
  ticketsEl.innerHTML = "";
  notesEl.innerHTML = "";

  try {
    const response = await fetch("/api/predict", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        bankroll: Number(bankrollEl.value),
        maxMatches: Number(maxMatchesEl.value),
        risk: Number(riskEl.value)
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

loadMatches();
