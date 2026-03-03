import "dotenv/config";
import express from "express";
import cors from "cors";
import * as cheerio from "cheerio";

const app = express();
const port = Number(process.env.PORT || 3000);
const sourceUrl =
  process.env.SOURCE_URL ||
  "https://www.mondopengwin.it/pronostici/calcio/serie-a/";
const sofaBaseUrl =
  process.env.SOFASCORE_BASE_URL ||
  "https://www.sofascore.com/api/v1";
const unifiedCache = new Map();
const aiTrainingState = {
  updatedAt: null,
  sampleSize: 0,
  marketHitRates: {}
};

const MARKET_UNIVERSE = {
  esito: [
    "1",
    "X",
    "2",
    "1X",
    "X2",
    "12",
    "Draw No Bet",
    "Qualificata"
  ],
  goalTotali: [
    "GG",
    "NG",
    "Over 0.5",
    "Over 1.5",
    "Over 2.5",
    "Over 3.5",
    "Under 0.5",
    "Under 1.5",
    "Under 2.5",
    "Under 3.5",
    "Over/Under squadra casa",
    "Over/Under squadra ospite",
    "Over/Under 1° tempo",
    "Over/Under 2° tempo"
  ],
  handicap: [
    "Handicap europeo",
    "Handicap asiatico",
    "Handicap 1° tempo",
    "Handicap 2° tempo"
  ],
  risultati: [
    "Risultato esatto",
    "Risultato esatto 1° tempo",
    "Parziale/Finale",
    "Doppia chance + risultato",
    "Multigol",
    "Multigol casa",
    "Multigol ospite"
  ],
  combo: [
    "1 + Over 1.5",
    "1X + Under 3.5",
    "GG + Over 2.5",
    "2 + Over 2.5",
    "Combo personalizzate"
  ],
  marcatori: [
    "Segna sì/no",
    "Primo marcatore",
    "Ultimo marcatore",
    "Doppietta",
    "Tripletta",
    "Segna su rigore"
  ],
  statsMatch: [
    "Calci d’angolo Over/Under",
    "Calci d’angolo squadra",
    "Cartellini Over/Under",
    "Cartellini squadra",
    "Tiri totali",
    "Tiri in porta",
    "Possesso palla",
    "Falli",
    "Rigore sì/no",
    "Espulsione sì/no"
  ],
  tempi: [
    "Esito 1° tempo",
    "Esito 2° tempo",
    "Goal 1° tempo",
    "Goal 2° tempo",
    "Squadra segna in entrambi i tempi",
    "Vince almeno un tempo"
  ],
  live: [
    "Modalità live su tutti i mercati",
    "Prossimo goal",
    "Squadra segna prossimo goal",
    "Goal nei prossimi X minuti"
  ]
};

function getMarketUniverseSummary() {
  return {
    ...MARKET_UNIVERSE,
    supportedNow: [
      "1",
      "2",
      "1X",
      "X2",
      "GG",
      "NG",
      "Over 1.5",
      "Over 2.5",
      "Under 2.5",
      "Under 3.5",
      "Multigol 1-3",
      "Multigol 2-4",
      "Over/Under squadra casa",
      "Over/Under squadra ospite",
      "Over/Under 1° tempo",
      "Over/Under 2° tempo",
      "1X + Under 3.5",
      "GG + Over 2.5"
    ]
  };
}

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

function normalizeWhitespace(value) {
  return value.replace(/\s+/g, " ").trim();
}

function normalizeSearchText(value) {
  return normalizeWhitespace(String(value || "")).toLowerCase();
}

function extractOdds(text) {
  const match = text.match(/\b([1-9]\d?|0)\.([0-9]{2})\b/g);
  if (!match?.length) {
    return null;
  }

  const values = match
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item) && item >= 1.01 && item <= 20);

  if (!values.length) {
    return null;
  }

  return Math.min(...values);
}

function extractPick(text) {
  const normalized = ` ${text.toUpperCase()} `;
  const patterns = [
    { regex: /\b1X\b/, label: "1X" },
    { regex: /\bX2\b/, label: "X2" },
    { regex: /\b12\b/, label: "12" },
    { regex: /\bOVER\s*2[.,]?5\b/, label: "OVER 2.5" },
    { regex: /\bUNDER\s*2[.,]?5\b/, label: "UNDER 2.5" },
    { regex: /\bGG\b|\bGOAL\b/, label: "GG" },
    { regex: /\bNG\b|\bNO\s+GOAL\b/, label: "NG" },
    { regex: /\b1\b/, label: "1" },
    { regex: /\bX\b/, label: "X" },
    { regex: /\b2\b/, label: "2" }
  ];

  for (const pattern of patterns) {
    if (pattern.regex.test(normalized)) {
      return pattern.label;
    }
  }

  return null;
}

function extractMatch(text) {
  const candidates = [
    /(\b[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ý'\-\s]{1,30})\s*(?:-|–|—|VS|V\.)\s*(\b[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ý'\-\s]{1,30})/i,
    /(\b[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ý'\-\s]{1,30})\s+(?:CONTRO)\s+(\b[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ý'\-\s]{1,30})/i
  ];

  for (const regex of candidates) {
    const match = text.match(regex);
    if (match) {
      const home = normalizeWhitespace(match[1]);
      const away = normalizeWhitespace(match[2]);
      if (home.length > 1 && away.length > 1) {
        return `${home} vs ${away}`;
      }
    }
  }

  return null;
}

function extractMatchday(text) {
  const match = text.match(/\bgiornata\s*(\d{1,2})\b/i);
  if (!match) {
    return null;
  }
  return `Giornata ${match[1]}`;
}

function extractKickoff(text) {
  const match = text.match(/\b(\d{1,2})[:.](\d{2})\b/);
  if (!match) {
    return null;
  }

  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
    return null;
  }

  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    return null;
  }

  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function slotFromKickoff(kickoff) {
  if (!kickoff) {
    return null;
  }
  const hour = Number(kickoff.split(":")[0]);
  if (hour < 12) {
    return "Mattina";
  }
  if (hour < 18) {
    return "Pomeriggio";
  }
  if (hour < 22) {
    return "Sera";
  }
  return "Notte";
}

function toStartOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function parseDateFromText(text, referenceDate = new Date()) {
  const match = text.match(/\b(\d{1,2})[\/\-.](\d{1,2})(?:[\/\-.](\d{2,4}))?\b/);
  if (!match) {
    return null;
  }

  const day = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isFinite(day) || !Number.isFinite(month) || day < 1 || day > 31 || month < 1 || month > 12) {
    return null;
  }

  let year;
  if (match[3]) {
    const rawYear = Number(match[3]);
    if (!Number.isFinite(rawYear)) {
      return null;
    }
    year = rawYear < 100 ? 2000 + rawYear : rawYear;
  } else {
    year = referenceDate.getFullYear();
  }

  const parsed = new Date(year, month - 1, day);
  if (
    parsed.getFullYear() !== year ||
    parsed.getMonth() !== month - 1 ||
    parsed.getDate() !== day
  ) {
    return null;
  }

  return parsed;
}

function toIsoDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseIsoDateInput(value, fallback = new Date()) {
  if (!value) {
    return toStartOfDay(fallback);
  }
  const raw = String(value).trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return toStartOfDay(fallback);
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const parsed = new Date(year, month - 1, day);
  if (
    parsed.getFullYear() !== year ||
    parsed.getMonth() !== month - 1 ||
    parsed.getDate() !== day
  ) {
    return toStartOfDay(fallback);
  }
  return toStartOfDay(parsed);
}

function withDateWindow(matches, days = 7, referenceDate = new Date(), strict = false) {
  const today = toStartOfDay(referenceDate);
  const end = new Date(today);
  end.setDate(end.getDate() + Math.max(1, days) - 1);

  const inWindow = matches.filter((item) => {
    if (!item.matchDate) {
      return false;
    }
    const eventDate = toStartOfDay(new Date(item.matchDate));
    return eventDate >= today && eventDate <= end;
  });

  if (inWindow.length) {
    return inWindow;
  }

  if (strict) {
    return [];
  }

  return matches;
}

function toKickoffFromDate(date) {
  return `${String(date.getHours()).padStart(2, "0")}:${String(
    date.getMinutes()
  ).padStart(2, "0")}`;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function parseInputNumber(value, fallback) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const normalized = value.replace(",", ".").trim();
    const parsed = Number(normalized);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function confidenceFromOdd(odd) {
  if (!odd) {
    return 0.54;
  }
  const implied = 1 / odd;
  return clamp(implied, 0.35, 0.82);
}

function backupPickFor(pick) {
  const map = {
    "1": "1X",
    "2": "X2",
    "X": "1X",
    "GG": "OVER 1.5",
    "NG": "UNDER 3.5",
    "OVER 2.5": "GG",
    "UNDER 2.5": "1X",
    "1X": "1",
    "X2": "2",
    "12": "OVER 1.5"
  };
  return map[pick] || "1X";
}

function oddForPick(baseOdd, isBackup) {
  if (!baseOdd) {
    return isBackup ? 1.38 : 1.68;
  }
  if (isBackup) {
    return clamp(baseOdd * 0.9, 1.2, 10);
  }
  return baseOdd;
}

function combinations(items, k, start = 0, current = [], out = []) {
  if (current.length === k) {
    out.push([...current]);
    return out;
  }

  for (let index = start; index < items.length; index += 1) {
    current.push(items[index]);
    combinations(items, k, index + 1, current, out);
    current.pop();
  }
  return out;
}

function scoreTicket(events) {
  return events.reduce(
    (acc, event) => {
      acc.probability *= event.confidence;
      acc.odd *= event.odd;
      return acc;
    },
    { probability: 1, odd: 1 }
  );
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      Accept: "application/json"
    }
  });

  if (!response.ok) {
    return null;
  }

  try {
    return await response.json();
  } catch {
    return null;
  }
}

function marketHitsForEvent(event) {
  const homeGoals = Number(event?.homeScore?.current ?? 0);
  const awayGoals = Number(event?.awayScore?.current ?? 0);
  const totalGoals = homeGoals + awayGoals;
  const firstHalfGoals =
    Number(event?.homeScore?.period1 ?? 0) + Number(event?.awayScore?.period1 ?? 0);
  const secondHalfGoals = Math.max(0, totalGoals - firstHalfGoals);

  return {
    "OVER 0.5": totalGoals >= 1,
    "OVER 1.5": totalGoals >= 2,
    "OVER 2.5": totalGoals >= 3,
    "UNDER 2.5": totalGoals <= 2,
    "UNDER 3.5": totalGoals <= 3,
    GG: homeGoals >= 1 && awayGoals >= 1,
    NG: homeGoals === 0 || awayGoals === 0,
    "OVER 0.5 1T": firstHalfGoals >= 1,
    "OVER 0.5 2T": secondHalfGoals >= 1,
    "HOME OVER 0.5": homeGoals >= 1,
    "AWAY OVER 0.5": awayGoals >= 1
  };
}

async function recalibrateAiFromRecentResults(days = 30) {
  const today = toStartOfDay(new Date());
  const stats = new Map();

  for (let offset = 1; offset <= days; offset += 1) {
    const target = new Date(today);
    target.setDate(today.getDate() - offset);
    const url = `${sofaBaseUrl}/sport/football/scheduled-events/${toIsoDate(target)}`;
    const payload = await fetchJson(url);
    const events = payload?.events || [];

    for (const event of events) {
      const tournamentName = event?.tournament?.uniqueTournament?.name || "";
      const countryName = event?.tournament?.uniqueTournament?.category?.name || "";
      const statusType = event?.status?.type;
      const isFinished = statusType === "finished" || statusType === "after_penalties";
      const isSerieA = /serie a/i.test(tournamentName) && /(italy|italia)/i.test(countryName);
      if (!isSerieA || !isFinished) {
        continue;
      }

      const hits = marketHitsForEvent(event);
      for (const [market, hit] of Object.entries(hits)) {
        if (!stats.has(market)) {
          stats.set(market, { hit: 0, total: 0 });
        }
        const row = stats.get(market);
        row.total += 1;
        row.hit += hit ? 1 : 0;
      }
    }
  }

  const marketHitRates = {};
  let sampleSize = 0;
  for (const [market, row] of stats.entries()) {
    if (!row.total) {
      continue;
    }
    sampleSize = Math.max(sampleSize, row.total);
    marketHitRates[market] = Number((row.hit / row.total).toFixed(3));
  }

  aiTrainingState.updatedAt = new Date().toISOString();
  aiTrainingState.sampleSize = sampleSize;
  aiTrainingState.marketHitRates = marketHitRates;
  return aiTrainingState;
}

function normalizeTeamForm(form) {
  if (!form) {
    return {
      pointsPerGame: 1,
      goalsForPerGame: 1,
      goalsAgainstPerGame: 1,
      sampleSize: 0
    };
  }
  return form;
}

function estimateOddFromConfidence(confidence, market) {
  const marginByMarket = {
    "1X": 1.04,
    X2: 1.04,
    "OVER 0.5": 0.9,
    "OVER 1.5": 1.05,
    "UNDER 3.5": 1.05,
    "HOME OVER 0.5": 1.05,
    "AWAY OVER 0.5": 1.05,
    "OVER 0.5 1T": 1.08,
    "OVER 0.5 2T": 1.07,
    "MULTIGOAL 1-3": 1.07,
    "MULTIGOAL 2-4": 1.07,
    "1X + UNDER 3.5": 1.08,
    "GG + OVER 2.5": 1.1,
    GG: 1.08,
    NG: 1.08,
    "UNDER 2.5": 1.09,
    "OVER 2.5": 1.09,
    "1": 1.1,
    "2": 1.1
  };
  const margin = marginByMarket[market] || 1.08;
  return clamp((1 / confidence) * margin, 1.2, 4.2);
}

function buildFormStatsFromEvents(events, teamId) {
  const relevant = (events || []).filter((event) => {
    const statusType = event?.status?.type;
    const isFinished = statusType === "finished" || statusType === "after_penalties";
    const homeId = event?.homeTeam?.id;
    const awayId = event?.awayTeam?.id;
    return isFinished && (homeId === teamId || awayId === teamId);
  });

  if (!relevant.length) {
    return normalizeTeamForm(null);
  }

  const aggregate = relevant.reduce(
    (acc, event) => {
      const isHome = event.homeTeam.id === teamId;
      const goalsFor = isHome
        ? Number(event?.homeScore?.current ?? 0)
        : Number(event?.awayScore?.current ?? 0);
      const goalsAgainst = isHome
        ? Number(event?.awayScore?.current ?? 0)
        : Number(event?.homeScore?.current ?? 0);

      let points = 0;
      if (goalsFor > goalsAgainst) {
        points = 3;
      } else if (goalsFor === goalsAgainst) {
        points = 1;
      }

      acc.points += points;
      acc.goalsFor += goalsFor;
      acc.goalsAgainst += goalsAgainst;
      acc.count += 1;
      return acc;
    },
    { points: 0, goalsFor: 0, goalsAgainst: 0, count: 0 }
  );

  if (!aggregate.count) {
    return normalizeTeamForm(null);
  }

  return {
    pointsPerGame: aggregate.points / aggregate.count,
    goalsForPerGame: aggregate.goalsFor / aggregate.count,
    goalsAgainstPerGame: aggregate.goalsAgainst / aggregate.count,
    sampleSize: aggregate.count
  };
}

function buildMarketCandidates(homeForm, awayForm) {
  const home = normalizeTeamForm(homeForm);
  const away = normalizeTeamForm(awayForm);

  const expectedHomeGoals = clamp(
    (home.goalsForPerGame + away.goalsAgainstPerGame) / 2,
    0.2,
    3.2
  );
  const expectedAwayGoals = clamp(
    (away.goalsForPerGame + home.goalsAgainstPerGame) / 2,
    0.2,
    3.2
  );
  const expectedTotalGoals = expectedHomeGoals + expectedAwayGoals;
  const expectedFirstHalfGoals = expectedTotalGoals * 0.46;
  const expectedSecondHalfGoals = expectedTotalGoals * 0.54;
  const ppgDiff = home.pointsPerGame - away.pointsPerGame;

  const candidates = [
    {
      market: "1X",
      confidence: clamp(0.58 + ppgDiff * 0.11, 0.42, 0.87),
      rationale: `Forma relativa casa favorevole (${home.pointsPerGame.toFixed(2)} vs ${away.pointsPerGame.toFixed(2)} PPG).`
    },
    {
      market: "X2",
      confidence: clamp(0.58 - ppgDiff * 0.11, 0.42, 0.87),
      rationale: `Forma relativa ospite favorevole (${away.pointsPerGame.toFixed(2)} vs ${home.pointsPerGame.toFixed(2)} PPG).`
    },
    {
      market: "1",
      confidence: clamp(0.42 + ppgDiff * 0.18, 0.2, 0.8),
      rationale: "Esito secco casa in base a differenza PPG."
    },
    {
      market: "2",
      confidence: clamp(0.42 - ppgDiff * 0.18, 0.2, 0.8),
      rationale: "Esito secco ospite in base a differenza PPG."
    },
    {
      market: "OVER 1.5",
      confidence: clamp(0.55 + (expectedTotalGoals - 1.5) * 0.14, 0.35, 0.86),
      rationale: `Totale gol atteso ${expectedTotalGoals.toFixed(2)}.`
    },
    {
      market: "OVER 0.5",
      confidence: clamp(0.68 + (expectedTotalGoals - 1.3) * 0.08, 0.45, 0.92),
      rationale: "Soglia minima gol con affidabilità elevata."
    },
    {
      market: "UNDER 0.5",
      confidence: clamp(0.18 - (expectedTotalGoals - 0.5) * 0.2, 0.05, 0.25),
      rationale: "Mercato molto restrittivo, quasi sempre evitato in ottica prudente."
    },
    {
      market: "UNDER 1.5",
      confidence: clamp(0.34 - (expectedTotalGoals - 1.4) * 0.17, 0.08, 0.46),
      rationale: "Partita da pochi gol."
    },
    {
      market: "UNDER 3.5",
      confidence: clamp(0.66 - Math.max(0, expectedTotalGoals - 2.7) * 0.16, 0.34, 0.83),
      rationale: `Partita con varianza gol contenuta (totale atteso ${expectedTotalGoals.toFixed(2)}).`
    },
    {
      market: "OVER 2.5",
      confidence: clamp(0.41 + (expectedTotalGoals - 2.5) * 0.2, 0.2, 0.79),
      rationale: "Spinta offensiva aggregata sopra soglia 2.5."
    },
    {
      market: "UNDER 2.5",
      confidence: clamp(0.56 - (expectedTotalGoals - 2.2) * 0.18, 0.24, 0.82),
      rationale: "Partita attesa tattica e con ritmo basso."
    },
    {
      market: "GG",
      confidence: clamp(0.4 + Math.min(expectedHomeGoals, expectedAwayGoals) * 0.19, 0.24, 0.79),
      rationale: `Entrambe le squadre con attacco atteso >0.8 (${expectedHomeGoals.toFixed(2)} / ${expectedAwayGoals.toFixed(2)}).`
    },
    {
      market: "NG",
      confidence: clamp(0.54 - Math.min(expectedHomeGoals, expectedAwayGoals) * 0.16, 0.22, 0.8),
      rationale: "Possibile clean sheet da almeno una parte."
    },
    {
      market: "MULTIGOAL 1-3",
      confidence: clamp(0.62 - Math.abs(expectedTotalGoals - 2.4) * 0.13, 0.28, 0.82),
      rationale: "Range gol stabile e prudente."
    },
    {
      market: "MULTIGOAL 2-4",
      confidence: clamp(0.6 - Math.abs(expectedTotalGoals - 3) * 0.12, 0.25, 0.8),
      rationale: "Range gol centrale con copertura estesa."
    },
    {
      market: "MULTIGOAL CASA 1-2",
      confidence: clamp(0.58 - Math.abs(expectedHomeGoals - 1.4) * 0.16, 0.24, 0.78),
      rationale: "Range gol casa su media attesa."
    },
    {
      market: "MULTIGOAL OSPITE 1-2",
      confidence: clamp(0.58 - Math.abs(expectedAwayGoals - 1.2) * 0.16, 0.22, 0.78),
      rationale: "Range gol ospite su media attesa."
    },
    {
      market: "HOME OVER 0.5",
      confidence: clamp(0.6 + (expectedHomeGoals - 0.8) * 0.15, 0.3, 0.86),
      rationale: `Gol casa atteso ${expectedHomeGoals.toFixed(2)}.`
    },
    {
      market: "AWAY OVER 0.5",
      confidence: clamp(0.6 + (expectedAwayGoals - 0.8) * 0.15, 0.3, 0.86),
      rationale: `Gol ospite atteso ${expectedAwayGoals.toFixed(2)}.`
    },
    {
      market: "OVER 0.5 1T",
      confidence: clamp(0.56 + (expectedFirstHalfGoals - 0.7) * 0.14, 0.28, 0.84),
      rationale: "Probabilità gol nel primo tempo."
    },
    {
      market: "OVER 0.5 2T",
      confidence: clamp(0.58 + (expectedSecondHalfGoals - 0.8) * 0.14, 0.3, 0.86),
      rationale: "Probabilità gol nel secondo tempo."
    },
    {
      market: "1X + UNDER 3.5",
      confidence: clamp(
        (clamp(0.58 + ppgDiff * 0.11, 0.42, 0.87) + clamp(0.66 - Math.max(0, expectedTotalGoals - 2.7) * 0.16, 0.34, 0.83)) / 2,
        0.35,
        0.86
      ),
      rationale: "Combo prudente tra copertura esito e limite gol."
    },
    {
      market: "GG + OVER 2.5",
      confidence: clamp(
        (clamp(0.4 + Math.min(expectedHomeGoals, expectedAwayGoals) * 0.19, 0.24, 0.79) + clamp(0.41 + (expectedTotalGoals - 2.5) * 0.2, 0.2, 0.79)) / 2,
        0.2,
        0.74
      ),
      rationale: "Combo offensiva con varianza maggiore."
    }
  ]
    .map((item) => ({
      ...item,
      confidence: Number(item.confidence.toFixed(3)),
      odd: Number(estimateOddFromConfidence(item.confidence, item.market).toFixed(2))
    }))
    .filter((item) => item.confidence >= 0.22)
    .sort((a, b) => b.confidence - a.confidence);

  return candidates;
}

async function fetchTeamForm(teamId, cache) {
  if (!teamId) {
    return normalizeTeamForm(null);
  }

  if (cache.has(teamId)) {
    return cache.get(teamId);
  }

  const url = `${sofaBaseUrl}/team/${teamId}/events/last/5`;
  const payload = await fetchJson(url);
  const form = buildFormStatsFromEvents(payload?.events || [], teamId);
  cache.set(teamId, form);
  return form;
}

function chooseSafestMarket(candidates, risk) {
  const safetyBias = {
    "1X": 0.06,
    X2: 0.06,
    "OVER 0.5": -0.06,
    "OVER 1.5": 0.05,
    "UNDER 3.5": 0.05,
    "HOME OVER 0.5": 0.045,
    "AWAY OVER 0.5": 0.045,
    "OVER 0.5 1T": 0.055,
    "OVER 0.5 2T": 0.045,
    "MULTIGOAL 1-3": 0.04,
    "MULTIGOAL 2-4": 0.035,
    "1X + UNDER 3.5": 0.05,
    "GG + OVER 2.5": -0.02,
    "UNDER 2.5": 0.02,
    GG: 0.015,
    NG: 0.015,
    "OVER 2.5": 0.01,
    "1": 0,
    "2": 0
  };

  const items = candidates?.length
    ? candidates
    : [{ market: "1X", confidence: 0.55, odd: 1.7, rationale: "Fallback prudente." }];

  const prudentialFloor = risk <= 0.2 ? 0.56 : risk <= 0.4 ? 0.52 : 0.48;
  const excludedWhenPrudent = new Set([
    "UNDER 0.5",
    "UNDER 1.5",
    "OVER 0.5",
    "1",
    "2",
    "GG + OVER 2.5"
  ]);

  const minOdd = risk <= 0.2 ? 1.45 : risk <= 0.45 ? 1.35 : 1.25;

  const filtered = items.filter((item) => {
    if (risk <= 0.25 && excludedWhenPrudent.has(item.market)) {
      return false;
    }
    return item.confidence >= prudentialFloor && item.odd >= minOdd;
  });

  const effectiveItems = filtered.length ? filtered : items;

  return [...effectiveItems]
    .map((item) => {
      const conservativeWeight = 1 - clamp(risk, 0.05, 0.95);
      const valueWeight = clamp(risk, 0.05, 0.95) * 0.08;
      const oddValue = clamp((item.odd - 1.25) / 2.8, 0, 0.2);
      const hitRate = Number(aiTrainingState.marketHitRates[item.market] || 0.5);
      const calibrationBoost = clamp((hitRate - 0.5) * 0.12, -0.03, 0.04);
      const score =
        item.confidence +
        conservativeWeight * (safetyBias[item.market] || 0.02) +
        valueWeight * oddValue +
        calibrationBoost;
      return {
        ...item,
        hitRate,
        score: Number(score.toFixed(4))
      };
    })
    .sort((a, b) => b.score - a.score)[0];
}

function marketStabilityBias(market) {
  const map = {
    "1X": 0.12,
    X2: 0.12,
    "OVER 1.5": 0.1,
    "UNDER 3.5": 0.1,
    "HOME OVER 0.5": 0.09,
    "AWAY OVER 0.5": 0.09,
    "MULTIGOAL 1-3": 0.08,
    "MULTIGOAL 2-4": 0.07,
    "UNDER 2.5": 0.05,
    GG: 0.04,
    NG: 0.04,
    "OVER 2.5": 0.03,
    "1": 0,
    "2": 0
  };
  return map[market] || 0.04;
}

function valueBiasFromOdd(odd) {
  if (!odd || !Number.isFinite(odd)) {
    return 0;
  }
  return clamp((odd - 1.35) / 2.8, 0, 0.25);
}

function daysFromToday(dateValue, referenceDate = new Date()) {
  if (!dateValue) {
    return 999;
  }
  const target = toStartOfDay(new Date(dateValue));
  const today = toStartOfDay(referenceDate);
  return Math.round((target.getTime() - today.getTime()) / 86400000);
}

function matchSelectionScore(match, risk) {
  const conservativeWeight = 1 - clamp(risk, 0.05, 0.95);
  const valueWeight = clamp(risk, 0.05, 0.95);
  const stability = marketStabilityBias(match.mainPick || match.pick);
  const confidencePart = Number(match.confidence || 0.5);
  const valuePart = valueBiasFromOdd(Number(match.odd || 1.7));
  const dayDistance = Math.abs(daysFromToday(match.matchDate));
  const datePriority = clamp(0.08 - dayDistance * 0.012, 0, 0.08);
  return Number(
    (
      confidencePart +
      conservativeWeight * stability +
      valueWeight * valuePart +
      datePriority
    ).toFixed(4)
  );
}

function competitionPriorityBoost(match, focusCountry = "Italia") {
  const country = String(match.country || "").toLowerCase();
  const tournament = String(match.tournament || "").toLowerCase();
  const wanted = String(focusCountry || "").toLowerCase();
  const isItalyFocus = wanted === "italia" || wanted === "italy";
  const countryMatch = isItalyFocus
    ? country.includes("italy") || country.includes("italia")
    : wanted && country.includes(wanted);

  let boost = 0;
  if (countryMatch) {
    boost += 0.16;
  }
  if (/coppa italia/.test(tournament)) {
    boost += 0.18;
  }
  if (/serie a|serie b/.test(tournament)) {
    boost += 0.1;
  }
  return boost;
}

function applyRiskProfileToMatches(matches, risk) {
  return matches.map((match) => {
    const chosen = chooseSafestMarket(match.marketCandidates || [], risk);
    if (!chosen) {
      return {
        ...match,
        safetyScore: Number(match.confidence || 0.5),
        selectionReason: match.selectionReason || "Fallback su mercato disponibile."
      };
    }

    return {
      ...match,
      pick: chosen.market,
      backupPick: backupPickFor(chosen.market),
      odd: chosen.odd,
      confidence: chosen.confidence,
      safetyScore: chosen.score,
      selectionReason: chosen.rationale
    };
  });
}

function rankMatchesForRisk(matches, risk, maxMatches, focusCountry = "Italia") {
  return [...matches]
    .sort((a, b) => {
      const scoreA =
        matchSelectionScore(a, risk) + competitionPriorityBoost(a, focusCountry);
      const scoreB =
        matchSelectionScore(b, risk) + competitionPriorityBoost(b, focusCountry);
      return scoreB - scoreA;
    })
    .slice(0, maxMatches);
}

async function fetchSofascoreFootball(
  days = 7,
  referenceDate = new Date(),
  maxEvents = 60
) {
  const today = toStartOfDay(referenceDate);
  const formCache = new Map();
  const events = [];

  for (let offset = 0; offset < days; offset += 1) {
    const target = new Date(today);
    target.setDate(today.getDate() + offset);
    const dateToken = toIsoDate(target);
    const url = `${sofaBaseUrl}/sport/football/scheduled-events/${dateToken}`;
    const payload = await fetchJson(url);
    if (!payload?.events?.length) {
      continue;
    }

    events.push(...payload.events);
  }

  const unique = new Map();
  for (const event of events) {
    const key = event?.id || event?.customId;
    if (!key || unique.has(key)) {
      continue;
    }
    unique.set(key, event);
  }

  const candidates = [...unique.values()]
    .filter((event) => {
      const startTimestamp = event?.startTimestamp;
      if (!startTimestamp) {
        return false;
      }
      const eventDate = new Date(startTimestamp * 1000);
      return toStartOfDay(eventDate) >= today;
    })
    .sort((a, b) => Number(a?.startTimestamp || 0) - Number(b?.startTimestamp || 0))
    .slice(0, Math.max(20, maxEvents));

  const picks = [];
  for (const event of candidates) {
    const startTimestamp = event?.startTimestamp;
    if (!startTimestamp) {
      continue;
    }

    const eventDate = new Date(startTimestamp * 1000);
    if (toStartOfDay(eventDate) < today) {
      continue;
    }

    const homeTeamName = event?.homeTeam?.name;
    const awayTeamName = event?.awayTeam?.name;
    if (!homeTeamName || !awayTeamName) {
      continue;
    }

    const homeTeamId = event?.homeTeam?.id;
    const awayTeamId = event?.awayTeam?.id;
    const [homeForm, awayForm] = await Promise.all([
      fetchTeamForm(homeTeamId, formCache),
      fetchTeamForm(awayTeamId, formCache)
    ]);
    const marketCandidates = buildMarketCandidates(homeForm, awayForm);
    const predicted = chooseSafestMarket(marketCandidates, 0.1);

    const kickoff = toKickoffFromDate(eventDate);
    picks.push({
      id: `sofa-${event.id}`,
      match: `${homeTeamName} vs ${awayTeamName}`,
      pick: predicted.market,
      backupPick: backupPickFor(predicted.market),
      odd: predicted.odd,
      confidence: predicted.confidence,
      selectionReason: predicted.rationale,
      safetyScore: predicted.score,
      marketCandidates: marketCandidates.slice(0, 5),
      matchday: event?.roundInfo?.round
        ? `Giornata ${event.roundInfo.round}`
        : "Giornata ND",
      tournament: event?.tournament?.uniqueTournament?.name || "Campionato ND",
      country: event?.tournament?.uniqueTournament?.category?.name || "Paese ND",
      kickoff,
      kickoffSlot: slotFromKickoff(kickoff),
      matchDate: toIsoDate(eventDate),
      source: "sofascore",
      raw: `Sofascore event ${event.id}`,
      pastStats: {
        homePpg: Number(homeForm.pointsPerGame.toFixed(2)),
        awayPpg: Number(awayForm.pointsPerGame.toFixed(2)),
        homeGF: Number(homeForm.goalsForPerGame.toFixed(2)),
        awayGF: Number(awayForm.goalsForPerGame.toFixed(2))
      }
    });
  }

  return picks;
}

async function getUnifiedMatches(timeRangeDays = 7, maxMatches = 10, startDate = new Date()) {
  const startDateIso = toIsoDate(toStartOfDay(startDate));
  const cacheKey = `${timeRangeDays}-${maxMatches}-${startDateIso}`;
  const cached = unifiedCache.get(cacheKey);
  if (cached && Date.now() - cached.createdAt < 5 * 60 * 1000) {
    return cached.payload;
  }

  const [sofaMatches, mondoMatches] = await Promise.all([
    fetchSofascoreFootball(timeRangeDays, startDate, Math.max(maxMatches * 4, 28)),
    scrapeSerieAPicks(sourceUrl)
  ]);

  const sofaInWindow = withDateWindow(sofaMatches, timeRangeDays, startDate, true);
  if (sofaInWindow.length >= 3) {
    const payload = {
      matches: sofaInWindow,
      source: "sofascore-football",
      startDate: startDateIso
    };
    unifiedCache.set(cacheKey, { createdAt: Date.now(), payload });
    return payload;
  }

  const mondoInWindow = withDateWindow(mondoMatches, timeRangeDays, startDate, true);
  const merged = new Map();

  for (const match of [...sofaInWindow, ...mondoInWindow]) {
    const key = normalizeWhitespace((match.match || "").toLowerCase());
    if (!key || merged.has(key)) {
      continue;
    }
    merged.set(key, match);
  }

  const finalMatches = [...merged.values()];
  if (finalMatches.length) {
    const payload = {
      matches: finalMatches,
      source: sofaInWindow.length ? "sofascore+mondopengwin" : "mondopengwin",
      startDate: startDateIso
    };
    unifiedCache.set(cacheKey, { createdAt: Date.now(), payload });
    return payload;
  }

  const payload = {
    matches: getFallbackMatches(),
    source: "fallback",
    startDate: startDateIso
  };
  unifiedCache.set(cacheKey, { createdAt: Date.now(), payload });
  return payload;
}

function filterMatchesByTeam(matches, teamQuery) {
  const query = normalizeSearchText(teamQuery);
  if (!query) {
    return matches;
  }

  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const queryRegex = new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, "i");

  return matches.filter((match) => {
    const raw = String(match.match || "");
    const [homeRaw = "", awayRaw = ""] = raw.split(/\s+vs\s+/i);
    const home = normalizeSearchText(homeRaw);
    const away = normalizeSearchText(awayRaw);

    return (
      queryRegex.test(home) ||
      queryRegex.test(away) ||
      home.startsWith(query) ||
      away.startsWith(query)
    );
  });
}

function chooseAlignedPool(items) {
  const byMatchDate = new Map();
  for (const item of items) {
    const key = item.matchDate || "DATA_ND";
    if (!byMatchDate.has(key)) {
      byMatchDate.set(key, []);
    }
    byMatchDate.get(key).push(item);
  }

  const dayEntries = [...byMatchDate.entries()].sort((a, b) => {
    if (b[1].length !== a[1].length) {
      return b[1].length - a[1].length;
    }
    return String(a[0]).localeCompare(String(b[0]));
  });
  const [selectedDate, dayItems] = dayEntries[0] || ["DATA_ND", []];

  const bySlot = new Map();
  for (const item of dayItems) {
    const key = item.kickoffSlot || "SLOT_ND";
    if (!bySlot.has(key)) {
      bySlot.set(key, []);
    }
    bySlot.get(key).push(item);
  }

  const slotEntries = [...bySlot.entries()].sort((a, b) => b[1].length - a[1].length);
  const [selectedSlot, slotItems] = slotEntries[0] || ["SLOT_ND", dayItems];

  const strictSlot = selectedSlot !== "SLOT_ND" && slotItems.length >= 3;
  const pool = strictSlot ? slotItems : dayItems;

  return {
    pool,
    selectedDate,
    selectedSlot: strictSlot ? selectedSlot : null,
    strictSlot
  };
}

function normalizedEvent(match, useBackup = false) {
  return {
    match: match.match,
    pick: useBackup ? match.backupPick : match.mainPick,
    odd: useBackup ? oddForPick(match.odd, true) : oddForPick(match.odd, false),
    confidence: useBackup
      ? clamp(match.confidence + 0.1, 0.35, 0.94)
      : match.confidence,
    role: useBackup ? "hedge" : "main"
  };
}

function buildCandidateTickets(coreMatches) {
  const mainEvents = coreMatches.map((item) => normalizedEvent(item, false));
  const pairEvents = combinations(mainEvents, 2);

  const tickets = [];

  for (let index = 0; index < pairEvents.length; index += 1) {
    const events = pairEvents[index];
    tickets.push({
      type: `Sistema 2/3 #${index + 1}`,
      events,
      format: "2/3",
      ...scoreTicket(events)
    });
  }

  tickets.push({
    type: "Sistema 3/3",
    events: mainEvents,
    format: "3/3",
    ...scoreTicket(mainEvents)
  });

  for (let index = 0; index < coreMatches.length; index += 1) {
    const hedgeEvents = coreMatches.map((item, innerIndex) =>
      normalizedEvent(item, innerIndex === index)
    );
    tickets.push({
      type: `Copertura distribuita ${coreMatches[index].match}`,
      events: hedgeEvents,
      format: "3/3-hedge",
      ...scoreTicket(hedgeEvents)
    });
  }

  return tickets.map((ticket) => ({
    ...ticket,
    evRatio: Number((ticket.probability * ticket.odd).toFixed(4))
  }));
}

function buildHeuristicStrategy(selectedMatches, risk, summary) {
  const profile =
    risk <= 0.2
      ? "Molto prudente"
      : risk <= 0.4
        ? "Prudente"
        : risk <= 0.6
          ? "Bilanciato"
          : risk <= 0.8
            ? "Spinto"
            : "Aggressivo";

  return {
    profile,
    criteria: [
      "Per ogni match vengono confrontati mercati esito, over/under, multigoal e team-goal.",
      "Il mercato finale è scelto con punteggio sicurezza (confidenza + bias prudenziale in base al rischio).",
      "Con rischio basso il modello privilegia mercati stabili (1X/X2, over 1.5, under 3.5, multigoal).",
      "Lo stake viene allocato solo su ticket con EV ratio >= 1; il resto resta in riserva prudente."
    ],
    matchDecisions: selectedMatches.map((item) => ({
      match: item.match,
      selectedMarket: item.mainPick,
      confidence: Number(item.confidence.toFixed(3)),
      safetyScore: Number((item.safetyScore || item.confidence).toFixed(3)),
      reason: item.selectionReason || "Scelta del mercato con miglior stabilità stimata.",
      alternatives: (item.marketCandidates || []).slice(0, 3).map((candidate) => ({
        market: candidate.market,
        confidence: candidate.confidence,
        odd: candidate.odd
      }))
    })),
    expectedEdge: summary.expectedEdge,
    successProbability: summary.successProbability
  };
}

function buildParachuteSystem(matches, options) {
  const bankroll = parseInputNumber(options.bankroll, 100);
  const maxMatches = clamp(parseInputNumber(options.maxMatches, 5), 2, 10);
  const risk = clamp(parseInputNumber(options.risk, 0.45), 0.05, 0.95);

  const enriched = matches
    .map((match, index) => {
      const bestMarket = chooseSafestMarket(match.marketCandidates || [], risk);
      const odd = Number(bestMarket?.odd || match.odd) || null;
      const confidence = clamp(
        Number(bestMarket?.confidence || match.confidence) || confidenceFromOdd(odd),
        0.35,
        0.88
      );
      const mainPick = bestMarket?.market || match.pick || "1X";
      const backupPick = match.backupPick || backupPickFor(mainPick);

      return {
        id: match.id || `m-${index + 1}`,
        match: match.match,
        mainPick,
        backupPick,
        odd,
        confidence,
        safetyScore: Number(bestMarket?.score || confidence),
        selectionReason: bestMarket?.rationale || match.selectionReason || "Scelta prudenziale su confidenza stimata.",
        marketCandidates: (match.marketCandidates || []).slice(0, 5),
        selectionScore: matchSelectionScore(
          {
            mainPick,
            confidence,
            odd,
            matchDate: match.matchDate
          },
          risk
        ),
        matchDate: match.matchDate || null,
        matchday: match.matchday || "Giornata ND",
        kickoff: match.kickoff || null,
        kickoffSlot: match.kickoffSlot || null,
        source: match.source || "scraping"
      };
    })
    .sort((a, b) => b.selectionScore - a.selectionScore);

  const aligned = chooseAlignedPool(enriched.slice(0, maxMatches));

  let selectedForSystem = aligned.pool
    .sort((a, b) => b.selectionScore - a.selectionScore)
    .slice(0, 3);

  if (selectedForSystem.length < 3) {
    const fromCurrentWindow = [...enriched]
      .sort((a, b) => b.selectionScore - a.selectionScore)
      .slice(0, 3);

    if (fromCurrentWindow.length >= 3) {
      selectedForSystem = fromCurrentWindow;
    } else {
      const fallbackAligned = getFallbackMatches()
        .map((item) => ({
          id: item.id,
          match: item.match,
          mainPick: item.pick,
          backupPick: item.backupPick,
          odd: item.odd,
          confidence: item.confidence,
          safetyScore: item.confidence,
          selectionReason: item.marketCandidates?.[0]?.rationale,
          marketCandidates: item.marketCandidates || [],
          selectionScore: matchSelectionScore(
            {
              mainPick: item.pick,
              confidence: item.confidence,
              odd: item.odd,
              matchDate: item.matchDate
            },
            risk
          ),
          matchDate: item.matchDate || null,
          matchday: item.matchday,
          kickoff: item.kickoff,
          kickoffSlot: item.kickoffSlot,
          source: item.source
        }))
        .sort((a, b) => b.selectionScore - a.selectionScore)
        .slice(0, 3);
      selectedForSystem = fallbackAligned;
    }
  }

  const candidateTickets = buildCandidateTickets(selectedForSystem);
  const evEligibleTickets = candidateTickets.filter((ticket) => ticket.evRatio >= 1);
  const activeTickets = evEligibleTickets.length ? evEligibleTickets : [];

  const totalProbabilityWeight =
    activeTickets.reduce((acc, ticket) => acc + ticket.probability, 0) || 1;

  const weightedEvRatio =
    activeTickets.reduce((acc, ticket) => acc + ticket.evRatio * ticket.probability, 0) /
    totalProbabilityWeight;

  const riskInvestFraction = clamp(0.2 + risk * 0.7, 0.2, 0.95);
  const investableBudget =
    activeTickets.length && weightedEvRatio >= 1
      ? bankroll * riskInvestFraction
      : 0;

  const settledCandidates = candidateTickets.map((ticket) => {
    const isActive = activeTickets.some((active) => active.type === ticket.type);
    const stake = isActive
      ? (ticket.probability / totalProbabilityWeight) * investableBudget
      : 0;
    const expectedReturn = stake * ticket.evRatio;
    return {
      ...ticket,
      stake: Number(stake.toFixed(2)),
      odd: Number(ticket.odd.toFixed(2)),
      probability: Number(ticket.probability.toFixed(4)),
      evRatio: Number(ticket.evRatio.toFixed(4)),
      expectedReturn: Number(expectedReturn.toFixed(2)),
      grossIfWin: Number((stake * ticket.odd).toFixed(2))
    };
  });

  const allocatedStake = settledCandidates.reduce((acc, item) => acc + item.stake, 0);
  const reserveStake = Number((bankroll - allocatedStake).toFixed(2));

  const reserveTicket = {
    type: "Capitale non investito (sicurezza)",
    format: "liquidita",
    events: [],
    probability: 1,
    odd: 1,
    evRatio: 1,
    stake: reserveStake,
    expectedReturn: reserveStake,
    grossIfWin: reserveStake
  };

  const settledTickets = [...settledCandidates, reserveTicket];

  const payoutMin = Math.min(...settledTickets.map((item) => item.grossIfWin));
  const payoutMax = Math.max(...settledTickets.map((item) => item.grossIfWin));
  const expectedPortfolio = settledTickets.reduce(
    (acc, item) => acc + item.expectedReturn,
    0
  );

  const activeSuccess = settledCandidates.filter((ticket) => ticket.stake > 0);
  const successProbabilityApprox =
    activeSuccess.length === 0
      ? 1
      : 1 -
        activeSuccess.reduce(
          (acc, item) => acc * (1 - clamp(item.probability, 0.001, 0.999)),
          1
        );

  const allMainCombinations = combinations(
    selectedForSystem.map((event) => normalizedEvent(event, false)),
    2
  );

  return {
    sourceMatches: matches.length,
    selectedMatches: selectedForSystem.length,
    risk,
    bankroll,
    tickets: settledTickets,
    summary: {
      payoutMin: Number(payoutMin.toFixed(2)),
      payoutMax: Number(payoutMax.toFixed(2)),
      expectedPortfolio: Number(expectedPortfolio.toFixed(2)),
      coverageTickets: settledTickets.length,
      combinationCount: allMainCombinations.length + 1,
      successProbability: Number(successProbabilityApprox.toFixed(4)),
      expectedEdge: Number((expectedPortfolio - bankroll).toFixed(2)),
      alignment: {
        matchday: selectedForSystem[0]?.matchday || "Giornata ND",
        matchDate: selectedForSystem[0]?.matchDate || aligned.selectedDate,
        strictMatchday: true,
        slot: selectedForSystem[0]?.kickoffSlot || aligned.selectedSlot,
        strictSlot: Boolean(aligned.strictSlot)
      },
      systemFormat: "2/3 + 3/3"
    },
    strategy: buildHeuristicStrategy(
      selectedForSystem,
      risk,
      {
        expectedEdge: Number((expectedPortfolio - bankroll).toFixed(2)),
        successProbability: Number(successProbabilityApprox.toFixed(4))
      }
    ),
    notes: [
      "Selezioni allineate sulla stessa giornata; slot orario applicato quando disponibile.",
      "Copertura distribuita su tutte le partite con ticket hedge dedicati.",
      "Stake allocato in proporzione alla probabilità dei ticket attivi.",
      "Vincolo EV non negativo: se EV<0 la quota resta in riserva prudente.",
      "Le probabilità sono stime e non garantiscono profitto.",
      "Verifica sempre quote aggiornate prima di giocare."
    ]
  };
}

function getFallbackMatches() {
  const today = toStartOfDay(new Date());
  const d1 = new Date(today);
  const d2 = new Date(today);
  const d3 = new Date(today);
  d1.setDate(today.getDate() + 1);
  d2.setDate(today.getDate() + 3);
  d3.setDate(today.getDate() + 6);

  return [
    {
      id: "fallback-1",
      match: "Inter vs Lazio",
      pick: "1",
      backupPick: "1X",
      odd: 1.65,
      confidence: 0.61,
      marketCandidates: [
        { market: "1X", confidence: 0.61, odd: 1.65, rationale: "Fallback forma casa." },
        { market: "OVER 1.5", confidence: 0.59, odd: 1.72, rationale: "Fallback gol attesi." },
        { market: "UNDER 3.5", confidence: 0.58, odd: 1.74, rationale: "Fallback varianza contenuta." }
      ],
      matchday: "Giornata 27",
      kickoff: "20:45",
      kickoffSlot: "Sera",
      matchDate: toIsoDate(d1),
      raw: "Dato di fallback",
      source: "fallback"
    },
    {
      id: "fallback-2",
      match: "Milan vs Bologna",
      pick: "GG",
      backupPick: "OVER 1.5",
      odd: 1.78,
      confidence: 0.57,
      marketCandidates: [
        { market: "GG", confidence: 0.57, odd: 1.78, rationale: "Fallback attacco bilanciato." },
        { market: "OVER 1.5", confidence: 0.6, odd: 1.7, rationale: "Fallback prudente gol." },
        { market: "MULTIGOAL 1-3", confidence: 0.58, odd: 1.76, rationale: "Fallback range gol." }
      ],
      matchday: "Giornata 27",
      kickoff: "20:45",
      kickoffSlot: "Sera",
      matchDate: toIsoDate(d2),
      raw: "Dato di fallback",
      source: "fallback"
    },
    {
      id: "fallback-3",
      match: "Roma vs Atalanta",
      pick: "1X",
      backupPick: "1",
      odd: 1.42,
      confidence: 0.68,
      marketCandidates: [
        { market: "1X", confidence: 0.68, odd: 1.42, rationale: "Fallback esito coperto." },
        { market: "UNDER 3.5", confidence: 0.63, odd: 1.58, rationale: "Fallback prudente." },
        { market: "HOME OVER 0.5", confidence: 0.62, odd: 1.6, rationale: "Fallback gol casa." }
      ],
      matchday: "Giornata 27",
      kickoff: "18:00",
      kickoffSlot: "Sera",
      matchDate: toIsoDate(d3),
      raw: "Dato di fallback",
      source: "fallback"
    }
  ];
}

async function scrapeSerieAPicks(url) {
  let html = "";
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
      }
    });

    if (!response.ok) {
      return getFallbackMatches();
    }

    html = await response.text();
  } catch {
    return getFallbackMatches();
  }

  const $ = cheerio.load(html);

  const lines = [];
  $("h1, h2, h3, h4, p, li, .entry-content, article").each((_, element) => {
    const raw = normalizeWhitespace($(element).text() || "");
    if (raw.length >= 12 && raw.length <= 300) {
      lines.push(raw);
    }
  });

  const pageMatchday =
    lines
      .map((line) => extractMatchday(line))
      .find((value) => Boolean(value)) || "Giornata ND";
  const referenceDate = new Date();

  const seen = new Set();
  const picks = [];

  for (const line of lines) {
    const match = extractMatch(line);
    const pick = extractPick(line);

    if (!match || !pick) {
      continue;
    }

    const key = `${match}-${pick}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);

    const odd = extractOdds(line);
    const confidence = confidenceFromOdd(odd);
    const kickoff = extractKickoff(line);
    const kickoffSlot = slotFromKickoff(kickoff);
    const matchday = extractMatchday(line) || pageMatchday;
    const parsedDate = parseDateFromText(line, referenceDate);

    picks.push({
      id: `s-${picks.length + 1}`,
      match,
      pick,
      backupPick: backupPickFor(pick),
      odd,
      confidence: Number(confidence.toFixed(3)),
      matchday,
      kickoff,
      kickoffSlot,
      matchDate: parsedDate ? toIsoDate(parsedDate) : null,
      raw: line,
      source: "mondopengwin"
    });
  }

  if (!picks.length) {
    return getFallbackMatches();
  }

  return picks;
}

async function askLlmForRefinement(payload) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return null;
  }

  const model = process.env.OPENAI_MODEL || "gpt-4o-mini";
  const prompt = [
    "Sei un analista di scommesse conservative.",
    "Ricevi un sistema paracadute già calcolato e devi proporre SOLO micro-migliorie.",
    "Regole: nessuna promessa di vincita certa, max 5 note pratiche, risposta JSON.",
    "JSON atteso: {\"adjustments\": string[], \"riskComment\": string}",
    `Dati: ${JSON.stringify(payload).slice(0, 12000)}`
  ].join("\n");

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model,
      input: prompt,
      max_output_tokens: 400,
      text: {
        format: {
          type: "json_object"
        }
      }
    })
  });

  if (!response.ok) {
    return null;
  }

  const result = await response.json();
  const outputText = result?.output_text;
  if (!outputText) {
    return null;
  }

  try {
    return JSON.parse(outputText);
  } catch {
    return null;
  }
}

app.get("/api/health", (_, res) => {
  res.json({
    ok: true,
    sourceUrl,
    sofaBaseUrl,
    aiTraining: aiTrainingState,
    marketUniverse: getMarketUniverseSummary()
  });
});

app.post("/api/train/recalibrate", async (req, res) => {
  try {
    const body = req.body || {};
    const days = clamp(parseInputNumber(body.days, 30), 7, 90);
    const training = await recalibrateAiFromRecentResults(days);
    res.json({
      ok: true,
      days,
      training
    });
  } catch (error) {
    res.status(500).json({
      error: "Errore durante ricalibrazione AI.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.get("/api/matches", async (_, res) => {
  try {
    const risk = clamp(parseInputNumber(_.query?.risk, 0.45), 0.05, 0.95);
    const maxMatches = clamp(parseInputNumber(_.query?.maxMatches, 10), 2, 20);
    const timeRangeDays = clamp(parseInputNumber(_.query?.timeRangeDays, 7), 1, 14);
    const focusCountry = String(_.query?.focusCountry || "Italia");
    const teamQuery = String(_.query?.teamQuery || "");
    const startDate = parseIsoDateInput(_.query?.startDate, new Date());
    const unified = await getUnifiedMatches(timeRangeDays, maxMatches, startDate);
    const teamFiltered = filterMatchesByTeam(unified.matches, teamQuery);
    const riskAware = rankMatchesForRisk(
      applyRiskProfileToMatches(teamFiltered, risk),
      risk,
      maxMatches,
      focusCountry
    );

    res.json({
      sourceUrl,
      sourceType: unified.source,
      focusCountry,
      teamQuery,
      startDate: unified.startDate,
      risk,
      count: riskAware.length,
      timeRangeDays,
      marketUniverse: getMarketUniverseSummary(),
      matches: riskAware
    });
  } catch (error) {
    res.status(500).json({
      error: "Impossibile leggere i dati dal sito sorgente.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/predict", async (req, res) => {
  try {
    const options = req.body || {};
    const timeRangeDays = clamp(parseInputNumber(options.timeRangeDays, 7), 1, 14);
    const risk = clamp(parseInputNumber(options.risk, 0.45), 0.05, 0.95);
    const maxMatches = clamp(parseInputNumber(options.maxMatches, 5), 2, 10);
    const focusCountry = String(options.focusCountry || "Italia");
    const teamQuery = String(options.teamQuery || "");
    const startDate = parseIsoDateInput(options.startDate, new Date());
    const unified = await getUnifiedMatches(timeRangeDays, maxMatches, startDate);
    const teamFiltered = filterMatchesByTeam(unified.matches, teamQuery);
    const riskAware = rankMatchesForRisk(
      applyRiskProfileToMatches(teamFiltered, risk),
      risk,
      maxMatches,
      focusCountry
    );
    const system = buildParachuteSystem(riskAware, options);
    const llm = await askLlmForRefinement(system);

    res.json({
      generatedAt: new Date().toISOString(),
      sourceUrl,
      sourceType: unified.source,
      focusCountry,
      teamQuery,
      startDate: unified.startDate,
      timeRangeDays,
      aiTraining: aiTrainingState,
      marketUniverse: getMarketUniverseSummary(),
      system,
      llm
    });
  } catch (error) {
    res.status(500).json({
      error: "Errore nella generazione del pronostico.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.listen(port, () => {
  console.log(`Predict app attiva su http://localhost:${port}`);
  recalibrateAiFromRecentResults(30).catch(() => null);
});
