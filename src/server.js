import "dotenv/config";
import express from "express";
import cors from "cors";
import * as cheerio from "cheerio";

const app = express();
const port = Number(process.env.PORT || 3000);
const sourceUrl =
  process.env.SOURCE_URL ||
  "https://www.mondopengwin.it/pronostici/calcio/serie-a/";

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

function normalizeWhitespace(value) {
  return value.replace(/\s+/g, " ").trim();
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

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
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
    return clamp(baseOdd * 0.78, 1.2, 10);
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

function buildParachuteSystem(matches, options) {
  const bankroll = Number(options.bankroll || 100);
  const maxMatches = clamp(Number(options.maxMatches || 5), 2, 10);
  const risk = clamp(Number(options.risk || 0.45), 0.05, 0.95);

  const enriched = matches
    .map((match, index) => {
      const odd = Number(match.odd) || null;
      const confidence = clamp(
        Number(match.confidence) || confidenceFromOdd(odd),
        0.35,
        0.88
      );
      const mainPick = match.pick || "1X";
      const backupPick = match.backupPick || backupPickFor(mainPick);

      return {
        id: match.id || `m-${index + 1}`,
        match: match.match,
        mainPick,
        backupPick,
        odd,
        confidence,
        source: match.source || "scraping"
      };
    })
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, maxMatches);

  const systemSize = clamp(Math.round(enriched.length * (0.6 + risk * 0.3)), 2, 6);
  const selectedForSystem = enriched.slice(0, Math.max(systemSize, 2));

  const baseEvents = selectedForSystem.map((event) => ({
    match: event.match,
    pick: event.mainPick,
    odd: oddForPick(event.odd, false),
    confidence: event.confidence,
    role: "main"
  }));

  const lowConfidenceCount = clamp(
    Math.round(selectedForSystem.length * (0.25 + risk * 0.45)),
    1,
    Math.max(1, selectedForSystem.length - 1)
  );

  const hedgable = [...selectedForSystem]
    .sort((a, b) => a.confidence - b.confidence)
    .slice(0, lowConfidenceCount);

  const tickets = [];

  tickets.push({
    type: "Base",
    events: baseEvents,
    ...scoreTicket(baseEvents)
  });

  for (const hedgeTarget of hedgable) {
    const hedgeEvents = selectedForSystem.map((event) => {
      const isTarget = event.id === hedgeTarget.id;
      const pick = isTarget ? event.backupPick : event.mainPick;
      const odd = oddForPick(event.odd, isTarget);
      const confidence = isTarget
        ? clamp(event.confidence + 0.08, 0.35, 0.92)
        : event.confidence;
      return {
        match: event.match,
        pick,
        odd,
        confidence,
        role: isTarget ? "hedge" : "main"
      };
    });

    tickets.push({
      type: `Hedge su ${hedgeTarget.match}`,
      events: hedgeEvents,
      ...scoreTicket(hedgeEvents)
    });
  }

  const baseStake = bankroll * (0.45 - risk * 0.2);
  const hedgeTotal = bankroll - baseStake;
  const hedgeStake = tickets.length > 1 ? hedgeTotal / (tickets.length - 1) : 0;

  const settledTickets = tickets.map((ticket, index) => {
    const stake = index === 0 ? baseStake : hedgeStake;
    const expectedReturn = stake * ticket.odd * ticket.probability;
    return {
      ...ticket,
      stake: Number(stake.toFixed(2)),
      odd: Number(ticket.odd.toFixed(2)),
      probability: Number(ticket.probability.toFixed(4)),
      expectedReturn: Number(expectedReturn.toFixed(2)),
      grossIfWin: Number((stake * ticket.odd).toFixed(2))
    };
  });

  const payoutMin = Math.min(...settledTickets.map((item) => item.grossIfWin));
  const payoutMax = Math.max(...settledTickets.map((item) => item.grossIfWin));
  const expectedPortfolio = settledTickets.reduce(
    (acc, item) => acc + item.expectedReturn,
    0
  );

  const allMainCombinations = combinations(baseEvents, Math.min(3, baseEvents.length));

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
      combinationCount: allMainCombinations.length
    },
    notes: [
      "Strategia paracadute: ticket base + coperture sulle partite a confidenza minore.",
      "Le probabilità sono stime e non garantiscono profitto.",
      "Verifica sempre quote aggiornate prima di giocare."
    ]
  };
}

function getFallbackMatches() {
  return [
    {
      id: "fallback-1",
      match: "Inter vs Lazio",
      pick: "1",
      backupPick: "1X",
      odd: 1.65,
      confidence: 0.61,
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

    picks.push({
      id: `s-${picks.length + 1}`,
      match,
      pick,
      backupPick: backupPickFor(pick),
      odd,
      confidence: Number(confidence.toFixed(3)),
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
  res.json({ ok: true, sourceUrl });
});

app.get("/api/matches", async (_, res) => {
  try {
    const matches = await scrapeSerieAPicks(sourceUrl);
    res.json({
      sourceUrl,
      count: matches.length,
      matches
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
    const matches = await scrapeSerieAPicks(sourceUrl);
    const system = buildParachuteSystem(matches, options);
    const llm = await askLlmForRefinement(system);

    res.json({
      generatedAt: new Date().toISOString(),
      sourceUrl,
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
});
