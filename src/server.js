import "dotenv/config";
import express from "express";
import cors from "cors";
import * as cheerio from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execSync } from "node:child_process";

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
const aiLearningState = {
  updatedAt: null,
  marketOutcomes: {},
  swapOutcomes: {}
};
const externalSignalState = {
  updatedAt: null,
  byMatch: {}
};
const predictionHistory = [];
const MAX_HISTORY_ITEMS = 20000;
const MEMORY_DIR = path.join(process.cwd(), "data");
const MEMORY_FILE = path.join(MEMORY_DIR, "ai-memory.json");
const RETENTION_POLICY = {
  keepDaysFull: 120,
  keepDaysMedium: 240,
  keepDaysLow: 540,
  mediumKeepRatio: 0.8,
  lowKeepRatio: 0.45,
  veryOldKeepRatio: 0.2
};
const QUALITY_CONFIG = {
  minCandidateConf: 0.54,
  minCandidateOdd: 1.35,
  minMainScore: 0.62,
  minMainConfidence: 0.56,
  minMainOdd: 1.35,
  maxPicksPerCoupon: 3,
  abstainQualityQuantile: 0.72,
  marketDrawdownWindow: 25,
  marketDrawdownMinAccuracy: 0.48,
  marketDrawdownMinDecided: 8
};
const DATA_QUALITY_CONFIG = {
  maxSameTeamsPerWeek: 1,
  requireMatchDate: true,
  allowWithoutRound: false,
  lockWindowMinutes: 60
};
const MAX_EXTERNAL_SIGNALS = 50000;
const qualityAuditLog = [];
const MAX_QUALITY_AUDIT = 500;
const TRAINING_CONFIG = {
  autoEnabled: true,
  intervalHours: 24,
  autoIntervalMs: 24 * 60 * 60 * 1000,
  lookbackDays: 45,
  backtestDays: 21,
  backtestMaxPerDay: 10,
  scope: "all"
};
const TRAINING_POLICY = {
  resolverMismatchRateWarn: 0.22,
  resolverMismatchRateFail: 0.45,
  maxCriticalAlertsBeforeFail: 2,
  logLossDegradationPctBlock: 12,
  overfitDeltaBlock: 0.09
};
const featureStoreState = {
  updatedAt: null,
  versions: []
};
const MAX_FEATURE_VERSIONS = 120;
const dailyTrainingState = {
  updatedAt: null,
  activeModelVersion: null,
  activeFeatureVersion: null,
  lastJobId: null,
  jobs: []
};
const MAX_DAILY_TRAINING_REPORTS = 400;
const calibrationState = {
  updatedAt: null,
  bins: []
};
const monitoringState = {
  updatedAt: null,
  ingestion: {
    lastRunAt: null,
    status: "idle",
    sources: {}
  },
  mapping: {
    lastRunAt: null,
    acceptedRate: null,
    rejectedRate: null
  },
  drift: {
    lastRunAt: null,
    baselineAccuracy: null,
    latestAccuracy: null,
    delta: null
  },
  alerts: []
};
const MAX_MONITOR_ALERTS = 300;

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
      "X",
      "2",
      "12",
      "1X",
      "X2",
      "DRAW NO BET 1",
      "DRAW NO BET 2",
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

function parseMatchTeams(matchLabel) {
  const raw = String(matchLabel || "");
  const [homeRaw = "", awayRaw = ""] = raw.split(/\s+vs\s+/i);
  return {
    home: normalizeSearchText(homeRaw),
    away: normalizeSearchText(awayRaw)
  };
}

function parseRoundNumber(matchdayLabel) {
  const match = String(matchdayLabel || "").match(/(\d{1,2})/);
  if (!match) {
    return null;
  }
  const round = Number(match[1]);
  return Number.isFinite(round) ? round : null;
}

function isoWeekToken(dateValue) {
  if (!dateValue) {
    return "WEEK_ND";
  }
  const date = new Date(dateValue);
  if (Number.isNaN(date.getTime())) {
    return "WEEK_ND";
  }

  const utc = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = utc.getUTCDay() || 7;
  utc.setUTCDate(utc.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(utc.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((utc - yearStart) / 86400000) + 1) / 7);
  return `${utc.getUTCFullYear()}-W${String(weekNo).padStart(2, "0")}`;
}

function lineupStateForMatch(signal, kickoffDate, nowDate = new Date()) {
  if (!signal) {
    return "PREDICTED";
  }

  if (signal.lineupConfirmed !== true) {
    return "ANNOUNCED";
  }

  if (!kickoffDate) {
    return "CONFIRMED";
  }

  const kickoff = new Date(kickoffDate);
  if (Number.isNaN(kickoff.getTime())) {
    return "CONFIRMED";
  }

  const diffMinutes = (kickoff.getTime() - nowDate.getTime()) / 60000;
  if (diffMinutes <= DATA_QUALITY_CONFIG.lockWindowMinutes) {
    return "LOCKED";
  }

  return "CONFIRMED";
}

function canonicalMatchId(match) {
  if (match?.id) {
    return String(match.id);
  }

  const teams = parseMatchTeams(match?.match || "");
  const date = String(match?.matchDate || "ND").slice(0, 10);
  const round = parseRoundNumber(match?.matchday) || "RND";
  const tournament = normalizeSearchText(match?.tournament || "tour");
  return `${tournament}|${date}|${round}|${teams.home}|${teams.away}`;
}

function matchKickoffUtc(match) {
  if (!match?.matchDate) {
    return null;
  }
  const kickoff = match.kickoff ? String(match.kickoff) : "12:00";
  const dateStr = String(match.matchDate).slice(0, 10);
  const iso = `${dateStr}T${kickoff.length === 5 ? kickoff : "12:00"}:00Z`;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function validateMatchRecord(match, context) {
  const reasons = [];
  const kickoffUtc = matchKickoffUtc(match);
  const round = parseRoundNumber(match.matchday);

  if (DATA_QUALITY_CONFIG.requireMatchDate && !match?.matchDate) {
    reasons.push("missing_match_date");
  }

  if (!kickoffUtc) {
    reasons.push("missing_kickoff_utc");
  }

  if (!round && !DATA_QUALITY_CONFIG.allowWithoutRound) {
    reasons.push("missing_round");
  }

  const week = isoWeekToken(match.matchDate);
  const teams = parseMatchTeams(match.match || "");
  const pairToken = `${teams.home}|${teams.away}|${week}`;

  if (context.pairSeen.get(pairToken) >= DATA_QUALITY_CONFIG.maxSameTeamsPerWeek) {
    reasons.push("duplicate_pair_same_week");
  }

  const valid = reasons.length === 0;
  if (valid) {
    context.pairSeen.set(pairToken, (context.pairSeen.get(pairToken) || 0) + 1);
  }

  const signal =
    match.externalSignal ||
    getExternalSignalForMatch(match.match, match.matchDate, match.id) ||
    null;
  const lineupState = lineupStateForMatch(signal, kickoffUtc ? new Date(kickoffUtc) : null);

  return {
    valid,
    reasons,
    normalized: {
      ...match,
      canonicalId: canonicalMatchId(match),
      kickoffUtc,
      round,
      week,
      lineupState,
      externalSignal: signal
    }
  };
}

function applyDataQualityGate(matches, meta = {}) {
  const pairSeen = new Map();
  const accepted = [];
  const rejected = [];

  for (const item of matches || []) {
    const result = validateMatchRecord(item, { pairSeen, meta });
    if (result.valid) {
      accepted.push(result.normalized);
    } else {
      rejected.push({
        match: item.match,
        id: item.id,
        reasons: result.reasons,
        matchDate: item.matchDate || null,
        matchday: item.matchday || null
      });
    }
  }

  const audit = {
    createdAt: new Date().toISOString(),
    input: (matches || []).length,
    accepted: accepted.length,
    rejected: rejected.length,
    rejectedItems: rejected.slice(0, 100)
  };

  qualityAuditLog.unshift(audit);
  if (qualityAuditLog.length > MAX_QUALITY_AUDIT) {
    qualityAuditLog.splice(MAX_QUALITY_AUDIT);
  }

  return {
    accepted,
    rejected,
    audit
  };
}

function pushMonitorAlert(type, message, detail = {}, severity = "warn", code = "GENERIC") {
  monitoringState.alerts.unshift({
    id: `alert-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    createdAt: new Date().toISOString(),
    type,
    severity,
    code,
    message,
    detail
  });
  if (monitoringState.alerts.length > MAX_MONITOR_ALERTS) {
    monitoringState.alerts.splice(MAX_MONITOR_ALERTS);
  }
}

function safeGitCommitHash() {
  if (process.env.GIT_COMMIT_HASH) {
    return String(process.env.GIT_COMMIT_HASH);
  }
  try {
    return String(execSync("git rev-parse --short HEAD", { stdio: ["ignore", "pipe", "ignore"] }))
      .trim()
      .slice(0, 40);
  } catch {
    return "unknown";
  }
}

function modelVersionToken(nowDate = new Date()) {
  const yyyy = String(nowDate.getUTCFullYear());
  const mm = String(nowDate.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(nowDate.getUTCDate()).padStart(2, "0");
  const hh = String(nowDate.getUTCHours()).padStart(2, "0");
  const min = String(nowDate.getUTCMinutes()).padStart(2, "0");
  return `model_${yyyy}${mm}${dd}_${hh}${min}`;
}

function aucFromBinaryScores(rows) {
  const positives = rows.filter((row) => row.y === 1);
  const negatives = rows.filter((row) => row.y === 0);
  if (!positives.length || !negatives.length) {
    return null;
  }

  const sorted = [...rows].sort((a, b) => b.p - a.p);
  let rank = 1;
  let sumPosRanks = 0;
  for (const row of sorted) {
    if (row.y === 1) {
      sumPosRanks += rank;
    }
    rank += 1;
  }

  const nPos = positives.length;
  const nNeg = negatives.length;
  const auc = (sumPosRanks - (nPos * (nPos + 1)) / 2) / (nPos * nNeg);
  return Number(clamp(auc, 0, 1).toFixed(4));
}

function metricsFromPicks(picks = []) {
  const rows = (picks || [])
    .filter((row) => typeof row.hit === "boolean")
    .map((row) => ({
      y: row.hit ? 1 : 0,
      p: clamp(Number(row.confidence || 0.5), 0.01, 0.99)
    }));

  if (!rows.length) {
    return {
      logLoss: null,
      brierScore: null,
      auc: null,
      ece: null
    };
  }

  const logLoss = rows.reduce((acc, row) => {
    const p = row.p;
    return acc - (row.y * Math.log(p) + (1 - row.y) * Math.log(1 - p));
  }, 0) / rows.length;

  const brier = rows.reduce((acc, row) => acc + (row.p - row.y) ** 2, 0) / rows.length;
  const auc = aucFromBinaryScores(rows);

  const bucketSize = 0.1;
  let ece = 0;
  for (let start = 0; start < 1; start += bucketSize) {
    const end = Number((start + bucketSize).toFixed(2));
    const bucket = rows.filter((row) => row.p >= start && row.p < end);
    if (!bucket.length) {
      continue;
    }
    const avgP = bucket.reduce((acc, row) => acc + row.p, 0) / bucket.length;
    const avgY = bucket.reduce((acc, row) => acc + row.y, 0) / bucket.length;
    ece += Math.abs(avgP - avgY) * (bucket.length / rows.length);
  }

  return {
    logLoss: Number(logLoss.toFixed(5)),
    brierScore: Number(brier.toFixed(5)),
    auc,
    ece: Number(ece.toFixed(5))
  };
}

function summarizeResolver(gateResult) {
  const rejected = Array.isArray(gateResult?.rejected) ? gateResult.rejected : [];
  const accepted = Array.isArray(gateResult?.accepted) ? gateResult.accepted : [];
  const reasonCounts = {
    duplicatesDropped: 0,
    timezoneMismatches: 0,
    roundMismatches: 0,
    kickoffOutOfWindow: 0
  };

  const issues = [];
  for (const row of rejected) {
    const reasons = Array.isArray(row.reasons) ? row.reasons : [];
    if (reasons.includes("duplicate_pair_same_week")) {
      reasonCounts.duplicatesDropped += 1;
    }
    if (reasons.includes("missing_kickoff_utc")) {
      reasonCounts.timezoneMismatches += 1;
      reasonCounts.kickoffOutOfWindow += 1;
    }
    if (reasons.includes("missing_round")) {
      reasonCounts.roundMismatches += 1;
    }

    issues.push({
      sourceMatchRef: row.id || row.match || "unknown",
      reason: reasons[0] || "unknown",
      home: parseMatchTeams(row.match || "").home || null,
      away: parseMatchTeams(row.match || "").away || null,
      kickoffUtc: row.matchDate ? `${String(row.matchDate).slice(0, 10)}T00:00:00Z` : null,
      league: null,
      season: currentOpenfootballSeason(new Date()),
      round: parseRoundNumber(row.matchday) || null
    });
  }

  return {
    matchesResolved: accepted.length,
    matchesUnresolved: rejected.length,
    duplicatesDropped: reasonCounts.duplicatesDropped,
    timezoneMismatches: reasonCounts.timezoneMismatches,
    roundMismatches: reasonCounts.roundMismatches,
    kickoffOutOfWindow: reasonCounts.kickoffOutOfWindow,
    resolverIssues: issues.slice(0, 10)
  };
}

function oddsSanitySummary(backtest, ingestionSummary) {
  const recent = Array.isArray(backtest?.recent) ? backtest.recent : [];
  const odds = recent.map((item) => Number(item.odd || 0)).filter((value) => Number.isFinite(value) && value > 0);
  const oddsOutliersCount = odds.filter((value) => value <= 1.01 || value >= 1000).length;
  const openingClosingCoveragePct = ingestionSummary?.football_data_uk?.rowsParsed
    ? Number(clamp((odds.length / Math.max(1, ingestionSummary.football_data_uk.rowsParsed)) * 100, 0, 100).toFixed(2))
    : 0;

  return {
    oddsRows: odds.length,
    openingClosingCoveragePct,
    oddsOutliersCount,
    marketMismatchCount: 0
  };
}

function defaultIngestionSource() {
  return {
    rowsFetched: 0,
    rowsParsed: 0,
    rowsInserted: 0,
    rowsUpdated: 0,
    httpErrors: 0,
    parseErrors: 0,
    schemaChanged: {
      changed: false,
      diff: []
    },
    rateLimited: false,
    sourceLatencyMs: 0
  };
}

function pushDailyTrainingReport(report) {
  dailyTrainingState.jobs.unshift(report);
  if (dailyTrainingState.jobs.length > MAX_DAILY_TRAINING_REPORTS) {
    dailyTrainingState.jobs.splice(MAX_DAILY_TRAINING_REPORTS);
  }
  dailyTrainingState.lastJobId = report.jobId;
  dailyTrainingState.updatedAt = new Date().toISOString();
}

function serializeDailyReport(report, include = []) {
  if (!report) {
    return null;
  }
  const includeIssues = include.includes("issues");
  const includeAlerts = include.includes("alerts");

  return {
    jobId: report.jobId,
    status: report.status,
    startedAt: report.startedAt,
    endedAt: report.endedAt,
    durationMs: report.durationMs,
    trigger: report.trigger,
    codeVersion: report.codeVersion,
    modelVersion: report.modelVersion,
    featureVersion: report.featureVersion,
    dataWindow: report.dataWindow,
    activeModelVersion: report.activeModelVersion,
    promoted: report.promoted,
    ingestion: report.ingestion,
    resolver: includeIssues
      ? report.resolver
      : {
          ...report.resolver,
          resolverIssues: undefined
        },
    training: report.training,
    oddsSanity: report.oddsSanity,
    alertsNewCount: report.alertsNewCount,
    alerts: includeAlerts ? report.alerts : undefined
  };
}

function buildCalibrationFromHistory() {
  const decided = predictionHistory.filter(
    (item) => (item.status === "win" || item.status === "loss") && Number.isFinite(item.confidence)
  );

  const bins = [];
  const step = 0.1;
  for (let start = 0.3; start < 1; start += step) {
    const end = Number((start + step).toFixed(2));
    const rows = decided.filter((item) => item.confidence >= start && item.confidence < end);
    const sample = rows.length;
    const wins = rows.filter((item) => item.status === "win").length;
    const observed = sample ? wins / sample : null;
    bins.push({
      from: Number(start.toFixed(2)),
      to: end,
      sample,
      observed: observed === null ? null : Number(observed.toFixed(4))
    });
  }

  calibrationState.updatedAt = new Date().toISOString();
  calibrationState.bins = bins;
}

function calibrateConfidence(rawConfidence) {
  const raw = clamp(Number(rawConfidence || 0.5), 0.05, 0.95);
  const bins = calibrationState.bins || [];
  const hit = bins.find((bin) => raw >= bin.from && raw < bin.to && Number(bin.sample || 0) >= 8);
  if (!hit || !Number.isFinite(hit.observed)) {
    return raw;
  }
  return Number(clamp(raw * 0.55 + Number(hit.observed) * 0.45, 0.05, 0.95).toFixed(4));
}

function pushFeatureStoreVersion(matches, meta = {}) {
  const features = (matches || []).slice(0, 400).map((item) => ({
    matchId: item.canonicalId || canonicalMatchId(item),
    match: item.match,
    matchDate: item.matchDate,
    kickoffUtc: item.kickoffUtc || matchKickoffUtc(item),
    lineupState: item.lineupState || lineupStateForMatch(item.externalSignal || null, item.kickoffUtc || null),
    market: item.pick || item.mainPick,
    odd: Number(item.odd || 0),
    confidence: Number(item.confidence || 0),
    safetyScore: Number(item.safetyScore || 0),
    qualityScore: Number(item.qualityScore || 0),
    source: item.source || "unknown"
  }));

  featureStoreState.versions.unshift({
    id: `fs-${Date.now()}`,
    createdAt: new Date().toISOString(),
    meta,
    count: features.length,
    features
  });

  if (featureStoreState.versions.length > MAX_FEATURE_VERSIONS) {
    featureStoreState.versions.splice(MAX_FEATURE_VERSIONS);
  }
  featureStoreState.updatedAt = new Date().toISOString();
}

function featureStoreSummary(limit = 5) {
  return {
    updatedAt: featureStoreState.updatedAt,
    versions: featureStoreState.versions.slice(0, limit).map((item) => ({
      id: item.id,
      createdAt: item.createdAt,
      count: item.count,
      meta: item.meta
    }))
  };
}

function monitoringSummary(limit = 25) {
  return {
    updatedAt: monitoringState.updatedAt,
    ingestion: monitoringState.ingestion,
    mapping: monitoringState.mapping,
    drift: monitoringState.drift,
    alerts: monitoringState.alerts.slice(0, limit)
  };
}

function normalizeMatchSignalKey(match, matchDate = null, eventId = null) {
  const dateToken = String(matchDate || "ND").slice(0, 10);
  const eventToken = String(eventId || "").trim();
  if (eventToken) {
    return `event:${eventToken}`;
  }
  return `match:${normalizeSearchText(match || "")}|${dateToken}`;
}

function toSignalNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeSignalMap(input) {
  if (!input || typeof input !== "object") {
    return {};
  }

  const out = {};
  for (const [market, value] of Object.entries(input)) {
    const numeric = toSignalNumber(value, 0);
    out[String(market)] = clamp(numeric, -0.2, 0.2);
  }
  return out;
}

function signalCompleteness(signal) {
  if (!signal) {
    return 0;
  }
  let count = 0;
  const keys = [
    "lineupConfirmed",
    "unavailableHome",
    "unavailableAway",
    "suspendedHome",
    "suspendedAway",
    "rotationRiskHome",
    "rotationRiskAway",
    "xgEdgeHome",
    "xgPace",
    "oddsDriftHome"
  ];

  for (const key of keys) {
    if (signal[key] !== null && signal[key] !== undefined) {
      count += 1;
    }
  }

  if (signal.marketConfidence && Object.keys(signal.marketConfidence).length) {
    count += 2;
  }
  return clamp(count / 12, 0, 1);
}

function signalBoostForCandidate(signal, market) {
  if (!signal || !market) {
    return 0;
  }

  const marketAdjustment = Number(signal.marketConfidence?.[market] || 0);
  const marketOddsAdjustment = Number(signal.marketOddsDrift?.[market] || 0);
  const unavailableHome = toSignalNumber(signal.unavailableHome, 0);
  const unavailableAway = toSignalNumber(signal.unavailableAway, 0);
  const suspendedHome = toSignalNumber(signal.suspendedHome, 0);
  const suspendedAway = toSignalNumber(signal.suspendedAway, 0);
  const rotationRiskHome = clamp(toSignalNumber(signal.rotationRiskHome, 0), 0, 1);
  const rotationRiskAway = clamp(toSignalNumber(signal.rotationRiskAway, 0), 0, 1);
  const xgPace = toSignalNumber(signal.xgPace, 0);
  const lineupConfirmed = Boolean(signal.lineupConfirmed);
  const completeness = signalCompleteness(signal);

  const absencesImpact = (unavailableHome + unavailableAway + suspendedHome + suspendedAway) * 0.002;
  const rotationImpact = (rotationRiskHome + rotationRiskAway) * 0.02;
  const paceBonus = /OVER|GG|MULTIGOAL/i.test(String(market))
    ? clamp(xgPace * 0.015, -0.03, 0.03)
    : /UNDER|NG/i.test(String(market))
      ? clamp(-xgPace * 0.015, -0.03, 0.03)
      : 0;

  const generic = (lineupConfirmed ? 0.012 : -0.006) + completeness * 0.01 - absencesImpact - rotationImpact + paceBonus;
  const finalBoost = generic + marketAdjustment + marketOddsAdjustment;
  return Number(clamp(finalBoost, -0.14, 0.14).toFixed(4));
}

function upsertExternalSignals(items = []) {
  let changed = 0;
  for (const item of items) {
    const key = normalizeMatchSignalKey(item.match, item.matchDate, item.eventId);
    const normalized = {
      source: String(item.source || "manual"),
      match: String(item.match || ""),
      matchDate: item.matchDate ? String(item.matchDate).slice(0, 10) : null,
      eventId: item.eventId ? String(item.eventId) : null,
      lineupConfirmed: item.lineupConfirmed === true,
      unavailableHome: Math.max(0, Math.round(toSignalNumber(item.unavailableHome, 0))),
      unavailableAway: Math.max(0, Math.round(toSignalNumber(item.unavailableAway, 0))),
      suspendedHome: Math.max(0, Math.round(toSignalNumber(item.suspendedHome, 0))),
      suspendedAway: Math.max(0, Math.round(toSignalNumber(item.suspendedAway, 0))),
      rotationRiskHome: clamp(toSignalNumber(item.rotationRiskHome, 0), 0, 1),
      rotationRiskAway: clamp(toSignalNumber(item.rotationRiskAway, 0), 0, 1),
      xgEdgeHome: clamp(toSignalNumber(item.xgEdgeHome, 0), -3, 3),
      xgPace: clamp(toSignalNumber(item.xgPace, 0), -3, 3),
      oddsDriftHome: clamp(toSignalNumber(item.oddsDriftHome, 0), -0.2, 0.2),
      marketConfidence: normalizeSignalMap(item.marketConfidence),
      marketOddsDrift: normalizeSignalMap(item.marketOddsDrift),
      updatedAt: new Date().toISOString()
    };

    externalSignalState.byMatch[key] = normalized;
    if (normalized.eventId) {
      const eventKey = normalizeMatchSignalKey(null, null, normalized.eventId);
      externalSignalState.byMatch[eventKey] = normalized;
    }
    changed += 1;
  }

  const entries = Object.entries(externalSignalState.byMatch)
    .sort((a, b) => Date.parse(b[1]?.updatedAt || 0) - Date.parse(a[1]?.updatedAt || 0))
    .slice(0, MAX_EXTERNAL_SIGNALS);
  externalSignalState.byMatch = Object.fromEntries(entries);
  if (changed) {
    externalSignalState.updatedAt = new Date().toISOString();
  }
  return changed;
}

function getExternalSignalForMatch(match, matchDate = null, eventId = null) {
  const byEvent = eventId
    ? externalSignalState.byMatch[normalizeMatchSignalKey(null, null, eventId)]
    : null;
  if (byEvent) {
    return byEvent;
  }

  return externalSignalState.byMatch[normalizeMatchSignalKey(match, matchDate, null)] || null;
}

function externalSignalSummary(limit = 8) {
  const rows = Object.values(externalSignalState.byMatch || {});
  const uniqueRows = [];
  const seen = new Set();

  for (const row of rows) {
    const key = normalizeMatchSignalKey(row.match, row.matchDate, row.eventId);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    uniqueRows.push(row);
  }

  const latest = [...uniqueRows]
    .sort((a, b) => Date.parse(b.updatedAt || 0) - Date.parse(a.updatedAt || 0))
    .slice(0, limit)
    .map((item) => ({
      match: item.match,
      matchDate: item.matchDate,
      source: item.source,
      lineupConfirmed: item.lineupConfirmed,
      updatedAt: item.updatedAt
    }));

  return {
    updatedAt: externalSignalState.updatedAt,
    totalSignals: uniqueRows.length,
    latest
  };
}

function toBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "yes", "si", "sì", "confirmed"].includes(normalized);
}

function parseCsvRows(csvText) {
  const text = String(csvText || "").trim();
  if (!text) {
    return [];
  }

  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length < 2) {
    return [];
  }

  const first = lines[0];
  const delimiter = (first.match(/;/g) || []).length > (first.match(/,/g) || []).length ? ";" : ",";
  const headers = first.split(delimiter).map((item) => item.trim());

  return lines.slice(1).map((line) => {
    const cols = line.split(delimiter).map((item) => item.trim());
    const row = {};
    headers.forEach((header, index) => {
      row[header] = cols[index] ?? "";
    });
    return row;
  });
}

function buildSignalsFromWhoScoredRows(rows = []) {
  const signals = [];

  for (const row of rows) {
    const match = String(row.match || row.Match || row.fixture || "").trim();
    if (!match) {
      continue;
    }

    const matchDate = String(
      row.matchDate || row.date || row.Date || row.fixtureDate || ""
    ).slice(0, 10);
    const eventId = String(row.eventId || row.id || "").trim() || null;

    const homeRating = toSignalNumber(row.homeRating ?? row.ratingHome, 0);
    const awayRating = toSignalNumber(row.awayRating ?? row.ratingAway, 0);
    const xgHome = toSignalNumber(row.xgHome ?? row.homeXg, 0);
    const xgAway = toSignalNumber(row.xgAway ?? row.awayXg, 0);
    const shotsHome = toSignalNumber(row.shotsHome ?? row.homeShots, 0);
    const shotsAway = toSignalNumber(row.shotsAway ?? row.awayShots, 0);
    const openHomeOdd = toSignalNumber(row.openHomeOdd ?? row.homeOddOpen, 0);
    const closeHomeOdd = toSignalNumber(row.closeHomeOdd ?? row.homeOddClose, 0);

    const xgEdgeHome = xgHome - xgAway;
    const xgPace = xgHome + xgAway - 2.5;
    const ratingEdge = homeRating - awayRating;
    const shotEdge = shotsHome - shotsAway;

    const marketConfidence = {};
    const marketOddsDrift = {};

    if (xgEdgeHome >= 0.3 || ratingEdge >= 0.35 || shotEdge >= 3) {
      marketConfidence["1X"] = clamp(0.02 + xgEdgeHome * 0.02 + ratingEdge * 0.03, -0.1, 0.1);
      marketConfidence["DRAW NO BET 1"] = clamp(0.03 + xgEdgeHome * 0.025, -0.1, 0.12);
    }

    if (xgEdgeHome <= -0.3 || ratingEdge <= -0.35 || shotEdge <= -3) {
      marketConfidence["X2"] = clamp(0.02 + Math.abs(xgEdgeHome) * 0.02 + Math.abs(ratingEdge) * 0.03, -0.1, 0.1);
      marketConfidence["DRAW NO BET 2"] = clamp(0.03 + Math.abs(xgEdgeHome) * 0.025, -0.1, 0.12);
    }

    if (xgPace >= 0.25) {
      marketConfidence["OVER 1.5"] = clamp(0.03 + xgPace * 0.03, -0.1, 0.12);
      marketConfidence["OVER 2.5"] = clamp(0.02 + xgPace * 0.02, -0.1, 0.1);
      marketConfidence["GG"] = clamp(0.01 + xgPace * 0.015, -0.08, 0.08);
    }

    if (xgPace <= -0.25) {
      marketConfidence["UNDER 3.5"] = clamp(0.03 + Math.abs(xgPace) * 0.02, -0.1, 0.12);
      marketConfidence["NG"] = clamp(0.01 + Math.abs(xgPace) * 0.015, -0.08, 0.08);
    }

    if (openHomeOdd > 1 && closeHomeOdd > 1) {
      const driftHome = clamp((openHomeOdd - closeHomeOdd) / openHomeOdd, -0.2, 0.2);
      marketOddsDrift["1X"] = clamp(driftHome * 0.08, -0.1, 0.1);
      marketOddsDrift["DRAW NO BET 1"] = clamp(driftHome * 0.09, -0.1, 0.1);
    }

    signals.push({
      source: "whoscored-import",
      match,
      matchDate: matchDate || null,
      eventId,
      lineupConfirmed: toBoolean(row.lineupConfirmed ?? row.confirmedLineup),
      unavailableHome: Math.max(0, Math.round(toSignalNumber(row.unavailableHome ?? row.homeMissing, 0))),
      unavailableAway: Math.max(0, Math.round(toSignalNumber(row.unavailableAway ?? row.awayMissing, 0))),
      suspendedHome: Math.max(0, Math.round(toSignalNumber(row.suspendedHome, 0))),
      suspendedAway: Math.max(0, Math.round(toSignalNumber(row.suspendedAway, 0))),
      rotationRiskHome: clamp(toSignalNumber(row.rotationRiskHome, 0), 0, 1),
      rotationRiskAway: clamp(toSignalNumber(row.rotationRiskAway, 0), 0, 1),
      xgEdgeHome,
      xgPace,
      oddsDriftHome:
        openHomeOdd > 1 && closeHomeOdd > 1
          ? clamp((openHomeOdd - closeHomeOdd) / openHomeOdd, -0.2, 0.2)
          : 0,
      marketConfidence,
      marketOddsDrift
    });
  }

  return signals;
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

function parseSofaEventId(rawId) {
  const match = String(rawId || "").match(/sofa-(\d+)/i);
  if (!match) {
    return null;
  }
  return Number(match[1]);
}

function safeRate(hit, total) {
  if (!total) {
    return 0.5;
  }
  return clamp(hit / total, 0.05, 0.95);
}

function historyDedupKey(item) {
  return [
    String(item.eventId || item.match || "ND"),
    String(item.matchDate || "ND"),
    String(item.mainMarket || "ND"),
    String(item.secondaryMarket || "ND")
  ].join("|");
}

function stableHash(value) {
  const str = String(value || "");
  let hash = 0;
  for (let index = 0; index < str.length; index += 1) {
    hash = (hash * 31 + str.charCodeAt(index)) % 1000000007;
  }
  return Math.abs(hash);
}

function daysSince(isoDate) {
  if (!isoDate) {
    return 99999;
  }
  const parsed = new Date(isoDate);
  if (Number.isNaN(parsed.getTime())) {
    return 99999;
  }
  const now = Date.now();
  return Math.max(0, Math.floor((now - parsed.getTime()) / 86400000));
}

function shouldRetainHistoryItem(item) {
  if (item.status === "pending") {
    return true;
  }

  const ageDays = daysSince(item.matchDate || item.generatedAt);
  if (ageDays <= RETENTION_POLICY.keepDaysFull) {
    return true;
  }

  const basis = `${item.eventId || item.match}|${item.mainMarket}|${item.generatedAt || ""}`;
  const bucket = stableHash(basis) % 10000;
  const keepByRatio = (ratio) => bucket < Math.floor(ratio * 10000);

  if (ageDays <= RETENTION_POLICY.keepDaysMedium) {
    return keepByRatio(RETENTION_POLICY.mediumKeepRatio);
  }

  if (ageDays <= RETENTION_POLICY.keepDaysLow) {
    return keepByRatio(RETENTION_POLICY.lowKeepRatio);
  }

  return keepByRatio(RETENTION_POLICY.veryOldKeepRatio);
}

function applyGradualHistoryRetention() {
  const retained = predictionHistory.filter((item) => shouldRetainHistoryItem(item));

  retained.sort((a, b) => {
    const timeA = Date.parse(a.generatedAt || 0) || 0;
    const timeB = Date.parse(b.generatedAt || 0) || 0;
    return timeB - timeA;
  });

  predictionHistory.splice(0, predictionHistory.length, ...retained.slice(0, MAX_HISTORY_ITEMS));
}

function dedupePredictionHistory() {
  const seen = new Set();
  const deduped = [];

  for (const item of predictionHistory) {
    const key = historyDedupKey(item);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(item);
  }

  predictionHistory.splice(0, predictionHistory.length, ...deduped);
  applyGradualHistoryRetention();
}

async function persistAiMemory() {
  const payload = {
    savedAt: new Date().toISOString(),
    predictionHistory: predictionHistory.slice(0, MAX_HISTORY_ITEMS),
    aiLearningState,
    externalSignalState,
    featureStoreState,
    calibrationState,
    monitoringState
  };

  await fs.mkdir(MEMORY_DIR, { recursive: true });
  await fs.writeFile(MEMORY_FILE, JSON.stringify(payload, null, 2), "utf-8");
}

async function loadAiMemory() {
  try {
    const raw = await fs.readFile(MEMORY_FILE, "utf-8");
    const parsed = JSON.parse(raw || "{}");

    const items = Array.isArray(parsed.predictionHistory) ? parsed.predictionHistory : [];
    predictionHistory.splice(0, predictionHistory.length, ...items.slice(0, MAX_HISTORY_ITEMS));
    dedupePredictionHistory();

    const storedLearning = parsed.aiLearningState;
    if (storedLearning && typeof storedLearning === "object") {
      aiLearningState.updatedAt = storedLearning.updatedAt || null;
      aiLearningState.marketOutcomes = storedLearning.marketOutcomes || {};
      aiLearningState.swapOutcomes = storedLearning.swapOutcomes || {};
    }

    const storedSignals = parsed.externalSignalState;
    if (storedSignals && typeof storedSignals === "object") {
      externalSignalState.updatedAt = storedSignals.updatedAt || null;
      externalSignalState.byMatch = storedSignals.byMatch || {};
    }

    const storedFeatureStore = parsed.featureStoreState;
    if (storedFeatureStore && typeof storedFeatureStore === "object") {
      featureStoreState.updatedAt = storedFeatureStore.updatedAt || null;
      featureStoreState.versions = Array.isArray(storedFeatureStore.versions)
        ? storedFeatureStore.versions
        : [];
    }

    const storedCalibration = parsed.calibrationState;
    if (storedCalibration && typeof storedCalibration === "object") {
      calibrationState.updatedAt = storedCalibration.updatedAt || null;
      calibrationState.bins = Array.isArray(storedCalibration.bins)
        ? storedCalibration.bins
        : [];
    }

    const storedMonitoring = parsed.monitoringState;
    if (storedMonitoring && typeof storedMonitoring === "object") {
      monitoringState.updatedAt = storedMonitoring.updatedAt || null;
      monitoringState.ingestion = storedMonitoring.ingestion || monitoringState.ingestion;
      monitoringState.mapping = storedMonitoring.mapping || monitoringState.mapping;
      monitoringState.drift = storedMonitoring.drift || monitoringState.drift;
      monitoringState.alerts = Array.isArray(storedMonitoring.alerts)
        ? storedMonitoring.alerts
        : monitoringState.alerts;
    }
  } catch {
    return;
  }
}

function registerMarketOutcome(market, hit) {
  if (typeof hit !== "boolean" || !market) {
    return;
  }

  if (!aiLearningState.marketOutcomes[market]) {
    aiLearningState.marketOutcomes[market] = {
      win: 0,
      total: 0,
      hitRate: 0.5
    };
  }

  const row = aiLearningState.marketOutcomes[market];
  row.total += 1;
  row.win += hit ? 1 : 0;
  row.hitRate = Number(safeRate(row.win, row.total).toFixed(3));
  aiLearningState.updatedAt = new Date().toISOString();
}

function registerSwapOutcome(mainMarket, secondaryMarket, mainHit, secondaryHit) {
  if (!mainMarket || !secondaryMarket) {
    return;
  }
  if (typeof mainHit !== "boolean" || typeof secondaryHit !== "boolean") {
    return;
  }

  const key = `${mainMarket}=>${secondaryMarket}`;
  if (!aiLearningState.swapOutcomes[key]) {
    aiLearningState.swapOutcomes[key] = {
      winsIfSwap: 0,
      total: 0,
      swapRate: 0.5
    };
  }

  const row = aiLearningState.swapOutcomes[key];
  row.total += 1;
  if (mainHit === false && secondaryHit === true) {
    row.winsIfSwap += 1;
  }
  row.swapRate = Number(safeRate(row.winsIfSwap, row.total).toFixed(3));
  aiLearningState.updatedAt = new Date().toISOString();
}

function swapPreference(mainMarket, secondaryMarket) {
  if (!mainMarket || !secondaryMarket) {
    return 0;
  }

  const key = `${mainMarket}=>${secondaryMarket}`;
  const row = aiLearningState.swapOutcomes?.[key];
  if (!row || Number(row.total || 0) < 3) {
    return 0;
  }

  const rate = safeRate(row.winsIfSwap, row.total);
  const confidence = clamp(row.total / 25, 0.15, 1);
  const centered = rate - 0.5;
  return Number((centered * 0.18 * confidence).toFixed(4));
}

function learningBoostForMarket(market) {
  const row = aiLearningState.marketOutcomes[market];
  if (!row || row.total < 3) {
    return 0;
  }

  const rate = safeRate(row.win, row.total);
  const sampleWeight = clamp(row.total / 30, 0.15, 1);
  const centered = rate - 0.5;
  return Number((centered * 0.14 * sampleWeight).toFixed(4));
}

function learningSummary() {
  const entries = Object.entries(aiLearningState.marketOutcomes || {});
  const swapEntries = Object.entries(aiLearningState.swapOutcomes || {});
  const sorted = entries
    .filter(([, row]) => Number(row?.total || 0) >= 2)
    .sort((a, b) => Number(b[1]?.hitRate || 0) - Number(a[1]?.hitRate || 0))
    .slice(0, 8)
    .map(([market, row]) => ({
      market,
      total: row.total,
      hitRate: row.hitRate
    }));

  return {
    updatedAt: aiLearningState.updatedAt,
    trackedMarkets: entries.length,
    trackedSwapRules: swapEntries.length,
    memoryCap: MAX_HISTORY_ITEMS,
    retention: {
      fullDays: RETENTION_POLICY.keepDaysFull,
      mediumDays: RETENTION_POLICY.keepDaysMedium,
      lowDays: RETENTION_POLICY.keepDaysLow
    },
    topMarkets: sorted
  };
}

function recentMarketStats(windowSize = QUALITY_CONFIG.marketDrawdownWindow) {
  const decided = predictionHistory
    .filter((item) => item.status === "win" || item.status === "loss")
    .slice(0, Math.max(10, windowSize * 4));

  const byMarket = new Map();
  for (const row of decided) {
    const market = row.mainMarket;
    if (!market) {
      continue;
    }
    if (!byMarket.has(market)) {
      byMarket.set(market, []);
    }
    byMarket.get(market).push(row.status === "win");
  }

  const result = {};
  for (const [market, hits] of byMarket.entries()) {
    const recent = hits.slice(0, windowSize);
    const total = recent.length;
    const win = recent.filter(Boolean).length;
    result[market] = {
      total,
      win,
      accuracy: total ? Number((win / total).toFixed(4)) : null
    };
  }
  return result;
}

function blockedMarketsByDrawdown() {
  const stats = recentMarketStats(QUALITY_CONFIG.marketDrawdownWindow);
  const blocked = [];
  for (const [market, row] of Object.entries(stats)) {
    if (
      Number(row.total || 0) >= QUALITY_CONFIG.marketDrawdownMinDecided &&
      Number(row.accuracy || 0) < QUALITY_CONFIG.marketDrawdownMinAccuracy
    ) {
      blocked.push(market);
    }
  }
  return blocked;
}

function isMarketBlocked(market) {
  return blockedMarketsByDrawdown().includes(String(market));
}

function marketReliability(market) {
  const calibratedRate = Number(aiTrainingState.marketHitRates?.[market] || 0.5);
  const learned = aiLearningState.marketOutcomes?.[market];
  if (!learned || !Number(learned.total || 0)) {
    return calibratedRate;
  }

  const learnedRate = Number(learned.hitRate || safeRate(learned.win, learned.total));
  const learnedWeight = clamp(Number(learned.total || 0) / 40, 0.2, 0.75);
  return Number((calibratedRate * (1 - learnedWeight) + learnedRate * learnedWeight).toFixed(4));
}

function dynamicMarketThreshold(market, risk) {
  const family = marketFamily(market);
  const conservativeWeight = 1 - clamp(risk, 0.05, 0.95);
  const baseByFamily = {
    esito: 0.58,
    goal: 0.6,
    multigol: 0.57,
    combo: 0.63,
    altro: 0.6
  };

  const base = Number(baseByFamily[family] || 0.6);
  return clamp(base + conservativeWeight * 0.06, 0.54, 0.72);
}

function qualityScoreFromCandidate(candidate, risk, signal = null) {
  const confidence = Number(candidate?.confidence || 0.5);
  const odd = Number(candidate?.odd || 1.35);
  const score = Number(candidate?.score || confidence);
  const reliability = marketReliability(candidate?.market);
  const signalBoost = signalBoostForCandidate(signal, candidate?.market);
  const valuePart = clamp((odd - 1.3) / 2.7, 0, 0.25);
  const conservativeWeight = 1 - clamp(risk, 0.05, 0.95);
  return Number(
    (
      confidence * 0.42 +
      score * 0.3 +
      reliability * 0.22 +
      valuePart * (0.06 + conservativeWeight * 0.03) +
      signalBoost
    ).toFixed(4)
  );
}

function isCandidateHighQuality(candidate, risk, signal = null) {
  if (!candidate) {
    return false;
  }

  if (isMarketBlocked(candidate.market)) {
    return false;
  }

  const confidence = Number(candidate.confidence || 0);
  const odd = Number(candidate.odd || 0);
  const threshold = dynamicMarketThreshold(candidate.market, risk);
  const qualityScore = qualityScoreFromCandidate(candidate, risk, signal);

  return (
    confidence >= Math.max(QUALITY_CONFIG.minCandidateConf, threshold) &&
    odd >= QUALITY_CONFIG.minCandidateOdd &&
    qualityScore >= QUALITY_CONFIG.minMainScore
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
    "1": homeGoals > awayGoals,
    "X": homeGoals === awayGoals,
    "2": awayGoals > homeGoals,
    "1X": homeGoals >= awayGoals,
    X2: awayGoals >= homeGoals,
    "12": homeGoals !== awayGoals,
    "OVER 0.5": totalGoals >= 1,
    "OVER 1.5": totalGoals >= 2,
    "OVER 2.5": totalGoals >= 3,
    "OVER 3.5": totalGoals >= 4,
    "UNDER 0.5": totalGoals === 0,
    "UNDER 1.5": totalGoals <= 1,
    "UNDER 2.5": totalGoals <= 2,
    "UNDER 3.5": totalGoals <= 3,
    GG: homeGoals >= 1 && awayGoals >= 1,
    NG: homeGoals === 0 || awayGoals === 0,
    "MULTIGOAL 1-3": totalGoals >= 1 && totalGoals <= 3,
    "MULTIGOAL 2-4": totalGoals >= 2 && totalGoals <= 4,
    "MULTIGOAL CASA 1-2": homeGoals >= 1 && homeGoals <= 2,
    "MULTIGOAL OSPITE 1-2": awayGoals >= 1 && awayGoals <= 2,
    "OVER 0.5 1T": firstHalfGoals >= 1,
    "OVER 0.5 2T": secondHalfGoals >= 1,
    "HOME OVER 0.5": homeGoals >= 1,
    "AWAY OVER 0.5": awayGoals >= 1,
    "1X + UNDER 3.5": homeGoals >= awayGoals && totalGoals <= 3,
    "GG + OVER 2.5": homeGoals >= 1 && awayGoals >= 1 && totalGoals >= 3
  };
}

function historyStatusFromHit(hit) {
  if (hit === true) {
    return "win";
  }
  if (hit === false) {
    return "loss";
  }
  return "pending";
}

function createHistorySummary(items) {
  const decided = items.filter((item) => item.status === "win" || item.status === "loss");
  const wins = decided.filter((item) => item.status === "win").length;
  const losses = decided.filter((item) => item.status === "loss").length;
  const pending = items.filter((item) => item.status === "pending").length;
  const accuracy = decided.length ? Number((wins / decided.length).toFixed(4)) : null;
  const avgOdd =
    decided.length > 0
      ? Number(
          (
            decided.reduce((acc, item) => acc + Number(item.mainOdd || 0), 0) /
            decided.length
          ).toFixed(2)
        )
      : null;

  return {
    total: items.length,
    decided: decided.length,
    wins,
    losses,
    pending,
    accuracy,
    avgOdd
  };
}

async function refreshPredictionHistory(limit = 120) {
  const today = toStartOfDay(new Date());
  const pending = predictionHistory
    .filter((item) => item.status === "pending")
    .filter((item) => {
      if (!item.matchDate) {
        return false;
      }
      return toStartOfDay(new Date(item.matchDate)) <= today;
    })
    .slice(0, clamp(limit, 1, 200));

  let hasUpdates = false;
  for (const item of pending) {
    const sofaId = parseSofaEventId(item.eventId);
    if (!sofaId) {
      continue;
    }

    const eventPayload = await fetchJson(`${sofaBaseUrl}/event/${sofaId}`);
    const event = eventPayload?.event;
    const statusType = event?.status?.type;
    const isFinished = statusType === "finished" || statusType === "after_penalties";
    if (!isFinished) {
      continue;
    }

    const hits = marketHitsForEvent(event);
    const mainHit = hits[item.mainMarket];
    const secondaryHit = item.secondaryMarket ? hits[item.secondaryMarket] : null;

    item.status = historyStatusFromHit(mainHit);
    item.mainHit = mainHit === true ? true : mainHit === false ? false : null;
    item.secondaryHit =
      secondaryHit === true ? true : secondaryHit === false ? false : null;
    item.evaluatedAt = new Date().toISOString();
    item.finalScore = `${Number(event?.homeScore?.current ?? 0)}-${Number(
      event?.awayScore?.current ?? 0
    )}`;

    if (item.status === "win" || item.status === "loss") {
      registerMarketOutcome(item.mainMarket, item.mainHit);
      registerMarketOutcome(item.secondaryMarket, item.secondaryHit);
      registerSwapOutcome(
        item.mainMarket,
        item.secondaryMarket,
        item.mainHit,
        item.secondaryHit
      );
      hasUpdates = true;
    }
  }

  if (hasUpdates) {
    dedupePredictionHistory();
    await persistAiMemory();
  }
}

function pushPredictionHistory(system, meta = {}) {
  const generatedAt = new Date().toISOString();
  const selectedEvents =
    (Array.isArray(system?.events) ? system.events : null) ||
    system?.selectedEvents ||
    [];

  for (const event of selectedEvents) {
    const candidate = {
      id: `pred-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      generatedAt,
      sourceType: meta.sourceType || "unknown",
      focusCountry: meta.focusCountry || "Italia",
      eventId: event.id || null,
      match: event.match,
      matchDate: event.matchDate || null,
      mainMarket: event.mainPick,
      secondaryMarket: event.secondaryPick || null,
      mainOdd: event.odd,
      secondaryOdd: event.secondaryOdd || null,
      confidence: Number(event.confidence || 0),
      safetyScore: Number(event.safetyScore || event.confidence || 0),
      status: "pending",
      mainHit: null,
      secondaryHit: null,
      finalScore: null,
      evaluatedAt: null
    };

    const duplicate = predictionHistory.find((item) => {
      const sameKey = historyDedupKey(item) === historyDedupKey(candidate);
      if (!sameKey) {
        return false;
      }
      const existingTime = item.generatedAt ? Date.parse(item.generatedAt) : 0;
      const nowTime = Date.parse(generatedAt);
      return Number.isFinite(existingTime) && nowTime - existingTime < 36 * 60 * 60 * 1000;
    });

    if (duplicate) {
      continue;
    }

    predictionHistory.unshift({
      ...candidate
    });
  }

  dedupePredictionHistory();

  if (predictionHistory.length > MAX_HISTORY_ITEMS) {
    predictionHistory.splice(MAX_HISTORY_ITEMS);
  }

  persistAiMemory().catch(() => null);
}

function buildTrackableEvents(matches, risk, limit = 60) {
  return (matches || [])
    .slice(0, clamp(parseInputNumber(limit, 60), 3, 200))
    .map((match) => {
      const signal =
        match.externalSignal ||
        getExternalSignalForMatch(match.match, match.matchDate, match.id) ||
        null;
      const top = pickTopMarkets(match.marketCandidates || [], risk, signal);
      const main = top.main;
      const secondary = top.secondary;
      if (!main) {
        return null;
      }

      return {
        id: match.id,
        match: match.match,
        matchDate: match.matchDate,
        mainPick: main.market,
        secondaryPick: secondary?.market || backupPickFor(main.market),
        odd: Number(main.odd || match.odd || 1.5),
        secondaryOdd: Number(secondary?.odd || oddForPick(main.odd, true)),
        confidence: Number(main.confidence || match.confidence || 0.5),
        safetyScore: Number(main.score || main.confidence || match.confidence || 0.5)
      };
    })
    .filter(Boolean);
}

async function recalibrateAiFromRecentResults(days = 30, scope = "all") {
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
      const isScopeAllowed = scope === "serieA" ? isSerieA : true;
      if (!isScopeAllowed || !isFinished) {
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

async function runRollingBacktest({ days = 21, risk = 0.4, maxPerDay = 10, scope = "all", abstainMode = true } = {}) {
  const today = toStartOfDay(new Date());
  const formCache = new Map();
  const marketStats = new Map();
  const picks = [];
  let abstained = 0;

  for (let offset = days; offset >= 1; offset -= 1) {
    const target = new Date(today);
    target.setDate(today.getDate() - offset);
    const payload = await fetchJson(`${sofaBaseUrl}/sport/football/scheduled-events/${toIsoDate(target)}`);
    const events = payload?.events || [];

    const finished = events
      .filter((event) => {
        const statusType = event?.status?.type;
        const isFinished = statusType === "finished" || statusType === "after_penalties";
        if (!isFinished) {
          return false;
        }
        if (scope === "serieA") {
          const tournamentName = event?.tournament?.uniqueTournament?.name || "";
          const countryName = event?.tournament?.uniqueTournament?.category?.name || "";
          return /serie a/i.test(tournamentName) && /(italy|italia)/i.test(countryName);
        }
        return true;
      })
      .slice(0, maxPerDay);

    for (const event of finished) {
      const homeTeamId = event?.homeTeam?.id;
      const awayTeamId = event?.awayTeam?.id;
      const homeName = event?.homeTeam?.name;
      const awayName = event?.awayTeam?.name;
      if (!homeTeamId || !awayTeamId || !homeName || !awayName) {
        continue;
      }

      const [homeForm, awayForm] = await Promise.all([
        fetchTeamForm(homeTeamId, formCache),
        fetchTeamForm(awayTeamId, formCache)
      ]);

      const candidates = diversifyMarketCandidates(buildMarketCandidates(homeForm, awayForm), 14);
      const signal = getExternalSignalForMatch(
        `${homeName} vs ${awayName}`,
        toIsoDate(target),
        `sofa-${event.id}`
      );
      const chosen = pickTopMarkets(candidates, risk, signal);
      const main = chosen.main;
      if (!main) {
        continue;
      }

      if (abstainMode && !isCandidateHighQuality(main, risk, signal)) {
        abstained += 1;
        continue;
      }

      const hits = marketHitsForEvent(event);
      const hit = hits[main.market];
      if (typeof hit !== "boolean") {
        continue;
      }

      const item = {
        date: toIsoDate(target),
        match: `${homeName} vs ${awayName}`,
        market: main.market,
        odd: Number(main.odd || 0),
        confidence: Number(main.confidence || 0),
        hit
      };
      picks.push(item);

      if (!marketStats.has(main.market)) {
        marketStats.set(main.market, { win: 0, total: 0 });
      }
      const row = marketStats.get(main.market);
      row.total += 1;
      row.win += hit ? 1 : 0;
    }
  }

  const total = picks.length;
  const wins = picks.filter((item) => item.hit).length;
  const accuracy = total ? Number((wins / total).toFixed(4)) : null;
  const byMarket = [...marketStats.entries()]
    .map(([market, row]) => ({
      market,
      total: row.total,
      accuracy: Number((row.win / Math.max(1, row.total)).toFixed(4))
    }))
    .sort((a, b) => b.accuracy - a.accuracy);

  return {
    generatedAt: new Date().toISOString(),
    config: {
      days,
      risk,
      maxPerDay,
      scope,
      abstainMode
    },
    summary: {
      evaluatedPicks: total,
      wins,
      losses: total - wins,
      abstained,
      accuracy
    },
    byMarket,
    recent: picks.slice(-20),
    evaluationRows: picks.slice(-600)
  };
}

function currentSeasonToken(referenceDate = new Date()) {
  const year = referenceDate.getFullYear();
  const month = referenceDate.getMonth() + 1;
  const startYear = month >= 7 ? year : year - 1;
  const endYear = startYear + 1;
  return `${String(startYear).slice(-2)}${String(endYear).slice(-2)}`;
}

function currentOpenfootballSeason(referenceDate = new Date()) {
  const year = referenceDate.getFullYear();
  const month = referenceDate.getMonth() + 1;
  const startYear = month >= 7 ? year : year - 1;
  const endYear = startYear + 1;
  return `${startYear}-${String(endYear).slice(-2)}`;
}

function mergeTrainingStatsIntoAi(statsMap) {
  const output = { ...aiTrainingState.marketHitRates };
  for (const [market, row] of statsMap.entries()) {
    if (!row.total) {
      continue;
    }
    const rate = row.hit / row.total;
    const previous = Number(output[market] || 0.5);
    const weight = clamp(row.total / 500, 0.1, 0.65);
    output[market] = Number((previous * (1 - weight) + rate * weight).toFixed(3));
  }
  aiTrainingState.marketHitRates = output;
  aiTrainingState.updatedAt = new Date().toISOString();
}

async function ingestFootballDataTraining(referenceDate = new Date()) {
  const previous = dailyTrainingState.jobs[0]?.ingestion?.football_data_uk || null;
  const leagues = ["I1", "E0", "D1", "SP1", "F1"];
  const season = currentSeasonToken(referenceDate);
  const stats = new Map();
  let rowsUsed = 0;
  const source = defaultIngestionSource();
  const schemas = new Set();
  const started = Date.now();

  for (const league of leagues) {
    const url = `https://www.football-data.co.uk/mmz4281/${season}/${league}.csv`;
    try {
      const response = await fetch(url);
      if (!response.ok) {
        source.httpErrors += 1;
        source.rateLimited = source.rateLimited || response.status === 429;
        continue;
      }

      const csvText = await response.text();
      const headers = String(csvText).split("\n")[0] || "";
      schemas.add(headers);

      const rows = parseCsvRows(csvText);
      source.rowsFetched += rows.length;
      source.rowsParsed += rows.length;
      for (const row of rows) {
        const homeGoals = Number(row.FTHG);
        const awayGoals = Number(row.FTAG);
        if (!Number.isFinite(homeGoals) || !Number.isFinite(awayGoals)) {
          source.parseErrors += 1;
          continue;
        }

        rowsUsed += 1;
        const event = {
          homeScore: { current: homeGoals, period1: Number(row.HTHG || 0) },
          awayScore: { current: awayGoals, period1: Number(row.HTAG || 0) }
        };
        const hits = marketHitsForEvent(event);
        for (const [market, hit] of Object.entries(hits)) {
          if (!stats.has(market)) {
            stats.set(market, { hit: 0, total: 0 });
          }
          const acc = stats.get(market);
          acc.total += 1;
          acc.hit += hit ? 1 : 0;
        }
      }
    } catch {
      source.httpErrors += 1;
    }
  }

  if (rowsUsed > 0) {
    mergeTrainingStatsIntoAi(stats);
  }

  source.rowsInserted = rowsUsed;
  source.rowsUpdated = 0;
  source.sourceLatencyMs = Date.now() - started;

  const schemaSnapshot = [...schemas].sort();
  const previousSchema = Array.isArray(previous?.schemaSnapshot) ? previous.schemaSnapshot : [];
  const diff = schemaSnapshot.filter((row) => !previousSchema.includes(row));
  source.schemaChanged = {
    changed: diff.length > 0,
    diff
  };
  source.schemaSnapshot = schemaSnapshot;

  return {
    source: "football_data_uk",
    season,
    ...source
  };
}

async function ingestOpenfootballSnapshot(referenceDate = new Date()) {
  const previous = dailyTrainingState.jobs[0]?.ingestion?.openfootball || null;
  const season = currentOpenfootballSeason(referenceDate);
  const files = ["it.1", "en.1", "de.1", "es.1", "fr.1"];
  const source = defaultIngestionSource();
  const signature = new Set();
  const started = Date.now();

  for (const code of files) {
    const url = `https://raw.githubusercontent.com/openfootball/football.json/master/${season}/${code}.json`;
    const payload = await fetchJson(url);
    if (!payload) {
      source.httpErrors += 1;
      continue;
    }

    signature.add(Object.keys(payload).sort().join("|"));
    const rounds = Array.isArray(payload.rounds) ? payload.rounds : [];
    source.rowsFetched += rounds.length;
    source.rowsParsed += rounds.length;
    for (const round of rounds) {
      const matches = Array.isArray(round.matches) ? round.matches : [];
      source.rowsInserted += matches.length;
    }
  }

  source.rowsUpdated = 0;
  source.sourceLatencyMs = Date.now() - started;
  const schemaSnapshot = [...signature].sort();
  const previousSchema = Array.isArray(previous?.schemaSnapshot) ? previous.schemaSnapshot : [];
  const diff = schemaSnapshot.filter((row) => !previousSchema.includes(row));
  source.schemaChanged = {
    changed: diff.length > 0,
    diff
  };
  source.schemaSnapshot = schemaSnapshot;

  return {
    source: "openfootball",
    season,
    ...source
  };
}

async function runDailyAutoTraining({ trigger = "cron" } = {}) {
  const started = Date.now();
  const startedAt = new Date(started).toISOString();
  const dataWindowStart = new Date(started);
  dataWindowStart.setUTCDate(dataWindowStart.getUTCDate() - TRAINING_CONFIG.backtestDays);
  const initialAlerts = monitoringState.alerts.length;
  const featureVersionCandidate = featureStoreState.versions.length + 1;
  const modelVersion = modelVersionToken(new Date(started));
  const codeVersion = safeGitCommitHash();
  const previousReport = dailyTrainingState.jobs[0] || null;
  const previousActiveModelVersion = dailyTrainingState.activeModelVersion;

  const jobReport = {
    jobId: randomUUID(),
    status: "success",
    startedAt,
    endedAt: null,
    durationMs: 0,
    trigger,
    codeVersion,
    modelVersion,
    featureVersion: featureVersionCandidate,
    dataWindow: {
      fromUtc: dataWindowStart.toISOString(),
      toUtc: new Date(started).toISOString()
    },
    ingestion: {
      understat: defaultIngestionSource(),
      football_data_uk: defaultIngestionSource(),
      openfootball: defaultIngestionSource()
    },
    resolver: {
      matchesResolved: 0,
      matchesUnresolved: 0,
      duplicatesDropped: 0,
      timezoneMismatches: 0,
      roundMismatches: 0,
      kickoffOutOfWindow: 0,
      resolverIssues: []
    },
    training: {
      trainSamples: 0,
      testSamples: 0,
      baselineAccuracy: null,
      latestAccuracy: null,
      logLoss: null,
      brierScore: null,
      auc: null,
      calibration: {
        method: "none",
        ece: null,
        calibrationUpdated: false
      },
      overfitGuard: {
        deltaTrainVsTest: null,
        blocked: false
      }
    },
    oddsSanity: {
      oddsRows: 0,
      openingClosingCoveragePct: 0,
      oddsOutliersCount: 0,
      marketMismatchCount: 0
    },
    alertsNewCount: 0,
    alerts: [],
    promoted: false,
    activeModelVersion: previousActiveModelVersion
  };

  monitoringState.ingestion = {
    ...monitoringState.ingestion,
    lastRunAt: startedAt,
    status: "running"
  };
  monitoringState.updatedAt = startedAt;

  try {
    const footballData = await ingestFootballDataTraining(new Date());
    const openfootball = await ingestOpenfootballSnapshot(new Date());
    jobReport.ingestion.football_data_uk = footballData;
    jobReport.ingestion.openfootball = openfootball;

    await recalibrateAiFromRecentResults(TRAINING_CONFIG.lookbackDays, TRAINING_CONFIG.scope);
    await refreshPredictionHistory(220);
    const previousCalibrationAt = calibrationState.updatedAt;
    buildCalibrationFromHistory();

    const backtest = await runRollingBacktest({
      days: TRAINING_CONFIG.backtestDays,
      risk: 0.4,
      maxPerDay: TRAINING_CONFIG.backtestMaxPerDay,
      scope: TRAINING_CONFIG.scope,
      abstainMode: true
    });

    const latestAccuracy = Number(backtest?.summary?.accuracy || 0);
    const previousAccuracy = Number(monitoringState.drift?.latestAccuracy || latestAccuracy);
    const delta = Number((latestAccuracy - previousAccuracy).toFixed(4));

    const metrics = metricsFromPicks(backtest?.evaluationRows || []);
    const previousLogLoss = Number(previousReport?.training?.logLoss || 0);
    const logLossDegradationPct = previousLogLoss > 0
      ? Number((((metrics.logLoss || 0) - previousLogLoss) / previousLogLoss * 100).toFixed(2))
      : 0;

    monitoringState.ingestion = {
      lastRunAt: new Date().toISOString(),
      status: "ok",
      sources: {
        footballData,
        openfootball
      }
    };
    monitoringState.mapping = {
      lastRunAt: new Date().toISOString(),
      acceptedRate: qualityAuditLog[0]
        ? Number((qualityAuditLog[0].accepted / Math.max(1, qualityAuditLog[0].input)).toFixed(4))
        : null,
      rejectedRate: qualityAuditLog[0]
        ? Number((qualityAuditLog[0].rejected / Math.max(1, qualityAuditLog[0].input)).toFixed(4))
        : null
    };
    monitoringState.drift = {
      lastRunAt: new Date().toISOString(),
      baselineAccuracy:
        monitoringState.drift?.baselineAccuracy === null || monitoringState.drift?.baselineAccuracy === undefined
          ? latestAccuracy
          : monitoringState.drift.baselineAccuracy,
      latestAccuracy,
      delta
    };
    monitoringState.calibration = {
      lastRunAt: new Date().toISOString(),
      method: calibrationState.bins.length ? "isotonic" : "none",
      ece: metrics.ece
    };
    monitoringState.updatedAt = new Date().toISOString();

    if (delta < -0.05) {
      pushMonitorAlert("drift", "Accuracy in calo oltre soglia", {
        previousAccuracy,
        latestAccuracy,
        delta
      }, "warn", "DRIFT_ACCURACY_DROP");
    }

    const unified = await getUnifiedMatches(7, 40, new Date());
    const gated = applyDataQualityGate(unified.matches, { source: unified.source });
    const riskAware = rankMatchesForRisk(applyRiskProfileToMatches(gated.accepted, 0.4), 0.4, 20, "Italia");
    const resolver = summarizeResolver(gated);
    const resolverMismatchRate = (resolver.matchesResolved + resolver.matchesUnresolved) > 0
      ? resolver.matchesUnresolved / (resolver.matchesResolved + resolver.matchesUnresolved)
      : 0;

    const trainSampleRates = Object.values(aiTrainingState.marketHitRates || {}).slice(0, 20);
    const trainAccuracyProxy = trainSampleRates.length
      ? trainSampleRates.reduce((acc, value) => acc + Number(value || 0), 0) / trainSampleRates.length
      : latestAccuracy;
    const deltaTrainVsTest = Number((trainAccuracyProxy - latestAccuracy).toFixed(4));

    const criticalAlerts = monitoringState.alerts.filter((alert) => alert.severity === "critical").length;
    const overfitBlocked = deltaTrainVsTest > TRAINING_POLICY.overfitDeltaBlock;
    const logLossBlocked = logLossDegradationPct > TRAINING_POLICY.logLossDegradationPctBlock;

    let status = "success";
    if (criticalAlerts >= TRAINING_POLICY.maxCriticalAlertsBeforeFail || resolverMismatchRate >= TRAINING_POLICY.resolverMismatchRateFail) {
      status = "failed";
    } else if (
      criticalAlerts > 0 ||
      resolverMismatchRate >= TRAINING_POLICY.resolverMismatchRateWarn ||
      overfitBlocked ||
      logLossBlocked
    ) {
      status = "partial_success";
    }

    if (resolverMismatchRate >= TRAINING_POLICY.resolverMismatchRateWarn) {
      pushMonitorAlert(
        "resolver",
        "Resolver mismatch oltre soglia",
        {
          resolverMismatchRate: Number(resolverMismatchRate.toFixed(4)),
          unresolved: resolver.matchesUnresolved,
          resolved: resolver.matchesResolved
        },
        resolverMismatchRate >= TRAINING_POLICY.resolverMismatchRateFail ? "critical" : "warn",
        "RESOLVER_MISMATCH"
      );
    }

    if (logLossBlocked) {
      pushMonitorAlert(
        "training",
        "LogLoss peggiorato oltre soglia",
        {
          previousLogLoss,
          latestLogLoss: metrics.logLoss,
          degradationPct: logLossDegradationPct
        },
        "critical",
        "LOGLOSS_DEGRADATION"
      );
    }

    if (overfitBlocked) {
      pushMonitorAlert(
        "training",
        "Overfit guard attivato",
        {
          deltaTrainVsTest,
          threshold: TRAINING_POLICY.overfitDeltaBlock
        },
        "warn",
        "OVERFIT_GUARD"
      );
    }

    pushFeatureStoreVersion(riskAware, {
      source: "daily-auto-training",
      accepted: gated.accepted.length,
      rejected: gated.rejected.length,
      backtestAccuracy: latestAccuracy
    });

    const featureVersionApplied = featureStoreState.versions.length;
    const shouldPromote = status === "success" && !overfitBlocked && !logLossBlocked;
    if (shouldPromote) {
      dailyTrainingState.activeModelVersion = modelVersion;
      dailyTrainingState.activeFeatureVersion = featureVersionApplied;
    }

    const endedAt = new Date().toISOString();
    const alertsSlice = monitoringState.alerts.slice(0, Math.max(0, monitoringState.alerts.length - initialAlerts));
    jobReport.status = status;
    jobReport.endedAt = endedAt;
    jobReport.durationMs = Math.max(1, Date.parse(endedAt) - started);
    jobReport.featureVersion = featureVersionApplied;
    jobReport.resolver = resolver;
    jobReport.training = {
      trainSamples: Number(aiTrainingState.sampleSize || 0),
      testSamples: Number(backtest?.summary?.evaluatedPicks || 0),
      baselineAccuracy: monitoringState.drift.baselineAccuracy,
      latestAccuracy,
      logLoss: metrics.logLoss,
      brierScore: metrics.brierScore,
      auc: metrics.auc,
      calibration: {
        method: calibrationState.bins.length ? "isotonic" : "none",
        ece: metrics.ece,
        calibrationUpdated: previousCalibrationAt !== calibrationState.updatedAt
      },
      overfitGuard: {
        deltaTrainVsTest,
        blocked: overfitBlocked
      }
    };
    jobReport.oddsSanity = oddsSanitySummary(backtest, jobReport.ingestion);
    jobReport.alertsNewCount = alertsSlice.length;
    jobReport.alerts = alertsSlice.map((alert) => ({
      severity: alert.severity || "warn",
      code: alert.code || alert.type || "GENERIC",
      message: alert.message,
      context: alert.detail || {}
    }));
    jobReport.promoted = shouldPromote;
    jobReport.activeModelVersion = dailyTrainingState.activeModelVersion || previousActiveModelVersion;

    if (!shouldPromote) {
      jobReport.activeModelVersion = previousActiveModelVersion;
    }

    pushDailyTrainingReport(jobReport);

    await persistAiMemory();

    return {
      ok: true,
      jobId: jobReport.jobId,
      startedAt,
      finishedAt: jobReport.endedAt,
      status: jobReport.status,
      promoted: jobReport.promoted,
      activeModelVersion: jobReport.activeModelVersion,
      ingestion: monitoringState.ingestion,
      drift: monitoringState.drift,
      featureStore: featureStoreSummary(1)
    };
  } catch (error) {
    monitoringState.ingestion = {
      ...monitoringState.ingestion,
      status: "error"
    };
    monitoringState.updatedAt = new Date().toISOString();
    pushMonitorAlert("ingestion", "Errore nel daily auto training", {
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    }, "critical", "TRAINING_RUNTIME_ERROR");

    const endedAt = new Date().toISOString();
    const failed = {
      ...jobReport,
      status: "failed",
      endedAt,
      durationMs: Math.max(1, Date.parse(endedAt) - started),
      alertsNewCount: 1,
      alerts: [
        {
          severity: "critical",
          code: "TRAINING_RUNTIME_ERROR",
          message: "Errore nel daily auto training",
          context: {
            detail: error instanceof Error ? error.message : "Errore sconosciuto"
          }
        }
      ],
      promoted: false,
      activeModelVersion: previousActiveModelVersion
    };
    pushDailyTrainingReport(failed);

    await persistAiMemory().catch(() => null);
    throw error;
  }
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
    "DRAW NO BET 1": 1.06,
    "DRAW NO BET 2": 1.06,
    "12": 1.04,
    GG: 1.08,
    NG: 1.08,
    "UNDER 2.5": 1.09,
    "OVER 2.5": 1.09,
    "OVER 3.5": 1.11,
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
      market: "X",
      confidence: clamp(
        0.2 + (1 - Math.min(Math.abs(ppgDiff), 1.4) / 1.4) * 0.34,
        0.16,
        0.54
      ),
      rationale: "Pareggio favorito quando le squadre sono equilibrate per forma."
    },
    {
      market: "2",
      confidence: clamp(0.42 - ppgDiff * 0.18, 0.2, 0.8),
      rationale: "Esito secco ospite in base a differenza PPG."
    },
    {
      market: "12",
      confidence: clamp(0.64 + Math.abs(ppgDiff) * 0.08, 0.42, 0.86),
      rationale: "Riduce il rischio pareggio in match sbilanciati."
    },
    {
      market: "DRAW NO BET 1",
      confidence: clamp(0.5 + ppgDiff * 0.14, 0.24, 0.82),
      rationale: "Protezione sul pareggio a favore casa."
    },
    {
      market: "DRAW NO BET 2",
      confidence: clamp(0.5 - ppgDiff * 0.14, 0.24, 0.82),
      rationale: "Protezione sul pareggio a favore ospite."
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
      market: "OVER 3.5",
      confidence: clamp(0.28 + (expectedTotalGoals - 3.1) * 0.2, 0.12, 0.66),
      rationale: "Match ad alta varianza gol."
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

function marketFamily(market) {
  const token = String(market || "").toUpperCase();
  if (["1", "X", "2", "1X", "X2", "12", "DRAW NO BET 1", "DRAW NO BET 2"].includes(token)) {
    return "esito";
  }
  if (token.includes("MULTIGOAL")) {
    return "multigol";
  }
  if (token.includes("+") || token.includes("COMBO")) {
    return "combo";
  }
  if (token.includes("OVER") || token.includes("UNDER") || token === "GG" || token === "NG") {
    return "goal";
  }
  return "altro";
}

function diversifyMarketCandidates(candidates, maxItems = 12) {
  const sorted = [...(candidates || [])].sort((a, b) => b.confidence - a.confidence);
  const byFamily = new Map();

  for (const item of sorted) {
    const family = marketFamily(item.market);
    if (!byFamily.has(family)) {
      byFamily.set(family, []);
    }
    byFamily.get(family).push(item);
  }

  const picked = [];
  const used = new Set();
  const familyPriority = ["esito", "goal", "multigol", "combo", "altro"];

  for (const family of familyPriority) {
    const bucket = byFamily.get(family) || [];
    const top = bucket[0];
    if (!top) {
      continue;
    }
    const key = `${top.market}-${top.odd}`;
    if (!used.has(key)) {
      picked.push(top);
      used.add(key);
    }
  }

  for (const family of familyPriority) {
    const bucket = byFamily.get(family) || [];
    const second = bucket[1];
    if (!second) {
      continue;
    }
    const key = `${second.market}-${second.odd}`;
    if (!used.has(key) && picked.length < maxItems) {
      picked.push(second);
      used.add(key);
    }
  }

  for (const item of sorted) {
    if (picked.length >= maxItems) {
      break;
    }
    const key = `${item.market}-${item.odd}`;
    if (used.has(key)) {
      continue;
    }
    picked.push(item);
    used.add(key);
  }

  return picked;
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

function chooseSafestMarket(candidates, risk, context = {}) {
  const signal = context?.signal || null;
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
    "DRAW NO BET 1": 0.04,
    "DRAW NO BET 2": 0.04,
    "12": 0.03,
    X: 0.02,
    GG: 0.015,
    NG: 0.015,
    "OVER 2.5": 0.01,
    "OVER 3.5": -0.01,
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
    "HOME OVER 0.5",
    "AWAY OVER 0.5",
    "OVER 0.5 1T",
    "OVER 0.5 2T",
    "OVER 3.5",
    "1",
    "2",
    "GG + OVER 2.5"
  ]);

  const minOdd = risk <= 0.2 ? 1.55 : risk <= 0.45 ? 1.45 : 1.35;

  const filtered = items.filter((item) => {
    if (excludedWhenPrudent.has(item.market)) {
      return false;
    }
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
      const adaptiveBoost = learningBoostForMarket(item.market);
      const signalBoost = signalBoostForCandidate(signal, item.market);
      const score =
        item.confidence +
        conservativeWeight * (safetyBias[item.market] || 0.02) +
        valueWeight * oddValue +
        calibrationBoost +
        adaptiveBoost +
        signalBoost;
      return {
        ...item,
        hitRate,
        adaptiveBoost,
        signalBoost,
        score: Number(score.toFixed(4))
      };
    })
    .sort((a, b) => b.score - a.score)[0];
}

function pickTopMarkets(candidates, risk, signal = null) {
  const lowValueMarkets = new Set([
    "OVER 0.5",
    "HOME OVER 0.5",
    "AWAY OVER 0.5",
    "OVER 0.5 1T",
    "OVER 0.5 2T"
  ]);
  const lineupState = lineupStateForMatch(signal, null);
  const lineupSensitiveMarkets = new Set([
    "OVER 2.5",
    "OVER 3.5",
    "GG + OVER 2.5",
    "1",
    "2"
  ]);

  const valueCandidates = (candidates || []).filter((item) => !lowValueMarkets.has(item.market));

  const scored = (candidates || [])
    .map((item) => {
      const conservativeWeight = 1 - clamp(risk, 0.05, 0.95);
      const valueWeight = clamp(risk, 0.05, 0.95) * 0.08;
      const oddValue = clamp((item.odd - 1.25) / 2.8, 0, 0.2);
      const hitRate = marketReliability(item.market);
      const calibrationBoost = clamp((hitRate - 0.5) * 0.12, -0.03, 0.04);
      const adaptiveBoost = learningBoostForMarket(item.market);
      const signalBoost = signalBoostForCandidate(signal, item.market);
      const safetyBias = marketStabilityBias(item.market) * 0.5;
      const score =
        item.confidence +
        conservativeWeight * safetyBias +
        valueWeight * oddValue +
        calibrationBoost +
        adaptiveBoost +
        signalBoost;
      return {
        ...item,
        hitRate,
        adaptiveBoost,
        signalBoost,
        qualityScore: qualityScoreFromCandidate(item, risk, signal),
        score: Number(score.toFixed(4))
      };
    })
    .filter((item) => !lowValueMarkets.has(item.market))
    .filter((item) => {
      if (risk <= 0.5 && lineupState !== "LOCKED" && lineupSensitiveMarkets.has(item.market)) {
        return false;
      }
      return true;
    })
    .filter((item) => item.odd >= QUALITY_CONFIG.minCandidateOdd)
    .filter((item) => item.confidence >= Math.max(QUALITY_CONFIG.minCandidateConf, dynamicMarketThreshold(item.market, risk)))
    .filter((item) => item.qualityScore >= QUALITY_CONFIG.minMainScore)
    .sort((a, b) => b.score - a.score);

  const fallbackMain = [...valueCandidates]
    .filter((item) => item.odd >= QUALITY_CONFIG.minCandidateOdd)
    .sort((a, b) => {
      const scoreA = a.confidence + marketStabilityBias(a.market) * 0.35;
      const scoreB = b.confidence + marketStabilityBias(b.market) * 0.35;
      return scoreB - scoreA;
    })[0];

  const main = scored[0] || fallbackMain || chooseSafestMarket(valueCandidates, risk, { signal });
  const secondary =
    scored.find((item) => item.market !== main?.market) ||
    [...valueCandidates]
      .filter((item) => item.market !== main?.market)
      .filter((item) => item.odd >= QUALITY_CONFIG.minCandidateOdd)
      .sort((a, b) => b.confidence - a.confidence)[0] ||
    null;

  let finalMain = main;
  let finalSecondary = secondary;

  if (main && secondary) {
    const scoreGap = Number(main.score || main.confidence || 0) - Number(secondary.score || secondary.confidence || 0);
    const swapBias = swapPreference(main.market, secondary.market);
    const cautiousOverrideMarkets = new Set(["OVER 1.5", "OVER 2.5", "GG", "OVER 3.5"]);
    const saferTargets = new Set(["12", "UNDER 3.5", "1X", "X2", "UNDER 2.5", "NG"]);
    const shouldCautiousSwap =
      risk <= 0.5 &&
      cautiousOverrideMarkets.has(String(main.market)) &&
      saferTargets.has(String(secondary.market)) &&
      scoreGap <= 0.045;

    if (swapBias > 0.02 || shouldCautiousSwap) {
      finalMain = secondary;
      finalSecondary = main;
    }
  }

  return {
    main: finalMain,
    secondary: finalSecondary
  };
}

function allocateDiscreteStake(activeTickets, investableBudget, minStakeUnit = 1) {
  const budget = Number(investableBudget || 0);
  const unit = clamp(parseInputNumber(minStakeUnit, 1), 0.5, 5);
  if (!activeTickets.length || budget < unit) {
    return new Map();
  }

  const sorted = [...activeTickets].sort((a, b) => {
    const scoreA = a.probability * 0.65 + a.evRatio * 0.35;
    const scoreB = b.probability * 0.65 + b.evRatio * 0.35;
    return scoreB - scoreA;
  });

  const alloc = new Map();
  const maxTickets = Math.min(sorted.length, Math.max(1, Math.floor(budget / unit)));
  const chosen = sorted.slice(0, maxTickets);
  let remaining = Number(budget.toFixed(2));

  for (const ticket of chosen) {
    if (remaining < unit) {
      break;
    }
    alloc.set(ticket.type, Number(unit.toFixed(2)));
    remaining = Number((remaining - unit).toFixed(2));
  }

  while (remaining >= unit && chosen.length) {
    const current = chosen.shift();
    const currentStake = alloc.get(current.type) || 0;
    alloc.set(current.type, Number((currentStake + unit).toFixed(2)));
    remaining = Number((remaining - unit).toFixed(2));
    chosen.push(current);
  }

  return alloc;
}

function marketStabilityBias(market) {
  const map = {
    "1X": 0.12,
    X2: 0.12,
    "DRAW NO BET 1": 0.1,
    "DRAW NO BET 2": 0.1,
    "12": 0.08,
    X: 0.06,
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
    "OVER 3.5": 0.01,
    "1": 0,
    "2": 0
  };
  return map[market] || 0.04;
}

function marketGroup(market) {
  const value = String(market || "").toUpperCase();
  if (["1", "X", "2", "1X", "X2", "12", "DRAW NO BET 1", "DRAW NO BET 2"].includes(value)) {
    return "esito";
  }
  if (value.includes("MULTIGOAL")) {
    return "multigol";
  }
  if (value.includes("+") || value.includes("COMBO")) {
    return "combo";
  }
  if (value.includes("OVER") || value.includes("UNDER") || value === "GG" || value === "NG") {
    return "goal";
  }
  return "altro";
}

function quantileThreshold(values, quantile = 0.7) {
  if (!values?.length) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const q = clamp(quantile, 0.5, 0.95);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)));
  return Number(sorted[index] || 0);
}

function diversifySelectedMatches(items, maxCount = 3) {
  const limit = Math.max(1, maxCount);
  const sorted = [...items].sort((a, b) => Number(b.selectionScore || 0) - Number(a.selectionScore || 0));
  const selected = [];
  const usedGroups = new Set();

  for (const item of sorted) {
    if (selected.length >= limit) {
      break;
    }
    const group = marketGroup(item.mainPick);
    if (usedGroups.has(group)) {
      continue;
    }
    selected.push(item);
    usedGroups.add(group);
  }

  if (selected.length < limit) {
    for (const item of sorted) {
      if (selected.length >= limit) {
        break;
      }
      if (selected.some((picked) => picked.id === item.id)) {
        continue;
      }
      selected.push(item);
    }
  }

  return selected;
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
  const signal =
    match.externalSignal || getExternalSignalForMatch(match.match, match.matchDate, match.id);
  const signalBonus = signal ? signalBoostForCandidate(signal, match.mainPick || match.pick) : 0;
  return Number(
    (
      confidencePart +
      conservativeWeight * stability +
      valueWeight * valuePart +
      datePriority +
      signalBonus
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
    const signal =
      match.externalSignal ||
      getExternalSignalForMatch(match.match, match.matchDate, match.id) ||
      null;
    const chosen = chooseSafestMarket(match.marketCandidates || [], risk, { signal });
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
      externalSignal: signal,
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
  const lineupCache = new Map();
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
    const signalFromMemory = getExternalSignalForMatch(
      `${homeTeamName} vs ${awayTeamName}`,
      toIsoDate(eventDate),
      `sofa-${event.id}`
    );

    if (!lineupCache.has(event.id)) {
      const lineupPayload = await fetchJson(`${sofaBaseUrl}/event/${event.id}/lineups`);
      lineupCache.set(event.id, lineupPayload || null);
    }
    const lineupPayload = lineupCache.get(event.id);
    const lineupSignal =
      lineupPayload && !lineupPayload.error
        ? {
            source: "sofascore-lineups",
            match: `${homeTeamName} vs ${awayTeamName}`,
            matchDate: toIsoDate(eventDate),
            eventId: `sofa-${event.id}`,
            lineupConfirmed: Boolean(lineupPayload.confirmed),
            unavailableHome: Number(lineupPayload?.home?.missingPlayers?.length || 0),
            unavailableAway: Number(lineupPayload?.away?.missingPlayers?.length || 0),
            suspendedHome: 0,
            suspendedAway: 0,
            rotationRiskHome: 0,
            rotationRiskAway: 0,
            xgEdgeHome: 0,
            xgPace: 0,
            oddsDriftHome: 0,
            marketConfidence: {},
            marketOddsDrift: {}
          }
        : null;

    if (lineupSignal) {
      upsertExternalSignals([lineupSignal]);
    }
    const externalSignal =
      signalFromMemory ||
      (lineupSignal
        ? getExternalSignalForMatch(
            `${homeTeamName} vs ${awayTeamName}`,
            toIsoDate(eventDate),
            `sofa-${event.id}`
          )
        : null);

    const marketCandidates = buildMarketCandidates(homeForm, awayForm);
    const diversifiedCandidates = diversifyMarketCandidates(marketCandidates, 14);
    const predicted = chooseSafestMarket(diversifiedCandidates, 0.1, {
      signal: externalSignal
    });

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
      externalSignal,
      marketCandidates: diversifiedCandidates,
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
  const backupPick = match.secondaryPick || match.backupPick;
  const backupOdd = Number(match.secondaryOdd || oddForPick(match.odd, true));
  const backupConfidence =
    Number(match.secondaryConfidence) || clamp(match.confidence + 0.08, 0.35, 0.94);

  return {
    match: match.match,
    pick: useBackup ? backupPick : match.mainPick,
    odd: useBackup ? backupOdd : oddForPick(match.odd, false),
    confidence: useBackup ? backupConfidence : match.confidence,
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
      secondaryMarket: item.secondaryPick,
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
  const riskInput = clamp(parseInputNumber(options.risk, 0.45), 0.05, 0.95);
  const precisionMode = String(options.precisionMode ?? "1") !== "0";
  const abstainMode = String(options.abstainMode ?? "1") !== "0";
  const diversifyMode = String(options.diversifyMode ?? "1") !== "0";
  const risk = precisionMode ? clamp(riskInput * 0.9, 0.05, 0.85) : riskInput;
  const minStakeUnit = clamp(parseInputNumber(options.minStake, 1), 0.5, 5);

  const enriched = matches
    .map((match, index) => {
      const signal =
        match.externalSignal || getExternalSignalForMatch(match.match, match.matchDate, match.id);
      const chosen = pickTopMarkets(match.marketCandidates || [], risk, signal);
      const bestMarket = chosen.main;
      const secondaryMarket = chosen.secondary;
      const odd = Number(bestMarket?.odd || match.odd) || null;
      const confidence = clamp(
        Number(bestMarket?.confidence || match.confidence) || confidenceFromOdd(odd),
        0.35,
        0.88
      );
      const mainPick = bestMarket?.market || match.pick || "1X";
      const backupPick =
        secondaryMarket?.market || match.backupPick || backupPickFor(mainPick);

      return {
        id: match.id || `m-${index + 1}`,
        match: match.match,
        mainPick,
        secondaryPick: secondaryMarket?.market || backupPick,
        backupPick,
        odd,
        secondaryOdd: Number(secondaryMarket?.odd || oddForPick(odd, true)),
        confidence,
        secondaryConfidence: Number(secondaryMarket?.confidence || clamp(confidence + 0.08, 0.35, 0.92)),
        safetyScore: Number(bestMarket?.score || confidence),
        qualityScore: qualityScoreFromCandidate(
          bestMarket || { confidence, odd, market: mainPick, score: confidence },
          risk,
          signal
        ),
        selectionReason: bestMarket?.rationale || match.selectionReason || "Scelta prudenziale su confidenza stimata.",
        marketCandidates: (match.marketCandidates || []).slice(0, 5),
        externalSignal: signal,
        selectionScore: matchSelectionScore(
          {
            id: match.id,
            match: match.match,
            mainPick,
            confidence,
            odd,
            matchDate: match.matchDate,
            externalSignal: signal
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

  const highQualityPool = enriched.filter((item) => {
    if (!abstainMode) {
      return true;
    }

    const candidate = {
      market: item.mainPick,
      confidence: item.confidence,
      odd: item.odd,
      score: item.safetyScore
    };
    return (
      isCandidateHighQuality(candidate, risk, item.externalSignal || null) &&
      item.qualityScore >= QUALITY_CONFIG.minMainScore &&
      item.confidence >= QUALITY_CONFIG.minMainConfidence &&
      Number(item.odd || 0) >= QUALITY_CONFIG.minMainOdd
    );
  });

  const qualityValues = enriched.map((item) => Number(item.qualityScore || 0));
  const quantileThresholdValue = quantileThreshold(
    qualityValues,
    QUALITY_CONFIG.abstainQualityQuantile
  );
  const quantilePool = enriched.filter(
    (item) => Number(item.qualityScore || 0) >= quantileThresholdValue
  );

  const candidatePool =
    abstainMode && quantilePool.length >= 3
      ? quantilePool
      : highQualityPool.length >= 3
        ? highQualityPool
        : enriched;
  const abstainedMatches = Math.max(0, enriched.length - candidatePool.length);

  const aligned = chooseAlignedPool(candidatePool.slice(0, maxMatches));

  const rankedAligned = [...aligned.pool].sort((a, b) => b.selectionScore - a.selectionScore);
  let selectedForSystem = diversifyMode
    ? diversifySelectedMatches(rankedAligned, QUALITY_CONFIG.maxPicksPerCoupon)
    : rankedAligned.slice(0, QUALITY_CONFIG.maxPicksPerCoupon);

  if (selectedForSystem.length < 3) {
    const rankedCurrentWindow = [...enriched].sort(
      (a, b) => b.selectionScore - a.selectionScore
    );
    const fromCurrentWindow = diversifyMode
      ? diversifySelectedMatches(rankedCurrentWindow, QUALITY_CONFIG.maxPicksPerCoupon)
      : rankedCurrentWindow.slice(0, QUALITY_CONFIG.maxPicksPerCoupon);

    if (fromCurrentWindow.length >= 3) {
      selectedForSystem = fromCurrentWindow;
    } else {
      const fallbackAlignedPool = getFallbackMatches()
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
        .sort((a, b) => b.selectionScore - a.selectionScore);
      selectedForSystem = diversifyMode
        ? diversifySelectedMatches(fallbackAlignedPool, QUALITY_CONFIG.maxPicksPerCoupon)
        : fallbackAlignedPool.slice(0, QUALITY_CONFIG.maxPicksPerCoupon);
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

  const discreteStakeMap = allocateDiscreteStake(
    activeTickets,
    investableBudget,
    minStakeUnit
  );

  const settledCandidates = candidateTickets.map((ticket) => {
    const isActive = activeTickets.some((active) => active.type === ticket.type);
    const proportionalStake = isActive
      ? (ticket.probability / totalProbabilityWeight) * investableBudget
      : 0;
    const stakeFromDiscrete = discreteStakeMap.get(ticket.type);
    const stake = Number.isFinite(stakeFromDiscrete)
      ? stakeFromDiscrete
      : proportionalStake;
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
    selectedEvents: selectedForSystem.map((item) => ({
      id: item.id,
      match: item.match,
      matchDate: item.matchDate,
      source: item.source,
      mainPick: item.mainPick,
      secondaryPick: item.secondaryPick,
      odd: item.odd,
      secondaryOdd: item.secondaryOdd,
      confidence: item.confidence,
      safetyScore: item.safetyScore,
      qualityScore: item.qualityScore
    })),
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
      budgetPlan: {
        bankroll: Number(bankroll.toFixed(2)),
        investableBudget: Number(investableBudget.toFixed(2)),
        reserveStake,
        minStakeUnit,
        activeTickets: activeSuccess.length,
        precisionMode,
        abstainMode,
        diversifyMode,
        effectiveRisk: Number(risk.toFixed(3))
      },
      quality: {
        highQualityPool: highQualityPool.length,
        quantilePool: quantilePool.length,
        candidatePool: candidatePool.length,
        abstainedMatches,
        minScore: QUALITY_CONFIG.minMainScore,
        minConfidence: QUALITY_CONFIG.minMainConfidence,
        minOdd: QUALITY_CONFIG.minMainOdd,
        quantileThreshold: Number(quantileThresholdValue.toFixed(4)),
        avgQualityScore:
          selectedForSystem.length > 0
            ? Number(
                (
                  selectedForSystem.reduce((acc, item) => acc + Number(item.qualityScore || 0), 0) /
                  selectedForSystem.length
                ).toFixed(4)
              )
            : 0
      },
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
      `Piano budget: investiti ${Number(investableBudget.toFixed(2))}€ su ticket con quota minima utile; riserva ${reserveStake}€ per protezione.`,
      `Filtro qualità: ${candidatePool.length}/${enriched.length} eventi ammessi (abstain ${abstainedMatches}).`,
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
  const summary = createHistorySummary(predictionHistory);
  const drawdownStats = recentMarketStats(QUALITY_CONFIG.marketDrawdownWindow);
  const blockedMarkets = blockedMarketsByDrawdown();
  res.json({
    ok: true,
    sourceUrl,
    sofaBaseUrl,
    aiTraining: aiTrainingState,
    aiLearning: learningSummary(),
    externalSignals: externalSignalSummary(),
    qualityConfig: QUALITY_CONFIG,
    marketDrawdown: {
      blockedMarkets,
      stats: drawdownStats
    },
    dataQuality: {
      config: DATA_QUALITY_CONFIG,
      lastAudit: qualityAuditLog[0] || null
    },
    predictionHistory: summary,
    marketUniverse: getMarketUniverseSummary()
  });
});

app.get("/api/quality/audit", (req, res) => {
  const limit = clamp(parseInputNumber(req.query?.limit, 20), 1, 100);
  res.json({
    generatedAt: new Date().toISOString(),
    config: DATA_QUALITY_CONFIG,
    items: qualityAuditLog.slice(0, limit)
  });
});

app.get("/api/signals/external", (req, res) => {
  const limit = clamp(parseInputNumber(req.query?.limit, 50), 1, 500);
  const rows = Object.values(externalSignalState.byMatch || {})
    .sort((a, b) => Date.parse(b.updatedAt || 0) - Date.parse(a.updatedAt || 0))
    .slice(0, limit);

  res.json({
    generatedAt: new Date().toISOString(),
    summary: externalSignalSummary(),
    items: rows
  });
});

app.post("/api/signals/external", async (req, res) => {
  try {
    const body = req.body || {};
    const items = Array.isArray(body.items) ? body.items : [];
    if (!items.length) {
      return res.status(400).json({
        error: "Nessun segnale ricevuto.",
        detail: "Invia un array 'items' con almeno un elemento."
      });
    }

    const changed = upsertExternalSignals(items);
    await persistAiMemory();

    return res.json({
      ok: true,
      changed,
      summary: externalSignalSummary()
    });
  } catch (error) {
    return res.status(500).json({
      error: "Errore ingest segnali esterni.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/signals/whoscored", async (req, res) => {
  try {
    const body = req.body || {};
    const rowsFromJson = Array.isArray(body.rows) ? body.rows : [];
    const rowsFromCsv = body.csvText ? parseCsvRows(body.csvText) : [];
    const rows = [...rowsFromJson, ...rowsFromCsv];

    if (!rows.length) {
      return res.status(400).json({
        error: "Nessun dato WhoScored ricevuto.",
        detail: "Invia 'rows' (array JSON) oppure 'csvText'."
      });
    }

    const signals = buildSignalsFromWhoScoredRows(rows);
    if (!signals.length) {
      return res.status(400).json({
        error: "Dati WhoScored non validi.",
        detail: "Ogni riga deve contenere almeno 'match' (e opzionale matchDate/eventId)."
      });
    }

    const changed = upsertExternalSignals(signals);
    await persistAiMemory();

    return res.json({
      ok: true,
      importedRows: rows.length,
      generatedSignals: signals.length,
      changed,
      summary: externalSignalSummary()
    });
  } catch (error) {
    return res.status(500).json({
      error: "Errore import WhoScored.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.get("/api/predictions/history", async (req, res) => {
  try {
    const limit = clamp(parseInputNumber(req.query?.limit, 60), 1, 200);
    const shouldRefresh = String(req.query?.refresh || "1") !== "0";
    if (shouldRefresh) {
      await refreshPredictionHistory(limit);
    }

    const items = predictionHistory.slice(0, limit);
    res.json({
      generatedAt: new Date().toISOString(),
      summary: createHistorySummary(items),
      learning: learningSummary(),
      externalSignals: externalSignalSummary(5),
      items
    });
  } catch (error) {
    res.status(500).json({
      error: "Impossibile leggere lo storico predizioni.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/train/recalibrate", async (req, res) => {
  try {
    const body = req.body || {};
    const days = clamp(parseInputNumber(body.days, 30), 7, 90);
    const scope = String(body.scope || "all");
    const training = await recalibrateAiFromRecentResults(days, scope === "serieA" ? "serieA" : "all");
    res.json({
      ok: true,
      days,
      scope,
      training
    });
  } catch (error) {
    res.status(500).json({
      error: "Errore durante ricalibrazione AI.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/train/backtest", async (req, res) => {
  try {
    const body = req.body || {};
    const days = clamp(parseInputNumber(body.days, 21), 7, 60);
    const risk = clamp(parseInputNumber(body.risk, 0.4), 0.05, 0.95);
    const maxPerDay = clamp(parseInputNumber(body.maxPerDay, 10), 3, 30);
    const scope = String(body.scope || "all");
    const abstainMode = String(body.abstainMode ?? "1") !== "0";

    const result = await runRollingBacktest({
      days,
      risk,
      maxPerDay,
      scope: scope === "serieA" ? "serieA" : "all",
      abstainMode
    });

    res.json({
      ok: true,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      error: "Errore durante backtest rolling.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/train/autotune", async (req, res) => {
  try {
    const body = req.body || {};
    const days = clamp(parseInputNumber(body.days, 21), 7, 60);
    const risk = clamp(parseInputNumber(body.risk, 0.4), 0.05, 0.95);
    const maxPerDay = clamp(parseInputNumber(body.maxPerDay, 10), 3, 30);
    const scope = String(body.scope || "all");
    const abstainMode = String(body.abstainMode ?? "1") !== "0";

    const candidateSettings = [
      { minMainScore: 0.62, minMainConfidence: 0.56, minMainOdd: 1.35, abstainQualityQuantile: 0.72 },
      { minMainScore: 0.64, minMainConfidence: 0.58, minMainOdd: 1.4, abstainQualityQuantile: 0.74 },
      { minMainScore: 0.66, minMainConfidence: 0.6, minMainOdd: 1.45, abstainQualityQuantile: 0.76 },
      { minMainScore: 0.68, minMainConfidence: 0.62, minMainOdd: 1.5, abstainQualityQuantile: 0.78 }
    ];

    const baselineConfig = {
      minMainScore: QUALITY_CONFIG.minMainScore,
      minMainConfidence: QUALITY_CONFIG.minMainConfidence,
      minMainOdd: QUALITY_CONFIG.minMainOdd,
      abstainQualityQuantile: QUALITY_CONFIG.abstainQualityQuantile
    };

    let best = null;
    const runs = [];

    for (const setting of candidateSettings) {
      QUALITY_CONFIG.minMainScore = setting.minMainScore;
      QUALITY_CONFIG.minMainConfidence = setting.minMainConfidence;
      QUALITY_CONFIG.minMainOdd = setting.minMainOdd;
      QUALITY_CONFIG.abstainQualityQuantile = setting.abstainQualityQuantile;

      const result = await runRollingBacktest({
        days,
        risk,
        maxPerDay,
        scope: scope === "serieA" ? "serieA" : "all",
        abstainMode
      });

      const accuracy = Number(result?.summary?.accuracy || 0);
      const evaluated = Number(result?.summary?.evaluatedPicks || 0);
      const score = Number((accuracy * Math.min(1, evaluated / 100)).toFixed(6));

      const run = {
        setting,
        summary: result.summary,
        score
      };
      runs.push(run);

      if (!best || score > best.score) {
        best = run;
      }
    }

    const applied = best?.setting || baselineConfig;
    QUALITY_CONFIG.minMainScore = applied.minMainScore;
    QUALITY_CONFIG.minMainConfidence = applied.minMainConfidence;
    QUALITY_CONFIG.minMainOdd = applied.minMainOdd;
    QUALITY_CONFIG.abstainQualityQuantile = applied.abstainQualityQuantile;

    await persistAiMemory();

    res.json({
      ok: true,
      config: {
        days,
        risk,
        maxPerDay,
        scope,
        abstainMode
      },
      baselineConfig,
      applied,
      best,
      runs
    });
  } catch (error) {
    res.status(500).json({
      error: "Errore durante autotuning soglie.",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.post("/api/train/daily", async (_req, res) => {
  try {
    const report = await runDailyAutoTraining();
    res.json(report);
  } catch (error) {
    console.error("/api/train/daily error", error);
    res.status(500).json({
      error: "Autotraining giornaliero fallito",
      detail: error instanceof Error ? error.message : "Errore sconosciuto"
    });
  }
});

app.get("/api/monitoring", (_req, res) => {
  res.json({
    updatedAt: monitoringState.updatedAt || null,
    ingestion: monitoringState.ingestion,
    mapping: monitoringState.mapping,
    calibration: monitoringState.calibration,
    drift: monitoringState.drift,
    alerts: monitoringState.alerts.slice(0, 100),
    featureStore: featureStoreSummary(5),
    calibrationState: {
      updatedAt: calibrationState.updatedAt,
      confidenceScale: calibrationState.confidenceScale,
      reliabilityByBucket: calibrationState.reliabilityByBucket
    }
  });
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
    const qualityGate = applyDataQualityGate(teamFiltered, {
      startDate: unified.startDate,
      timeRangeDays
    });
    const riskAware = rankMatchesForRisk(
      applyRiskProfileToMatches(qualityGate.accepted, risk),
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
      qualityAudit: {
        accepted: qualityGate.accepted.length,
        rejected: qualityGate.rejected.length
      },
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
    const trackAllPredictions = String(options.trackAllPredictions ?? "1") !== "0";
    const trackLimit = clamp(parseInputNumber(options.trackLimit, 50), 3, 200);
    const startDate = parseIsoDateInput(options.startDate, new Date());
    const unified = await getUnifiedMatches(timeRangeDays, maxMatches, startDate);
    const teamFiltered = filterMatchesByTeam(unified.matches, teamQuery);
    const qualityGate = applyDataQualityGate(teamFiltered, {
      startDate: unified.startDate,
      timeRangeDays
    });
    const riskAware = rankMatchesForRisk(
      applyRiskProfileToMatches(qualityGate.accepted, risk),
      risk,
      maxMatches,
      focusCountry
    );
    const system = buildParachuteSystem(riskAware, options);
    const trackableEvents = trackAllPredictions
      ? buildTrackableEvents(riskAware, risk, trackLimit)
      : system.selectedEvents || [];

    pushPredictionHistory({ events: trackableEvents }, {
      sourceType: unified.source,
      focusCountry
    });
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
      tracking: {
        trackAllPredictions,
        trackLimit,
        trackedEvents: trackableEvents.length
      },
      qualityAudit: {
        accepted: qualityGate.accepted.length,
        rejected: qualityGate.rejected.length
      },
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
  loadAiMemory().catch(() => null);
  recalibrateAiFromRecentResults(30).catch(() => null);
  if (TRAINING_CONFIG.autoEnabled) {
    const intervalMs = Number(TRAINING_CONFIG.autoIntervalMs) > 0
      ? Number(TRAINING_CONFIG.autoIntervalMs)
      : Number(TRAINING_CONFIG.intervalHours || 24) * 60 * 60 * 1000;

    setTimeout(() => {
      runDailyAutoTraining().catch((error) => {
        console.error("Daily auto training bootstrap error", error);
      });
    }, 12_000);

    setInterval(() => {
      runDailyAutoTraining().catch((error) => {
        console.error("Daily auto training interval error", error);
      });
    }, intervalMs);
  }
});
