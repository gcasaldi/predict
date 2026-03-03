# Predict Serie A (sistema a paracadute)

Web app che:
- legge pronostici pubblici dalla pagina Serie A di MondoPengWin;
- estrae partite/esiti/quota in modo robusto (best effort);
- genera un piano "a sistema" con copertura (paracadute) usando un motore euristico + opzionale LLM;
- mostra ticket, stake consigliato e stima di payout.

## Nota importante

Non esistono vincite sicure nelle scommesse sportive. Questa app **non può garantire** una vincita certa: fornisce una strategia di copertura probabilistica per ridurre il rischio.

## Requisiti

- Node.js 20+

## Avvio rapido

1. Installa dipendenze:

```bash
npm install
```

2. (Opzionale) configura API key per supporto LLM:

```bash
cp .env.example .env
```

Compila `OPENAI_API_KEY` se vuoi usare l'analisi LLM oltre al motore locale.

3. Avvia:

```bash
npm run dev
```

4. Apri:

```text
http://localhost:3000
```

## Parametri principali

- **Budget totale**: importo da distribuire sui ticket.
- **Massimo partite**: quante partite includere nel sistema.
- **Soglia rischio (0-1)**: più alta = più ticket di copertura.

## Come funziona il paracadute

1. Costruisce un ticket base con gli esiti principali.
2. Aggiunge ticket hedge sostituendo, una alla volta, le partite meno sicure con un esito alternativo.
3. Distribuisce lo stake (base + coperture) e stima payout minimo/massimo.

## Limiti

- Lo scraping dipende dalla struttura HTML della pagina sorgente.
- Le quote possono non essere presenti su tutti i contenuti.
- Le stime sono probabilistiche e non costituiscono consulenza finanziaria.