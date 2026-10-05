import "dotenv/config";
import express from "express";
import http from "http";
import WebSocket from "ws";
import { LabelerServer } from "./labeler-server.js";
import { startJetstream, currentStats } from "./jetstream.js";
import * as db from "./db.js";

// @skyware/jetstream expects a global WebSocket; Node 20 doesn't have one built-in
(global as any).WebSocket = WebSocket;

const { LABELER_DID, LABELER_SIGNING_KEY } = process.env;

if (!LABELER_DID || !LABELER_DID.startsWith("did:")) {
  console.error("LABELER_DID must be set and start with 'did:'");
  process.exit(1);
}
if (!LABELER_SIGNING_KEY) {
  console.error("LABELER_SIGNING_KEY must be set");
  process.exit(1);
}

const app = express();
const server = http.createServer(app);

// Fly.io proxies idle connections for up to 60s; these values ensure Node outlasts that
server.keepAliveTimeout = 61_000;
server.headersTimeout = 65_000;

const PORT = parseInt(process.env.PORT ?? "8080", 10);
const labeler = new LabelerServer(LABELER_DID);

// DISABLE_AUTO_LABEL=1 serves the label stream without watching Jetstream,
// so labels are only emitted through /admin/label.
const AUTO_LABEL = process.env.DISABLE_AUTO_LABEL !== "1";
const { ADMIN_TOKEN } = process.env;

app.get("/", (_req, res) => res.send("CrossNote Labeler is running"));
app.get("/stats", (_req, res) => res.json(currentStats));

// Manual labelling for one-off scripts. Disabled unless ADMIN_TOKEN is set.
if (ADMIN_TOKEN) {
  const admin = express.Router();
  admin.use(express.json());
  admin.use((req, res, next) => {
    if (req.get("authorization") !== `Bearer ${ADMIN_TOKEN}`) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    next();
  });

  admin.get("/status", (_req, res) => {
    res.json({ autoLabel: AUTO_LABEL, subscribers: labeler.subscriberCount(), supportsNeg: true });
  });

  admin.post("/label", async (req, res) => {
    const { uri, cid, val, text, comment } = req.body ?? {};
    if (typeof uri !== "string" || !uri.startsWith("at://") || typeof val !== "string") {
      res.status(400).json({ error: "InvalidRequest", message: "uri (at://) and val are required" });
      return;
    }
    // With no subscriber the label would be signed and silently dropped.
    if (labeler.subscriberCount() === 0) {
      res.status(503).json({ error: "NoSubscribers", message: "No client is subscribed to the label stream" });
      return;
    }
    const neg = req.body.neg === true;
    await labeler.emitLabel(uri, val, cid, neg);
    if (neg) {
      db.removeLabel(uri, val);
    } else {
      db.logLabel(uri, cid, val, typeof text === "string" ? text : "", comment);
    }
    console.log(`[ADMIN ${neg ? "UNLABEL" : "LABEL"}] ${val} → ${uri}`);
    res.json({ ok: true, subscribers: labeler.subscriberCount() });
  });

  app.use("/admin", admin);
}

server.on("upgrade", (req, socket, head) => labeler.handleUpgrade(req, socket, head));

server.listen(PORT, "0.0.0.0", async () => {
  await labeler.loadKey(LABELER_SIGNING_KEY!);
  console.log(`Listening on port ${PORT}`);
  if (AUTO_LABEL) {
    startJetstream(labeler);
  } else {
    console.log("DISABLE_AUTO_LABEL=1 — Jetstream not started, labels only via /admin/label");
  }
});
