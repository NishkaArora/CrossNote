import "dotenv/config";
import express from "express";
import http from "http";
import WebSocket from "ws";
import { LabelerServer } from "./labeler-server.js";
import { startJetstream, currentStats } from "./jetstream.js";

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

app.get("/", (_req, res) => res.send("CrossNote Labeler is running"));
app.get("/stats", (_req, res) => res.json(currentStats));

server.on("upgrade", (req, socket, head) => labeler.handleUpgrade(req, socket, head));

server.listen(PORT, "0.0.0.0", async () => {
  await labeler.loadKey(LABELER_SIGNING_KEY!);
  console.log(`Listening on port ${PORT}`);
  startJetstream(labeler);
});
