import { WebSocketServer, WebSocket } from "ws";
import { Secp256k1Keypair } from "@atproto/crypto";
import * as cbor from "@ipld/dag-cbor";

export class LabelerServer {
  private wss: WebSocketServer;
  private keypair: Secp256k1Keypair | null = null;
  private did: string;

  constructor(did: string) {
    this.did = did;
    this.wss = new WebSocketServer({ noServer: true });
    this.wss.on("connection", () => console.log("Client subscribed to label stream"));
  }

  async loadKey(hexKey: string): Promise<void> {
    const clean = hexKey.startsWith("0x") ? hexKey.slice(2) : hexKey;
    this.keypair = await Secp256k1Keypair.import(Buffer.from(clean, "hex"));
    console.log("Signing key loaded");
  }

  handleUpgrade(request: any, socket: any, head: any): void {
    if (request.url?.startsWith("/xrpc/com.atproto.label.subscribeLabels")) {
      this.wss.handleUpgrade(request, socket, head, (ws) => {
        this.wss.emit("connection", ws, request);
      });
    } else {
      socket.destroy();
    }
  }

  async emitLabel(uri: string, val: string, cid?: string): Promise<void> {
    if (!this.keypair) throw new Error("Signing key not loaded");

    const label = {
      ver: 1,
      src: this.did,
      uri,
      val,
      neg: false,
      cts: new Date().toISOString(),
      ...(cid && { cid }),
    };

    const sig = await this.keypair.sign(cbor.encode(label));

    // AT Protocol wire format: two concatenated CBOR frames
    const frame = Buffer.concat([
      Buffer.from(cbor.encode({ t: "#labels", op: 1 })),
      Buffer.from(cbor.encode({ seq: Date.now(), labels: [{ ...label, sig }] })),
    ]);

    for (const client of this.wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(frame);
      }
    }
  }
}
