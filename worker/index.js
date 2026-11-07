export class WorkflowStatusDO {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.stepStatuses = new Map();
    this.currentStep = null;
    this.workflowStatus = "running";
    ctx.blockConcurrencyWhile(async () => {
      const storedStatuses = await ctx.storage.get("stepStatuses");
      const storedCurrent = await ctx.storage.get("currentStep");
      const storedWorkflowStatus = await ctx.storage.get("workflowStatus");
      if (storedStatuses) {
        this.stepStatuses = new Map(Object.entries(storedStatuses));
      } else {
        const steps = ["process data", "wait 2 seconds", "wait for approval", "final"];
        steps.forEach((s) => this.stepStatuses.set(s, "pending"));
      }
      this.currentStep = storedCurrent ?? null;
      this.workflowStatus = storedWorkflowStatus ?? "running";
    });
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.send(JSON.stringify(this.getStateMessage()));
      return new Response(null, { status: 101, webSocket: client });
    }
    return new Response("Expected WebSocket", { status: 400 });
  }

  async updateStep(stepName, status) {
    this.stepStatuses.set(stepName, status);
    if (status === "running" || status === "waiting") {
      this.currentStep = stepName;
    }
    const allCompleted = Array.from(this.stepStatuses.values()).every((s) => s === "completed");
    if (allCompleted) {
      this.workflowStatus = "completed";
      this.currentStep = null;
    }
    await this.ctx.storage.put("stepStatuses", Object.fromEntries(this.stepStatuses));
    await this.ctx.storage.put("currentStep", this.currentStep);
    await this.ctx.storage.put("workflowStatus", this.workflowStatus);
    this.broadcast(this.getStateMessage());
  }

  async webSocketMessage(ws, _message) {
    ws.send(JSON.stringify(this.getStateMessage()));
  }

  async webSocketClose(ws, code, reason, _wasClean) {
    ws.close(code, reason);
  }

  broadcast(message) {
    const sockets = this.ctx.getWebSockets();
    const json = JSON.stringify(message);
    for (const socket of sockets) {
      try {
        socket.send(json);
      } catch {}
    }
  }

  getStateMessage() {
    return {
      type: "workflow_update",
      currentStep: this.currentStep,
      stepStatuses: Object.fromEntries(this.stepStatuses),
      workflowStatus: this.workflowStatus,
      timestamp: Date.now()
    };
  }
}

import { WorkflowEntrypoint } from "cloudflare:workers";

export class MyWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const instanceId = event.instanceId;
    const notifyStep = async (stepName, status) => {
      try {
        const doId = this.env.WORKFLOW_STATUS.idFromName(instanceId);
        const stub = this.env.WORKFLOW_STATUS.get(doId);
        await stub.updateStep(stepName, status);
      } catch {}
    };

    await notifyStep("process data", "running");
    const result = await step.do("process data", async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      return { processed: true, timestamp: Date.now() };
    });
    await notifyStep("process data", "completed");

    await notifyStep("wait 2 seconds", "running");
    await step.sleep("wait 2 seconds", "2 seconds");
    await notifyStep("wait 2 seconds", "completed");

    await notifyStep("wait for approval", "waiting");
    const approval = await step.waitForEvent("wait for approval", {
      type: "user-approval",
      timeout: "60 minutes"
    });
    await notifyStep("wait for approval", "completed");

    await notifyStep("final", "running");
    await step.do("final", async () => {
      console.log("Results:", { result, approval: approval.payload });
      await new Promise((resolve) => setTimeout(resolve, 1000));
    });
    await notifyStep("final", "completed");
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS Proxy / Relay for FPL API (?url=...)
    const targetUrl = url.searchParams.get("url");
    if (targetUrl) {
      try {
        const response = await fetch(targetUrl, {
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
          }
        });
        const data = await response.text();
        return new Response(data, {
          status: response.status,
          headers: {
            "Content-Type": response.headers.get("Content-Type") || "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "*"
          }
        });
      } catch (err) {
        return Response.json(
          { error: "Proxy fetch failed", details: err?.message || String(err) },
          { status: 500, headers: { "Access-Control-Allow-Origin": "*" } }
        );
      }
    }

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "*"
        }
      });
    }

    if (url.pathname === "/api/workflow/start" && request.method === "POST") {
      try {
        const instance = await env.MY_WORKFLOW.create({
          params: { timestamp: Date.now() }
        });
        return Response.json({
          instanceId: instance.id,
          message: "Workflow started successfully"
        }, {
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      } catch {
        return Response.json(
          { error: "Failed to start workflow" },
          { status: 500, headers: { "Access-Control-Allow-Origin": "*" } }
        );
      }
    }

    if (url.pathname.startsWith("/api/workflow/status/")) {
      const instanceId = url.pathname.split("/").pop();
      if (!instanceId) {
        return Response.json({ error: "Instance ID required" }, { status: 400 });
      }
      try {
        const instance = await env.MY_WORKFLOW.get(instanceId);
        const status = await instance.status();
        return Response.json(status, {
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      } catch {
        return Response.json(
          { error: "Failed to get workflow status" },
          { status: 500, headers: { "Access-Control-Allow-Origin": "*" } }
        );
      }
    }

    if (url.pathname.startsWith("/api/workflow/event/") && request.method === "POST") {
      const instanceId = url.pathname.split("/").pop();
      if (!instanceId) {
        return Response.json({ error: "Instance ID required" }, { status: 400 });
      }
      try {
        const body = await request.json();
        const instance = await env.MY_WORKFLOW.get(instanceId);
        await instance.sendEvent({
          type: "user-approval",
          payload: body
        });
        return Response.json({ success: true, message: "Event sent successfully" }, {
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      } catch {
        return Response.json(
          { error: "Failed to send event" },
          { status: 500, headers: { "Access-Control-Allow-Origin": "*" } }
        );
      }
    }

    if (url.pathname === "/ws") {
      const instanceId = url.searchParams.get("instanceId");
      if (!instanceId) {
        return new Response("instanceId query parameter required", { status: 400 });
      }
      const upgradeHeader = request.headers.get("Upgrade");
      if (upgradeHeader !== "websocket") {
        return new Response("Expected Upgrade: websocket", { status: 426 });
      }
      try {
        const doId = env.WORKFLOW_STATUS.idFromName(instanceId);
        const stub = env.WORKFLOW_STATUS.get(doId);
        return stub.fetch(request);
      } catch {
        return new Response("Failed to establish WebSocket connection", { status: 500 });
      }
    }

    return Response.json({ error: "Not Found" }, { status: 404, headers: { "Access-Control-Allow-Origin": "*" } });
  }
};
