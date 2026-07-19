/**
 * digest-worker — Cloudflare Worker
 *
 * Fully self-contained: owns the email digest feature end to end. Nothing
 * else in the rebuilt pipeline knows or cares how this works internally —
 * queue-worker just calls POST /accumulate for "email" type destinations
 * and forgets about them.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * TWO SEPARATE JOBS IN ONE WORKER
 * ─────────────────────────────────────────────────────────────────────────
 * 1. ACCUMULATION (POST /accumulate, called via Service Binding from
 *    queue-worker — NOT a queue consumer, see note below): each call is one
 *    lead routed to an "email" destination. Appends it into a per-dealer/
 *    branch bucket in EMAIL_DIGEST KV (key "digest:{dealerKey}:{branchCode}").
 *    The read-modify-write race this bucket is exposed to (see next
 *    paragraph) is UNCHANGED by moving off queues — do not parallelize
 *    calls into this route without addressing that properly first.
 * 2. CRON + HTTP (twice daily, 06:00 & 12:00 UTC = 08:00 & 14:00 SAST):
 *    for every dealer/branch with an "email" destination and a non-empty
 *    bucket, generates a random link token, stores { password, bucket }
 *    under "digest-link:{dealerKey}:{branchCode}:{token}" (20h TTL —
 *    comfortably covers the up-to-18h gap between the two daily runs),
 *    emails a link via Resend, and clears the bucket on success only. The
 *    link goes to /digest/view — NOT an attachment. Visiting it prompts
 *    for the branch code; correct code issues a 15-min session token, then
 *    shows an HTML table + a "Download as Excel" button that builds the
 *    .xlsx on-demand at /digest/download.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * NO LONGER A QUEUE CONSUMER — converted to a Service Binding target
 * ─────────────────────────────────────────────────────────────────────────
 * Originally consumed digest-accumulate-queue with max_concurrency: 1 to
 * guard the KV read-modify-write race above. Converted to a direct
 * Service Binding call (queue-worker → env.DIGEST_WORKER.fetch(...))
 * because Cloudflare Queues costs ~3 operations per message with a
 * 10k/day budget on the free plan — per-lead traffic through queues at
 * every hand-off in this pipeline blew well past that. See cron-worker's
 * file header "QUEUES OPERATIONS BUDGET" note for the full numbers. The
 * race-condition risk from concurrent writers is UNCHANGED by this move —
 * it's mitigated today only because queue-worker calls this route
 * sequentially per lead, not because Service Bindings are inherently safe
 * against it. If any upstream caller ever starts firing concurrent calls
 * here, the race returns.
 *
 * WHY A LINK INSTEAD OF AN ENCRYPTED ATTACHMENT: earlier versions attached
 * a password-protected ZIP directly. Real problems in practice — Windows'
 * built-in zip extractor can't open AES-encrypted zips at all (errors out
 * instead of prompting for a password), legacy ZipCrypto is
 * cryptographically weak, and many corporate email gateways auto-quarantine
 * password-protected zip attachments outright since scanners can't inspect
 * encrypted content. A server-side branch-code check sidesteps all of it.
 *
 * REQUIRED wrangler.toml:
 *   [vars] WORKER_BASE_URL = "https://digest-worker.<subdomain>.workers.dev"
 *     (or a custom domain) — needed to build the /digest/view link, since a
 *     cron invocation has no incoming request to derive the host from.
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CONFIG"  (find dealers/branches
 *     with an email destination + their branchCode as the access code)
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"   (mark cacheKey done
 *     after accumulation, matching every other destination's behaviour)
 *   [[kv_namespaces]] binding = "EMAIL_DIGEST"        (buckets, links, sessions)
 *   [triggers] crons = ["0 6,12 * * *"]
 *   RESEND_API_KEY set as a secret (dashboard → Settings → Variables and
 *   Secrets). Optionally ALERT_FROM_EMAIL the same way (defaults to
 *   "leads@findndrive.co.za" if unset).
 *
 * CALL CONTRACT — POST /accumulate (from queue-worker, via Service Binding):
 *   { dealerKey, branchCode, intent, lead, cacheKey }
 *   — no `dest` needed here at all; appendToDigest never used it even in
 *     the old combined worker. recipientEmail is looked up fresh from
 *     LEADS_SYNC_CONFIG at send time instead, not carried on the call.
 */

import crypto from "node:crypto";
import * as XLSX from "xlsx";

const SHARED_CREDENTIALS_KEY = "__shared_credentials__"; // present in LEADS_SYNC_CONFIG but irrelevant here — just skipped when listing.
const DONE_MARKER_TTL = 604800;           // 7 days — matches every other destination's dedup marker TTL.
const DIGEST_LINK_TTL_SECONDS = 72000;    // 20 hours — comfortably covers the up-to-18h gap between the two daily digest runs.
const DIGEST_SESSION_TTL_SECONDS = 900;   // 15 minutes — short-lived, only needed to bridge "code verified" → "file downloaded".
const DIGEST_CRON = "0 6,12 * * *";       // must match wrangler.toml's crons array exactly.

export default {
  async scheduled(event, env, ctx) {
    await runEmailDigests(env);
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/accumulate" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
        await processAccumulateMessage(body, env);
        return new Response("OK", { status: 200 });
      } catch (err) {
        const { dealerKey, branchCode, cacheKey } = body || {};
        const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
        console.error(`❌ [digest:accumulate] Failed for ${label}: ${err.message}. Cache key: ${cacheKey}.`);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }
    if (path === "/run-digest") {
      await runEmailDigests(env);
      return new Response("Digest run complete", { status: 200 });
    }
    if (path === "/digest/view") {
      if (request.method === "POST") return handleDigestViewSubmit(request, env);
      return handleDigestViewForm(url, env);
    }
    if (path === "/digest/download") {
      return handleDigestDownload(url, env);
    }
    return new Response("digest-worker", { status: 200 });
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// ACCUMULATION — called via Service Binding from queue-worker (POST /accumulate)
// ═══════════════════════════════════════════════════════════════════════════
//
// NO LONGER A QUEUE CONSUMER — see cron-worker's file header "QUEUES
// OPERATIONS BUDGET" note for why every per-lead hand-off in this pipeline
// moved from Queues to direct Service Binding calls. The max_concurrency: 1
// requirement below is UNCHANGED and just as critical as before — moving
// off queues doesn't remove the underlying read-modify-write race on
// EMAIL_DIGEST, it just means Cloudflare's Queues-specific concurrency
// scaling isn't the mechanism to worry about anymore. Since Service
// Binding calls from queue-worker happen once per lead, sequentially
// within queue-worker's own per-lead loop (which itself is called
// sequentially by cron-worker per lead within a branch), concurrent calls
// to THIS route should be naturally rare — but if queue-worker or
// cron-worker's call pattern ever changes to fire concurrently, this race
// returns. Worth real caution before parallelizing anything upstream of
// this endpoint.

async function processAccumulateMessage(msg, env) {
  const { dealerKey, branchCode, intent, lead, cacheKey } = msg;
  await appendToDigest(env, dealerKey, branchCode, intent, lead);
  await env.LEADS_SYNC_CACHE.put(cacheKey, "1", { expirationTtl: DONE_MARKER_TTL });
}

// KNOWN LIMITATION: read-modify-write on a single KV key, not atomic — see
// file header. Mitigated by max_concurrency = 1 on digest-accumulate-queue
// — do not remove that without addressing this properly (e.g. moving
// accumulation to D1, which supports real transactions).
async function appendToDigest(env, dealerKey, branchCode, intent, lead) {
  const key = `digest:${dealerKey}:${branchCode || "default"}`;
  let bucket;
  try {
    const raw = await env.EMAIL_DIGEST.get(key);
    bucket = raw ? JSON.parse(raw) : { highIntent: [], lowIntent: [] };
  } catch {
    bucket = { highIntent: [], lowIntent: [] };
  }
  const list = intent === "highIntent" ? bucket.highIntent : bucket.lowIntent;
  list.push(summarizeLeadForDigest(lead));
  await env.EMAIL_DIGEST.put(key, JSON.stringify(bucket));
  // NOTE: deliberately no per-append log — was one Workers Logs event per
  // lead accumulated (96 lines during one real backlog). Final bucket size
  // is shown once at send time via [digest:branch] below.
}

function summarizeLeadForDigest(lead) {
  return {
    firstName: lead.firstName || "",
    lastName: lead.lastName || "",
    mobileNumber: lead.mobileNumber || "",
    idNumber: lead.idNumber || "",
    emailAddress: lead.emailAddress || "",
    netIncome: lead.netIncome || "",
    estimatedAmount: lead.estimatedAmount || "",
    approvalChance: lead.approvalChance || "",
    vehicleMake: lead.vehicleMake || "",
    vehicleModel: lead.vehicleModel || "",
    vehicleCondition: lead.vehicleCondition || "",
    date: lead.date || "",
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// CRON — twice-daily digest send
// ═══════════════════════════════════════════════════════════════════════════

function getEffectiveBranches(dealer) {
  if (Array.isArray(dealer.branches) && dealer.branches.length > 0) {
    return dealer.branches;
  }
  if (Array.isArray(dealer.destinations) && dealer.destinations.length > 0) {
    return [{
      branchCode: null,
      seritiDealershipId: dealer.seritiDealershipId,
      destinations: dealer.destinations,
    }];
  }
  return [];
}

async function runEmailDigests(env) {
  console.log("📧 [digest:run] Email digest run starting...");

  const workerBaseUrl = env.WORKER_BASE_URL || "https://digest-worker.findndrive.co.za";

  const { keys } = await env.LEADS_SYNC_CONFIG.list();
  const dealerKeys = keys.filter(({ name }) => name !== SHARED_CREDENTIALS_KEY);
  console.log(`[digest:run] Found ${dealerKeys.length} dealer config key(s) in LEADS_SYNC_CONFIG.`);

  let branchesWithEmailDest = 0;

  for (const { name } of dealerKeys) {
    const raw = await env.LEADS_SYNC_CONFIG.get(name);
    if (!raw) continue;

    let dealer;
    try {
      dealer = JSON.parse(raw);
    } catch {
      console.error(`[digest:run] ❌ Invalid JSON for dealer config: ${name} — skipping.`);
      continue;
    }

    const branches = getEffectiveBranches(dealer);

    for (const branch of branches) {
      const emailDest = (branch.destinations || []).find(d => d.type === "email");
      if (!emailDest) continue;

      branchesWithEmailDest++;

      const password = branch.branchCode || dealer.branchCode;
      const label = branch.branchCode ? `${dealer.key} [${branch.branchCode}]` : dealer.key;

      if (!password) {
        console.error(`❌ [digest] ${label} has an email destination but no branchCode to gate the link with — skipping. Add "branchCode" to this dealer's LEADS_SYNC_CONFIG entry.`);
        continue;
      }

      await sendDigestForBranch(env, dealer.key, branch.branchCode, emailDest, password, workerBaseUrl);
    }
  }

  console.log(`[digest:run] Checked all dealers — ${branchesWithEmailDest} branch(es) had an "email" destination configured.`);
  console.log("✅ [digest:run] Email digest run complete.");
}

async function sendDigestForBranch(env, dealerKey, branchCode, emailDest, password, workerBaseUrl) {
  const digestKey = `digest:${dealerKey}:${branchCode || "default"}`;
  const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;

  const raw = await env.EMAIL_DIGEST.get(digestKey);
  const bucket = raw ? JSON.parse(raw) : { highIntent: [], lowIntent: [] };
  const totalLeads = bucket.highIntent.length + bucket.lowIntent.length;

  console.log(`[digest:branch] ${label} — bucket key "${digestKey}" — ${totalLeads} lead(s) (${bucket.highIntent.length} high, ${bucket.lowIntent.length} low). Recipient: ${emailDest.recipientEmail || "MISSING"}. Password set: ${password ? "yes" : "NO"}.`);

  if (totalLeads === 0) {
    console.log(`ℹ️  [digest] ${label} — no new leads since last digest, skipping send.`);
    return;
  }

  try {
    const token = crypto.randomUUID();
    const linkKey = `digest-link:${dealerKey}:${branchCode || "default"}:${token}`;
    await env.EMAIL_DIGEST.put(linkKey, JSON.stringify({ password, bucket }), {
      expirationTtl: DIGEST_LINK_TTL_SECONDS,
    });

    const viewUrl = `${workerBaseUrl}/digest/view?d=${encodeURIComponent(dealerKey)}&b=${encodeURIComponent(branchCode || "")}&t=${encodeURIComponent(token)}`;

    await sendDigestEmail(env, emailDest.recipientEmail, dealerKey, branchCode, viewUrl, totalLeads);
    await env.EMAIL_DIGEST.delete(digestKey);
    console.log(`✅ [digest] Sent link for ${totalLeads} lead(s) to ${emailDest.recipientEmail} for ${label}.`);
  } catch (err) {
    console.error(`❌ [digest] Failed to send for ${label}: ${err.message}`);
    // Deliberately leave the accumulation bucket intact — the NEXT digest
    // run will retry with these plus anything accumulated in between.
  }
}

async function sendDigestEmail(env, recipientEmail, dealerKey, branchCode, viewUrl, leadCount) {
  const label = branchCode || dealerKey;

  if (!env.RESEND_API_KEY) {
    throw new Error("RESEND_API_KEY is not set on this Worker (dashboard → digest-worker → Settings → Variables and Secrets). Cannot send digest email.");
  }
  if (!recipientEmail) {
    throw new Error(`No recipientEmail configured for ${label}'s email destination.`);
  }

  console.log(`[digest:email] Sending digest link for ${label} (${leadCount} lead(s)) to ${recipientEmail}.`);

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: env.ALERT_FROM_EMAIL || "leads@findndrive.co.za",
      to: [recipientEmail],
      subject: `E-fficient Finance Widget — ${leadCount} new lead${leadCount === 1 ? "" : "s"}`,
      html: `<p>You have ${leadCount} new lead${leadCount === 1 ? "" : "s"} for ${escapeHtml(label)}.</p>` +
            `<p><a href="${viewUrl}">View your leads here</a> — you will need your Seriti branch code to gain access.</p>` +
            `<p style="color:#9ca3af;font-size:12px;">This link stays active until the next digest is sent.</p>`,
    }),
  });

  const result = await res.json().catch(() => ({}));
  console.log(`[digest:email] Resend responded ${res.status}: ${JSON.stringify(result)}`);

  if (!res.ok) {
    throw new Error(`Resend digest email failed: ${res.status} — ${JSON.stringify(result)}`);
  }

  console.log(`[digest:email] ✅ Accepted by Resend — id: ${result.id ?? "unknown"}`);
  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// HTTP — /digest/view (branch-code gate + table) and /digest/download
// ═══════════════════════════════════════════════════════════════════════════

function buildDigestWorkbook(bucket) {
  console.log(`[digest:xlsx] Building workbook — ${bucket.highIntent.length} high-intent, ${bucket.lowIntent.length} low-intent row(s).`);

  const wb = XLSX.utils.book_new();
  const highSheet = XLSX.utils.json_to_sheet(bucket.highIntent.map(formatDigestRow));
  XLSX.utils.book_append_sheet(wb, highSheet, "High Intent");
  const lowSheet = XLSX.utils.json_to_sheet(bucket.lowIntent.map(formatDigestRow));
  XLSX.utils.book_append_sheet(wb, lowSheet, "Low Intent");

  const out = XLSX.write(wb, { type: "array", bookType: "xlsx" });
  const bytes = new Uint8Array(out);

  console.log(`[digest:xlsx] Workbook built — ${bytes.length} byte(s).`);
  return bytes;
}

function formatDigestRow(lead) {
  return {
    "First Name": lead.firstName,
    "Last Name": lead.lastName,
    "Mobile": lead.mobileNumber,
    "ID Number": lead.idNumber,
    "Email": lead.emailAddress,
    "Net Income": lead.netIncome,
    "Estimated Amount": lead.estimatedAmount,
    "Approval Chance": lead.approvalChance,
    "Vehicle Make": lead.vehicleMake,
    "Vehicle Model": lead.vehicleModel,
    "Condition": lead.vehicleCondition,
    "Date": lead.date,
  };
}

function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function digestPageShell(bodyHtml) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lead Digest</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f9fafb; color: #111827; margin: 0; padding: 2rem 1rem; }
  .container { max-width: 900px; margin: 0 auto; }
  h1 { font-size: 20px; margin-bottom: 4px; }
  p.sub { color: #6b7280; font-size: 14px; margin-top: 0; }
  .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; }
  input[type=text], input[type=password] { width: 100%; height: 40px; padding: 0 12px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 14px; box-sizing: border-box; }
  label { display: block; font-size: 13px; font-weight: 500; color: #374151; margin-bottom: 6px; }
  button, .btn { display: inline-block; background: #6C3FC5; color: white; border: none; border-radius: 8px; padding: 10px 20px; font-size: 14px; font-weight: 600; cursor: pointer; text-decoration: none; margin-top: 12px; }
  button:hover, .btn:hover { background: #5a34a8; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #f3f4f6; white-space: nowrap; }
  th { color: #6b7280; font-weight: 600; font-size: 11px; text-transform: uppercase; }
  .error { background: #fef2f2; color: #dc2626; border: 1px solid #fecaca; border-radius: 8px; padding: 10px 14px; font-size: 13px; margin-bottom: 14px; }
  .section-title { font-size: 14px; font-weight: 700; margin: 1.5rem 0 6px; }
  .empty { color: #9ca3af; font-size: 13px; padding: 8px 0; }
  .table-wrap { overflow-x: auto; }
</style>
</head>
<body>
<div class="container">${bodyHtml}</div>
</body>
</html>`;
}

function renderLeadTable(title, leads) {
  if (!leads.length) {
    return `<div class="section-title">${escapeHtml(title)}</div><div class="empty">No leads.</div>`;
  }
  const rows = leads.map(l => `
    <tr>
      <td>${escapeHtml(l.firstName)} ${escapeHtml(l.lastName)}</td>
      <td>${escapeHtml(l.mobileNumber)}</td>
      <td>${escapeHtml(l.idNumber)}</td>
      <td>${escapeHtml(l.emailAddress)}</td>
      <td>${escapeHtml(l.netIncome)}</td>
      <td>${escapeHtml(l.estimatedAmount)}</td>
      <td>${escapeHtml(l.approvalChance)}</td>
      <td>${escapeHtml(l.vehicleMake)} ${escapeHtml(l.vehicleModel)}</td>
      <td>${escapeHtml(l.vehicleCondition)}</td>
      <td>${escapeHtml(l.date)}</td>
    </tr>`).join("");

  return `<div class="section-title">${escapeHtml(title)} (${leads.length})</div>
  <div class="table-wrap">
  <table>
    <thead><tr>
      <th>Name</th><th>Mobile</th><th>ID Number</th><th>Email</th><th>Net Income</th>
      <th>Est. Amount</th><th>Predicted Approval</th><th>Vehicle</th><th>Condition</th><th>Date</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>
  </div>`;
}

async function handleDigestViewForm(url, env) {
  const d = url.searchParams.get("d") || "";
  const b = url.searchParams.get("b") || "";
  const t = url.searchParams.get("t") || "";

  if (!d || !t) {
    return new Response(digestPageShell(`<div class="card"><div class="error">Invalid or incomplete link.</div></div>`), {
      status: 400,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const linkKey = `digest-link:${d}:${b || "default"}:${t}`;
  const raw = await env.EMAIL_DIGEST.get(linkKey);
  if (!raw) {
    return new Response(digestPageShell(`<div class="card"><div class="error">This digest link has expired or was already used. Wait for the next digest email or contact support.</div></div>`), {
      status: 410,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const body = `
    <h1>Lead Digest</h1>
    <p class="sub">${escapeHtml(b || d)}</p>
    <div class="card">
      <form method="POST" action="/digest/view">
        <input type="hidden" name="d" value="${escapeHtml(d)}">
        <input type="hidden" name="b" value="${escapeHtml(b)}">
        <input type="hidden" name="t" value="${escapeHtml(t)}">
        <label for="code">Enter your branch code</label>
        <input type="password" id="code" name="code" autocomplete="off" required>
        <button type="submit">View leads</button>
      </form>
    </div>`;

  return new Response(digestPageShell(body), {
    status: 200,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

async function handleDigestViewSubmit(request, env) {
  const form = await request.formData();
  const d = form.get("d") || "";
  const b = form.get("b") || "";
  const t = form.get("t") || "";
  const code = form.get("code") || "";

  const linkKey = `digest-link:${d}:${b || "default"}:${t}`;
  const raw = await env.EMAIL_DIGEST.get(linkKey);

  if (!raw) {
    return new Response(digestPageShell(`<div class="card"><div class="error">This digest link has expired. Wait for the next digest email or contact support.</div></div>`), {
      status: 410,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const record = JSON.parse(raw); // { password, bucket }

  if (code !== record.password) {
    const retryUrl = `/digest/view?d=${encodeURIComponent(d)}&b=${encodeURIComponent(b)}&t=${encodeURIComponent(t)}`;
    return new Response(digestPageShell(`
      <h1>Lead Digest</h1>
      <div class="card">
        <div class="error">Incorrect branch code. Try again.</div>
        <a class="btn" href="${retryUrl}">Back</a>
      </div>`), {
      status: 403,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const sessionToken = crypto.randomUUID();
  await env.EMAIL_DIGEST.put(`digest-session:${sessionToken}`, JSON.stringify({ d, b, t }), {
    expirationTtl: DIGEST_SESSION_TTL_SECONDS,
  });

  const totalLeads = record.bucket.highIntent.length + record.bucket.lowIntent.length;
  const downloadUrl = `/digest/download?s=${sessionToken}`;

  const body = `
    <h1>Lead Digest</h1>
    <p class="sub">${escapeHtml(b || d)} — ${totalLeads} lead${totalLeads === 1 ? "" : "s"}</p>
    <div class="card">
      <a class="btn" href="${downloadUrl}">Download as Excel (.xlsx)</a>
      <p style="font-size:12px;color:#9ca3af;margin-top:10px;">This download link stays active for ${Math.round(DIGEST_SESSION_TTL_SECONDS / 60)} minutes. Refresh and re-enter your code if it expires.</p>
      ${renderLeadTable("High Intent", record.bucket.highIntent)}
      ${renderLeadTable("Low Intent", record.bucket.lowIntent)}
    </div>`;

  console.log(`[digest:view] ${d}${b ? ` [${b}]` : ""} — branch code verified, showing ${totalLeads} lead(s).`);

  return new Response(digestPageShell(body), {
    status: 200,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

async function handleDigestDownload(url, env) {
  const s = url.searchParams.get("s") || "";
  const sessionRaw = s ? await env.EMAIL_DIGEST.get(`digest-session:${s}`) : null;

  if (!sessionRaw) {
    return new Response(digestPageShell(`
      <h1>Lead Digest</h1>
      <div class="card">
        <div class="error">Your download session has expired.</div>
        <p style="font-size:14px;color:#374151;">Open the original link from your digest email and re-enter your branch code to view your leads again.</p>
      </div>`), {
      status: 410,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const { d, b, t } = JSON.parse(sessionRaw);
  const linkKey = `digest-link:${d}:${b || "default"}:${t}`;
  const raw = await env.EMAIL_DIGEST.get(linkKey);

  if (!raw) {
    return new Response(digestPageShell(`
      <h1>Lead Digest</h1>
      <div class="card">
        <div class="error">This digest has expired.</div>
        <p style="font-size:14px;color:#374151;">Open the original link from your digest email and re-enter your branch code to view your leads again. If it's also expired, wait for the next digest email.</p>
      </div>`), {
      status: 410,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }

  const record = JSON.parse(raw);
  const bytes = buildDigestWorkbook(record.bucket);
  const filename = `leads-${b || d}-${new Date().toISOString().slice(0, 10)}.xlsx`;

  console.log(`[digest:download] ${d}${b ? ` [${b}]` : ""} — serving ${bytes.length} byte(s) as "${filename}".`);

  return new Response(bytes, {
    status: 200,
    headers: {
      "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "Content-Disposition": `attachment; filename="${filename}"`,
    },
  });
}
