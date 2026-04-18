/**
 * FoldifyCase — Transaction Fees from Shopify Payments Payout
 * Separate serverless function: api/tx-fees.js
 * Upload this to GitHub at: api/tx-fees.js
 * Requires: read_shopify_payments_payouts scope on your Shopify custom app
 */
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin",  "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  const STORE = process.env.SHOPIFY_STORE;
  const TOKEN = process.env.SHOPIFY_TOKEN;
  if (!STORE || !TOKEN) {
    return res.status(500).json({ error: "Missing SHOPIFY_STORE or SHOPIFY_TOKEN env vars" });
  }

  const REST    = `https://${STORE}/admin/api/2024-10`;
  const HEADERS = { "X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json" };
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: "from and to required" });

  const fees = {}; // orderId -> actual fee amount

  try {
    // Step 1: Get timezone
    let tz = "Australia/Melbourne";
    try {
      const tzRes = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
      const tzJson = await tzRes.json();
      if (tzJson.shop?.iana_timezone) tz = tzJson.shop.iana_timezone;
    } catch(e) {}

    // Step 2: Compute UTC window
    const probe   = new Date(from + "T12:00:00Z");
    const localH  = parseInt(new Intl.DateTimeFormat("en-AU", { timeZone: tz, hour: "2-digit", hour12: false }).format(probe));
    const offsetMs = (localH - 12) * 3600000;
    const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
    const toUTC    = new Date(new Date(to   + "T00:00:00Z").getTime() - offsetMs + 86399999).toISOString();

    // Step 3: Get orders with transactions (to map txn ID -> order ID)
    const txnToOrder = {};
    let url = `${REST}/orders.json?created_at_min=${fromUTC}&created_at_max=${toUTC}&status=any&limit=250&fields=id,transactions`;
    while (url) {
      const r = await fetch(url, { headers: HEADERS });
      if (!r.ok) break;
      const j = await r.json();
      (j.orders || []).forEach(o => {
        (o.transactions || []).forEach(t => {
          if (t.id) txnToOrder[String(t.id)] = String(o.id);
        });
      });
      // Pagination
      const link = r.headers.get("Link") || "";
      const nextPart = link.split(",").find(p => p.includes('rel="next"'));
      url = nextPart ? nextPart.match(/<([^>]+)>/)?.[1] : null;
    }

    // Step 4: Fetch actual Shopify Payments fees from balance transactions
    // source_id = order transaction ID -> look up order ID via txnToOrder
    const btRes = await fetch(
      `${REST}/shopify_payments/balance/transactions.json?transaction_type=charge&limit=250`,
      { headers: HEADERS }
    );

    if (!btRes.ok) {
      return res.status(200).json({
        fees: {},
        error: `Shopify Payments API returned ${btRes.status}. Ensure read_shopify_payments_payouts scope is enabled.`
      });
    }

    const btJson = await btRes.json();
    (btJson.transactions || []).forEach(t => {
      if (t.source_id && t.fee != null) {
        const orderId = txnToOrder[String(t.source_id)];
        if (orderId) {
          fees[orderId] = Math.round(((fees[orderId] || 0) + Math.abs(parseFloat(t.fee))) * 100) / 100;
        }
      }
    });

    return res.status(200).json({
      fees,
      count: Object.keys(fees).length,
      orders_mapped: Object.keys(txnToOrder).length,
    });

  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
};
