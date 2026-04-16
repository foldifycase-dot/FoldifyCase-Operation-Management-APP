/**
 * FoldifyCase Operation Management — Proxy v5
 * - COGS via GraphQL (reliable)
 * - Transaction fees calculated per payment gateway using user-configured rates
 * - Rates configurable via dashboard UI, stored in localStorage, sent as query params
 */
module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin",  "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { service, action, from, to } = req.query;

  // ── Diagnostic ───────────────────────────────────────────────────────────────
  if (!service) {
    return res.status(200).json({
      status: "FoldifyCase Proxy v5 OK",
      env: {
        SHOPIFY_STORE:   process.env.SHOPIFY_STORE   ? "✓ " + process.env.SHOPIFY_STORE : "✗ MISSING",
        SHOPIFY_TOKEN:   process.env.SHOPIFY_TOKEN   ? "✓ set" : "✗ MISSING",
        META_TOKEN:      process.env.META_ACCESS_TOKEN  ? "✓ set" : "✗ MISSING",
        META_AD_ACCOUNT: process.env.META_AD_ACCOUNT_ID ? "✓ " + process.env.META_AD_ACCOUNT_ID : "✗ MISSING",
      }
    });
  }

  // ── SHOPIFY ──────────────────────────────────────────────────────────────────
  if (service === "shopify") {
    const STORE = process.env.SHOPIFY_STORE;
    const TOKEN = process.env.SHOPIFY_TOKEN;
    if (!STORE || !TOKEN) {
      return res.status(500).json({
        error: "Missing Shopify env vars",
        SHOPIFY_STORE: STORE ? "✓" : "✗ MISSING",
        SHOPIFY_TOKEN: TOKEN ? "✓" : "✗ MISSING",
      });
    }

    const REST    = `https://${STORE}/admin/api/2024-10`;
    const GQL     = `https://${STORE}/admin/api/2024-10/graphql.json`;
    const HEADERS = { "X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json" };

    // ── DAILY SALES ──────────────────────────────────────────────────────────
    if (action === "daily-sales") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // --- Fee rates from query params (sent by dashboard from localStorage) ---
      // Each rate: pct = percentage (e.g. 2.9 means 2.9%), flat = flat fee per txn (e.g. 0.30)
      const feeRates = {
        "shopify payments": {
          pct:  parseFloat(req.query.fee_shopify_pct  ?? 1.75) / 100,
          flat: parseFloat(req.query.fee_shopify_flat ?? 0.30),
        },
        "stripe": {
          pct:  parseFloat(req.query.fee_stripe_pct   ?? 2.90) / 100,
          flat: parseFloat(req.query.fee_stripe_flat  ?? 0.30),
        },
        "paypal": {
          pct:  parseFloat(req.query.fee_paypal_pct   ?? 4.40) / 100,
          flat: parseFloat(req.query.fee_paypal_flat  ?? 0.30),
        },
        "klarna": {
          pct:  parseFloat(req.query.fee_klarna_pct   ?? 2.90) / 100,
          flat: parseFloat(req.query.fee_klarna_flat  ?? 0.30),
        },
        "afterpay": {
          pct:  parseFloat(req.query.fee_afterpay_pct  ?? 0.00) / 100,
          flat: parseFloat(req.query.fee_afterpay_flat ?? 0.00),
        },
        "manual": { pct: 0, flat: 0 },
        "other": {
          pct:  parseFloat(req.query.fee_other_pct   ?? 2.90) / 100,
          flat: parseFloat(req.query.fee_other_flat  ?? 0.30),
        },
      };

      const calcTxFee = (gateways, subtotal) => {
        // gateways is an array from payment_gateway_names
        const gateway = (gateways && gateways[0] || "other").toLowerCase();
        // Try exact match first, then partial match
        const key = Object.keys(feeRates).find(k => gateway.includes(k)) || "other";
        const rate = feeRates[key];
        return (subtotal * rate.pct) + rate.flat;
      };

      // Step 1: Store timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}
      console.log("[proxy] tz:", tz);

      // Step 2: UTC window
      const getOffsetMs = (dateStr) => {
        const probe  = new Date(dateStr + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU", {
          timeZone: tz, hour: "2-digit", hour12: false,
        }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();
      console.log(`[proxy] UTC: ${fromUTC} → ${toUTC}`);

      // Step 3: Fetch orders
      const params = new URLSearchParams({
        status:           "any",
        financial_status: "any",
        created_at_min:   fromUTC,
        created_at_max:   toUTC,
        limit:            "250",
        fields:           "id,created_at,total_price,subtotal_price,payment_gateway_names,total_shipping_price_set,shipping_lines,line_items,financial_status,cancelled_at",
      });

      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) {
          const body = await r.text();
          console.error("[proxy] orders HTTP", r.status, body.slice(0, 200));
          return res.status(502).json({
            error: `Shopify orders HTTP ${r.status}`,
            detail: body.slice(0, 200),
            hint: r.status === 401 ? "SHOPIFY_TOKEN invalid"
                : r.status === 404 ? "SHOPIFY_STORE domain wrong"
                : "Check Vercel runtime logs",
          });
        }
        const j     = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        console.log(`[proxy] batch ${batch.length}, total ${allOrders.length}`);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }

      // Step 4: Filter
      const orders = allOrders.filter(o =>
        !o.cancelled_at &&
        o.financial_status !== "refunded" &&
        o.financial_status !== "voided"
      );
      console.log(`[proxy] ${allOrders.length} raw → ${orders.length} kept`);

      // Step 5: COGS via GraphQL productVariant.inventoryItem.unitCost
      const variantIds = [...new Set(
        orders.flatMap(o =>
          (o.line_items || []).map(li => li.variant_id).filter(Boolean)
        )
      )];
      console.log(`[proxy] COGS: ${variantIds.length} variants`);

      const costMap = {};
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i + 50);
        const aliases = chunk.map((id, idx) =>
          `v${idx}: productVariant(id: "gid://shopify/ProductVariant/${id}") {
            id
            inventoryItem { unitCost { amount } }
          }`
        ).join("\n");
        try {
          const gr = await fetch(GQL, {
            method: "POST", headers: HEADERS,
            body: JSON.stringify({ query: `{ ${aliases} }` }),
          });
          if (!gr.ok) { console.warn(`[proxy] GQL HTTP ${gr.status} chunk ${i}`); continue; }
          const gj = await gr.json();
          if (gj.errors) console.warn(`[proxy] GQL errors chunk ${i}:`, JSON.stringify(gj.errors).slice(0, 200));
          const data = gj.data || {};
          chunk.forEach((variantId, idx) => {
            const node   = data[`v${idx}`];
            const amount = node?.inventoryItem?.unitCost?.amount;
            if (amount !== null && amount !== undefined) {
              costMap[variantId] = parseFloat(amount);
            }
          });
        } catch(e) { console.warn(`[proxy] GQL exception chunk ${i}:`, e.message); }
      }
      console.log(`[proxy] COGS: ${Object.keys(costMap).length}/${variantIds.length} variants mapped`);

      // Step 6: Group by store-local date
      const byDate = {};
      const round2 = n => Math.round(n * 100) / 100;

      orders.forEach(o => {
        const localDate = new Intl.DateTimeFormat("en-CA", {
          timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
        }).format(new Date(o.created_at));

        if (!byDate[localDate]) byDate[localDate] = {
          date: localDate, revenue: 0, subtotal: 0, orders: 0,
          cogs: 0, shipping_charged: 0, shipping_cost: 0,
          transaction_fees: 0, has_cogs: false,
        };
        const d        = byDate[localDate];
        const revTotal = parseFloat(o.total_price    || 0);
        const subTotal = parseFloat(o.subtotal_price || 0);

        d.revenue  += revTotal;
        d.subtotal += subTotal;
        d.orders   += 1;

        // Transaction fee: calculated per order using payment gateway + configured rates
        // Fee is on subtotal (excluding shipping) — gateways don't charge on shipping
        d.transaction_fees += calcTxFee(o.payment_gateway_names, subTotal);

        // COGS
        (o.line_items || []).forEach(li => {
          const cost = costMap[li.variant_id];
          if (cost !== undefined && cost > 0) {
            d.has_cogs = true;
            d.cogs += cost * (parseInt(li.quantity) || 1);
          }
        });

        // Shipping
        d.shipping_charged += parseFloat(o.total_shipping_price_set?.shop_money?.amount || 0);
        (o.shipping_lines || []).forEach(sl => {
          if (sl.source === "shopify-shipping" || sl.carrier_identifier) {
            d.shipping_cost += parseFloat(sl.price || 0);
          }
        });
      });

      // Step 7: Response
      const daily = Object.values(byDate)
        .filter(d => d.date >= from && d.date <= to)
        .map(d => ({
          date:             d.date,
          revenue:          round2(d.revenue),
          subtotal:         round2(d.subtotal),
          orders:           d.orders,
          cogs:             round2(d.cogs),
          shipping_charged: round2(d.shipping_charged),
          shipping_cost:    round2(d.shipping_cost),
          transaction_fees: round2(d.transaction_fees),
          has_cogs:         d.has_cogs,
          aov:              d.orders > 0 ? round2(d.revenue / d.orders) : 0,
        }))
        .sort((a, b) => a.date.localeCompare(b.date));

      const totals = daily.reduce((acc, d) => ({
        revenue:          round2(acc.revenue + d.revenue),
        subtotal:         round2(acc.subtotal + d.subtotal),
        orders:           acc.orders + d.orders,
        cogs:             round2(acc.cogs + d.cogs),
        shipping_charged: round2(acc.shipping_charged + d.shipping_charged),
        shipping_cost:    round2(acc.shipping_cost + d.shipping_cost),
        transaction_fees: round2(acc.transaction_fees + d.transaction_fees),
      }), { revenue:0, subtotal:0, orders:0, cogs:0, shipping_charged:0, shipping_cost:0, transaction_fees:0 });

      return res.status(200).json({
        daily, totals,
        has_cogs:     daily.some(d => d.has_cogs),
        total_orders: orders.length,
        fee_rates:    feeRates, // echo back so dashboard can verify
        debug: {
          store_timezone:  tz,
          utc_offset_hrs:  offsetMs / 3600000,
          raw_orders:      allOrders.length,
          kept_orders:     orders.length,
          variants_found:  variantIds.length,
          variants_costed: Object.keys(costMap).length,
          utc_window:      { from: fromUTC, to: toUTC },
        },
      });
    }

    if (action === "products") {
      const r = await fetch(`${REST}/products.json?limit=50&fields=id,title,variants,images,handle,tags`, { headers: HEADERS });
      return res.status(200).json(await r.json());
    }

    return res.status(400).json({ error: "Unknown shopify action", received: action });
  }

  // ── META ADS ─────────────────────────────────────────────────────────────────
  if (service === "meta-ads") {
    const TOKEN   = process.env.META_ACCESS_TOKEN;
    const ACCOUNT = process.env.META_AD_ACCOUNT_ID;
    if (!TOKEN || !ACCOUNT) return res.status(500).json({ error: "Meta not configured" });
    if (!from || !to) return res.status(400).json({ error: "from and to required" });

    const params = new URLSearchParams({
      fields: "spend,actions,action_values,impressions,clicks",
      time_increment: "1",
      time_range: JSON.stringify({ since: from, until: to }),
      level: "account",
      access_token: TOKEN,
    });
    const r    = await fetch(`https://graph.facebook.com/v21.0/${ACCOUNT}/insights?${params}`);
    const data = await r.json();
    if (data.error) return res.status(400).json({ error: "Meta API error", details: data.error });

    const round2 = n => Math.round(n * 100) / 100;
    const daily = (data.data || []).map(row => {
      const acts = row.actions || [], vals = row.action_values || [];
      const find = (arr, type) => parseFloat(arr.find(a => a.action_type === type)?.value || 0);
      const purchases = find(acts, "purchase");
      const purVal    = find(vals, "purchase");
      const spend     = parseFloat(row.spend || 0);
      return {
        date:        row.date_start,
        spend:       round2(spend),
        revenue:     round2(purVal),
        conversions: Math.round(purchases),
        clicks:      parseInt(row.clicks || 0),
        roas:        spend > 0 ? round2(purVal / spend) : 0,
      };
    }).sort((a, b) => a.date.localeCompare(b.date));

    const total = daily.reduce((acc, d) => ({
      spend:       round2(acc.spend + d.spend),
      revenue:     round2(acc.revenue + d.revenue),
      conversions: acc.conversions + d.conversions,
    }), { spend: 0, revenue: 0, conversions: 0 });
    total.roas = total.spend > 0 ? round2(total.revenue / total.spend) : 0;
    total.cpa  = total.conversions > 0 ? round2(total.spend / total.conversions) : 0;

    return res.status(200).json({ platform: "meta", daily, total });
  }

  // ── Stubs ─────────────────────────────────────────────────────────────────────
  if (service === "google-ads") return res.status(200).json({ platform: "google", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "ms-ads")     return res.status(200).json({ platform: "microsoft", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "blob")       return res.status(200).json({ ok: true });
  if (service === "alert")      return res.status(200).json({ ok: true });

  return res.status(400).json({ error: "Unknown service", received: service });
};
