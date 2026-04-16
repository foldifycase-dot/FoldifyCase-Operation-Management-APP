/**
 * FoldifyCase Operation Management — Proxy v4
 * Fixes: COGS via GraphQL (more reliable), tx fee on subtotal only
 */
module.exports = async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin",  "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { service, action, from, to } = req.query;

  // ── Diagnostic ───────────────────────────────────────────────────────────────
  if (!service) {
    return res.status(200).json({
      status:  "FoldifyCase Proxy v4 OK",
      env: {
        SHOPIFY_STORE:   process.env.SHOPIFY_STORE   ? "✓ " + process.env.SHOPIFY_STORE : "✗ MISSING",
        SHOPIFY_TOKEN:   process.env.SHOPIFY_TOKEN   ? "✓ set" : "✗ MISSING",
        META_TOKEN:      process.env.META_ACCESS_TOKEN    ? "✓ set" : "✗ MISSING",
        META_AD_ACCOUNT: process.env.META_AD_ACCOUNT_ID   ? "✓ " + process.env.META_AD_ACCOUNT_ID : "✗ MISSING",
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
    const TX_RATE = 0.057424; // 5.7424% blended: Shopify + PayPal + Afterpay

    // ── DAILY SALES ────────────────────────────────────────────────────────────
    if (action === "daily-sales") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // Step 1: Store timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) { console.warn("[proxy] tz fetch failed, using default"); }
      console.log("[proxy] timezone:", tz);

      // Step 2: UTC window — use Intl for reliable offset
      const getOffsetMs = (dateStr) => {
        const probe   = new Date(dateStr + "T12:00:00Z");
        const localH  = parseInt(new Intl.DateTimeFormat("en-AU", {
          timeZone: tz, hour: "2-digit", hour12: false
        }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();
      console.log(`[proxy] UTC window: ${fromUTC} → ${toUTC} (offset ${offsetMs/3600000}h)`);

      // Step 3: Fetch all orders via REST with pagination
      const params = new URLSearchParams({
        status:           "any",
        financial_status: "any",
        created_at_min:   fromUTC,
        created_at_max:   toUTC,
        limit:            "250",
        fields:           [
          "id","created_at","total_price","subtotal_price",
          "total_shipping_price_set","shipping_lines",
          "line_items","financial_status","cancelled_at"
        ].join(","),
      });

      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) {
          const body = await r.text();
          console.error("[proxy] orders HTTP", r.status, body.slice(0,200));
          return res.status(502).json({
            error:  `Shopify orders error HTTP ${r.status}`,
            detail: body.slice(0,200),
          });
        }
        const j     = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        console.log(`[proxy] fetched batch ${batch.length}, total ${allOrders.length}`);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }

      // Step 4: Filter cancelled/refunded/voided
      const orders = allOrders.filter(o =>
        !o.cancelled_at &&
        o.financial_status !== "refunded" &&
        o.financial_status !== "voided"
      );
      console.log(`[proxy] orders: ${allOrders.length} raw → ${orders.length} kept`);

      // Step 5: COGS via GraphQL productVariant.inventoryItem.unitCost
      // This is more reliable than REST variants + inventory_items chain
      const variantIds = [...new Set(
        orders.flatMap(o =>
          (o.line_items || [])
            .map(li => li.variant_id)
            .filter(id => id)
        )
      )];
      console.log(`[proxy] COGS: fetching costs for ${variantIds.length} variants`);

      const costMap = {}; // variantId (number) → cost (float)

      // GraphQL can fetch up to 50 nodes at once reliably
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i + 50);
        // Build GraphQL query for this chunk
        const aliases = chunk.map((id, idx) =>
          `v${idx}: productVariant(id: "gid://shopify/ProductVariant/${id}") {
            id
            inventoryItem { unitCost { amount } }
          }`
        ).join("\n");

        const gqlBody = JSON.stringify({ query: `{ ${aliases} }` });
        try {
          const gr = await fetch(GQL, {
            method:  "POST",
            headers: HEADERS,
            body:    gqlBody,
          });
          if (!gr.ok) {
            console.warn(`[proxy] COGS GraphQL HTTP ${gr.status} for chunk ${i}`);
            continue;
          }
          const gj = await gr.json();
          if (gj.errors) {
            console.warn(`[proxy] COGS GraphQL errors chunk ${i}:`, JSON.stringify(gj.errors).slice(0,200));
          }
          const data = gj.data || {};
          chunk.forEach((variantId, idx) => {
            const node = data[`v${idx}`];
            if (!node) return;
            const amount = node.inventoryItem?.unitCost?.amount;
            if (amount !== null && amount !== undefined) {
              costMap[variantId] = parseFloat(amount);
            }
          });
        } catch(e) {
          console.warn(`[proxy] COGS GraphQL exception chunk ${i}:`, e.message);
        }
      }
      console.log(`[proxy] COGS: mapped ${Object.keys(costMap).length}/${variantIds.length} variants`);

      // Step 6: Group orders by store-local date
      const byDate = {};
      orders.forEach(o => {
        // Store-local date for this order
        const localDate = new Intl.DateTimeFormat("en-CA", {
          timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
        }).format(new Date(o.created_at));

        if (!byDate[localDate]) byDate[localDate] = {
          date: localDate, revenue: 0, subtotal: 0, orders: 0,
          cogs: 0, shipping_charged: 0, shipping_cost: 0,
          transaction_fees: 0, has_cogs: false,
        };
        const d = byDate[localDate];

        const revTotal = parseFloat(o.total_price    || 0);
        const subTotal = parseFloat(o.subtotal_price || 0);

        d.revenue  += revTotal;
        d.subtotal += subTotal;
        d.orders   += 1;

        // TX fee on subtotal only (payment gateways don't charge on shipping)
        d.transaction_fees += subTotal * TX_RATE;

        // COGS from GraphQL cost map
        (o.line_items || []).forEach(li => {
          const cost = costMap[li.variant_id];
          if (cost !== undefined && cost > 0) {
            d.has_cogs = true;
            d.cogs += cost * (parseInt(li.quantity) || 1);
          }
        });

        // Shipping charged to customer (included in revenue)
        d.shipping_charged += parseFloat(
          o.total_shipping_price_set?.shop_money?.amount || 0
        );
        // Shipping label cost (what you paid)
        (o.shipping_lines || []).forEach(sl => {
          if (sl.source === "shopify-shipping" || sl.carrier_identifier) {
            d.shipping_cost += parseFloat(sl.price || 0);
          }
        });
      });

      // Step 7: Build response — only days in requested range
      const round2 = n => Math.round(n * 100) / 100;

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
        daily,
        totals,
        has_cogs:     daily.some(d => d.has_cogs),
        total_orders: orders.length,
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

    // Products
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
      fields:         "spend,actions,action_values,impressions,clicks",
      time_increment: "1",
      time_range:     JSON.stringify({ since: from, until: to }),
      level:          "account",
      access_token:   TOKEN,
    });
    const r    = await fetch(`https://graph.facebook.com/v21.0/${ACCOUNT}/insights?${params}`);
    const data = await r.json();
    if (data.error) return res.status(400).json({ error: "Meta API error", details: data.error });

    const daily = (data.data || []).map(row => {
      const acts = row.actions || [], vals = row.action_values || [];
      const find = (arr, type) => parseFloat(arr.find(a => a.action_type === type)?.value || 0);
      const purchases = find(acts, "purchase");
      const purVal    = find(vals, "purchase");
      const spend     = parseFloat(row.spend || 0);
      return {
        date:        row.date_start,
        spend:       Math.round(spend * 100) / 100,
        revenue:     Math.round(purVal * 100) / 100,
        conversions: Math.round(purchases),
        clicks:      parseInt(row.clicks || 0),
        roas:        spend > 0 ? Math.round(purVal / spend * 100) / 100 : 0,
      };
    }).sort((a, b) => a.date.localeCompare(b.date));

    const total = daily.reduce((acc, d) => ({
      spend:       Math.round((acc.spend + d.spend) * 100) / 100,
      revenue:     Math.round((acc.revenue + d.revenue) * 100) / 100,
      conversions: acc.conversions + d.conversions,
    }), { spend: 0, revenue: 0, conversions: 0 });
    total.roas = total.spend > 0 ? Math.round(total.revenue / total.spend * 100) / 100 : 0;
    total.cpa  = total.conversions > 0 ? Math.round(total.spend / total.conversions * 100) / 100 : 0;

    return res.status(200).json({ platform: "meta", daily, total });
  }

  // ── Stubs for other services ──────────────────────────────────────────────────
  if (service === "google-ads") return res.status(200).json({ platform: "google", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "ms-ads")     return res.status(200).json({ platform: "microsoft", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "blob")       return res.status(200).json({ ok: true });
  if (service === "alert")      return res.status(200).json({ ok: true });

  return res.status(400).json({ error: "Unknown service", received: service });
};
