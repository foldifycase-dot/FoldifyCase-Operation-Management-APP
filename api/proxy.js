/**
 * FoldifyCase Operation Management — Proxy
 */

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

module.exports = async function handler(req, res) {
  // CORS preflight
  Object.entries(CORS).forEach(([k, v]) => res.setHeader(k, v));
  if (req.method === "OPTIONS") return res.status(200).end();

  const { service, action, from, to } = req.query;

  // ── Diagnostic ping ─────────────────────────────────────────────────────────
  if (!service) {
    return res.status(200).json({
      status:  "FoldifyCase Proxy OK",
      version: "2024-10",
      env: {
        SHOPIFY_STORE:   process.env.SHOPIFY_STORE  ? "✓ " + process.env.SHOPIFY_STORE : "✗ MISSING",
        SHOPIFY_TOKEN:   process.env.SHOPIFY_TOKEN  ? "✓ set" : "✗ MISSING",
        META_TOKEN:      process.env.META_ACCESS_TOKEN   ? "✓ set" : "✗ MISSING",
        META_AD_ACCOUNT: process.env.META_AD_ACCOUNT_ID  ? "✓ " + process.env.META_AD_ACCOUNT_ID : "✗ MISSING",
      }
    });
  }

  // ── Shopify ─────────────────────────────────────────────────────────────────
  if (service === "shopify") {
    const store = process.env.SHOPIFY_STORE;
    const token = process.env.SHOPIFY_TOKEN;

    if (!store || !token) {
      return res.status(500).json({
        error: "Missing env vars",
        SHOPIFY_STORE: store ? "✓ set" : "✗ MISSING",
        SHOPIFY_TOKEN: token ? "✓ set" : "✗ MISSING",
      });
    }

    if (action === "daily-sales") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      const base    = `https://${store}/admin/api/2024-10`;
      const headers = { "X-Shopify-Access-Token": token, "Content-Type": "application/json" };

      // ── Get store timezone ──────────────────────────────────────────────────
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${base}/shop.json?fields=iana_timezone`, { headers });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}
      console.log("[proxy] store tz:", tz);

      // ── Build UTC window using Intl ─────────────────────────────────────────
      // Get the UTC offset in ms for this timezone
      const getOffsetMs = (dateStr, timezone) => {
        // Format a known UTC time in the store timezone, compare hours
        const utcDate = new Date(dateStr + "T12:00:00Z");
        const localStr = new Intl.DateTimeFormat("en-AU", {
          timeZone: timezone,
          hour: "2-digit", hour12: false,
        }).format(utcDate);
        const localHour = parseInt(localStr);
        // localHour = 12 + offset, so offset = localHour - 12
        return (localHour - 12) * 60 * 60 * 1000;
      };

      const offsetMs = getOffsetMs(from, tz);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();
      console.log(`[proxy] ${from}→${to} | tz offset: ${offsetMs/3600000}h | UTC: ${fromUTC} → ${toUTC}`);

      // ── Fetch orders ────────────────────────────────────────────────────────
      const params = new URLSearchParams({
        status:           "any",
        financial_status: "any",
        created_at_min:   fromUTC,
        created_at_max:   toUTC,
        limit:            "250",
        fields:           "id,created_at,total_price,subtotal_price,total_shipping_price_set,shipping_lines,line_items,financial_status,cancelled_at",
      });

      let allOrders = [];
      let url = `${base}/orders.json?${params}`;

      while (url) {
        const r = await fetch(url, { headers });
        if (!r.ok) {
          const body = await r.text();
          console.error("[proxy] Shopify orders error:", r.status, body.slice(0,300));
          return res.status(502).json({
            error:  `Shopify returned HTTP ${r.status}`,
            detail: body.slice(0, 300),
            hint:   r.status === 401 ? "SHOPIFY_TOKEN is invalid or expired"
                  : r.status === 404 ? "SHOPIFY_STORE domain is wrong"
                  : "Check Vercel runtime logs",
          });
        }
        const j     = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        console.log(`[proxy] fetched ${batch.length} orders, total ${allOrders.length}`);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = next ? next[1] : null;
        if (batch.length < 250) break;
      }

      // ── Filter ──────────────────────────────────────────────────────────────
      const orders = allOrders.filter(o =>
        !o.cancelled_at &&
        o.financial_status !== "refunded" &&
        o.financial_status !== "voided"
      );
      console.log(`[proxy] ${allOrders.length} raw → ${orders.length} after filter`);

      // ── COGS ────────────────────────────────────────────────────────────────
      // Build unique variant ID list from all orders
      const variantIds = [...new Set(
        orders.flatMap(o => (o.line_items||[])
          .map(li => li.variant_id)
          .filter(id => id && id !== null)
        )
      )];
      console.log(`[proxy] COGS: ${variantIds.length} unique variants to price`);

      const costMap = {}; // variantId → cost
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i + 50);
        try {
          // Step 1: variant_id → inventory_item_id
          const vUrl = `${base}/variants.json?ids=${chunk.join(",")}&fields=id,inventory_item_id`;
          const vr   = await fetch(vUrl, { headers });
          if (!vr.ok) { console.warn(`[proxy] variants fetch HTTP ${vr.status}`); continue; }
          const { variants = [] } = await vr.json();
          console.log(`[proxy] COGS chunk ${i}: got ${variants.length} variants`);

          const invIds = variants.map(v => v.inventory_item_id).filter(Boolean);
          if (!invIds.length) continue;

          // Step 2: inventory_item_id → cost
          const iUrl = `${base}/inventory_items.json?ids=${invIds.join(",")}&fields=id,cost`;
          const ir   = await fetch(iUrl, { headers });
          if (!ir.ok) { console.warn(`[proxy] inventory_items fetch HTTP ${ir.status}`); continue; }
          const { inventory_items = [] } = await ir.json();
          console.log(`[proxy] COGS chunk ${i}: got ${inventory_items.length} inventory items`);

          // Build lookup: inventory_item_id → cost
          const invMap = {};
          inventory_items.forEach(item => {
            invMap[item.id] = parseFloat(item.cost || 0);
          });

          // Map variant_id → cost
          variants.forEach(v => {
            const cost = invMap[v.inventory_item_id];
            if (cost !== undefined) costMap[v.id] = cost;
          });
        } catch(e) {
          console.warn(`[proxy] COGS chunk ${i} error:`, e.message);
        }
      }
      console.log(`[proxy] COGS: mapped ${Object.keys(costMap).length} of ${variantIds.length} variants`);

      // ── Group by store-local date ───────────────────────────────────────────
      const byDate = {};
      const TX_RATE = 0.057424; // 5.7424% blended transaction fee

      orders.forEach(o => {
        // Convert order UTC time → store local date
        const localDate = new Intl.DateTimeFormat("en-CA", {
          timeZone: tz,
          year: "numeric", month: "2-digit", day: "2-digit",
        }).format(new Date(o.created_at));

        if (!byDate[localDate]) byDate[localDate] = {
          date: localDate, revenue: 0, orders: 0, cogs: 0,
          shipping_charged: 0, shipping_cost: 0, transaction_fees: 0, has_cogs: false,
        };

        const d        = byDate[localDate];
        const revTotal = parseFloat(o.total_price    || 0); // includes shipping
        const subTotal = parseFloat(o.subtotal_price || 0); // product only, excl. shipping

        d.revenue += revTotal;
        d.orders  += 1;

        // ── Transaction fee: apply rate to SUBTOTAL only (matches Shopify) ──
        // Shopify/PayPal/Afterpay don't charge fees on the shipping amount
        d.transaction_fees += subTotal * TX_RATE;

        // ── COGS: sum cost × qty for each line item ──────────────────────────
        (o.line_items || []).forEach(li => {
          const cost = costMap[li.variant_id];
          if (cost !== undefined && cost > 0) {
            d.has_cogs = true;
            d.cogs += cost * (li.quantity || 1);
          }
        });

        // ── Shipping ─────────────────────────────────────────────────────────
        d.shipping_charged += parseFloat(o.total_shipping_price_set?.shop_money?.amount || 0);
        (o.shipping_lines || []).forEach(sl => {
          if (sl.source === "shopify-shipping" || sl.carrier_identifier) {
            d.shipping_cost += parseFloat(sl.price || 0);
          }
        });
      });

      // ── Only return days within requested range ─────────────────────────────
      const daily = Object.values(byDate)
        .filter(d => d.date >= from && d.date <= to)
        .map(d => ({
          ...d,
          revenue:          +d.revenue.toFixed(2),
          cogs:             +d.cogs.toFixed(2),
          shipping_charged: +d.shipping_charged.toFixed(2),
          shipping_cost:    +d.shipping_cost.toFixed(2),
          transaction_fees: +d.transaction_fees.toFixed(2),
          aov:              d.orders > 0 ? +(d.revenue / d.orders).toFixed(2) : 0,
        }))
        .sort((a, b) => a.date.localeCompare(b.date));

      const totals = daily.reduce((acc, d) => ({
        revenue:          +(acc.revenue + d.revenue).toFixed(2),
        orders:           acc.orders + d.orders,
        cogs:             +(acc.cogs + d.cogs).toFixed(2),
        shipping_charged: +(acc.shipping_charged + d.shipping_charged).toFixed(2),
        shipping_cost:    +(acc.shipping_cost + d.shipping_cost).toFixed(2),
        transaction_fees: +(acc.transaction_fees + d.transaction_fees).toFixed(2),
      }), { revenue:0, orders:0, cogs:0, shipping_charged:0, shipping_cost:0, transaction_fees:0 });

      return res.status(200).json({
        daily,
        totals,
        has_cogs:     daily.some(d => d.has_cogs),
        total_orders: orders.length,
        debug: {
          store_timezone: tz,
          utc_offset_hrs: offsetMs / 3600000,
          raw_orders:     allOrders.length,
          kept_orders:    orders.length,
          utc_window:     { from: fromUTC, to: toUTC },
        },
      });
    }

    // other shopify actions (products, orders for fulfillment hub etc)
    if (action === "products") {
      const r = await fetch(`${base}/products.json?limit=50&fields=id,title,variants,images,handle,tags`, { headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" } });
      return res.status(200).json(await r.json());
    }

    return res.status(400).json({ error: "Unknown shopify action", action });
  }

  // ── Meta Ads ────────────────────────────────────────────────────────────────
  if (service === "meta-ads") {
    const token   = process.env.META_ACCESS_TOKEN;
    const account = process.env.META_AD_ACCOUNT_ID;
    if (!token || !account) return res.status(500).json({ error: "Meta not configured", META_TOKEN: token?"✓":"✗", META_ACCOUNT: account?"✓":"✗" });
    if (!from || !to) return res.status(400).json({ error: "from and to required" });

    const params = new URLSearchParams({
      fields:         "spend,actions,action_values,impressions,clicks",
      time_increment: "1",
      time_range:     JSON.stringify({ since: from, until: to }),
      level:          "account",
      access_token:   token,
    });

    const r    = await fetch(`https://graph.facebook.com/v21.0/${account}/insights?${params}`);
    const data = await r.json();
    if (data.error) return res.status(400).json({ error: "Meta API error", details: data.error });

    const daily = (data.data || []).map(row => {
      const actions      = row.actions       || [];
      const actionValues = row.action_values  || [];
      const find = (arr, type) => parseFloat(arr.find(a => a.action_type === type)?.value || 0);
      const purchases = find(actions,      "purchase");
      const purVal    = find(actionValues, "purchase");
      const spend     = parseFloat(row.spend || 0);
      return {
        date:        row.date_start,
        spend:       +spend.toFixed(2),
        revenue:     +purVal.toFixed(2),
        conversions: Math.round(purchases),
        clicks:      parseInt(row.clicks || 0),
        roas:        spend > 0 ? +(purVal / spend).toFixed(2) : 0,
      };
    }).sort((a, b) => a.date.localeCompare(b.date));

    const total = daily.reduce((acc, d) => ({
      spend:       +(acc.spend + d.spend).toFixed(2),
      revenue:     +(acc.revenue + d.revenue).toFixed(2),
      conversions: acc.conversions + d.conversions,
    }), { spend:0, revenue:0, conversions:0 });
    total.roas = total.spend > 0 ? +(total.revenue / total.spend).toFixed(2) : 0;
    total.cpa  = total.conversions > 0 ? +(total.spend / total.conversions).toFixed(2) : 0;

    return res.status(200).json({ platform: "meta", daily, total });
  }

  // ── Blob / Alert / Google / Microsoft — stubs ───────────────────────────────
  if (service === "blob")       return res.status(200).json({ ok: true, service: "blob" });
  if (service === "alert")      return res.status(200).json({ ok: true, service: "alert" });
  if (service === "google-ads") return res.status(200).json({ platform: "google", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "ms-ads")     return res.status(200).json({ platform: "microsoft", daily: [], total: { spend:0, revenue:0, roas:0 } });

  return res.status(400).json({ error: "Unknown service", received: service });
};
