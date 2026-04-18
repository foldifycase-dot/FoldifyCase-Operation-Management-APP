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

    // ── P&L REPORT ─────────────────────────────────────────────────────────────
    if (action === "pnl") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // Step 1: Store timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}

      // Step 2: UTC window
      const getOffsetMs = (d) => {
        const probe  = new Date(d + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU", { timeZone: tz, hour: "2-digit", hour12: false }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();

      // Step 3: Fetch orders (include discounts + refund info)
      const params = new URLSearchParams({
        status: "any", financial_status: "any",
        created_at_min: fromUTC, created_at_max: toUTC, limit: "250",
        fields: "id,created_at,total_price,subtotal_price,total_discounts,payment_gateway_names,total_shipping_price_set,shipping_lines,line_items,financial_status,cancelled_at,refunds",
      });
      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) return res.status(502).json({ error: `Shopify HTTP ${r.status}` });
        const j = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }

      // Separate active vs refunded/cancelled
      const orders   = allOrders.filter(o => !o.cancelled_at && o.financial_status !== "voided");
      const refunded = allOrders.filter(o => o.financial_status === "refunded" || o.financial_status === "partially_refunded");

      // Step 4: COGS via GraphQL
      const variantIds = [...new Set(orders.flatMap(o => (o.line_items||[]).map(li => li.variant_id).filter(Boolean)))];
      const costMap = {};
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i+50);
        const aliases = chunk.map((id, idx) => `v${idx}: productVariant(id:"gid://shopify/ProductVariant/${id}"){inventoryItem{unitCost{amount}}}`).join("\n");
        try {
          const gr = await fetch(GQL, { method:"POST", headers:HEADERS, body:JSON.stringify({query:`{${aliases}}`}) });
          const gj = await gr.json();
          const data = gj.data || {};
          chunk.forEach((vid, idx) => {
            const amount = data[`v${idx}`]?.inventoryItem?.unitCost?.amount;
            if (amount != null) costMap[vid] = parseFloat(amount);
          });
        } catch(e) {}
      }

      // Step 5: Fee rates from query params
      const feeRates = {
        "shopify payments": { pct: parseFloat(req.query.fee_shopify_pct  ?? 1.75)/100, flat: parseFloat(req.query.fee_shopify_flat  ?? 0.30) },
        "stripe":           { pct: parseFloat(req.query.fee_stripe_pct   ?? 2.90)/100, flat: parseFloat(req.query.fee_stripe_flat   ?? 0.30) },
        "paypal":           { pct: parseFloat(req.query.fee_paypal_pct   ?? 4.40)/100, flat: parseFloat(req.query.fee_paypal_flat   ?? 0.30) },
        "klarna":           { pct: parseFloat(req.query.fee_klarna_pct   ?? 2.90)/100, flat: parseFloat(req.query.fee_klarna_flat   ?? 0.30) },
        "afterpay":         { pct: parseFloat(req.query.fee_afterpay_pct ?? 0.00)/100, flat: parseFloat(req.query.fee_afterpay_flat ?? 0.00) },
        "manual":           { pct: 0, flat: 0 },
        "other":            { pct: parseFloat(req.query.fee_other_pct    ?? 2.90)/100, flat: parseFloat(req.query.fee_other_flat    ?? 0.30) },
      };
      const getRate = (gateways) => {
        const gw  = ((gateways && gateways[0]) || "other").toLowerCase();
        const key = Object.keys(feeRates).find(k => gw.includes(k)) || "other";
        return feeRates[key];
      };

      const round2 = n => Math.round(n * 100) / 100;

      // Step 6: Aggregate by local date
      const byDate = {};
      orders.forEach(o => {
        const localDate = new Intl.DateTimeFormat("en-CA", { timeZone: tz, year:"numeric", month:"2-digit", day:"2-digit" }).format(new Date(o.created_at));
        if (!byDate[localDate]) byDate[localDate] = { date:localDate, grossSales:0, discounts:0, refundAmt:0, orders:0, cogs:0, adSpend:0, txFee:0, shippingCharged:0, shippingCost:0 };
        const d = byDate[localDate];
        const sub = parseFloat(o.subtotal_price||0);
        const rate = getRate(o.payment_gateway_names);
        d.grossSales += parseFloat(o.total_price||0);
        d.discounts  += parseFloat(o.total_discounts||0);
        d.orders     += 1;
        d.txFee      += sub * rate.pct + rate.flat;
        (o.line_items||[]).forEach(li => { d.cogs += (costMap[li.variant_id]||0) * (parseInt(li.quantity)||1); });
        d.shippingCharged += parseFloat(o.total_shipping_price_set?.shop_money?.amount||0);
        // refunds on this order
        (o.refunds||[]).forEach(ref => {
          (ref.refund_line_items||[]).forEach(rli => { d.refundAmt += parseFloat(rli.subtotal||0); });
        });
      });

      // Build daily array
      const daily = Object.values(byDate)
        .filter(d => d.date >= from && d.date <= to)
        .map(d => {
          const netRev       = round2(d.grossSales - d.discounts - d.refundAmt);
          const grossProfit  = round2(netRev - d.cogs);
          const grossMargin  = netRev > 0 ? round2(grossProfit / netRev * 100) : 0;
          const contribMargin = round2(grossProfit - d.txFee - d.adSpend);
          const netProfit    = round2(contribMargin);
          const netMargin    = netRev > 0 ? round2(netProfit / netRev * 100) : 0;
          return {
            date: d.date,
            orders: d.orders,
            grossSales:    round2(d.grossSales),
            discounts:     round2(d.discounts),
            refundAmt:     round2(d.refundAmt),
            netRevenue:    netRev,
            cogs:          round2(d.cogs),
            grossProfit,   grossMargin,
            txFee:         round2(d.txFee),
            adSpend:       round2(d.adSpend),
            shippingCharged: round2(d.shippingCharged),
            contribMargin, netProfit, netMargin,
          };
        }).sort((a,b) => a.date.localeCompare(b.date));

      // Totals
      const totals = daily.reduce((acc, d) => ({
        orders:       acc.orders + d.orders,
        grossSales:   round2(acc.grossSales + d.grossSales),
        discounts:    round2(acc.discounts + d.discounts),
        refundAmt:    round2(acc.refundAmt + d.refundAmt),
        netRevenue:   round2(acc.netRevenue + d.netRevenue),
        cogs:         round2(acc.cogs + d.cogs),
        grossProfit:  round2(acc.grossProfit + d.grossProfit),
        txFee:        round2(acc.txFee + d.txFee),
        adSpend:      round2(acc.adSpend + d.adSpend),
        shippingCharged: round2(acc.shippingCharged + d.shippingCharged),
        contribMargin: round2(acc.contribMargin + d.contribMargin),
        netProfit:    round2(acc.netProfit + d.netProfit),
      }), { orders:0, grossSales:0, discounts:0, refundAmt:0, netRevenue:0, cogs:0, grossProfit:0, txFee:0, adSpend:0, shippingCharged:0, contribMargin:0, netProfit:0 });
      totals.grossMargin  = totals.netRevenue > 0 ? round2(totals.grossProfit / totals.netRevenue * 100) : 0;
      totals.contribPct   = totals.netRevenue > 0 ? round2(totals.contribMargin / totals.netRevenue * 100) : 0;
      totals.netMargin    = totals.netRevenue > 0 ? round2(totals.netProfit / totals.netRevenue * 100) : 0;
      totals.totalCosts   = round2(totals.cogs + totals.txFee + totals.adSpend);
      totals.costPct      = totals.netRevenue > 0 ? round2(totals.totalCosts / totals.netRevenue * 100) : 0;

      return res.status(200).json({
        daily, totals,
        total_orders: orders.length,
        debug: { store_timezone: tz, utc_window: { from: fromUTC, to: toUTC }, raw_orders: allOrders.length, kept_orders: orders.length },
      });
    }

    // ── ORDER REPORT ───────────────────────────────────────────────────────────
    if (action === "order-report") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // Timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}

      const getOffsetMs = (d) => {
        const probe  = new Date(d + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU", { timeZone: tz, hour:"2-digit", hour12:false }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();

      // Fetch orders with full detail
      const params = new URLSearchParams({
        status: "any", financial_status: "any",
        created_at_min: fromUTC, created_at_max: toUTC, limit: "250",
        fields: "id,name,created_at,total_price,subtotal_price,total_discounts,total_shipping_price_set,shipping_lines,line_items,financial_status,cancelled_at,payment_gateway_names,customer,fulfillment_status",
      });
      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) return res.status(502).json({ error: `Shopify HTTP ${r.status}` });
        const j = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }

      // COGS via GraphQL for all variants
      const variantIds = [...new Set(allOrders.flatMap(o => (o.line_items||[]).map(li => li.variant_id).filter(Boolean)))];
      const costMap = {};
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i+50);
        const aliases = chunk.map((id, idx) => `v${idx}: productVariant(id:"gid://shopify/ProductVariant/${id}"){inventoryItem{unitCost{amount}}}`).join("\n");
        try {
          const gr = await fetch(GQL, { method:"POST", headers:HEADERS, body:JSON.stringify({query:`{${aliases}}`}) });
          const gj = await gr.json();
          const data = gj.data || {};
          chunk.forEach((vid, idx) => {
            const amount = data[`v${idx}`]?.inventoryItem?.unitCost?.amount;
            if (amount != null) costMap[vid] = parseFloat(amount);
          });
        } catch(e) {}
      }

      // Fee rates
      const feeRates = {
        "shopify payments": { pct: parseFloat(req.query.fee_shopify_pct  ?? 1.75)/100, flat: parseFloat(req.query.fee_shopify_flat  ?? 0.30) },
        "stripe":           { pct: parseFloat(req.query.fee_stripe_pct   ?? 2.90)/100, flat: parseFloat(req.query.fee_stripe_flat   ?? 0.30) },
        "paypal":           { pct: parseFloat(req.query.fee_paypal_pct   ?? 4.40)/100, flat: parseFloat(req.query.fee_paypal_flat   ?? 0.30) },
        "klarna":           { pct: parseFloat(req.query.fee_klarna_pct   ?? 2.90)/100, flat: parseFloat(req.query.fee_klarna_flat   ?? 0.30) },
        "afterpay":         { pct: parseFloat(req.query.fee_afterpay_pct ?? 6.00)/100, flat: parseFloat(req.query.fee_afterpay_flat ?? 0.30) },
        "manual":           { pct: 0, flat: 0 },
        "other":            { pct: parseFloat(req.query.fee_other_pct    ?? 2.90)/100, flat: parseFloat(req.query.fee_other_flat    ?? 0.30) },
      };
      const getRate = (gateways) => {
        const gw  = ((gateways && gateways[0]) || "other").toLowerCase();
        const key = Object.keys(feeRates).find(k => gw.includes(k)) || "other";
        return feeRates[key];
      };

      const round2 = n => Math.round(n * 100) / 100;
      const localDate = (iso) => new Intl.DateTimeFormat("en-CA", {
        timeZone: tz, year:"numeric", month:"2-digit", day:"2-digit"
      }).format(new Date(iso));

      // Build order rows
      const orders = allOrders.map(o => {
        const revenue   = parseFloat(o.total_price || 0);
        const subtotal  = parseFloat(o.subtotal_price || 0);
        const discounts = parseFloat(o.total_discounts || 0);
        const shipping  = parseFloat(o.total_shipping_price_set?.shop_money?.amount || 0);
        const rate      = getRate(o.payment_gateway_names);
        const txFee     = round2(subtotal * rate.pct + rate.flat);
        let cogs = 0;
        const items = (o.line_items || []).length;
        (o.line_items || []).forEach(li => {
          cogs += (costMap[li.variant_id] || 0) * (parseInt(li.quantity) || 1);
        });
        cogs = round2(cogs);
        const profit  = round2(revenue - cogs - txFee);
        const margin  = revenue > 0 ? round2(profit / revenue * 100) : 0;
        const custName = o.customer
          ? `${o.customer.first_name||''} ${o.customer.last_name||''}`.trim() || o.customer.email || 'Guest'
          : 'Guest';

        return {
          id:          o.id,
          name:        o.name,
          date:        localDate(o.created_at),
          customer:    custName,
          items,
          revenue,
          discounts,
          shipping,
          cogs,
          txFee,
          profit,
          margin,
          status:      o.financial_status,
          gateway:     (o.payment_gateway_names && o.payment_gateway_names[0]) || 'unknown',
          cancelled:   !!o.cancelled_at,
          fulfillment: o.fulfillment_status || 'unfulfilled',
          hasCogs:     cogs > 0,
        };
      });

      return res.status(200).json({
        orders,
        total_count: orders.length,
        debug: { store_timezone: tz, utc_window: { from: fromUTC, to: toUTC }, variants_with_cost: Object.keys(costMap).length },
      });
    }
    if (action === "clv") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // Timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}

      const getOffsetMs = (d) => {
        const probe  = new Date(d + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU", { timeZone: tz, hour:"2-digit", hour12:false }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();

      // Fetch ALL orders (no date filter — need full customer history for LTV)
      // But fetch within the reporting window for new/returning split
      // Step 1: fetch customers with orders in window
      const params = new URLSearchParams({
        status: "any", financial_status: "any",
        created_at_min: fromUTC, created_at_max: toUTC, limit: "250",
        fields: "id,created_at,total_price,subtotal_price,customer,financial_status,cancelled_at",
      });
      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) return res.status(502).json({ error: `Shopify HTTP ${r.status}` });
        const j = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }
      const orders = allOrders.filter(o => !o.cancelled_at && o.financial_status !== "voided" && o.financial_status !== "refunded");

      // Step 2: fetch top customers (by total spent) for LTV analysis
      const customerParams = new URLSearchParams({
        limit: "250", order: "total_spent desc",
        fields: "id,email,first_name,last_name,orders_count,total_spent,created_at,updated_at,last_order_id",
      });
      let allCustomers = [];
      let custUrl = `${REST}/customers.json?${customerParams}`;
      while (custUrl) {
        const r = await fetch(custUrl, { headers: HEADERS });
        if (!r.ok) break;
        const j = await r.json();
        const batch = j.customers || [];
        allCustomers.push(...batch);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        custUrl = (next && batch.length === 250) ? next[1] : null;
        if (allCustomers.length >= 500) break; // cap for performance
      }

      const round2 = n => Math.round(n * 100) / 100;
      const now    = Date.now();

      // Step 3: Build customer-order map from window orders
      const custOrderMap = {}; // customerId → [orders]
      orders.forEach(o => {
        if (!o.customer?.id) return;
        const cid = o.customer.id;
        if (!custOrderMap[cid]) custOrderMap[cid] = [];
        custOrderMap[cid].push({ date: o.created_at, revenue: parseFloat(o.total_price||0) });
      });

      // Step 4: New vs returning in window
      const custIdSet = new Set(Object.keys(custOrderMap));
      let newCusts = 0, retCusts = 0, newRev = 0, retRev = 0;
      orders.forEach(o => {
        if (!o.customer?.id) return;
        const isNew = o.customer.orders_count <= 1;
        const rev   = parseFloat(o.total_price||0);
        if (isNew) { newCusts++; newRev += rev; }
        else       { retCusts++; retRev += rev; }
      });
      const totalOrders   = orders.length;
      const totalRev      = orders.reduce((s,o) => s + parseFloat(o.total_price||0), 0);
      const totalCustIds  = custIdSet.size;
      const aov           = totalOrders > 0 ? round2(totalRev / totalOrders) : 0;
      const frequency     = totalCustIds > 0 ? round2(totalOrders / totalCustIds) : 0;

      // Step 5: Days between orders (from returning customers in window)
      const intervalDays = [];
      Object.values(custOrderMap).forEach(orderList => {
        if (orderList.length < 2) return;
        const sorted = [...orderList].sort((a,b) => new Date(a.date) - new Date(b.date));
        for (let i=1; i<sorted.length; i++) {
          const diff = (new Date(sorted[i].date) - new Date(sorted[i-1].date)) / 86400000;
          if (diff > 0 && diff < 365) intervalDays.push(diff);
        }
      });
      const avgDaysBetween = intervalDays.length > 0
        ? round2(intervalDays.reduce((s,d)=>s+d,0) / intervalDays.length) : null;

      // Step 6: CLV formula = AOV × frequency × estimated lifespan
      const lifespanYears = avgDaysBetween ? round2((avgDaysBetween * frequency) / 365 * 2) : 1;
      const clv = round2(aov * frequency * Math.max(lifespanYears, 1));

      // Step 7: Cohort analysis — group customers by first-purchase month
      // Use customers created_at as proxy for first purchase month
      const cohortMap = {}; // "YYYY-MM" → { total, returnsByMonth: {0:n, 1:n, ...} }
      allCustomers.forEach(c => {
        const cohortKey = c.created_at.slice(0,7);
        if (!cohortMap[cohortKey]) cohortMap[cohortKey] = { total:0, orders: c.orders_count, returners: 0 };
        cohortMap[cohortKey].total++;
        if (parseInt(c.orders_count) >= 2) cohortMap[cohortKey].returners++;
      });

      // Build simplified cohort table: months with retention rate
      const cohorts = Object.entries(cohortMap)
        .sort((a,b) => a[0].localeCompare(b[0]))
        .slice(-12) // last 12 months
        .map(([month, data]) => ({
          month,
          total:      data.total,
          returners:  data.returners,
          retRate:    data.total > 0 ? round2(data.returners / data.total * 100) : 0,
        }));

      // Step 8: RFM scoring of top customers
      const repeatPurchaseRate = totalCustIds > 0 ? round2(retCusts / totalCustIds * 100) : 0;
      const rfmCustomers = allCustomers.slice(0,200).map(c => {
        const totalSpent    = parseFloat(c.total_spent||0);
        const ordersCount   = parseInt(c.orders_count||0);
        const lastOrderDate = c.updated_at;
        const daysSinceLast = Math.floor((now - new Date(lastOrderDate)) / 86400000);
        const avgInterval   = avgDaysBetween || 90;

        // Recency score 1-5
        const rScore = daysSinceLast < avgInterval ? 5
                     : daysSinceLast < avgInterval*1.5 ? 4
                     : daysSinceLast < avgInterval*2 ? 3
                     : daysSinceLast < avgInterval*3 ? 2 : 1;
        // Frequency score 1-5
        const fScore = ordersCount >= 10 ? 5 : ordersCount >= 5 ? 4 : ordersCount >= 3 ? 3 : ordersCount >= 2 ? 2 : 1;
        // Monetary score 1-5
        const mScore = totalSpent >= 1000 ? 5 : totalSpent >= 500 ? 4 : totalSpent >= 200 ? 3 : totalSpent >= 100 ? 2 : 1;
        const rfmScore = rScore + fScore + mScore;

        const segment = rfmScore >= 13 ? 'Champions'
                      : rfmScore >= 10 ? 'Loyal'
                      : rfmScore >= 7  ? (rScore >= 4 ? 'Promising' : 'At-Risk')
                      : rScore <= 2    ? 'Lost'
                      : 'Hibernating';

        return {
          id:           c.id,
          email:        c.email || 'Guest',
          name:         `${c.first_name||''} ${c.last_name||''}`.trim() || c.email || 'Guest',
          ordersCount,
          totalSpent:   round2(totalSpent),
          aov:          ordersCount > 0 ? round2(totalSpent / ordersCount) : 0,
          daysSinceLast,
          lastOrderDate: lastOrderDate ? lastOrderDate.slice(0,10) : '—',
          rScore, fScore, mScore, rfmScore, segment,
        };
      }).sort((a,b) => b.totalSpent - a.totalSpent);

      // RFM segment summary
      const rfmSummary = {};
      rfmCustomers.forEach(c => {
        if (!rfmSummary[c.segment]) rfmSummary[c.segment] = { count:0, revenue:0, orders:0 };
        rfmSummary[c.segment].count++;
        rfmSummary[c.segment].revenue += c.totalSpent;
        rfmSummary[c.segment].orders  += c.ordersCount;
      });

      return res.status(200).json({
        totals: {
          totalOrders, totalRev: round2(totalRev), totalCustomers: totalCustIds,
          aov, frequency, clv, avgDaysBetween, repeatPurchaseRate, lifespanYears,
          newCusts, retCusts, newRev: round2(newRev), retRev: round2(retRev),
          newAov: newCusts>0 ? round2(newRev/newCusts) : 0,
          retAov: retCusts>0 ? round2(retRev/retCusts) : 0,
        },
        cohorts,
        rfmSummary,
        topCustomers: rfmCustomers.slice(0, 20),
        debug: { store_timezone: tz, raw_orders: allOrders.length, kept_orders: orders.length, customers_fetched: allCustomers.length },
      });
    }

    if (action === "products") {
      const r = await fetch(`${REST}/products.json?limit=50&fields=id,title,variants,images,handle,tags`, { headers: HEADERS });
      return res.status(200).json(await r.json());
    }

    // ── PRODUCT ANALYTICS ──────────────────────────────────────────────────
    if (action === "product-analytics") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });

      // Step 1: Store timezone
      let tz = "Australia/Melbourne";
      try {
        const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
        const j = await s.json();
        if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
      } catch(e) {}

      // Step 2: UTC window
      const getOffsetMs = (dateStr) => {
        const probe  = new Date(dateStr + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU", { timeZone: tz, hour: "2-digit", hour12: false }).format(probe));
        return (localH - 12) * 3600000;
      };
      const offsetMs = getOffsetMs(from);
      const fromUTC  = new Date(new Date(from + "T00:00:00Z").getTime() - offsetMs).toISOString();
      const toUTC    = new Date(new Date(to   + "T23:59:59Z").getTime() - offsetMs).toISOString();

      // Step 3: Fetch all orders
      const params = new URLSearchParams({
        status: "any", financial_status: "any",
        created_at_min: fromUTC, created_at_max: toUTC,
        limit: "250",
        fields: "id,created_at,total_price,subtotal_price,line_items,financial_status,cancelled_at",
      });
      let allOrders = [];
      let url = `${REST}/orders.json?${params}`;
      while (url) {
        const r = await fetch(url, { headers: HEADERS });
        if (!r.ok) return res.status(502).json({ error: `Shopify orders HTTP ${r.status}` });
        const j = await r.json();
        const batch = j.orders || [];
        allOrders.push(...batch);
        const link = r.headers.get("Link") || "";
        const next = link.match(/<([^>]+)>;\s*rel="next"/);
        url = (next && batch.length === 250) ? next[1] : null;
      }
      const orders = allOrders.filter(o =>
        !o.cancelled_at && o.financial_status !== "refunded" && o.financial_status !== "voided"
      );

      // Step 4: Get variant costs via GraphQL
      const variantIds = [...new Set(orders.flatMap(o => (o.line_items||[]).map(li => li.variant_id).filter(Boolean)))];
      const costMap = {};
      const titleMap = {}; // variantId → { productTitle, variantTitle, productType }
      for (let i = 0; i < variantIds.length; i += 50) {
        const chunk = variantIds.slice(i, i+50);
        const aliases = chunk.map((id, idx) => `v${idx}: productVariant(id: "gid://shopify/ProductVariant/${id}") {
          id title
          product { title productType }
          inventoryItem { unitCost { amount } }
        }`).join("\n");
        try {
          const gr = await fetch(GQL, { method: "POST", headers: HEADERS, body: JSON.stringify({ query: `{ ${aliases} }` }) });
          const gj = await gr.json();
          const data = gj.data || {};
          chunk.forEach((variantId, idx) => {
            const node = data[`v${idx}`];
            if (!node) return;
            const amount = node.inventoryItem?.unitCost?.amount;
            if (amount !== null && amount !== undefined) costMap[variantId] = parseFloat(amount);
            titleMap[variantId] = {
              productTitle: node.product?.title || "Unknown",
              variantTitle: node.title || "",
              productType:  node.product?.productType || "",
            };
          });
        } catch(e) { console.warn("[proxy] GQL chunk error:", e.message); }
      }

      // Step 5: Fetch current inventory levels
      const inventoryMap = {}; // variantId → qty on hand
      try {
        const locRes = await fetch(`${REST}/locations.json?fields=id`, { headers: HEADERS });
        const { locations = [] } = await locRes.json();
        if (locations.length > 0) {
          // Get inventory for first location (primary warehouse)
          const locId = locations[0].id;
          let invUrl = `${REST}/inventory_levels.json?location_ids=${locId}&limit=250`;
          while (invUrl) {
            const ir = await fetch(invUrl, { headers: HEADERS });
            const ij = await ir.json();
            (ij.inventory_levels || []).forEach(lv => {
              inventoryMap[lv.inventory_item_id] = (inventoryMap[lv.inventory_item_id] || 0) + (lv.available || 0);
            });
            const link2 = ir.headers.get("Link") || "";
            const next2 = link2.match(/<([^>]+)>;\s*rel="next"/);
            invUrl = next2 ? next2[1] : null;
          }
        }
      } catch(e) { console.warn("[proxy] Inventory fetch error:", e.message); }

      // inventory_item_id → variant_id map (from our costMap fetches above via GQL)
      // We need variant → inventory_item_id for inventory lookup
      const variantInvItemMap = {}; // variantId → inventoryItemId
      for (let i = 0; i < variantIds.length; i += 100) {
        try {
          const chunk = variantIds.slice(i, i+100);
          const vr = await fetch(`${REST}/variants.json?ids=${chunk.join(",")}&fields=id,inventory_item_id`, { headers: HEADERS });
          const { variants = [] } = await vr.json();
          variants.forEach(v => { variantInvItemMap[v.id] = v.inventory_item_id; });
        } catch(e) {}
      }

      // Step 6: Aggregate by product
      const byProduct = {}; // productTitle → { units, revenue, cogs, orders, variants: {} }
      const round2 = n => Math.round(n * 100) / 100;

      orders.forEach(o => {
        const subTotal = parseFloat(o.subtotal_price || 0);
        const txFee = subTotal * 0.057424; // blended rate
        (o.line_items || []).forEach(li => {
          const vid   = li.variant_id;
          const info  = titleMap[vid] || { productTitle: li.title || "Unknown", variantTitle: li.variant_title || "", productType: "" };
          const key   = info.productTitle;
          const units = parseInt(li.quantity) || 1;
          const rev   = parseFloat(li.price || 0) * units;
          const cost  = (costMap[vid] || 0) * units;
          const itemTxFee = (parseFloat(li.price||0) * units / (subTotal||1)) * txFee;

          if (!byProduct[key]) byProduct[key] = { title: key, productType: info.productType, units: 0, revenue: 0, cogs: 0, txFee: 0, variants: {}, orderSet: new Set() };
          byProduct[key].units   += units;
          byProduct[key].revenue += rev;
          byProduct[key].cogs    += cost;
          byProduct[key].txFee   += itemTxFee;
          byProduct[key].orderSet.add(o.id);

          const vKey = `${vid}`;
          if (!byProduct[key].variants[vKey]) byProduct[key].variants[vKey] = { variantId: vid, title: info.variantTitle, units: 0, revenue: 0, cogs: 0 };
          byProduct[key].variants[vKey].units   += units;
          byProduct[key].variants[vKey].revenue += rev;
          byProduct[key].variants[vKey].cogs    += cost;
        });
      });

      // Step 7: Build products array with ABC grading
      let products = Object.values(byProduct).map(p => {
        const profit = p.revenue - p.cogs - p.txFee;
        const margin = p.revenue > 0 ? (profit / p.revenue * 100) : 0;
        const orders = p.orderSet.size;
        return {
          title:       p.title,
          productType: p.productType,
          units:       p.units,
          orders,
          revenue:     round2(p.revenue),
          cogs:        round2(p.cogs),
          txFee:       round2(p.txFee),
          profit:      round2(profit),
          margin:      round2(margin),
          variants:    Object.values(p.variants).map(v => ({
            ...v,
            variantInvItemId: variantInvItemMap[v.variantId],
            stock: inventoryMap[variantInvItemMap[v.variantId]] ?? null,
            profit: round2(v.revenue - v.cogs),
            margin: v.revenue > 0 ? round2((v.revenue - v.cogs) / v.revenue * 100) : 0,
          })).sort((a,b) => b.revenue - a.revenue),
        };
      }).sort((a,b) => b.revenue - a.revenue);

      // ABC grading by revenue contribution
      const totalRev = products.reduce((s,p) => s + p.revenue, 0);
      let cumRev = 0;
      products.forEach(p => {
        cumRev += p.revenue;
        const pct = totalRev > 0 ? cumRev / totalRev : 0;
        p.abc = pct <= 0.80 ? "A" : pct <= 0.95 ? "B" : "C";
        p.revPct = round2(p.revenue / (totalRev||1) * 100);
      });

      // Step 8: Summary totals
      const totals = products.reduce((acc, p) => ({
        revenue: round2(acc.revenue + p.revenue),
        units:   acc.units + p.units,
        orders:  acc.orders + p.orders,
        cogs:    round2(acc.cogs + p.cogs),
        profit:  round2(acc.profit + p.profit),
      }), { revenue:0, units:0, orders:0, cogs:0, profit:0 });
      totals.margin = totals.revenue > 0 ? round2(totals.profit / totals.revenue * 100) : 0;
      totals.aov    = totals.orders  > 0 ? round2(totals.revenue / totals.orders) : 0;

      // Step 9: Inventory health per product
      const inventoryHealth = products.map(p => {
        const allVariants = p.variants;
        const totalStock = allVariants.reduce((s,v) => s + (v.stock ?? 0), 0);
        const dailySales = p.units / Math.max(1, (new Date(to) - new Date(from)) / 86400000);
        const daysLeft   = dailySales > 0 ? round2(totalStock / dailySales) : null;
        const sellThru   = totalStock + p.units > 0 ? round2(p.units / (totalStock + p.units) * 100) : null;
        const status     = daysLeft === null ? "unknown" : daysLeft < 7 ? "critical" : daysLeft < 21 ? "watch" : "healthy";
        return { title: p.title, totalStock, daysLeft, sellThru, dailySales: round2(dailySales), status };
      });

      return res.status(200).json({
        products,
        totals,
        inventoryHealth,
        total_products: products.length,
        total_variants: variantIds.length,
        debug: { store_timezone: tz, raw_orders: allOrders.length, kept_orders: orders.length, utc_window: { from: fromUTC, to: toUTC } },
      });
    }


    // ── TX-FEES: Actual fees from Shopify Payments Finance > Payouts ─────────
    if (action === "tx-fees") {
      if (!from || !to) return res.status(400).json({ error: "from and to required" });
      const fees = {};
      try {
        // Timezone
        let tz = "Australia/Melbourne";
        try {
          const s = await fetch(`${REST}/shop.json?fields=iana_timezone`, { headers: HEADERS });
          const j = await s.json();
          if (j.shop?.iana_timezone) tz = j.shop.iana_timezone;
        } catch(e) {}

        // UTC window
        const probe  = new Date(from + "T12:00:00Z");
        const localH = parseInt(new Intl.DateTimeFormat("en-AU",{ timeZone:tz, hour:"2-digit", hour12:false }).format(probe));
        const off    = (localH - 12) * 3600000;
        const fromUTC = new Date(new Date(from+"T00:00:00Z").getTime()-off).toISOString();
        const toUTC   = new Date(new Date(to  +"T00:00:00Z").getTime()-off+86399999).toISOString();

        // Step 1: Get orders + their transaction IDs
        const txnToOrder = {};
        let url = `${REST}/orders.json?created_at_min=${fromUTC}&created_at_max=${toUTC}&status=any&limit=250&fields=id,transactions`;
        while (url) {
          const r = await fetch(url, { headers: HEADERS });
          if (!r.ok) break;
          const j = await r.json();
          (j.orders || []).forEach(o => (o.transactions||[]).forEach(t => { if(t.id) txnToOrder[String(t.id)]=String(o.id); }));
          const lnk = r.headers.get("Link")||"";
          const nx  = lnk.split(",").find(p=>p.includes('rel="next"'));
          url = nx ? nx.match(/<([^>]+)>/)?.[1] : null;
        }

        // Step 2: Fetch payout balance transactions (actual fees)
        const br = await fetch(`${REST}/shopify_payments/balance/transactions.json?transaction_type=charge&limit=250`, { headers: HEADERS });
        if (br.ok) {
          const bj = await br.json();
          (bj.transactions||[]).forEach(t => {
            if (t.source_id && t.fee != null) {
              const oid = txnToOrder[String(t.source_id)];
              if (oid) fees[oid] = Math.round(((fees[oid]||0) + Math.abs(parseFloat(t.fee)))*100)/100;
            }
          });
          console.log("[tx-fees] loaded:", Object.keys(fees).length, "orders, mapped:", Object.keys(txnToOrder).length, "txns");
        } else {
          console.log("[tx-fees] payout HTTP", br.status, "- check read_shopify_payments_payouts scope");
          return res.status(200).json({ fees:{}, error:`Payout API returned ${br.status}` });
        }
      } catch(e) {
        console.log("[tx-fees] error:", e.message);
      }
      return res.status(200).json({ fees, count: Object.keys(fees).length });
    }

    return res.status(400).json({ error: "Unknown shopify action", received: action });
  }

  // ── META ADS ─────────────────────────────────────────────────────────────────
  if (service === "meta-ads") {
    const TOKEN   = process.env.META_ACCESS_TOKEN;
    const ACCOUNT = process.env.META_AD_ACCOUNT_ID;
    if (!TOKEN || !ACCOUNT) return res.status(500).json({ error: "Meta not configured" });
    if (!from || !to) return res.status(400).json({ error: "from and to required" });

    const round2 = n => Math.round(n * 100) / 100;
    const extractMetrics = (row) => {
      const acts = row.actions || [], vals = row.action_values || [];
      const find  = (arr, type) => parseFloat(arr.find(a => a.action_type === type)?.value || 0);
      const purchases = find(acts, "purchase");
      const purVal    = find(vals, "purchase");
      const spend     = parseFloat(row.spend || 0);
      return {
        spend:       round2(spend),
        revenue:     round2(purVal),
        conversions: Math.round(purchases),
        clicks:      parseInt(row.clicks || 0),
        impressions: parseInt(row.impressions || 0),
        ctr:         round2(parseFloat(row.ctr || 0)),
        roas:        spend > 0 ? round2(purVal / spend) : 0,
        cpa:         purchases > 0 ? round2(spend / purchases) : 0,
      };
    };

    // 1) Account-level daily breakdown
    const dailyParams = new URLSearchParams({
      fields: "spend,actions,action_values,impressions,clicks,ctr",
      time_increment: "1",
      time_range: JSON.stringify({ since: from, until: to }),
      level: "account",
      access_token: TOKEN,
    });
    const dailyRes  = await fetch(`https://graph.facebook.com/v21.0/${ACCOUNT}/insights?${dailyParams}`);
    const dailyData = await dailyRes.json();
    if (dailyData.error) return res.status(400).json({ error: "Meta API error", details: dailyData.error });

    const daily = (dailyData.data || []).map(row => ({
      date: row.date_start,
      ...extractMetrics(row),
    })).sort((a, b) => a.date.localeCompare(b.date));

    // 2) Campaign-level breakdown (aggregate over date range)
    let campaigns = [];
    try {
      const campParams = new URLSearchParams({
        fields: "campaign_name,spend,actions,action_values,impressions,clicks,ctr",
        time_range: JSON.stringify({ since: from, until: to }),
        level: "campaign",
        access_token: TOKEN,
        limit: "50",
      });
      const campRes  = await fetch(`https://graph.facebook.com/v21.0/${ACCOUNT}/insights?${campParams}`);
      const campData = await campRes.json();
      if (!campData.error) {
        campaigns = (campData.data || []).map(row => ({
          name: row.campaign_name || 'Unknown Campaign',
          ...extractMetrics(row),
        })).sort((a, b) => b.spend - a.spend);
      }
    } catch(e) { console.warn('[proxy] Campaign fetch error:', e.message); }

    // 3) Totals
    const total = daily.reduce((acc, d) => ({
      spend:       round2(acc.spend + d.spend),
      revenue:     round2(acc.revenue + d.revenue),
      conversions: acc.conversions + d.conversions,
      clicks:      acc.clicks + d.clicks,
      impressions: acc.impressions + d.impressions,
    }), { spend:0, revenue:0, conversions:0, clicks:0, impressions:0 });
    total.roas = total.spend > 0 ? round2(total.revenue / total.spend) : 0;
    total.cpa  = total.conversions > 0 ? round2(total.spend / total.conversions) : 0;
    total.ctr  = total.impressions > 0 ? round2(total.clicks / total.impressions * 100) : 0;

    return res.status(200).json({ platform: "meta", daily, total, campaigns });
  }

  // ── Stubs ─────────────────────────────────────────────────────────────────────
  if (service === "google-ads") return res.status(200).json({ platform: "google", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "ms-ads")     return res.status(200).json({ platform: "microsoft", daily: [], total: { spend:0, revenue:0, roas:0 } });
  if (service === "blob")       return res.status(200).json({ ok: true });
  if (service === "alert")      return res.status(200).json({ ok: true });

  return res.status(400).json({ error: "Unknown service", received: service });
};
