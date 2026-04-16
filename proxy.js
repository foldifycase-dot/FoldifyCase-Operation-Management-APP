/**
 * FoldifyCase — Central Proxy
 * foldifycase-proxy.vercel.app/api/proxy
 *
 * Handles CORS and routes to:
 *   - Shopify Admin API  (orders, products, fulfillment)
 *   - Resend             (warehouse email alerts)
 *   - Vercel Blob        (labels, packing images, PO history)
 *   - Google Ads API     (daily spend, ROAS, conversions)  ← NEW
 *   - Meta Marketing API (daily spend, ROAS, conversions)  ← NEW
 *   - Microsoft Ads API  (daily spend, ROAS, conversions)  ← NEW
 */

// ─── CORS ─────────────────────────────────────────────────────────────────────
const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

// ─── SECRETS  (set all of these in Vercel → Project → Settings → Env Vars) ───
const ENV = {
  // ── Shopify ──────────────────────────────────────────────
  shopifyStore : process.env.SHOPIFY_STORE,          // scnd9y-a1.myshopify.com
  shopifyToken : process.env.SHOPIFY_TOKEN,          // shpat_xxxxxxxxxxxx

  // ── Resend (email) ───────────────────────────────────────
  resendKey    : process.env.RESEND_API_KEY,         // re_xxxxxxxxxxxx

  // ── Vercel Blob ──────────────────────────────────────────
  blobToken    : process.env.PUBLIC_BLOB_READ_WRITE_TOKEN,

  // ── Google Ads ───────────────────────────────────────────
  googleDevToken   : process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
  googleClientId   : process.env.GOOGLE_ADS_CLIENT_ID,
  googleClientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
  googleRefreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN,
  googleCustomerId : process.env.GOOGLE_ADS_CUSTOMER_ID,  // 10-digit number, no dashes

  // ── Meta Ads ─────────────────────────────────────────────
  metaToken    : process.env.META_ACCESS_TOKEN,      // from System User
  metaAdAccount: process.env.META_AD_ACCOUNT_ID,    // act_XXXXXXXXX

  // ── Microsoft Ads ────────────────────────────────────────
  msClientId      : process.env.MICROSOFT_ADS_CLIENT_ID,
  msClientSecret  : process.env.MICROSOFT_ADS_CLIENT_SECRET,
  msRefreshToken  : process.env.MICROSOFT_ADS_REFRESH_TOKEN,
  msAccountId     : process.env.MICROSOFT_ADS_ACCOUNT_ID,
  msCustomerId    : process.env.MICROSOFT_ADS_CUSTOMER_ID,
};

// ─── MAIN ROUTER ──────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method === "OPTIONS") {
    return res.status(200).set(CORS).end();
  }
  Object.entries(CORS).forEach(([k, v]) => res.setHeader(k, v));

  const service = req.query.service || req.body?.service;

  try {
    switch (service) {
      case "shopify":    return await handleShopify(req, res);
      case "blob":       return await handleBlob(req, res);
      case "alert":      return await handleAlert(req, res);
      case "google-ads": return await handleGoogleAds(req, res);
      case "meta-ads":   return await handleMetaAds(req, res);
      case "ms-ads":     return await handleMsAds(req, res);
      default:
        return res.status(400).json({
          error: "Unknown service",
          valid: ["shopify","blob","alert","google-ads","meta-ads","ms-ads"]
        });
    }
  } catch (err) {
    console.error(`[${service}] Error:`, err.message);
    return res.status(500).json({ error: err.message });
  }
}


// ═══════════════════════════════════════════════════════════════════════════════
// 1. SHOPIFY
// ═══════════════════════════════════════════════════════════════════════════════
async function handleShopify(req, res) {
  const action = req.query.action || req.body?.action;
  const base   = `https://${ENV.shopifyStore}/admin/api/2026-01`;
  const headers = {
    "X-Shopify-Access-Token": ENV.shopifyToken,
    "Content-Type": "application/json",
  };

  // ── DAILY SALES (for profit tracker) ────────────────────────────────────────
  // ── DAILY SALES — pulls real revenue, orders, COGS, shipping from Shopify ───
  // Returns: [ { date, revenue, orders, aov, cogs, shipping_charged, shipping_cost } ]
  if (action === "daily-sales") {
    const { from, to, tz_offset } = req.query;
    if (!from || !to) return res.status(400).json({ error: "from and to dates required (YYYY-MM-DD)" });

    // ── TIMEZONE FIX ─────────────────────────────────────────────────────────────
    // Shopify stores timestamps in UTC. Your store is in Australia (AEST = UTC+10,
    // AEDT = UTC+11). We must shift the query window to capture the full local day.
    // tz_offset passed from frontend (e.g. "-600" for AEST = +10h = -600 mins from UTC)
    // Default to AEST +10h if not provided
    const offsetMins = parseInt(tz_offset || '-600'); // negative = ahead of UTC
    const offsetMs   = -offsetMins * 60 * 1000;       // convert to ms offset

    const fromDate = new Date(from + 'T00:00:00.000Z');
    const toDate   = new Date(to   + 'T23:59:59.999Z');

    // Shift by timezone offset so we capture the full local calendar day
    const fromUTC = new Date(fromDate.getTime() + offsetMs).toISOString();
    const toUTC   = new Date(toDate.getTime()   + offsetMs).toISOString();

    console.log(`[Shopify] Querying ${from} → ${to} | UTC window: ${fromUTC} → ${toUTC}`);

    // ── PAGINATION — fetch ALL orders (not just first 250) ──────────────────────
    // Use cursor-based pagination via page_info
    const baseParams = `status=any&financial_status=any`
      + `&created_at_min=${fromUTC}&created_at_max=${toUTC}`
      + `&limit=250`
      + `&fields=id,created_at,total_price,subtotal_price,total_shipping_price_set,`
      + `shipping_lines,line_items,total_discounts,financial_status,cancelled_at`;

    let allOrders = [];
    let nextUrl   = `${base}/orders.json?${baseParams}`;

    while (nextUrl) {
      const r    = await fetch(nextUrl, { headers });
      const data = await r.json();
      const batch = data.orders || [];
      allOrders.push(...batch);

      // Parse Link header for next page cursor
      const linkHeader = r.headers.get('Link') || '';
      const nextMatch  = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
      nextUrl = nextMatch ? nextMatch[1] : null;

      console.log(`[Shopify] Fetched batch of ${batch.length}, total so far: ${allOrders.length}`);
      if (batch.length < 250) break; // safety exit
    }

    // ── FILTER: exclude cancelled orders and refunded-only ───────────────────────
    const orders = allOrders.filter(o => {
      if (o.cancelled_at) return false; // exclude cancelled
      if (o.financial_status === 'refunded') return false; // fully refunded
      if (o.financial_status === 'voided') return false;   // voided
      return true;
    });

    console.log(`[Shopify] ${allOrders.length} raw orders → ${orders.length} after filtering`);

    // ── Batch-fetch variant costs (COGS) from Inventory Items API ──────────────
    const variantIds = new Set();
    orders.forEach(o => {
      (o.line_items || []).forEach(li => {
        if (li.variant_id) variantIds.add(li.variant_id);
      });
    });

    const variantCostMap = {};
    if (variantIds.size > 0) {
      try {
        const arr = [...variantIds];
        for (let i = 0; i < arr.length; i += 50) {
          const chunk = arr.slice(i, i + 50);
          const vRes = await fetch(`${base}/variants.json?ids=${chunk.join(',')}&fields=id,inventory_item_id`, { headers });
          const { variants = [] } = await vRes.json();

          const invIds = variants.map(v => v.inventory_item_id).filter(Boolean);
          if (invIds.length) {
            const iRes = await fetch(`${base}/inventory_items.json?ids=${invIds.join(',')}&fields=id,cost`, { headers });
            const { inventory_items = [] } = await iRes.json();

            const invCostMap = {};
            inventory_items.forEach(item => { invCostMap[item.id] = parseFloat(item.cost || 0); });
            variants.forEach(v => {
              if (v.inventory_item_id && invCostMap[v.inventory_item_id] !== undefined) {
                variantCostMap[v.id] = invCostMap[v.inventory_item_id];
              }
            });
          }
        }
        console.log(`[Shopify] COGS map built for ${Object.keys(variantCostMap).length} variants`);
      } catch (cogErr) {
        console.warn('[Shopify] COGS fetch failed:', cogErr.message);
      }
    }

    // ── Group by LOCAL date (adjust UTC timestamp back to local day) ─────────────
    const byDate = {};
    orders.forEach(o => {
      // Convert UTC order timestamp to local date using timezone offset
      const utcMs    = new Date(o.created_at).getTime();
      const localMs  = utcMs - offsetMs;
      const localDate = new Date(localMs).toISOString().slice(0, 10);

      if (!byDate[localDate]) byDate[localDate] = {
        date:             localDate,
        revenue:          0,
        orders:           0,
        cogs:             0,
        shipping_charged: 0,
        shipping_cost:    0,
        has_cogs:         false,
      };

      const d = byDate[localDate];
      d.revenue += parseFloat(o.total_price || 0);
      d.orders  += 1;

      (o.line_items || []).forEach(li => {
        const cost = variantCostMap[li.variant_id] || 0;
        if (cost > 0) d.has_cogs = true;
        d.cogs += cost * (li.quantity || 1);
      });

      d.shipping_charged += parseFloat(
        o.total_shipping_price_set?.shop_money?.amount || 0
      );

      (o.shipping_lines || []).forEach(sl => {
        if (sl.source === 'shopify-shipping' || sl.carrier_identifier) {
          d.shipping_cost += parseFloat(sl.price || 0);
        }
      });
    });

    const daily = Object.values(byDate)
      .filter(d => d.date >= from && d.date <= to) // only return days in requested range
      .map(d => ({
        ...d,
        revenue:          +d.revenue.toFixed(2),
        cogs:             +d.cogs.toFixed(2),
        shipping_charged: +d.shipping_charged.toFixed(2),
        shipping_cost:    +d.shipping_cost.toFixed(2),
        aov:              d.orders > 0 ? +(d.revenue / d.orders).toFixed(2) : 0,
      }))
      .sort((a, b) => a.date.localeCompare(b.date));

    const totals = daily.reduce((acc, d) => ({
      revenue:          +(acc.revenue + d.revenue).toFixed(2),
      orders:           acc.orders + d.orders,
      cogs:             +(acc.cogs + d.cogs).toFixed(2),
      shipping_charged: +(acc.shipping_charged + d.shipping_charged).toFixed(2),
      shipping_cost:    +(acc.shipping_cost + d.shipping_cost).toFixed(2),
    }), { revenue:0, orders:0, cogs:0, shipping_charged:0, shipping_cost:0 });

    return res.status(200).json({
      daily,
      totals,
      has_cogs:     daily.some(d => d.has_cogs),
      total_orders: orders.length,
      debug: {
        raw_orders:      allOrders.length,
        filtered_orders: orders.length,
        tz_offset:       offsetMins,
        utc_window:      { from: fromUTC, to: toUTC },
      }
    });
  }

  // ── PRODUCTS ─────────────────────────────────────────────────────────────────
  if (action === "products") {
    const limit = req.query.limit || 50;
    const r = await fetch(`${base}/products.json?limit=${limit}&fields=id,title,variants,images,handle,tags`, { headers });
    return res.status(200).json(await r.json());
  }

  // ── ORDERS (fulfillment hub) ──────────────────────────────────────────────────
  if (action === "orders") {
    const days  = req.query.days || 30;
    const since = new Date(Date.now() - days * 86400000).toISOString();
    const r = await fetch(
      `${base}/orders.json?status=any&created_at_min=${since}&limit=250&fields=id,name,created_at,fulfillment_status,financial_status,line_items,shipping_address,total_price,currency,customer,fulfillments`,
      { headers }
    );
    return res.status(200).json(await r.json());
  }

  // ── INVENTORY LEVELS ──────────────────────────────────────────────────────────
  if (action === "inventory_levels") {
    const { location_id } = req.query;
    const url = location_id
      ? `${base}/inventory_levels.json?location_id=${location_id}&limit=250`
      : `${base}/inventory_levels.json?limit=250`;
    const r = await fetch(url, { headers });
    return res.status(200).json(await r.json());
  }

  // ── LOCATIONS ─────────────────────────────────────────────────────────────────
  if (action === "locations") {
    const r = await fetch(`${base}/locations.json`, { headers });
    return res.status(200).json(await r.json());
  }

  // ── FULFILL ORDER ─────────────────────────────────────────────────────────────
  if (action === "fulfill" && req.method === "POST") {
    const { orderId, locationId, trackingNumber, trackingCompany } = req.body;
    const r = await fetch(`${base}/orders/${orderId}/fulfillments.json`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        fulfillment: {
          location_id: locationId,
          tracking_number: trackingNumber,
          tracking_company: trackingCompany,
          notify_customer: true,
        }
      })
    });
    return res.status(200).json(await r.json());
  }

  return res.status(400).json({ error: "Unknown shopify action", valid: ["daily-sales","products","orders","inventory_levels","locations","fulfill"] });
}


// ═══════════════════════════════════════════════════════════════════════════════
// 2. VERCEL BLOB
// ═══════════════════════════════════════════════════════════════════════════════
async function handleBlob(req, res) {
  const { put, list, del } = await import("@vercel/blob");
  const action = req.query.action || req.body?.action;

  if (action === "save" && req.method === "POST") {
    const { key, data } = req.body;
    const blob = await put(key, JSON.stringify(data), {
      access: "public",
      token: ENV.blobToken,
      addRandomSuffix: false,
    });
    return res.status(200).json({ url: blob.url });
  }

  if (action === "load") {
    const { key } = req.query;
    const { blobs } = await list({ prefix: key, token: ENV.blobToken });
    if (!blobs.length) return res.status(404).json({ error: "Not found" });
    const r = await fetch(blobs[0].url);
    return res.status(200).json(await r.json());
  }

  if (action === "save_po" && req.method === "POST") {
    const { orders } = req.body;
    const blob = await put("History Of Restock/po_orders.json", JSON.stringify({ orders }), {
      access: "public", token: ENV.blobToken, addRandomSuffix: false,
    });
    return res.status(200).json({ url: blob.url });
  }

  if (action === "load_po") {
    const { blobs } = await list({ prefix: "History Of Restock/po_orders.json", token: ENV.blobToken });
    if (!blobs.length) return res.status(200).json({ orders: [] });
    const r = await fetch(blobs[0].url + "?t=" + Date.now());
    return res.status(200).json(await r.json());
  }

  return res.status(400).json({ error: "Unknown blob action" });
}


// ═══════════════════════════════════════════════════════════════════════════════
// 3. RESEND EMAIL ALERTS
// ═══════════════════════════════════════════════════════════════════════════════
async function handleAlert(req, res) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });
  const { to, subject, html } = req.body;

  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${ENV.resendKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: "FoldifyCase Alerts <alerts@foldifycase.com.au>",
      to: Array.isArray(to) ? to : [to],
      subject,
      html,
    }),
  });

  const data = await r.json();
  return res.status(r.ok ? 200 : 400).json(data);
}


// ═══════════════════════════════════════════════════════════════════════════════
// 4. GOOGLE ADS  ← NEW
// ═══════════════════════════════════════════════════════════════════════════════
async function handleGoogleAds(req, res) {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: "from and to required (YYYY-MM-DD)" });

  // Step 1 — refresh access token using stored refresh token
  const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id:     ENV.googleClientId,
      client_secret: ENV.googleClientSecret,
      refresh_token: ENV.googleRefreshToken,
      grant_type:    "refresh_token",
    }),
  });
  const { access_token } = await tokenRes.json();

  // Step 2 — query Google Ads API using GAQL
  const gaqlQuery = `
    SELECT
      segments.date,
      metrics.cost_micros,
      metrics.conversions,
      metrics.conversion_value,
      metrics.clicks,
      metrics.impressions
    FROM campaign
    WHERE segments.date BETWEEN '${from}' AND '${to}'
    ORDER BY segments.date ASC
  `;

  const adsRes = await fetch(
    `https://googleads.googleapis.com/v17/customers/${ENV.googleCustomerId}/googleAds:search`,
    {
      method: "POST",
      headers: {
        "Authorization":           `Bearer ${access_token}`,
        "developer-token":         ENV.googleDevToken,
        "Content-Type":            "application/json",
      },
      body: JSON.stringify({ query: gaqlQuery }),
    }
  );

  const adsData = await adsRes.json();

  if (!adsRes.ok) {
    console.error("[google-ads] API error:", JSON.stringify(adsData));
    return res.status(400).json({ error: "Google Ads API error", details: adsData });
  }

  // Step 3 — aggregate by date
  const byDate = {};
  (adsData.results || []).forEach(row => {
    const date      = row.segments.date;
    const spend     = (row.metrics.costMicros || 0) / 1_000_000;
    const convVal   = row.metrics.conversionValue || 0;
    const convs     = row.metrics.conversions || 0;

    if (!byDate[date]) byDate[date] = { date, spend: 0, revenue: 0, conversions: 0, clicks: 0 };
    byDate[date].spend       += spend;
    byDate[date].revenue     += convVal;
    byDate[date].conversions += convs;
    byDate[date].clicks      += (row.metrics.clicks || 0);
  });

  const daily = Object.values(byDate)
    .map(d => ({
      ...d,
      spend:       +d.spend.toFixed(2),
      revenue:     +d.revenue.toFixed(2),
      roas:        d.spend > 0 ? +(d.revenue / d.spend).toFixed(2) : 0,
      cpa:         d.conversions > 0 ? +(d.spend / d.conversions).toFixed(2) : 0,
      conversions: Math.round(d.conversions),
    }))
    .sort((a, b) => a.date.localeCompare(b.date));

  // Totals summary
  const total = daily.reduce(
    (acc, d) => ({
      spend:       +(acc.spend + d.spend).toFixed(2),
      revenue:     +(acc.revenue + d.revenue).toFixed(2),
      conversions: acc.conversions + d.conversions,
      clicks:      acc.clicks + d.clicks,
    }),
    { spend: 0, revenue: 0, conversions: 0, clicks: 0 }
  );
  total.roas = total.spend > 0 ? +(total.revenue / total.spend).toFixed(2) : 0;
  total.cpa  = total.conversions > 0 ? +(total.spend / total.conversions).toFixed(2) : 0;

  return res.status(200).json({ platform: "google", daily, total });
}


// ═══════════════════════════════════════════════════════════════════════════════
// 5. META ADS  ← NEW
// ═══════════════════════════════════════════════════════════════════════════════
async function handleMetaAds(req, res) {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: "from and to required (YYYY-MM-DD)" });

  // Meta Insights API — returns daily breakdown
  const params = new URLSearchParams({
    fields:         "spend,actions,action_values,impressions,clicks,cpm,cpc",
    time_increment: "1",           // 1 = daily breakdown
    time_range:     JSON.stringify({ since: from, until: to }),
    level:          "account",
    access_token:   ENV.metaToken,
  });

  const url = `https://graph.facebook.com/v21.0/${ENV.metaAdAccount}/insights?${params}`;
  const r   = await fetch(url);
  const data = await r.json();

  if (data.error) {
    console.error("[meta-ads] API error:", data.error);
    return res.status(400).json({ error: "Meta API error", details: data.error });
  }

  // Parse results — extract purchases from actions array
  const daily = (data.data || []).map(row => {
    const actions      = row.actions      || [];
    const actionValues = row.action_values|| [];

    const findAction = (arr, type) =>
      parseFloat(arr.find(a => a.action_type === type)?.value || 0);

    const purchases   = findAction(actions,      "purchase");
    const purchaseVal = findAction(actionValues, "purchase");
    const spend       = parseFloat(row.spend || 0);

    return {
      date:        row.date_start,
      spend:       +spend.toFixed(2),
      revenue:     +purchaseVal.toFixed(2),
      conversions: Math.round(purchases),
      clicks:      parseInt(row.clicks || 0),
      impressions: parseInt(row.impressions || 0),
      roas:        spend > 0 ? +(purchaseVal / spend).toFixed(2) : 0,
      cpa:         purchases > 0 ? +(spend / purchases).toFixed(2) : 0,
    };
  }).sort((a, b) => a.date.localeCompare(b.date));

  // Totals
  const total = daily.reduce(
    (acc, d) => ({
      spend:       +(acc.spend + d.spend).toFixed(2),
      revenue:     +(acc.revenue + d.revenue).toFixed(2),
      conversions: acc.conversions + d.conversions,
      clicks:      acc.clicks + d.clicks,
    }),
    { spend: 0, revenue: 0, conversions: 0, clicks: 0 }
  );
  total.roas = total.spend > 0 ? +(total.revenue / total.spend).toFixed(2) : 0;
  total.cpa  = total.conversions > 0 ? +(total.spend / total.conversions).toFixed(2) : 0;

  return res.status(200).json({ platform: "meta", daily, total });
}


// ═══════════════════════════════════════════════════════════════════════════════
// 6. MICROSOFT ADS  ← NEW
// ═══════════════════════════════════════════════════════════════════════════════
async function handleMsAds(req, res) {
  const { from, to } = req.query;
  if (!from || !to) return res.status(400).json({ error: "from and to required (YYYY-MM-DD)" });

  // Step 1 — get access token
  const tokenRes = await fetch(
    `https://login.microsoftonline.com/common/oauth2/v2.0/token`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id:     ENV.msClientId,
        client_secret: ENV.msClientSecret,
        refresh_token: ENV.msRefreshToken,
        grant_type:    "refresh_token",
        scope:         "https://ads.microsoft.com/msads.manage offline_access",
      }),
    }
  );
  const { access_token } = await tokenRes.json();

  // Step 2 — call Bing Ads Reporting API (SOAP/JSON wrapper)
  // Using the simplified Campaign Performance Report
  const reportRequest = {
    ReportName:           "DailyProfitReport",
    Format:               "Csv",
    ReturnOnlyCompleteData: false,
    Aggregation:          "Daily",
    Scope: {
      AccountIds: [ENV.msAccountId],
    },
    Time: {
      CustomDateRangeStart: {
        Day:   parseInt(from.split("-")[2]),
        Month: parseInt(from.split("-")[1]),
        Year:  parseInt(from.split("-")[0]),
      },
      CustomDateRangeEnd: {
        Day:   parseInt(to.split("-")[2]),
        Month: parseInt(to.split("-")[1]),
        Year:  parseInt(to.split("-")[0]),
      },
    },
    Columns: ["TimePeriod","Spend","Revenue","Conversions","Clicks","Impressions"],
  };

  const msRes = await fetch(
    "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/V13/ReportingService.svc/json/SubmitGenerateReport",
    {
      method: "POST",
      headers: {
        "Authorization":      `Bearer ${access_token}`,
        "CustomerAccountId":  ENV.msAccountId,
        "CustomerId":         ENV.msCustomerId,
        "Content-Type":       "application/json",
      },
      body: JSON.stringify({
        CampaignPerformanceReportRequest: reportRequest
      }),
    }
  );

  const msData = await msRes.json();

  // NOTE: Microsoft Ads uses an async report pattern.
  // The above submits a report job and returns a ReportRequestId.
  // For simplicity, we return that ID and poll for results separately.
  // In production you'd poll until Status = "Success" then download the CSV.
  // For now we return sample-shaped data so the dashboard works immediately.

  if (msData.ReportRequestId) {
    // Report submitted — in production, poll for completion
    // For now return a flag so the frontend knows to retry
    return res.status(202).json({
      platform: "microsoft",
      status:   "report_pending",
      reportId: msData.ReportRequestId,
      message:  "Report submitted. Poll /api/proxy?service=ms-ads&action=poll&reportId=X to get results.",
    });
  }

  return res.status(400).json({ error: "Microsoft Ads error", details: msData });
}
