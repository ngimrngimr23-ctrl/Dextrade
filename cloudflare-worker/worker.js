/**
 * Прокси для бота Dextrade: Steam Community Market и CSFloat Market API.
 *
 * Зачем нужен: у Render датацентровый IP. Для CSFloat это подтверждённая
 * проблема — площадка сама отвечает 429 с телом "Please disable your VPN or
 * try a different network", то есть режет именно по адресу. Воркер стучится
 * с edge-IP Cloudflare, и это попытка обойти ровно её.
 *
 * ЧЕСТНАЯ ОГОВОРКА про Steam: раньше здесь было написано, что воркер нужен,
 * потому что "Render банит IP датацентра". Это оказалось неверно. Реальной
 * причиной проблем со Steam была бета-версия торговой площадки на аккаунте
 * (легаси-эндпоинт /render/ отдавал HTML вместо JSON), а починила её кука
 * bMarketOptOut=1. Прокси там не решал ничего и сейчас в боте не используется
 * (STEAM_PROXY_URL пуст). Steam-ветка оставлена рабочей на случай, если
 * понадобится, но не выдавай её за решённую задачу.
 *
 * Использование:
 *   GET https://<воркер>.workers.dev/proxy?url=<целевой URL>
 *   GET https://<воркер>.workers.dev/proxy?url=<...>&debug=1   — ручная проверка
 *   GET https://<воркер>.workers.dev/                          — health-check
 *
 * Разрешены ТОЛЬКО хосты из ALLOWED_HOSTS — иначе воркер превращается в
 * открытый прокси на что угодно, чего мы не хотим.
 */

const ALLOWED_HOSTS = ["steamcommunity.com", "csfloat.com"];

// Заголовки ответа, которые НЕЛЬЗЯ пересылать как есть: тело мы уже прочитали
// через arrayBuffer(), то есть оно распаковано, и старые content-encoding /
// content-length описывали бы сжатый оригинал — клиент на этом ломается.
const SKIP_RESPONSE_HEADERS = new Set([
  "content-encoding",
  "content-length",
  "transfer-encoding",
  "connection",
]);

/**
 * Заголовки к Steam: имитируем AJAX штатного фронтенда со страницы предмета.
 * Referer — на страницу самого предмета (без /render/ и query), как если бы
 * запрос ушёл со страницы листингов внутри браузера.
 *
 * ВАЖНО: Cookie пересылаем. Без этого до Steam не доезжает bMarketOptOut=1 —
 * ровно та кука, которая и чинит выдачу JSON вместо страницы бета-маркета.
 * Прежняя версия воркера её роняла, так что включённый STEAM_PROXY_URL молча
 * ломал бы то, что бот чинит на своей стороне.
 */
function buildSteamHeaders(targetUrl, incomingRequest) {
  const refererUrl = new URL(targetUrl.toString());
  refererUrl.pathname = refererUrl.pathname.replace(/\/render\/?$/, "");
  refererUrl.search = "";

  const headers = {
    "User-Agent": "Mozilla/5.0",
    Accept: "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
    Referer: refererUrl.toString(),
  };

  const cookie = incomingRequest.headers.get("Cookie");
  if (cookie) headers["Cookie"] = cookie;

  return headers;
}

/**
 * Заголовки к CSFloat: честный API-клиент, ровно как curl из их документации.
 *
 * НИКАКОЙ имитации браузера — ни Origin, ни Referer, ни Sec-Fetch-*. На проде
 * это уже проверено трижды: обрезанный Mozilla/5.0, полный набор заголовков
 * Chrome и честный клиент дали идентичный 429. Заголовки там ни на что не
 * влияют, а имитация браузера с не-браузерным TLS-отпечатком только выглядит
 * подозрительнее.
 *
 * User-Agent и Authorization берём из запроса бота, чтобы их источником
 * оставался бот (CSFLOAT_USER_AGENT / CSFLOAT_API_KEY), а не этот файл.
 */
function buildCsfloatHeaders(targetUrl, incomingRequest) {
  const headers = {
    "User-Agent": incomingRequest.headers.get("User-Agent") || "Dextrade/1.0",
    Accept: "application/json",
  };

  // GET /v1/listings по документации ключа не требует, но эндпоинты, которые
  // требуют, есть — пересылаем, если бот прислал.
  const auth = incomingRequest.headers.get("Authorization");
  if (auth) headers["Authorization"] = auth;

  return headers;
}

const HOST_HANDLERS = {
  "steamcommunity.com": buildSteamHeaders,
  "csfloat.com": buildCsfloatHeaders,
};

export default {
  async fetch(request) {
    const incoming = new URL(request.url);

    if (incoming.pathname === "/" || incoming.pathname === "") {
      return new Response("ok", { status: 200 });
    }

    if (incoming.pathname !== "/proxy") {
      return new Response("not found", { status: 404 });
    }

    const target = incoming.searchParams.get("url");
    if (!target) {
      return new Response('missing "url" query param', { status: 400 });
    }

    let targetUrl;
    try {
      targetUrl = new URL(target);
    } catch {
      return new Response("bad url", { status: 400 });
    }

    if (!ALLOWED_HOSTS.includes(targetUrl.hostname)) {
      return new Response(`host not allowed: ${targetUrl.hostname}`, { status: 403 });
    }

    const buildHeaders = HOST_HANDLERS[targetUrl.hostname];
    const upstreamResp = await fetch(targetUrl.toString(), {
      headers: buildHeaders(targetUrl, request),
      // не кэшируем — цены и листинги живые
      cf: { cacheTtl: 0, cacheEverything: false },
    });

    const body = await upstreamResp.arrayBuffer();

    // Диагностический режим для ручной проверки в браузере — не влияет на
    // обычную работу бота (тот debug=1 никогда не передаёт).
    if (incoming.searchParams.get("debug") === "1") {
      const decoder = new TextDecoder("utf-8", { fatal: false });
      const bodyText = decoder.decode(body);
      return new Response(
        JSON.stringify(
          {
            upstream_status: upstreamResp.status,
            upstream_host: targetUrl.hostname,
            upstream_content_type: upstreamResp.headers.get("Content-Type"),
            upstream_headers: Object.fromEntries(upstreamResp.headers),
            requested_url: targetUrl.toString(),
            body_snippet: bodyText.slice(0, 500),
          },
          null,
          2
        ),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // Пересылаем заголовки ответа как есть. Раньше воркер оставлял только
    // Content-Type, и это ломало диагностику на стороне бота: он отличает
    // настоящую квоту от блока по IP именно по наличию Retry-After и
    // X-RateLimit-*. Без них любой 429 выглядел бы как блок по IP.
    const outHeaders = new Headers();
    for (const [key, value] of upstreamResp.headers) {
      if (!SKIP_RESPONSE_HEADERS.has(key.toLowerCase())) {
        outHeaders.set(key, value);
      }
    }
    if (!outHeaders.has("Content-Type")) {
      outHeaders.set("Content-Type", "application/json");
    }
    // Чтобы в логах бота было видно, что ответ прошёл через прокси и от кого
    // он на самом деле: иначе ошибка воркера и ошибка площадки неразличимы.
    outHeaders.set("X-Proxy-Upstream", targetUrl.hostname);

    return new Response(body, {
      status: upstreamResp.status,
      headers: outHeaders,
    });
  },
};
