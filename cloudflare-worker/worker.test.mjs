import worker from "./worker.js";

let captured = null;
globalThis.fetch = async (url, init) => {
  captured = { url, headers: init.headers };
  return new Response(JSON.stringify([{ id: "1" }]), {
    status: 429,
    headers: {
      "Content-Type": "application/json",
      "Retry-After": "30",
      "X-RateLimit-Remaining": "0",
      "Server": "cloudflare",
      "CF-RAY": "abc-PDX",
      "Content-Encoding": "gzip",     // должен быть отброшен
      "Content-Length": "9999",       // должен быть отброшен
    },
  });
};

const W = "https://w.workers.dev";
const call = (path, headers = {}) =>
  worker.fetch(new Request(W + path, { headers }));

let fails = 0;
const ok = (cond, msg) => { console.log((cond ? "  ✓ " : "  ✗ ") + msg); if (!cond) fails++; };

console.log("health-check и маршрутизация:");
ok((await call("/")).status === 200, "/ отдаёт 200");
ok((await call("/nope")).status === 404, "/nope отдаёт 404");
ok((await call("/proxy")).status === 400, "/proxy без url отдаёт 400");
ok((await call("/proxy?url=нетURL")).status === 400, "битый url отдаёт 400");

console.log("\nбелый список хостов:");
let r = await call("/proxy?url=" + encodeURIComponent("https://evil.example.com/x"));
ok(r.status === 403 && (await r.text()).includes("host not allowed"), "чужой хост отбит 403");
ok((await call("/proxy?url=" + encodeURIComponent("https://csfloat.com/api/v1/listings"))).status === 429,
   "csfloat.com пропущен (был 403 — это и был баг)");
ok((await call("/proxy?url=" + encodeURIComponent("https://steamcommunity.com/market/listings/730/X/render/"))).status === 429,
   "steamcommunity.com по-прежнему пропущен");

console.log("\nзаголовки К CSFloat (честный клиент, без имитации браузера):");
await call("/proxy?url=" + encodeURIComponent("https://csfloat.com/api/v1/listings?limit=50"),
           { "User-Agent": "Dextrade/1.0", "Authorization": "SECRETKEY" });
ok(captured.url === "https://csfloat.com/api/v1/listings?limit=50", "целевой URL с query сохранён");
ok(captured.headers["Authorization"] === "SECRETKEY", "Authorization переслан");
ok(captured.headers["User-Agent"] === "Dextrade/1.0", "User-Agent взят из запроса бота");
const bad = ["Origin", "Referer", "Sec-Fetch-Mode", "sec-ch-ua", "X-Requested-With"];
ok(!bad.some(h => h in captured.headers), "нет заголовков имитации браузера: " + bad.join(", "));

console.log("\nзаголовки К Steam (AJAX-имитация сохранена + Cookie):");
await call("/proxy?url=" + encodeURIComponent("https://steamcommunity.com/market/listings/730/AK/render/?start=0"),
           { "Cookie": "bMarketOptOut=1" });
ok(captured.headers["X-Requested-With"] === "XMLHttpRequest", "X-Requested-With на месте");
ok(captured.headers["Referer"] === "https://steamcommunity.com/market/listings/730/AK", "Referer без /render/ и query");
ok(captured.headers["Cookie"] === "bMarketOptOut=1", "Cookie переслан (раньше терялся — ломало bMarketOptOut)");

console.log("\nзаголовки ОТВЕТА обратно боту:");
r = await call("/proxy?url=" + encodeURIComponent("https://csfloat.com/api/v1/listings"));
ok(r.status === 429, "статус upstream сохранён");
ok(r.headers.get("Retry-After") === "30", "Retry-After доехал (раньше терялся)");
ok(r.headers.get("X-RateLimit-Remaining") === "0", "X-RateLimit-* доехал (раньше терялся)");
ok(r.headers.get("CF-RAY") === "abc-PDX", "CF-RAY доехал");
ok(r.headers.get("X-Proxy-Upstream") === "csfloat.com", "видно, что ответ через прокси и от кого");
ok(!r.headers.get("Content-Encoding"), "Content-Encoding отброшен (тело уже распаковано)");
ok(!r.headers.get("Content-Length"), "Content-Length отброшен");

console.log("\ndebug=1:");
r = await call("/proxy?url=" + encodeURIComponent("https://csfloat.com/api/v1/listings") + "&debug=1");
const d = await r.json();
ok(r.status === 200 && d.upstream_status === 429, "debug отдаёт 200 с настоящим статусом внутри");
ok(d.upstream_headers["retry-after"] === "30", "debug показывает все заголовки upstream");

console.log(fails === 0 ? "\nВСЕ ПРОВЕРКИ ПРОЙДЕНЫ" : `\nПРОВАЛЕНО: ${fails}`);
process.exit(fails === 0 ? 0 : 1);
