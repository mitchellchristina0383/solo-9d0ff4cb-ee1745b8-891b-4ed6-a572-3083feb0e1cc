const $ = (id) => document.getElementById(id);

const state = {
  globalTxid: 7,
};

function spec(prefix, txidOverride) {
  return {
    address: Number($(`${prefix}-address`).value),
    quantity: Number($(`${prefix}-quantity`).value),
    transaction_id: txidOverride,
    unit_id: Number($(`${prefix}-unit`).value),
  };
}

function renderResult(channel, result, expectedTxid) {
  const card = $(`result-${channel}`).querySelector(".body");
  const txidLabel = $(`txid-${channel}`);
  if (result.ok) {
    card.innerHTML = `
      <div class="value">${result.registers.join(", ")}</div>
      <div class="meta">事务号 <b>${result.transaction_id}</b> · 从站 ${result.unit_id} ·
        ${result.registers.length} 个寄存器 · ${result.elapsed_ms} ms</div>`;
    txidLabel.textContent = `事务号 0x${Number(expectedTxid).toString(16).padStart(4, "0").toUpperCase()}`;
  } else {
    const code = result.exception_code != null
      ? ` · 异常码 0x${Number(result.exception_code).toString(16).padStart(2, "0").toUpperCase()} (${result.exception_name || ""})`
      : "";
    card.innerHTML = `
      <div class="value error">${result.error || "读取失败"}</div>
      <div class="meta">${result.elapsed_ms != null ? result.elapsed_ms + " ms" : ""}${code}</div>`;
    txidLabel.textContent = expectedTxid != null
      ? `事务号 0x${Number(expectedTxid).toString(16).padStart(4, "0").toUpperCase()}`
      : "";
  }
}

function markPending(channel) {
  $(`result-${channel}`).querySelector(".body").innerHTML =
    `<div class="value pending">读取中…</div>`;
}

async function readBoth() {
  const button = $("read-both");
  button.disabled = true;
  try {
    const same = $("same-txid").checked;
    const txidA = Number($("a-txid").value);
    const txidB = same ? txidA : Number($("b-txid").value);
    $("b-txid").value = txidB;
    const reads = [spec("a", txidA), spec("b", txidB)];

    markPending("a");
    markPending("b");
    const started = performance.now();
    const response = await fetch("/api/read_both", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({reads}),
    });
    const payload = await response.json();
    const results = payload.reads || [];
    renderResult("a", results[0] || {ok: false, error: "无结果"}, txidA);
    renderResult("b", results[1] || {ok: false, error: "无结果"}, txidB);
    $("control-status").textContent =
      `两路读取已返回，墙钟耗时 ${Math.round(performance.now() - started)} ms`;
  } catch (err) {
    renderResult("a", {ok: false, error: String(err)}, null);
    renderResult("b", {ok: false, error: String(err)}, null);
  } finally {
    button.disabled = false;
    refreshStats();
  }
}

async function meterAction(action, value, button) {
  const previous = button.textContent;
  button.disabled = true;
  button.textContent = "…";
  try {
    let payload = {};
    if (action === "delay") {
      payload = {ms: value !== undefined ? value : $("delay-ms").value};
    } else if (action === "mode") {
      payload = {value};
    } else if (action === "offline") {
      payload = {value};
    } else if (action === "stall") {
      payload = {count: 1, ms: $("stall-ms").value};
    }
    const response = await fetch(`/api/meter/${action}`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      $("control-status").textContent = `控制失败：${data.error || response.status}`;
    } else {
      const settings = data.settings || {};
      $("control-status").textContent =
        `已应用：延迟 ${settings.delay_ms} ms · 顺序 ${settings.mode} · ` +
        `仪表${settings.offline ? "离线" : "在线"} · 剩余卡住次数 ${settings.stall_next}`;
      if (settings.mode) $("mode-label").textContent = settings.mode;
    }
  } catch (err) {
    $("control-status").textContent = `控制失败：${err}`;
  } finally {
    button.disabled = false;
    button.textContent = previous;
    refreshStats();
  }
}

function renderDl(element, rows) {
  element.innerHTML = rows
    .map(([key, value, cls]) =>
      `<dt>${key}</dt><dd class="${cls || ""}">${value}</dd>`)
    .join("");
}

async function refreshStats() {
  try {
    const gateway = await fetch("/api/gateway/state").then((r) => r.json());
    if (gateway.ok === false) throw new Error(gateway.error || "gateway unavailable");
    renderDl($("gateway-stats"), [
      ["仪表连接", gateway.upstream_connected ? "已连接" : "断开",
        gateway.upstream_connected ? "ok" : "bad"],
      ["在途读取", gateway.inflight],
      ["累计请求", gateway.requests_total],
      ["成功回包", gateway.responses_total],
      ["超时", gateway.timeouts_total, gateway.timeouts_total ? "bad" : ""],
      ["丢弃迟到回包", gateway.late_responses_dropped,
        gateway.late_responses_dropped ? "bad" : ""],
      ["仪表异常转发", gateway.upstream_exceptions_relayed],
      ["仪表重连次数", gateway.upstream_connects],
      ["最近错误", gateway.last_upstream_error || "—"],
    ]);
    $("banner").textContent = gateway.upstream_connected
      ? `网关在线 · 仪表连接正常 · 在途 ${gateway.inflight}`
      : "网关在线 · 仪表断开（重连中）";
    $("banner").className = "banner " + (gateway.upstream_connected ? "up" : "down");
  } catch (err) {
    $("banner").textContent = "无法读取网关状态";
    $("banner").className = "banner down";
  }

  try {
    const meter = await fetch("/api/meter/state").then((r) => r.json());
    if (meter.ok === false) throw new Error(meter.error || "meter unavailable");
    const settings = meter.settings || {};
    const txids = (meter.received || [])
      .slice(-6)
      .map((entry) => `0x${Number(entry.txid).toString(16).padStart(4, "0").toUpperCase()}`)
      .join(" ");
    renderDl($("meter-stats"), [
      ["仪表在线", settings.offline ? "离线" : "在线", settings.offline ? "bad" : "ok"],
      ["接入客户端", meter.connected ? "是" : "否", meter.connected ? "ok" : "bad"],
      ["延迟", `${settings.delay_ms} ms`],
      ["回包顺序", settings.mode],
      ["剩余卡住", settings.stall_next],
      ["累计接受连接", meter.connections_accepted],
      ["拒绝的额外连接", meter.connections_rejected],
      ["instrument txids", txids || "—"],
    ]);
    if (settings.mode) $("mode-label").textContent = settings.mode;
  } catch (err) {
    renderDl($("meter-stats"), [["状态", "不可用", "bad"]]);
  }
}

$("read-both").addEventListener("click", readBoth);
$("same-txid").addEventListener("change", (event) => {
  $("b-txid").disabled = event.target.checked;
  if (event.target.checked) $("b-txid").value = $("a-txid").value;
});
$("a-txid").addEventListener("input", () => {
  if ($("same-txid").checked) $("b-txid").value = $("a-txid").value;
});
$("b-txid").disabled = $("same-txid").checked;
document.querySelectorAll("button[data-action]").forEach((button) => {
  button.addEventListener("click", () =>
    meterAction(button.dataset.action, button.dataset.value, button));
});

refreshStats();
setInterval(refreshStats, 2000);
