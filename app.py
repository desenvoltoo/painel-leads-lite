// static/js/app.js
// V14 — compatível com:
//   GET  /api/options  -> { ok:true, data:{ status:[], cursos:[], polos:[], consultores:[] } }
//   GET  /api/leads    -> { ok:true, total:N, data:[...] }
//   GET  /api/kpis     -> { ok:true, total:N, top_status:{status,cnt} }
//   POST /api/upload   -> { ok:true, message:"..." }
//   GET  /api/export   -> CSV (download)
//
// HTML base (ids):
// upload: #uploadFile #uploadSource #btnUpload #uploadStatus
// filtros: #fStatus #fCurso #fPolo #fConsultor #fIni #fFim #fLimit #fBusca
// ações: #btnApply #btnClear #btnReload #btnExport
// kpis: #kpiCount #kpiTopStatus
// tabela: #tbl tbody
// labels: #statusLine #lblTotal

const $ = (sel) => document.querySelector(sel);

let tsStatus, tsCurso, tsPolo, tsConsultor;

/* =========================
   Helpers UI
========================= */
function setStatus(msg, type = "ok") {
  const el = $("#statusLine");
  if (!el) return;
  el.textContent = msg;
  el.className = type === "err" ? "error" : "";
}

function setUploadStatus(msg, type = "ok") {
  const el = $("#uploadStatus");
  if (!el) return;
  el.textContent = msg;
  el.className = type === "err" ? "error" : "muted";
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str).replace(/[&<>"']/g, (m) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[m]));
}

function fmtDate(d) {
  if (!d || d === "None") return "-";
  const s = String(d);
  const datePart = s.slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
    return datePart.split("-").reverse().join("/");
  }
  return s;
}

/* =========================
   API
========================= */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;

    if (Array.isArray(v)) {
      if (v.length === 0) return;
      url.searchParams.set(k, v.join(" || "));
      return;
    }

    const s = String(v).trim();
    if (!s) return;
    url.searchParams.set(k, s);
  });

  const res = await fetch(url.toString(), { cache: "no-store" });
  const data = await res.json().catch(() => ({}));

  if (!res.ok) throw new Error(data?.error || data?.message || "Erro na API");
  return data;
}

async function apiPostForm(path, formData) {
  const url = new URL(path, window.location.origin);
  const res = await fetch(url.toString(), { method: "POST", body: formData });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || data?.message || "Erro na API");
  return data;
}

/* =========================
   TomSelect (multi + checkbox)
========================= */
function makeTomSelect(selector) {
  const el = $(selector);
  if (!el) return null;

  const pluginCheckbox = window.__TOMSELECT_PLUGINS__?.checkbox || "checkbox_options";
  const pluginRemove = window.__TOMSELECT_PLUGINS__?.remove_button || "remove_button";

  return new TomSelect(selector, {
    plugins: [pluginCheckbox, pluginRemove],
    maxItems: null,
    hideSelected: false,
    closeAfterSelect: false,
    persist: false,
    create: false,
    valueField: "value",
    labelField: "text",
    searchField: ["text"],
    onChange: () => loadLeadsAndKpisDebounced()
  });
}

function initMultiSelects() {
  tsStatus = makeTomSelect("#fStatus");
  tsCurso = makeTomSelect("#fCurso");
  tsPolo = makeTomSelect("#fPolo");
  tsConsultor = makeTomSelect("#fConsultor");
}

/* =========================
   Options (dims)
========================= */
function fillSelect(ts, values) {
  if (!ts) return;
  ts.clearOptions();
  ts.clear(true);

  (values || []).forEach((v) => {
    const s = String(v);
    ts.addOption({ value: s, text: s });
  });

  ts.refreshOptions(false);
}

async function loadOptions() {
  try {
    const resp = await apiGet("/api/options");
    const data = resp?.data || resp;

    fillSelect(tsStatus, data?.status || []);
    fillSelect(tsCurso, data?.cursos || []);
    fillSelect(tsPolo, data?.polos || []);
    fillSelect(tsConsultor, data?.consultores || []);
  } catch (e) {
    console.error("Erro ao carregar opções:", e);
    setStatus("Falha ao carregar filtros (options).", "err");
  }
}

/* =========================
   Leads + KPIs
========================= */
function getMulti(ts) {
  if (!ts) return [];
  const v = ts.getValue();
  if (Array.isArray(v)) return v;
  if (!v) return [];
  return String(v).split(",").map(s => s.trim()).filter(Boolean);
}

function parseBuscaRapida(txt) {
  const t = (txt || "").trim();
  if (!t) return {};
  if (t.includes("@")) return { email: t };

  const onlyDigits = t.replace(/\D/g, "");
  if (onlyDigits.length === 11) return { cpf: onlyDigits };
  if (onlyDigits.length >= 10) return { celular: onlyDigits };

  return { nome: t };
}

function buildLeadsParams() {
  const status = getMulti(tsStatus);
  const cursos = getMulti(tsCurso);
  const polos = getMulti(tsPolo);
  const consultores = getMulti(tsConsultor);

  const data_ini = $("#fIni")?.value || "";
  const data_fim = $("#fFim")?.value || "";
  const limit = $("#fLimit")?.value || 500;

  const busca = parseBuscaRapida($("#fBusca")?.value);

  return {
    status,
    curso: cursos,
    polo: polos,
    consultor: consultores,
    data_ini,
    data_fim,
    limit,
    ...busca
  };
}

async function loadLeadsAndKpis() {
  setStatus("Consultando BigQuery...", "ok");

  const params = buildLeadsParams();

  try {
    const [leadsResp, kpisResp] = await Promise.all([
      apiGet("/api/leads", params),
      apiGet("/api/kpis", params),
    ]);

    const rows = leadsResp?.data || [];
    const total = leadsResp?.total ?? rows.length;

    renderTable(rows);
    renderTotals(total, rows.length);
    renderKpis(kpisResp);

    setStatus(`${rows.length} registros carregados.`, "ok");
  } catch (e) {
    console.error(e);
    setStatus(e.message || "Erro ao consultar leads.", "err");
    renderTable([]);
    renderTotals(0, 0);
    renderKpis(null);
  }
}

const loadLeadsAndKpisDebounced = (() => {
  let t;
  return () => {
    clearTimeout(t);
    t = setTimeout(loadLeadsAndKpis, 450);
  };
})();

function renderTotals(total, shown) {
  if ($("#lblTotal")) $("#lblTotal").textContent = `${shown} / ${total ?? shown}`;
  if ($("#kpiCount")) $("#kpiCount").textContent = total ?? 0;
}

function renderTable(rows) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="10" style="text-align:center">Nenhum dado encontrado</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map((r) => `
    <tr>
      <td>${escapeHtml(fmtDate(r.data_inscricao_dt))}</td>
      <td>${escapeHtml(r.nome)}</td>
      <td>${escapeHtml(r.cpf)}</td>
      <td>${escapeHtml(r.celular)}</td>
      <td>${escapeHtml(r.origem || "-")}</td>
      <td>${escapeHtml(r.polo || "-")}</td>
      <td>${escapeHtml(r.curso || "-")}</td>
      <td><span class="badge">${escapeHtml(r.status || "Lead")}</span></td>
      <td>${escapeHtml(r.consultor || "-")}</td>
      <td>${escapeHtml(r.campanha || "-")}</td>
    </tr>
  `).join("");
}

function renderKpis(k) {
  if (!k) {
    if ($("#kpiTopStatus")) $("#kpiTopStatus").textContent = "-";
    return;
  }
  const top = k?.top_status;
  if ($("#kpiTopStatus")) $("#kpiTopStatus").textContent = top ? `${top.status} (${top.cnt})` : "-";
}

/* =========================
   Upload
========================= */
async function doUpload() {
  const fileInput = $("#uploadFile");
  if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
    setUploadStatus("Selecione um arquivo primeiro.", "err");
    return;
  }

  const file = fileInput.files[0];
  setUploadStatus("Enviando e processando... (staging + procedure)", "ok");

  try {
    const fd = new FormData();
    fd.append("file", file);

    const src = ($("#uploadSource")?.value || "").trim();
    if (src) fd.append("source", src);

    const resp = await apiPostForm("/api/upload", fd);
    setUploadStatus(resp?.message || "Processado com sucesso!", "ok");

    await loadOptions();
    await loadLeadsAndKpis();
  } catch (e) {
    console.error(e);
    setUploadStatus(e.message || "Erro no upload.", "err");
  }
}

/* =========================
   Export CSV (server-side)
========================= */
function doExport() {
  const params = buildLeadsParams();
  const url = new URL("/api/export", window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;

    if (Array.isArray(v)) {
      if (v.length === 0) return;
      url.searchParams.set(k, v.join(" || "));
      return;
    }

    const s = String(v).trim();
    if (!s) return;
    url.searchParams.set(k, s);
  });

  window.location.href = url.toString();
}

/* =========================
   Limpar
========================= */
function clearFilters() {
  tsStatus?.clear(true);
  tsCurso?.clear(true);
  tsPolo?.clear(true);
  tsConsultor?.clear(true);

  if ($("#fIni")) $("#fIni").value = "";
  if ($("#fFim")) $("#fFim").value = "";
  if ($("#fLimit")) $("#fLimit").value = "500";
  if ($("#fBusca")) $("#fBusca").value = "";

  loadLeadsAndKpis();
}

/* =========================
   Eventos
========================= */
document.addEventListener("DOMContentLoaded", async () => {
  initMultiSelects();

  $("#btnApply")?.addEventListener("click", loadLeadsAndKpis);
  $("#btnReload")?.addEventListener("click", async () => {
    await loadOptions();
    await loadLeadsAndKpis();
  });
  $("#btnClear")?.addEventListener("click", clearFilters);

  $("#btnUpload")?.addEventListener("click", doUpload);

  $("#btnExport")?.addEventListener("click", doExport);

  // Carregamento inicial
  await loadOptions();
  await loadLeadsAndKpis();
});
