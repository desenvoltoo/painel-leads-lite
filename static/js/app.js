// static/js/app.js
// V14 — compatível com:
//   GET  /api/options  -> { ok:true, data:{ status:[], cursos:[], polos:[], consultores:[] } }
//   GET  /api/leads    -> { ok:true, total:N, data:[...] }
//   GET  /api/kpis     -> { ok:true, total:N, top_status:{status,cnt} }
//   POST /api/upload   -> { ok:true, message:"..." }
//
// HTML base (ids):
// upload: #uploadFile #btnUpload #uploadStatus
// filtros: #fStatus #fCurso #fModalidade #fPolo #fConsultor #fIni #fFim #fLimit #fBusca
// ações: #btnApply #btnClear #btnReload #btnExport
// kpis: #kpiCount #kpiTopStatus
// tabela: #tbl tbody
// labels: #statusLine #lblTotal

const $ = (sel) => document.querySelector(sel);

let tsStatus, tsCurso, tsModalidade, tsPolo, tsConsultor;

/* =========================
   Helpers UI
========================= */
function setStatus(msg, type = "ok") {
  const el = $("#statusLine");
  if (!el) return;
  el.textContent = msg;
  el.className = `status-line ${type === "err" ? "error" : "status-ok"}`;
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
  // pode vir "YYYY-MM-DD" ou "YYYY-MM-DDTHH:MM:SS"
  const datePart = s.slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
    return datePart.split("-").reverse().join("/");
  }
  return s;
}

function toCsvValue(v) {
  const s = v === null || v === undefined ? "" : String(v);
  // CSV seguro
  if (/[",\n;]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
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
      // envia repetido (melhor prática); backend hoje aceita string -> vamos mandar como string também
      // como seu backend ainda não interpreta listas nativas, vamos mandar como "A || B"
      url.searchParams.set(k, v.join(" || "));
      return;
    }
    const s = String(v).trim();
    if (!s) return;
    url.searchParams.set(k, s);
  });

  const res = await fetch(url.toString(), { cache: "no-store" });
  const data = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(data?.error || data?.message || "Erro na API");
  }
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
    render: {
      option: (data, escape) => `
        <div class="ts-opt">
          <span class="ts-opt-check" aria-hidden="true"></span>
          <span class="ts-opt-text">${escape(data.text)}</span>
        </div>
      `,
      item: (data, escape) => `<div>${escape(data.text)}</div>`
    },
    onChange: () => loadLeadsAndKpisDebounced()
  });
}

function ensureModalidadeField() {
  if (document.querySelector('#fModalidade')) return;

  const filtersCard = document.querySelector('.card .filters.filters-6');
  if (!filtersCard) return;

  const field = document.createElement('div');
  field.className = 'field col-2';
  field.innerHTML = `
    <label>Modalidade (Multi)</label>
    <select id="fModalidade" multiple placeholder="Todas as modalidades..."></select>
  `;

  const cursoField = document.querySelector('#fCurso')?.closest('.field');
  if (cursoField && cursoField.parentElement === filtersCard) {
    cursoField.insertAdjacentElement('afterend', field);
    return;
  }

  filtersCard.prepend(field);
}

function initMultiSelects() {
  tsStatus = makeTomSelect("#fStatus");
  tsCurso = makeTomSelect("#fCurso");
  tsModalidade = makeTomSelect("#fModalidade");
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
    const data = resp?.data || resp; // fallback se algum dia você retornar direto

    fillSelect(tsStatus, data?.status || []);
    fillSelect(tsCurso, data?.cursos || []);
    fillSelect(tsModalidade, data?.modalidades || []);
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
  // TomSelect pode devolver string ou array dependendo config; normaliza
  if (Array.isArray(v)) return v;
  if (!v) return [];
  return String(v).split(",").map(s => s.trim()).filter(Boolean);
}

function parseBuscaRapida(txt) {
  // Heurística leve:
  // - se tiver "@": email
  // - se só números e len >= 10: celular/cpf (preferir cpf se 11)
  // - caso contrário: nome
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
  const modalidades = getMulti(tsModalidade);
  const polos = getMulti(tsPolo);
  const consultores = getMulti(tsConsultor);

  const data_ini = $("#fIni")?.value || "";
  const data_fim = $("#fFim")?.value || "";
  const limit = $("#fLimit")?.value || 500;

  const busca = parseBuscaRapida($("#fBusca")?.value);

  return {
    // multi -> backend atual entende melhor 1 valor por vez;
    // aqui mandamos como "A || B" e no backend você pode evoluir depois para split.
    // enquanto isso, o filtro exato funciona quando 1 selecionado.
    status: status,
    curso: cursos,
    modalidade: modalidades,
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
  renderTable([], { loading: true });

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
  if ($("#kpiCount")) $("#kpiCount").textContent = total ?? 0;
  if ($("#lblTotal")) $("#lblTotal").textContent = `${shown} / ${total ?? shown}`;
}

function renderTable(rows, { loading = false } = {}) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  if (loading) {
    tbody.innerHTML = `<tr><td colspan="10" class="table-feedback">Carregando dados...</td></tr>`;
    return;
  }

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="10" class="table-feedback">Nenhum dado encontrado</td></tr>`;
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
  // kpis endpoint retorna: { ok:true, total, top_status:{status,cnt} }
  const total = k?.total ?? 0;
  const top = k?.top_status;
  if ($("#kpiCount")) $("#kpiCount").textContent = total;
  if ($("#kpiTopStatus")) {
    $("#kpiTopStatus").textContent = top ? `${top.status} (${top.cnt})` : "-";
  }
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

    // A API atual ignora source, mas deixo pronto para evoluir
    const src = ($("#uploadSource")?.value || "").trim();
    if (src) fd.append("source", src);

    const resp = await apiPostForm("/api/upload", fd);
    setUploadStatus(resp?.message || "Processado com sucesso!", "ok");

    // Recarrega opções (dims podem ter aumentado) e recarrega tabela
    await loadOptions();
    await loadLeadsAndKpis();
  } catch (e) {
    console.error(e);
    setUploadStatus(e.message || "Erro no upload.", "err");
  }
}

/* =========================
   Export CSV (client-side)
========================= */
function exportCsvFromTable() {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  const rows = Array.from(tbody.querySelectorAll("tr"));
  if (rows.length === 0) {
    setStatus("Nada para exportar.", "err");
    return;
  }

  const headers = Array.from($("#tbl thead tr").children).map(th => th.textContent.trim());
  const lines = [];
  lines.push(headers.map(toCsvValue).join(";"));

  rows.forEach(tr => {
    const cols = Array.from(tr.children).map(td => td.textContent.trim());
    lines.push(cols.map(toCsvValue).join(";"));
  });

  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  a.href = URL.createObjectURL(blob);
  a.download = `leads_v14_${ts}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

/* =========================
   Limpar
========================= */
function clearFilters() {
  tsStatus?.clear(true);
  tsCurso?.clear(true);
  tsModalidade?.clear(true);
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
  ensureModalidadeField();
  initMultiSelects();

  $("#btnApply")?.addEventListener("click", loadLeadsAndKpis);
  $("#btnReload")?.addEventListener("click", async () => {
    await loadOptions();
    await loadLeadsAndKpis();
  });
  $("#btnClear")?.addEventListener("click", clearFilters);

  $("#btnUpload")?.addEventListener("click", doUpload);

  $("#btnExport")?.addEventListener("click", exportCsvFromTable);

  // carregamento inicial
  await loadOptions();
  await loadLeadsAndKpis();
});
