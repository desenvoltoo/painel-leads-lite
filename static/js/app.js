// static/js/app.js
// STAR — compatível com:
//   GET  /api/options  -> { ok:true, data:{ status:[], cursos:[], modalidades:[], turnos:[], polos:[], origens:[], canais:[], campanhas:[], consultores_disparo:[], consultores_comercial:[], tipos_disparo:[], tipos_negocio:[] } }
//   GET  /api/leads    -> { ok:true, total:N, data:[...] }
//   GET  /api/kpis     -> { ok:true, total:N, top_status:{status,cnt} }
//   GET  /api/export/xlsx -> XLSX (download)
//   POST /api/upload   -> { ok:true, message:"..." , saved_xlsx:"..." }
//
// HTML (ids):
// upload: #uploadFile #btnUpload #uploadStatus #uploadSource
// filtros: #fStatus #fCurso #fModalidade #fTurno #fPolo #fOrigem
//          #fConsultorDisparo #fConsultorComercial #fCanal #fCampanha
//          #fTipoDisparo #fTipoNegocio
//          #fIni #fFim #fMatriculado #fLimit #fBusca
// ações: #btnApply #btnClear #btnReload #btnExport
// kpis: #kpiCount #kpiTopStatus
// tabela: #tbl tbody
// labels: #statusLine #lblTotal

const $ = (sel) => document.querySelector(sel);

let tsStatus, tsCurso, tsModalidade, tsTurno, tsPolo, tsOrigem;
let tsConsultorDisparo, tsConsultorComercial, tsCanal, tsCampanha;
let tsTipoDisparo, tsTipoNegocio;

const TABLE_COLS = 13;

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
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
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

function fmtBool(b) {
  if (b === true || String(b).toLowerCase() === "true") return "Sim";
  if (b === false || String(b).toLowerCase() === "false") return "Não";
  return "-";
}

/* =========================
   API (com erro detalhado)
========================= */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;

    if (Array.isArray(v)) {
      if (v.length === 0) return;
      // manda "A || B" (backend faz split com _as_list)
      url.searchParams.set(k, v.join(" || "));
      return;
    }

    const s = String(v).trim();
    if (!s) return;
    url.searchParams.set(k, s);
  });

  const res = await fetch(url.toString(), {
    cache: "no-store",
    credentials: "same-origin",
  });

  // tenta entender o corpo sempre (pra erro útil)
  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { message: text || "" };
  }

  if (!res.ok) {
    if (res.status === 401 && data?.redirect_to) {
      window.location.href = data.redirect_to;
      throw new Error("Sessão expirada");
    }
    const msg =
      data?.error ||
      data?.message ||
      (typeof data === "string" ? data : "") ||
      `Erro na API (${res.status})`;
    throw new Error(msg);
  }

  return data;
}

async function apiPostForm(path, formData) {
  const url = new URL(path, window.location.origin);
  const res = await fetch(url.toString(), {
    method: "POST",
    body: formData,
    credentials: "same-origin",
  });

  const text = await res.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { message: text || "" };
  }

  if (!res.ok) {
    if (res.status === 401 && data?.redirect_to) {
      window.location.href = data.redirect_to;
      throw new Error("Sessão expirada");
    }
    const msg =
      data?.error ||
      data?.message ||
      (typeof data === "string" ? data : "") ||
      `Erro na API (${res.status})`;
    throw new Error(msg);
  }

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

  const tomSelect = new TomSelect(selector, {
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
      item: (data, escape) => `<div>${escape(data.text)}</div>`,
    },
    onChange: () => loadLeadsAndKpisDebounced(),
  });

  addSearchSelectButton(tomSelect);
  return tomSelect;
}

function addSearchSelectButton(ts) {
  if (!ts?.dropdown || !ts.dropdown_content) return;
  if (ts.dropdown.querySelector(".ts-dropdown-toolbar")) return;

  const toolbar = document.createElement("div");
  toolbar.className = "ts-dropdown-toolbar";

  const button = document.createElement("button");
  button.type = "button";
  button.className = "ts-dropdown-toolbar-btn";
  button.textContent = "Selecionar resultados da busca";

  button.addEventListener("mousedown", (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
  });

  button.addEventListener("click", (ev) => {
    ev.preventDefault();
    ev.stopPropagation();

    const resultItems = ts.currentResults?.items || [];
    const valuesToSelect = resultItems
      .map((item) => item.id)
      .filter((value) => !ts.items.includes(value));

    if (valuesToSelect.length === 0) return;
    ts.addItems(valuesToSelect);
    ts.refreshOptions(false);
    loadLeadsAndKpisDebounced();
  });

  toolbar.appendChild(button);
  ts.dropdown.insertBefore(toolbar, ts.dropdown_content);
}

function initMultiSelects() {
  tsStatus = makeTomSelect("#fStatus");
  tsCurso = makeTomSelect("#fCurso");
  tsModalidade = makeTomSelect("#fModalidade");
  tsTurno = makeTomSelect("#fTurno");
  tsPolo = makeTomSelect("#fPolo");
  tsOrigem = makeTomSelect("#fOrigem");

  tsConsultorDisparo = makeTomSelect("#fConsultorDisparo");
  tsConsultorComercial = makeTomSelect("#fConsultorComercial");

  tsCanal = makeTomSelect("#fCanal");
  tsCampanha = makeTomSelect("#fCampanha");

  tsTipoDisparo = makeTomSelect("#fTipoDisparo");
  tsTipoNegocio = makeTomSelect("#fTipoNegocio");
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
    fillSelect(tsModalidade, data?.modalidades || []);
    fillSelect(tsTurno, data?.turnos || []);
    fillSelect(tsPolo, data?.polos || []);
    fillSelect(tsOrigem, data?.origens || []);

    fillSelect(tsConsultorDisparo, data?.consultores_disparo || []);
    fillSelect(tsConsultorComercial, data?.consultores_comercial || []);

    fillSelect(tsCanal, data?.canais || []);
    fillSelect(tsCampanha, data?.campanhas || []);

    fillSelect(tsTipoDisparo, data?.tipos_disparo || []);
    fillSelect(tsTipoNegocio, data?.tipos_negocio || []);
  } catch (e) {
    console.error("Erro ao carregar opções:", e);
    setStatus(`Falha ao carregar filtros (options): ${e.message || "erro"}`, "err");
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
  return String(v).split(",").map((s) => s.trim()).filter(Boolean);
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

// normaliza datas (input date) para YYYY-MM-DD (o backend espera DATE)
function safeDate(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  // já vem yyyy-mm-dd
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return s; // fallback
}

function buildLeadsParams() {
  const status = getMulti(tsStatus);
  const curso = getMulti(tsCurso);
  const modalidade = getMulti(tsModalidade);
  const turno = getMulti(tsTurno);
  const polo = getMulti(tsPolo);
  const origem = getMulti(tsOrigem);

  const consultor_disparo = getMulti(tsConsultorDisparo);
  const consultor_comercial = getMulti(tsConsultorComercial);

  const canal = getMulti(tsCanal);
  const campanha = getMulti(tsCampanha);

  const tipo_disparo = getMulti(tsTipoDisparo);
  const tipo_negocio = getMulti(tsTipoNegocio);

  const data_ini = safeDate($("#fIni")?.value || "");
  const data_fim = safeDate($("#fFim")?.value || "");
  const matriculado = ($("#fMatriculado")?.value || "").trim(); // "" | "true" | "false"
  const limit = Number($("#fLimit")?.value || 500) || 500;

  const busca = parseBuscaRapida($("#fBusca")?.value);

  return {
    status,
    curso,
    modalidade,
    turno,
    polo,
    origem,
    consultor_disparo,
    consultor_comercial,
    canal,
    campanha,
    tipo_disparo,
    tipo_negocio,
    matriculado,
    data_ini,
    data_fim,
    limit,
    ...busca,
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
    tbody.innerHTML = `<tr><td colspan="${TABLE_COLS}" class="table-feedback">Carregando dados...</td></tr>`;
    return;
  }

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${TABLE_COLS}" class="table-feedback">Nenhum dado encontrado</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (r) => `
    <tr>
      <td>${escapeHtml(fmtDate(r.data_inscricao))}</td>
      <td>${escapeHtml(r.nome || "-")}</td>
      <td>${escapeHtml(r.cpf || "-")}</td>
      <td>${escapeHtml(r.celular || "-")}</td>
      <td>${escapeHtml(r.origem || "-")}</td>
      <td>${escapeHtml(r.polo || "-")}</td>
      <td>${escapeHtml(r.curso || "-")}</td>
      <td>${escapeHtml(r.modalidade || "-")}</td>
      <td><span class="badge">${escapeHtml(r.status_inscricao || r.status || "LEAD")}</span></td>
      <td>${escapeHtml(fmtBool(r.flag_matriculado))}</td>
      <td>${escapeHtml(r.consultor_disparo || "-")}</td>
      <td>${escapeHtml(r.campanha || "-")}</td>
      <td>${escapeHtml(r.canal || "-")}</td>
    </tr>
  `
    )
    .join("");
}

function renderKpis(k) {
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
   Export XLSX (server-side)
========================= */
function exportXlsxServerSide() {
  const params = buildLeadsParams();
  const url = new URL("/api/export/xlsx", window.location.origin);

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
  tsModalidade?.clear(true);
  tsTurno?.clear(true);
  tsPolo?.clear(true);
  tsOrigem?.clear(true);

  tsConsultorDisparo?.clear(true);
  tsConsultorComercial?.clear(true);

  tsCanal?.clear(true);
  tsCampanha?.clear(true);

  tsTipoDisparo?.clear(true);
  tsTipoNegocio?.clear(true);

  if ($("#fIni")) $("#fIni").value = "";
  if ($("#fFim")) $("#fFim").value = "";
  if ($("#fMatriculado")) $("#fMatriculado").value = "";
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
  $("#btnExport")?.addEventListener("click", exportXlsxServerSide);

  // busca rápida com debounce (não precisa clicar aplicar)
  $("#fBusca")?.addEventListener("input", loadLeadsAndKpisDebounced);

  await loadOptions();
  await loadLeadsAndKpis();
});
