// static/js/app.js
// STAR — compatível com:
//   GET  /api/options  -> { ok:true, data:{ status:[], cursos:[], modalidades:[], turnos:[], polos:[], origens:[], canais:[], campanhas:[], consultores_disparo:[], consultores_comercial:[], tipos_disparo:[], tipos_negocio:[] } }
//   POST /api/leads/search -> { ok:true, total:N, data:[...] }
//   POST /api/kpis/search  -> { ok:true, total:N, top_status:{status,cnt} }
//   GET  /api/export/xlsx -> XLSX (download)
//   POST /api/upload -> upload direto de CSV/XLSX
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
const EMPTY_FILTER_TOKEN = "__EMPTY__";
const EMPTY_FILTER_LABEL = "(Sem preenchimento)";
const SAVED_FILTERS_STORAGE_KEY = "painel_leads_saved_filters_v1";
const MAX_SAVED_FILTERS = 5;
let currentPage = 1;
let totalLeads = 0;
let isLoadingLeads = false;
let activeExportJobId = null;

/* =========================
   Helpers UI
========================= */
function setStatus(msg, type = "ok") {
  const el = $("#statusLine");
  if (!el) return;
  el.textContent = msg;
  el.className = `status-line ${type === "err" ? "error" : "status-ok"}`;
}

function setUploadStatus(msg, type = "info") {
  const el = $("#uploadStatus");
  if (!el) return;
  const normalized = type === "err" ? "danger" : type === "ok" ? "success" : type;
  el.textContent = msg;
  el.className = `alert alert-${normalized}`;
}

function setUploadButtonLoading(loading) {
  const btn = $("#btnUpload");
  if (!btn) return;
  btn.disabled = loading;
  btn.classList.toggle("is-loading", loading);
  btn.textContent = loading ? "Importando planilha..." : "Importar planilha";
}

function updateSelectedFileName() {
  const input = $("#uploadFile");
  const label = $("#uploadFileName");
  if (!input || !label) return;
  const file = input.files?.[0];
  label.textContent = file ? file.name : "Nenhum arquivo selecionado";
  if (file) setUploadStatus("Arquivo recebido. Pronto para enviar para staging.", "info");
}


function formatImportReport(report) {
  if (!report) return "";
  const reasons = report.motivos_rejeicoes || {};
  const reasonText = Object.keys(reasons).length
    ? Object.entries(reasons).map(([motivo, qtd]) => `${motivo}: ${qtd}`).join("; ")
    : "nenhuma";
  return `Arquivo: ${report.arquivo || "-"} | Linhas recebidas: ${report.linhas_recebidas ?? 0} | `
    + `Importadas: ${report.linhas_importadas ?? 0} | Rejeitadas: ${report.linhas_rejeitadas ?? 0} | `
    + `Motivo das rejeições: ${reasonText} | Tempo: ${report.tempo_processamento_s ?? 0}s`;
}

function setSearchLoading(loading) {
  const row = $("#searchLoading");
  if (!row) return;
  row.hidden = !loading;
}

function setExportProgress({ visible, text = "", progress = 0 }) {
  const box = $("#exportProgress");
  const label = $("#exportProgressText");
  const bar = $("#exportProgressBar");
  if (!box || !label || !bar) return;
  box.hidden = !visible;
  label.textContent = text;
  bar.style.width = `${Math.max(0, Math.min(100, progress))}%`;
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

function badgeClass(value, kind = "status") {
  const text = String(value || "").toLowerCase();
  if (kind === "matriculado") return text === "sim" ? "badge-success" : text === "não" ? "badge-warning" : "badge-neutral";
  if (/matric|conclu|aprov|ativo|convert|sucesso/.test(text)) return "badge-success";
  if (/erro|cancel|reprov|falh|perdid|inv[aá]lid/.test(text)) return "badge-danger";
  if (/pend|aguard|aten|andamento|process/.test(text)) return "badge-warning";
  if (/lead|novo|contato/.test(text)) return "badge-info";
  return "badge-neutral";
}

function tableCell(value, extraClass = "") {
  const safe = escapeHtml(value || "-");
  return `<span class="${extraClass}" title="${safe}">${safe}</span>`;
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

async function apiPostJson(path, payload = {}) {
  const url = new URL(path, window.location.origin);
  const res = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
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
    onChange: () => {
      currentPage = 1;
      loadLeadsAndKpisDebounced();
    },
  });

  addSearchSelectButton(tomSelect);
  return tomSelect;
}

function addSearchSelectButton(ts) {
  if (!ts?.dropdown || !ts.dropdown_content) return;
  if (ts.dropdown.querySelector(".ts-dropdown-toolbar")) return;

  const toolbar = document.createElement("div");
  toolbar.className = "ts-dropdown-toolbar";

  const deselectButton = document.createElement("button");
  deselectButton.type = "button";
  deselectButton.className = "ts-dropdown-toolbar-btn ts-dropdown-toolbar-btn-secondary";
  deselectButton.textContent = "Deselecionar selecionados";

  const selectButton = document.createElement("button");
  selectButton.type = "button";
  selectButton.className = "ts-dropdown-toolbar-btn";
  selectButton.textContent = "Selecionar resultados da busca";

  deselectButton.addEventListener("mousedown", (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
  });

  selectButton.addEventListener("mousedown", (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
  });

  deselectButton.addEventListener("click", (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
    if (!ts.items.length) return;
    ts.clear(true);
    ts.refreshOptions(false);
    loadLeadsAndKpisDebounced();
  });

  selectButton.addEventListener("click", (ev) => {
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

  toolbar.appendChild(deselectButton);
  toolbar.appendChild(selectButton);
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
  ts.addOption({ value: EMPTY_FILTER_TOKEN, text: EMPTY_FILTER_LABEL });
  (values || []).forEach((v) => {
    const s = String(v);
    if (!s || s === EMPTY_FILTER_TOKEN) return;
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

function normalizeArrayValues(v) {
  if (!Array.isArray(v)) return [];
  return v.map((item) => String(item || "").trim()).filter(Boolean);
}

function setTomValues(ts, values) {
  if (!ts) return;
  const list = normalizeArrayValues(values);
  list.forEach((v) => {
    if (!ts.options[v]) {
      ts.addOption({ value: v, text: v });
    }
  });
  ts.setValue(list, true);
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
  const offset = Math.max(0, (currentPage - 1) * limit);

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
    offset,
    order_by: "data_disparo",
    order_dir: "ASC",
    ...busca,
  };
}

function getCurrentFilterState() {
  return {
    status: getMulti(tsStatus),
    curso: getMulti(tsCurso),
    modalidade: getMulti(tsModalidade),
    turno: getMulti(tsTurno),
    polo: getMulti(tsPolo),
    origem: getMulti(tsOrigem),
    consultor_disparo: getMulti(tsConsultorDisparo),
    consultor_comercial: getMulti(tsConsultorComercial),
    canal: getMulti(tsCanal),
    campanha: getMulti(tsCampanha),
    tipo_disparo: getMulti(tsTipoDisparo),
    tipo_negocio: getMulti(tsTipoNegocio),
    data_ini: safeDate($("#fIni")?.value || ""),
    data_fim: safeDate($("#fFim")?.value || ""),
    matriculado: ($("#fMatriculado")?.value || "").trim(),
    limit: Number($("#fLimit")?.value || 500) || 500,
    busca_rapida: ($("#fBusca")?.value || "").trim(),
  };
}

function applyFilterState(state = {}) {
  setTomValues(tsStatus, state.status);
  setTomValues(tsCurso, state.curso);
  setTomValues(tsModalidade, state.modalidade);
  setTomValues(tsTurno, state.turno);
  setTomValues(tsPolo, state.polo);
  setTomValues(tsOrigem, state.origem);
  setTomValues(tsConsultorDisparo, state.consultor_disparo);
  setTomValues(tsConsultorComercial, state.consultor_comercial);
  setTomValues(tsCanal, state.canal);
  setTomValues(tsCampanha, state.campanha);
  setTomValues(tsTipoDisparo, state.tipo_disparo);
  setTomValues(tsTipoNegocio, state.tipo_negocio);

  if ($("#fIni")) $("#fIni").value = state.data_ini || "";
  if ($("#fFim")) $("#fFim").value = state.data_fim || "";
  if ($("#fMatriculado")) $("#fMatriculado").value = state.matriculado || "";
  if ($("#fLimit")) $("#fLimit").value = String(state.limit || 500);
  if ($("#fBusca")) $("#fBusca").value = state.busca_rapida || "";
}

function readSavedFilters() {
  try {
    const raw = window.localStorage.getItem(SAVED_FILTERS_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((item) => item && typeof item === "object" && item.id && item.name && item.state);
  } catch {
    return [];
  }
}

function writeSavedFilters(items) {
  window.localStorage.setItem(SAVED_FILTERS_STORAGE_KEY, JSON.stringify(items || []));
}

function refreshSavedFiltersSelect(selectedId = "") {
  const select = $("#savedFilterSelect");
  if (!select) return;
  const saved = readSavedFilters();

  select.innerHTML = `<option value="">Filtros salvos...</option>`;
  saved.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.id;
    option.textContent = item.name;
    select.appendChild(option);
  });

  if (selectedId) {
    select.value = selectedId;
  }
}

function saveCurrentFilterView() {
  const currentSelectedId = ($("#savedFilterSelect")?.value || "").trim();
  const currentSelected = readSavedFilters().find((item) => item.id === currentSelectedId);
  const suggestedName = currentSelected?.name || "";
  const name = (window.prompt("Nome do filtro salvo:", suggestedName) || "").trim();
  if (!name) {
    setStatus("Informe um nome para salvar a visualização.", "err");
    return;
  }

  const state = getCurrentFilterState();
  const saved = readSavedFilters();
  const normalizedName = name.toLowerCase();
  const existing = saved.find((item) => String(item.name).toLowerCase() === normalizedName);

  if (existing) {
    const shouldOverwrite = window.confirm(`Já existe uma visualização chamada "${name}". Deseja sobrescrever?`);
    if (!shouldOverwrite) return;
    existing.state = state;
    existing.updated_at = new Date().toISOString();
    writeSavedFilters(saved);
    refreshSavedFiltersSelect(existing.id);
    setStatus(`Visualização "${name}" atualizada com sucesso.`, "ok");
    return;
  }

  if (saved.length >= MAX_SAVED_FILTERS) {
    setStatus(`Limite de ${MAX_SAVED_FILTERS} filtros salvos atingido. Exclua um para salvar outro.`, "err");
    return;
  }

  const newItem = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    name,
    state,
    created_at: new Date().toISOString(),
  };
  saved.unshift(newItem);
  writeSavedFilters(saved);
  refreshSavedFiltersSelect(newItem.id);
  setStatus(`Visualização "${name}" salva com sucesso.`, "ok");
}

function applySavedFilterView() {
  const select = $("#savedFilterSelect");
  const id = (select?.value || "").trim();
  if (!id) {
    setStatus("Selecione uma visualização salva para carregar.", "err");
    return;
  }

  const saved = readSavedFilters();
  const selected = saved.find((item) => item.id === id);
  if (!selected) {
    setStatus("Visualização não encontrada.", "err");
    refreshSavedFiltersSelect();
    return;
  }

  applyFilterState(selected.state || {});
  loadLeadsAndKpis();
  setStatus(`Visualização "${selected.name}" carregada.`, "ok");
}

function deleteSavedFilterView() {
  const select = $("#savedFilterSelect");
  const id = (select?.value || "").trim();
  if (!id) {
    setStatus("Selecione uma visualização salva para excluir.", "err");
    return;
  }

  const saved = readSavedFilters();
  const selected = saved.find((item) => item.id === id);
  if (!selected) {
    setStatus("Visualização não encontrada.", "err");
    refreshSavedFiltersSelect();
    return;
  }

  const shouldDelete = window.confirm(`Excluir a visualização "${selected.name}"?`);
  if (!shouldDelete) return;

  const filtered = saved.filter((item) => item.id !== id);
  writeSavedFilters(filtered);
  refreshSavedFiltersSelect();
  setStatus(`Visualização "${selected.name}" excluída.`, "ok");
}

async function loadLeadsAndKpis() {
  if (isLoadingLeads) return;
  isLoadingLeads = true;
  setStatus("Consultando BigQuery...", "ok");
  renderTable([], { loading: true });
  setSearchLoading(true);

  const params = buildLeadsParams();

  try {
    const [leadsResp, kpisResp] = await Promise.all([
      apiPostJson("/api/leads/search", params),
      apiPostJson("/api/kpis/search", params),
    ]);

    const rows = leadsResp?.data || [];
    const total = leadsResp?.total ?? rows.length;
    totalLeads = Number(total) || 0;

    renderTable(rows);
    renderTotals(total, rows.length);
    renderKpis(kpisResp);

    setStatus(`${rows.length} registros carregados.`, "ok");
  } catch (e) {
    console.error(e);
    setStatus(e.message || "Erro ao consultar leads.", "err");
    renderTable([]);
    renderTotals(0, 0);
  } finally {
    isLoadingLeads = false;
    setSearchLoading(false);
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
  const limit = Number($("#fLimit")?.value || 500) || 500;
  const start = total > 0 ? ((currentPage - 1) * limit) + 1 : 0;
  const end = total > 0 ? start + shown - 1 : 0;
  if ($("#lblRange")) $("#lblRange").textContent = `Mostrando ${start}-${Math.max(end, 0)} de ${total ?? 0} leads`;
  if ($("#lblPage")) $("#lblPage").textContent = `Página ${currentPage}`;
  if ($("#btnPrevPage")) $("#btnPrevPage").disabled = currentPage <= 1;
  if ($("#btnNextPage")) $("#btnNextPage").disabled = end >= (total ?? 0);
}

function renderTable(rows, { loading = false } = {}) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  if (loading) {
    tbody.innerHTML = `<tr><td colspan="${TABLE_COLS}" class="table-feedback"><div class="spinner" aria-hidden="true"></div><strong>Carregando dados...</strong><span>Consultando leads consolidados no BigQuery.</span></td></tr>`;
    return;
  }

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${TABLE_COLS}" class="table-feedback"><strong>Nenhum lead encontrado</strong><span>Ajuste os filtros ou limpe a busca rápida para ampliar a consulta.</span></td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (r) => `
    <tr>
      <td>${tableCell(fmtDate(r.data_inscricao), "cell-muted")}</td>
      <td>${tableCell(r.nome || "-", "lead-name")}</td>
      <td>${tableCell(r.cpf || "-", "cell-muted")}</td>
      <td>${tableCell(r.celular || "-", "cell-muted")}</td>
      <td>${tableCell(r.origem || "-")}</td>
      <td>${tableCell(r.polo || "-")}</td>
      <td>${tableCell(r.curso || "-")}</td>
      <td>${tableCell(r.modalidade || "-")}</td>
      <td><span class="badge ${badgeClass(r.status || "LEAD")}" title="${escapeHtml(r.status || "LEAD")}">${escapeHtml(r.status || "LEAD")}</span></td>
      <td><span class="badge ${badgeClass(fmtBool(r.flag_matriculado), "matriculado")}">${escapeHtml(fmtBool(r.flag_matriculado))}</span></td>
      <td>${tableCell(r.consultor_disparo || "-")}</td>
      <td>${tableCell(r.campanha || "-")}</td>
      <td>${tableCell(r.canal || "-")}</td>
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


function notifyGestaoUploadConcluido() {
  const payload = { type: "upload-concluido", at: new Date().toISOString() };
  try {
    window.dispatchEvent(new CustomEvent("gestao:upload-concluido", { detail: payload }));
  } catch (_) {}
  try {
    localStorage.setItem("gestaoUploadConcluido", JSON.stringify(payload));
  } catch (_) {}
  try {
    const channel = new BroadcastChannel("gestao-cache");
    channel.postMessage(payload);
    channel.close();
  } catch (_) {}
}

/* =========================
   Upload
========================= */
async function uploadDirectToServer(file, source) {
  const fd = new FormData();
  fd.append("file", file);
  if (source) fd.append("source", source);
  return apiPostForm("/api/upload", fd);
}

async function pollUploadStatus(jobId) {
  if (!jobId) return;

  const maxAttempts = 80;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const resp = await apiGet("/api/upload/status", { job_id: jobId });
    const data = resp?.data || {};

    if (data.state === "DONE") {
      if (data.ok === false) {
        throw new Error(data?.error?.message || JSON.stringify(data.error) || "Procedure falhou.");
      }

      setUploadStatus("Processamento concluído com sucesso. Atualizando painel...", "ok");
      notifyGestaoUploadConcluido();
      await loadOptions();
      await loadLeadsAndKpis();
      return;
    }

    setUploadStatus(`Dados carregados. Executando procedure no BigQuery... (${attempt}/${maxAttempts})`, "info");
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }

  setUploadStatus("Upload iniciado. O processamento ainda está em andamento; atualize o painel em instantes.", "warning");
}

async function doUpload() {
  const fileInput = $("#uploadFile");
  if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
    setUploadStatus("Selecione um arquivo .xlsx, .xls ou .csv antes de importar.", "err");
    return;
  }

  const file = fileInput.files[0];
  const source = ($("#uploadSource")?.value || "").trim();
  const validExtension = /\.(csv|xlsx|xls)$/i.test(file.name || "");
  if (!validExtension) {
    setUploadStatus("Formato inválido. Envie uma planilha CSV, XLS ou XLSX.", "err");
    return;
  }

  setUploadStatus("Arquivo recebido. Enviando para staging...", "info");
  setUploadButtonLoading(true);

  try {
    const resp = await uploadDirectToServer(file, source);
    const jobId = resp?.job_id;
    const reportText = formatImportReport(resp?.report);
    const jobText = jobId ? ` Job BigQuery: ${jobId}.` : "";
    setUploadStatus(reportText ? `${reportText}${jobText}` : (resp?.message || `Arquivo enviado com sucesso. Processamento iniciado.${jobText}`), "success");
    await pollUploadStatus(jobId);
    if (!jobId) notifyGestaoUploadConcluido();
    if (reportText) setUploadStatus(`Processamento concluído com sucesso. ${jobText} ${reportText}`, "success");
  } catch (e) {
    console.error(e);
    setUploadStatus(e.message || "Não foi possível concluir a importação. Verifique o arquivo ou tente novamente.", "err");
  } finally {
    setUploadButtonLoading(false);
  }
}

/* =========================
   Export XLSX (server-side)
========================= */
async function exportXlsxServerSide() {
  const btn = $("#btnExport");
  const previousText = btn?.textContent;
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Exportando...";
  }

  const params = buildLeadsParams();
  delete params.limit;
  delete params.offset;
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

  try {
    const resp = await fetch(url.toString(), {
      method: "GET",
      cache: "no-store",
      credentials: "same-origin",
    });

    if (!resp.ok) {
      const text = await resp.text();
      let message = `Erro ao exportar XLSX (${resp.status})`;
      try {
        const payload = text ? JSON.parse(text) : {};
        message = payload?.error || payload?.message || message;
      } catch {
        if (text) message = text;
      }
      throw new Error(message);
    }

    const blob = await resp.blob();
    const blobUrl = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = "leads_export.xlsx";
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(blobUrl);
    setStatus("Exportação XLSX concluída.", "ok");
  } catch (e) {
    console.error(e);
    setStatus(e.message || "Falha ao exportar XLSX.", "err");
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = previousText || "Exportar XLSX";
    }
  }
}

async function startBatchExport() {
  const payload = { ...buildLeadsParams(), batch_size: 1000 };
  delete payload.limit;
  delete payload.offset;

  try {
    setExportProgress({ visible: true, text: "Iniciando exportação...", progress: 5 });
    const resp = await apiPostJson("/api/export/batch", payload);
    activeExportJobId = resp?.job_id;
    if (!activeExportJobId) throw new Error("Job não retornado pela API.");
    pollBatchExportStatus();
  } catch (e) {
    setExportProgress({ visible: true, text: `Falha ao iniciar exportação: ${e.message}`, progress: 0 });
  }
}

async function pollBatchExportStatus() {
  if (!activeExportJobId) return;

  const timer = setInterval(async () => {
    try {
      const resp = await apiGet("/api/export/batch/status", { job_id: activeExportJobId });
      const data = resp?.data || {};
      const total = Number(data.total || 0);
      const processed = Number(data.processed || 0);
      const pct = total > 0 ? Math.round((processed / total) * 100) : 10;
      const msg = data.message || "Processando...";
      setExportProgress({ visible: true, text: `${msg} (${processed}/${total})`, progress: pct });

      if (data.status === "done") {
        clearInterval(timer);
        setExportProgress({ visible: true, text: "Concluído. Baixando arquivo...", progress: 100 });
        window.location.href = `/api/export/batch/download?job_id=${encodeURIComponent(activeExportJobId)}`;
        activeExportJobId = null;
      } else if (data.status === "error") {
        clearInterval(timer);
        setExportProgress({ visible: true, text: data.error || "Falha no processamento.", progress: 0 });
        activeExportJobId = null;
      }
    } catch (e) {
      clearInterval(timer);
      setExportProgress({ visible: true, text: `Erro ao consultar progresso: ${e.message}`, progress: 0 });
      activeExportJobId = null;
    }
  }, 1500);
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

  currentPage = 1;
  loadLeadsAndKpis();
}


function initUploadInteractions() {
  const input = $("#uploadFile");
  const dropzone = $("#uploadDropzone");
  input?.addEventListener("change", updateSelectedFileName);
  if (!dropzone || !input) return;

  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.add("is-dragover");
    });
  });

  ["dragleave", "drop"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.remove("is-dragover");
    });
  });

  dropzone.addEventListener("drop", (event) => {
    const files = event.dataTransfer?.files;
    if (!files || files.length === 0) return;
    input.files = files;
    updateSelectedFileName();
  });
}

/* =========================
   Eventos
========================= */
document.addEventListener("DOMContentLoaded", async () => {
  initMultiSelects();
  initUploadInteractions();

  refreshSavedFiltersSelect();

  $("#btnApply")?.addEventListener("click", () => {
    currentPage = 1;
    loadLeadsAndKpis();
  });
  $("#btnSaveFilterView")?.addEventListener("click", saveCurrentFilterView);
  $("#btnDeleteFilterView")?.addEventListener("click", deleteSavedFilterView);
  $("#savedFilterSelect")?.addEventListener("change", () => {
    const select = $("#savedFilterSelect");
    const selectedId = (select?.value || "").trim();
    if (!selectedId) return;
    applySavedFilterView();
  });
  $("#btnReload")?.addEventListener("click", async () => {
    await loadOptions();
    await loadLeadsAndKpis();
  });
  $("#btnClear")?.addEventListener("click", clearFilters);

  $("#btnUpload")?.addEventListener("click", doUpload);
  $("#btnExport")?.addEventListener("click", exportXlsxServerSide);
  $("#btnBatchExport")?.addEventListener("click", startBatchExport);
  $("#btnPrevPage")?.addEventListener("click", () => {
    if (currentPage <= 1) return;
    currentPage -= 1;
    loadLeadsAndKpis();
  });
  $("#btnNextPage")?.addEventListener("click", () => {
    const limit = Number($("#fLimit")?.value || 500) || 500;
    if ((currentPage * limit) >= totalLeads) return;
    currentPage += 1;
    loadLeadsAndKpis();
  });

  $("#fIni")?.addEventListener("change", () => {
    currentPage = 1;
    loadLeadsAndKpisDebounced();
  });
  $("#fFim")?.addEventListener("change", () => {
    currentPage = 1;
    loadLeadsAndKpisDebounced();
  });
  $("#fMatriculado")?.addEventListener("change", () => {
    currentPage = 1;
    loadLeadsAndKpisDebounced();
  });
  $("#fLimit")?.addEventListener("change", () => {
    currentPage = 1;
    loadLeadsAndKpisDebounced();
  });
  $("#fBusca")?.addEventListener("input", () => {
    currentPage = 1;
    loadLeadsAndKpisDebounced();
  });

  await loadOptions();
  await loadLeadsAndKpis();
});
