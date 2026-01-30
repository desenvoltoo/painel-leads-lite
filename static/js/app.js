/* ============================================================
   Painel Leads Lite — app.js (Versão TomSelect)
   - Suporte a seleção múltipla com checkbox para Curso e Polo
   - Integração com a API do BigQuery
============================================================ */

const $ = (sel) => document.querySelector(sel);

// Instâncias globais do TomSelect para controle via código
let tsCurso, tsPolo;

/* ============================================================
   Helpers & UI
============================================================ */
function showToast(msg, type = "ok") {
  const statusLine = $("#statusLine");
  if (statusLine) {
    statusLine.textContent = msg;
    statusLine.className = type === "err" ? "error" : "";
  }
}

function setUploadStatus(msg, type = "muted") {
  const el = $("#uploadStatus");
  if (!el) return;
  el.textContent = msg || "";
  el.className = type;
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
}

function fmtDate(d) {
  if (!d) return "";
  const s = String(d);
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const ts = Date.parse(s);
  if (!Number.isNaN(ts)) {
    const dd = new Date(ts);
    return dd.toISOString().slice(0, 10);
  }
  return s.slice(0, 10);
}

/** Ordenação amigável (sem acentos) */
function sortSmart(arr) {
  return (arr || []).slice().sort((a, b) => {
    return a.localeCompare(b, 'pt-BR', { sensitivity: 'base' });
  });
}

/* ============================================================
   API Core
============================================================ */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);
  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined || v === "") return;
    // Se for array (seleção múltipla), o backend espera "Item1 || Item2"
    const val = Array.isArray(v) ? v.join(" || ") : v;
    url.searchParams.set(k, val);
  });
  const res = await fetch(url.toString(), { cache: "no-store" });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || `Erro HTTP ${res.status}`);
  return data;
}

/* ============================================================
   Inicialização do TomSelect (Dropdown com Checkbox)
============================================================ */
function initMultiSelects() {
  const config = {
    plugins: ['remove_button', 'checkbox_options'],
    create: false,
    allowEmptyOption: true,
    maxItems: null,
    onDropdownOpen: () => { /* Fix para layout se necessário */ },
    onChange: () => loadLeadsAndKpisDebounced()
  };

  tsCurso = new TomSelect("#fCurso", config);
  tsPolo = new TomSelect("#fPolo", config);
}

/* ============================================================
   Filtros & Carga de Dados
============================================================ */
function getFilters() {
  return {
    status: $("#fStatus")?.value || "",
    curso: tsCurso ? tsCurso.getValue() : [], // Array de strings
    polo: tsPolo ? tsPolo.getValue() : [],   // Array de strings
    origem: $("#fOrigem")?.value || "",
    data_ini: $("#fIni")?.value || "",
    data_fim: $("#fFim")?.value || "",
    limit: $("#fLimit")?.value || 500
  };
}

async function loadOptions() {
  try {
    const data = await apiGet("/api/options");
    
    // Status e Origem continuam no Datalist padrão
    fillDatalist("dlStatus", sortSmart(data.status));
    fillDatalist("dlOrigem", sortSmart(data.origem));

    // Cursos e Polos vão para o TomSelect
    if (tsCurso) {
      tsCurso.clearOptions();
      sortSmart(data.curso).forEach(c => tsCurso.addOption({ value: c, text: c }));
    }
    if (tsPolo) {
      tsPolo.clearOptions();
      sortSmart(data.polo).forEach(p => tsPolo.addOption({ value: p, text: p }));
    }
  } catch (e) {
    console.error("Erro ao carregar opções:", e);
  }
}

function fillDatalist(id, values) {
  const dl = document.getElementById(id);
  if (!dl) return;
  dl.innerHTML = "";
  values.forEach(v => {
    const opt = document.createElement("option");
    opt.value = v;
    dl.appendChild(opt);
  });
}

async function loadLeadsAndKpis() {
  const statusLine = $("#statusLine");
  if (statusLine) statusLine.textContent = "Consultando BigQuery...";

  const params = getFilters();

  try {
    const [leads, kpis] = await Promise.all([
      apiGet("/api/leads", params),
      apiGet("/api/kpis", params),
    ]);

    renderTable(leads?.rows || []);
    renderKpis(kpis);

    if (statusLine) statusLine.textContent = `${leads?.count || 0} registros encontrados.`;
  } catch (e) {
    showToast(`Erro: ${e.message}`, "err");
  }
}

const loadLeadsAndKpisDebounced = (function(fn, delay) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), delay);
  };
})(loadLeadsAndKpis, 300);

/* ============================================================
   Renderização
============================================================ */
function renderTable(rows) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${escapeHtml(fmtDate(r.data_inscricao))}</td>
      <td>${escapeHtml(r.nome)}</td>
      <td>${escapeHtml(r.cpf)}</td>
      <td>${escapeHtml(r.celular)}</td>
      <td>${escapeHtml(r.email)}</td>
      <td>${escapeHtml(r.origem)}</td>
      <td>${escapeHtml(r.polo)}</td>
      <td>${escapeHtml(r.curso)}</td>
      <td>${escapeHtml(r.status)}</td>
      <td>${escapeHtml(r.consultor)}</td>
    </tr>
  `).join("") || '<tr><td colspan="10">Nenhum dado encontrado.</td></tr>';
}

function renderKpis(k) {
  if ($("#kpiCount")) $("#kpiCount").textContent = k?.total ?? 0;
  if ($("#kpiTopStatus")) {
    const top = k?.top_status;
    $("#kpiTopStatus").textContent = top ? `${top.status} (${top.cnt})` : "-";
  }
  if ($("#kpiLastDate")) $("#kpiLastDate").textContent = fmtDate(k?.last_date) || "-";
}

/* ============================================================
   Event Bindings
============================================================ */
function bindActions() {
  $("#btnApply")?.addEventListener("click", loadLeadsAndKpis);

  $("#btnClear")?.addEventListener("click", () => {
    $("#fStatus").value = "";
    $("#fOrigem").value = "";
    $("#fIni").value = "";
    $("#fFim").value = "";
    tsCurso?.clear();
    tsPolo?.clear();
    loadLeadsAndKpis();
  });

  $("#btnReload")?.addEventListener("click", () => {
    loadOptions();
    loadLeadsAndKpis();
  });

  // Bind para o Upload
  $("#btnUpload")?.addEventListener("click", async () => {
    const fileInput = $("#uploadFile");
    if (!fileInput.files.length) return alert("Selecione um arquivo.");
    
    const formData = new FormData();
    formData.append("file", fileInput.files[0]);
    formData.append("source", $("#uploadSource")?.value || "");

    setUploadStatus("Enviando para BigQuery...", "muted");
    try {
      const res = await fetch("/api/upload", { method: "POST", body: formData });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
      setUploadStatus(`Sucesso! ${data.rows_loaded} linhas processadas.`, "ok");
      loadLeadsAndKpis();
    } catch (e) {
      setUploadStatus(e.message, "error");
    }
  });
}

/* ============================================================
   Init
============================================================ */
document.addEventListener("DOMContentLoaded", async () => {
  initMultiSelects();
  bindActions();
  await loadOptions();
  await loadLeadsAndKpis();
});
