/* ============================================================
   Painel Leads Lite — app.js (compatível com seu index.html)
   - Multi-filtro por campo com "||" (seguro)
   - NÃO envia parâmetros repetidos (não usa array)
   - Ordena opções ignorando acento (UX), mas NÃO altera o texto enviado pro backend
============================================================ */

const $ = (sel) => document.querySelector(sel);

function showToast(msg, type = "ok") {
  const statusLine = $("#statusLine");
  if (statusLine) {
    statusLine.textContent = msg;
    statusLine.className = type === "err" ? "error" : "";
  } else {
    alert(msg);
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
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtDate(d) {
  if (!d) return "";
  const s = String(d);

  // ISO direto
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  // tenta parsear RFC/Date string
  const ts = Date.parse(s);
  if (!Number.isNaN(ts)) {
    const dd = new Date(ts);
    const yyyy = dd.getUTCFullYear();
    const mm = String(dd.getUTCMonth() + 1).padStart(2, "0");
    const day = String(dd.getUTCDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${day}`;
  }

  return s.slice(0, 10);
}

function debounce(fn, delay = 300) {
  let t = null;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), delay);
  };
}

/** Só para ordenação (UX), NÃO use isso para enviar pro backend */
function normTxtForSort(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();
}

/** ✅ Sanitiza multi: mantém o conteúdo, mas limpa espaços e separadores */
function cleanMultiRaw(v) {
  const raw = String(v || "").trim();
  if (!raw) return "";

  // divide por ||, remove vazios, trim, e junta novamente
  const parts = raw
    .split("||")
    .map((x) => String(x || "").trim())
    .filter(Boolean);

  return parts.join("||");
}

/* ============================================================
   State
============================================================ */
const state = {
  filters: {
    status: "",
    curso: "",
    polo: "",
    origem: "",
    data_ini: "",
    data_fim: "",
    limit: 500,
  },
};

/* ============================================================
   API
============================================================ */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;
    const s = String(v).trim();
    if (s) url.searchParams.set(k, s);
  });

  const res = await fetch(url.toString(), { cache: "no-store" });
  const data = await res.json().catch(() => ({}));

  if (!res.ok) {
    const msg = data?.error || data?.message || data?.details || `HTTP ${res.status}`;
    throw new Error(msg);
  }

  return data;
}

async function apiPostForm(path, formData) {
  const res = await fetch(path, { method: "POST", body: formData });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || data?.message || `HTTP ${res.status}`);
  return data;
}

/* ============================================================
   UI: render
============================================================ */
function renderTable(rows) {
  const tbody = $("#tbl tbody");
  if (!tbody) return;

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="10" class="muted">Nenhum lead encontrado.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((r) => {
      return `
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
      `;
    })
    .join("");
}

function renderKpis(k) {
  const total = $("#kpiCount");
  const top = $("#kpiTopStatus");
  const last = $("#kpiLastDate");

  if (total) total.textContent = k?.total ?? 0;

  if (top) {
    const s = k?.top_status?.status ?? "-";
    const c = k?.top_status?.cnt ?? 0;
    top.textContent = s === "-" ? "-" : `${s} (${c})`;
  }

  if (last) last.textContent = k?.last_date ? fmtDate(k.last_date) : "-";
}

function fillDatalist(id, values) {
  const dl = document.getElementById(id);
  if (!dl) return;

  dl.innerHTML = "";

  // evita travar o browser se vier coisa absurda
  const MAX = 50000;
  const arr = Array.isArray(values) ? values.slice(0, MAX) : [];

  arr.forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    dl.appendChild(opt);
  });
}

/* ============================================================
   Filters
============================================================ */
function readFiltersFromUI() {
  // ✅ multi seguro: limpa espaços e separadores, mas NÃO altera acento/letras
  state.filters.status = cleanMultiRaw($("#fStatus")?.value || "");
  state.filters.curso = cleanMultiRaw($("#fCurso")?.value || "");
  state.filters.polo = cleanMultiRaw($("#fPolo")?.value || "");
  state.filters.origem = cleanMultiRaw($("#fOrigem")?.value || "");

  state.filters.data_ini = ($("#fIni")?.value || "").trim();
  state.filters.data_fim = ($("#fFim")?.value || "").trim();

  const lim = parseInt($("#fLimit")?.value || "500", 10);
  state.filters.limit = Number.isFinite(lim) ? Math.max(50, Math.min(lim, 5000)) : 500;
}

function buildParams() {
  readFiltersFromUI();

  // ✅ mantém exatamente a string digitada, incluindo "||" (já saneada)
  return {
    status: state.filters.status,
    curso: state.filters.curso,
    polo: state.filters.polo,
    origem: state.filters.origem,
    data_ini: state.filters.data_ini,
    data_fim: state.filters.data_fim,
    limit: state.filters.limit,
  };
}

/* ============================================================
   Loaders
============================================================ */
async function loadOptions() {
  const data = await apiGet("/api/options");

  const sortSmart = (arr) =>
    (arr || []).slice().sort((a, b) => {
      const na = normTxtForSort(a);
      const nb = normTxtForSort(b);
      if (na < nb) return -1;
      if (na > nb) return 1;
      return 0;
    });

  fillDatalist("dlStatus", sortSmart(data.status));
  fillDatalist("dlCurso", sortSmart(data.curso));
  fillDatalist("dlPolo", sortSmart(data.polo));
  fillDatalist("dlOrigem", sortSmart(data.origem));
}

async function loadLeadsAndKpis() {
  const statusLine = $("#statusLine");
  if (statusLine) statusLine.textContent = "Carregando...";

  const params = buildParams();

  try {
    const [leads, kpis] = await Promise.all([
      apiGet("/api/leads", params),
      apiGet("/api/kpis", params),
    ]);

    const rows = leads?.rows || [];
    renderTable(rows);
    renderKpis(kpis);

    const count = leads?.count ?? rows.length ?? 0;
    if (statusLine) statusLine.textContent = `${count} registros carregados.`;
  } catch (e) {
    console.error(e);
    renderTable([]);
    renderKpis({ total: 0, top_status: null, last_date: null });
    showToast(`Falha ao carregar dados: ${e.message}`, "err");
  }
}

const loadLeadsAndKpisDebounced = debounce(loadLeadsAndKpis, 250);

/* ============================================================
   Upload
============================================================ */
function bindUpload() {
  const file = $("#uploadFile");
  const source = $("#uploadSource");
  const btn = $("#btnUpload");
  if (!file || !btn) return;

  btn.addEventListener("click", async (e) => {
    e.preventDefault();

    if (!file.files || file.files.length === 0) {
      setUploadStatus("Selecione um arquivo CSV ou XLSX.", "warn");
      return;
    }

    const f = file.files[0];
    const fd = new FormData();
    fd.append("file", f);
    if (source && source.value) fd.append("source", source.value);

    btn.disabled = true;
    setUploadStatus("Enviando...", "muted");

    try {
      const data = await apiPostForm("/api/upload", fd);

      const rows = data.rows_loaded ?? 0;
      const fname = data.filename ?? f.name ?? "arquivo";
      const msg = data.message ?? "Upload concluído.";

      setUploadStatus(`${msg} (${rows} linhas) — ${fname}`, "ok");
      showToast("Upload finalizado. Atualizando dados...", "ok");

      await loadOptions();
      await loadLeadsAndKpis();

      showToast("Painel atualizado com os novos dados.", "ok");
    } catch (err) {
      console.error(err);
      setUploadStatus(`Falha: ${err.message}`, "error");
      showToast(`Falha no upload: ${err.message}`, "err");
    } finally {
      btn.disabled = false;
      file.value = "";
    }
  });
}

/* ============================================================
   Reload + Export
============================================================ */
function bindReload() {
  const btn = $("#btnReload");
  if (!btn) return;
  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    await loadOptions();
    await loadLeadsAndKpis();
  });
}

function buildCsv(rows, headers, sep = ";") {
  const escapeCell = (v) => {
    const s = String(v ?? "");
    const safe = s.replaceAll('"', '""');
    return `"${safe}"`;
  };

  const lines = [];
  lines.push(headers.map(escapeCell).join(sep));
  rows.forEach((r) => {
    lines.push(headers.map((h) => escapeCell(r?.[h])).join(sep));
  });

  return "\uFEFF" + lines.join("\n"); // BOM
}

function bindExport() {
  const btn = $("#btnExport");
  if (!btn) return;

  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    try {
      const params = buildParams();
      params.limit = "5000";

      const data = await apiGet("/api/leads", params);
      const rows = data?.rows || [];

      if (!rows.length) {
        showToast("Nada para exportar com esses filtros.", "warn");
        return;
      }

      const headers = [
        "data_inscricao",
        "nome",
        "cpf",
        "celular",
        "email",
        "origem",
        "polo",
        "curso",
        "status",
        "consultor",
      ];

      const csv = buildCsv(
        rows.map((r) => ({ ...r, data_inscricao: fmtDate(r?.data_inscricao) })),
        headers,
        ";"
      );

      const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
      const url = URL.createObjectURL(blob);

      const a = document.createElement("a");
      a.href = url;
      a.download = `leads_export_${new Date().toISOString().slice(0, 10)}.csv`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);

      showToast("CSV exportado.", "ok");
    } catch (err) {
      console.error(err);
      showToast(`Falha ao exportar CSV: ${err.message}`, "err");
    }
  });
}

/* ============================================================
   Bind filters
============================================================ */
function bindFilters() {
  const ids = ["#fStatus", "#fCurso", "#fPolo", "#fOrigem", "#fIni", "#fFim", "#fLimit"];

  ids.forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("change", loadLeadsAndKpisDebounced);
    el.addEventListener("keyup", loadLeadsAndKpisDebounced);
  });

  $("#btnApply")?.addEventListener("click", (e) => {
    e.preventDefault();
    loadLeadsAndKpis();
  });

  $("#btnClear")?.addEventListener("click", (e) => {
    e.preventDefault();

    ["#fStatus", "#fCurso", "#fPolo", "#fOrigem", "#fIni", "#fFim"].forEach((id) => {
      const el = $(id);
      if (el) el.value = "";
    });

    const lim = $("#fLimit");
    if (lim) lim.value = "500";

    setUploadStatus("");
    showToast("Filtros limpos.", "ok");
    loadLeadsAndKpis();
  });
}

/* ============================================================
   Init
============================================================ */
document.addEventListener("DOMContentLoaded", async () => {
  try {
    bindUpload();
    bindFilters();
    bindReload();
    bindExport();

    await loadOptions();
    await loadLeadsAndKpis();
  } catch (e) {
    console.error(e);
    showToast(`Erro ao iniciar o painel: ${e.message}`, "err");
  }
});
