/* ============================================================
   Painel Leads Lite — app.js (compatível com seu index.html)
============================================================ */

const $ = (sel) => document.querySelector(sel);

function showToast(msg, type = "ok") {
  // Se não existir toast no HTML, usa statusLine/uploadStatus
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
  return String(d).slice(0, 10);
}

function debounce(fn, delay = 300) {
  let t = null;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), delay);
  };
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
    if (v !== null && v !== undefined) url.searchParams.set(k, v);
  });

  const res = await fetch(url.toString(), { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function apiPostForm(path, formData) {
  const res = await fetch(path, { method: "POST", body: formData });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
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
  // IDs do seu HTML:
  // total: #kpiCount
  // top status: #kpiTopStatus
  // last: #kpiLastDate
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
  (values || []).forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    dl.appendChild(opt);
  });
}

/* ============================================================
   Filters
============================================================ */
function readFiltersFromUI() {
  state.filters.status = ($("#fStatus")?.value || "").trim();
  state.filters.curso = ($("#fCurso")?.value || "").trim();
  state.filters.polo = ($("#fPolo")?.value || "").trim();
  state.filters.origem = ($("#fOrigem")?.value || "").trim();

  // seu HTML usa fIni e fFim
  state.filters.data_ini = ($("#fIni")?.value || "").trim();
  state.filters.data_fim = ($("#fFim")?.value || "").trim();

  const lim = parseInt($("#fLimit")?.value || "500", 10);
  state.filters.limit = Number.isFinite(lim) ? lim : 500;
}

/* ============================================================
   Loaders
============================================================ */
async function loadOptions() {
  const data = await apiGet("/api/options");
  fillDatalist("dlStatus", data.status);
  fillDatalist("dlCurso", data.curso);
  fillDatalist("dlPolo", data.polo);
  fillDatalist("dlOrigem", data.origem);
}

async function loadLeadsAndKpis() {
  readFiltersFromUI();

  const statusLine = $("#statusLine");
  if (statusLine) statusLine.textContent = "Carregando...";

  try {
    const [leads, kpis] = await Promise.all([
      apiGet("/api/leads", state.filters),
      apiGet("/api/kpis", state.filters),
    ]);

    const rows = leads?.rows || [];
    renderTable(rows);
    renderKpis(kpis);

    const count = leads?.count ?? rows.length ?? 0;
    if (statusLine) statusLine.textContent = `${count} registros carregados.`;

  } catch (e) {
    console.error(e);
    renderTable([]);
    showToast("Falha ao carregar dados do BigQuery.", "err");
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

      // Atualiza painel após promote
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
   Extras: Reload + Export
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

/* ============================================================
   Export helper (reaproveitado pelos 2 botões)
============================================================ */
async function exportCsvFromFilters() {
  try {
    readFiltersFromUI();
    const data = await apiGet("/api/leads", state.filters);
    const rows = data?.rows || [];

    if (!rows.length) {
      showToast("Nada para exportar com esses filtros.", "warn");
      return;
    }

    const headers = [
      "data_inscricao","nome","cpf","celular","email",
      "origem","polo","curso","status","consultor"
    ];

    const csv = [
      headers.join(","),
      ...rows.map(r => headers.map(h => {
        const v = r[h] ?? "";
        const s = String(v).replaceAll('"', '""');
        return `"${s}"`;
      }).join(","))
    ].join("\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `leads_export_${new Date().toISOString().slice(0,10)}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);

    showToast("CSV exportado.", "ok");
  } catch (err) {
    console.error(err);
    showToast("Falha ao exportar CSV.", "err");
  }
}

function bindExport() {
  // Botão do topo (já existe)
  const btnTop = $("#btnExport");

  // Botão novo ao lado do Limpar
  const btnFilters = $("#btnExportFilters");

  const handler = async (e) => {
    e.preventDefault();
    e.stopPropagation();

    showToast("Exportando CSV...", "ok");
    await exportCsvFromFilters();
  };

  if (btnTop) btnTop.addEventListener("click", handler);
  if (btnFilters) btnFilters.addEventListener("click", handler);
}


/* ============================================================
   Bind filters (apply/clear + auto refresh on typing)
============================================================ */
function bindFilters() {
  const ids = ["#fStatus", "#fCurso", "#fPolo", "#fOrigem", "#fIni", "#fFim", "#fLimit"];

  ids.forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("change", loadLeadsAndKpisDebounced);
    el.addEventListener("keyup", loadLeadsAndKpisDebounced);
  });

  const btnApply = $("#btnApply");
  if (btnApply) {
    btnApply.addEventListener("click", (e) => {
      e.preventDefault();
      loadLeadsAndKpis();
    });
  }

  const btnClear = $("#btnClear");
  if (btnClear) {
    btnClear.addEventListener("click", (e) => {
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
    showToast("Erro ao iniciar o painel.", "err");
  }
});
