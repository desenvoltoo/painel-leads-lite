/* ============================================================
   Painel Leads Lite — app.js (multi-select + modais + export server)
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
  options: {
    status: [],
    curso: [],
    polo: [],
    origem: [],
  },
  filters: {
    status: "",
    origem: "",
    data_ini: "",
    data_fim: "",
    limit: 500,

    // multi
    curso_list: [],
    polo_list: [],
  },
};

/* ============================================================
   API
============================================================ */
async function apiGet(path, params = {}) {
  const url = new URL(path, window.location.origin);

  Object.entries(params).forEach(([k, v]) => {
    if (v === null || v === undefined) return;

    // arrays -> repetição de query param: curso=A&curso=B
    if (Array.isArray(v)) {
      v.forEach((item) => {
        if (item === null || item === undefined) return;
        const s = String(item).trim();
        if (s) url.searchParams.append(k, s);
      });
      return;
    }

    const s = String(v).trim();
    if (s) url.searchParams.set(k, s);
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
   UI: table + KPIs
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
  (values || []).forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    dl.appendChild(opt);
  });
}

/* ============================================================
   Multi-select chips
============================================================ */
function _uniqPush(arr, value) {
  const v = String(value || "").trim();
  if (!v) return;
  if (!arr.some((x) => String(x).toUpperCase() === v.toUpperCase())) arr.push(v);
}

function _removeValue(arr, value) {
  const v = String(value || "").trim().toUpperCase();
  const idx = arr.findIndex((x) => String(x).trim().toUpperCase() === v);
  if (idx >= 0) arr.splice(idx, 1);
}

function renderChips(kind) {
  // kind: "curso" | "polo"
  const box = kind === "curso" ? $("#cursoChipBox") : $("#poloChipBox");
  const hidden = kind === "curso" ? $("#fCursoMulti") : $("#fPoloMulti");
  const list = kind === "curso" ? state.filters.curso_list : state.filters.polo_list;

  if (!box || !hidden) return;

  hidden.value = list.join("||"); // só pra debug/inspeção
  box.innerHTML = "";

  if (!list.length) {
    const empty = document.createElement("div");
    empty.className = "muted";
    empty.textContent = "Nenhum selecionado.";
    box.appendChild(empty);
    return;
  }

  list.forEach((v) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.innerHTML = `<span class="chip-text">${escapeHtml(v)}</span><span class="chip-x">×</span>`;
    chip.addEventListener("click", () => {
      _removeValue(list, v);
      renderChips(kind);
    });
    box.appendChild(chip);
  });
}

function addFromInput(kind) {
  const input = kind === "curso" ? $("#fCurso") : $("#fPolo");
  if (!input) return;

  const raw = (input.value || "").trim();
  if (!raw) return;

  if (kind === "curso") _uniqPush(state.filters.curso_list, raw);
  if (kind === "polo") _uniqPush(state.filters.polo_list, raw);

  input.value = "";
  renderChips(kind);
}

/* ============================================================
   Modais (Cursos/Polos) com busca + scroll
============================================================ */
function openModal(modalEl) {
  if (!modalEl) return;
  modalEl.classList.add("open");
  modalEl.setAttribute("aria-hidden", "false");
}

function closeModal(modalEl) {
  if (!modalEl) return;
  modalEl.classList.remove("open");
  modalEl.setAttribute("aria-hidden", "true");
}

function buildModalList(kind) {
  // kind: "curso" | "polo"
  const listEl = kind === "curso" ? $("#modalCursosList") : $("#modalPolosList");
  const searchEl = kind === "curso" ? $("#modalCursosSearch") : $("#modalPolosSearch");

  const values = kind === "curso" ? (state.options.curso || []) : (state.options.polo || []);
  const selected = kind === "curso" ? state.filters.curso_list : state.filters.polo_list;

  if (!listEl) return;

  const q = (searchEl?.value || "").trim().toUpperCase();

  const filtered = values.filter((v) => {
    const s = String(v || "");
    if (!s) return false;
    if (!q) return true;
    return s.toUpperCase().includes(q);
  });

  listEl.innerHTML = "";

  if (!filtered.length) {
    listEl.innerHTML = `<div class="muted">Nada encontrado.</div>`;
    return;
  }

  filtered.forEach((v) => {
    const id = `${kind}_${btoa(unescape(encodeURIComponent(v))).slice(0, 24)}`;

    const row = document.createElement("label");
    row.className = "modal-item";

    const chk = document.createElement("input");
    chk.type = "checkbox";
    chk.value = v;
    chk.checked = selected.some((x) => String(x).trim().toUpperCase() === String(v).trim().toUpperCase());

    const span = document.createElement("span");
    span.className = "modal-item-text";
    span.textContent = v;

    row.appendChild(chk);
    row.appendChild(span);
    listEl.appendChild(row);
  });
}

function applyModalSelection(kind) {
  const listEl = kind === "curso" ? $("#modalCursosList") : $("#modalPolosList");
  if (!listEl) return;

  const checks = Array.from(listEl.querySelectorAll('input[type="checkbox"]'));
  const chosen = checks.filter((c) => c.checked).map((c) => c.value);

  if (kind === "curso") state.filters.curso_list = chosen;
  if (kind === "polo") state.filters.polo_list = chosen;

  renderChips(kind);
}

function clearModalSelection(kind) {
  if (kind === "curso") state.filters.curso_list = [];
  if (kind === "polo") state.filters.polo_list = [];
  renderChips(kind);
  buildModalList(kind);
}

/* ============================================================
   Filters: read UI -> state
============================================================ */
function readFiltersFromUI() {
  state.filters.status = ($("#fStatus")?.value || "").trim();
  state.filters.origem = ($("#fOrigem")?.value || "").trim();
  state.filters.data_ini = ($("#fIni")?.value || "").trim();
  state.filters.data_fim = ($("#fFim")?.value || "").trim();

  const lim = parseInt($("#fLimit")?.value || "500", 10);
  state.filters.limit = Number.isFinite(lim) ? lim : 500;

  // curso/polo multi já ficam no state (chips + modal),
  // mas se tiver algo digitado e o usuário não deu Enter, não adicionamos automaticamente.
}

/* ============================================================
   Loaders
============================================================ */
async function loadOptions() {
  const data = await apiGet("/api/options");
  state.options = {
    status: data.status || [],
    curso: data.curso || [],
    polo: data.polo || [],
    origem: data.origem || [],
  };

  fillDatalist("dlStatus", state.options.status);
  fillDatalist("dlCurso", state.options.curso);
  fillDatalist("dlPolo", state.options.polo);
  fillDatalist("dlOrigem", state.options.origem);

  // re-render chips (caso existam)
  renderChips("curso");
  renderChips("polo");
}

async function loadLeadsAndKpis() {
  readFiltersFromUI();

  const statusLine = $("#statusLine");
  if (statusLine) statusLine.textContent = "Carregando...";

  const params = {
    status: state.filters.status,
    origem: state.filters.origem,
    data_ini: state.filters.data_ini,
    data_fim: state.filters.data_fim,
    limit: state.filters.limit,

    // ✅ multi: vira curso=A&curso=B ...
    curso: state.filters.curso_list,
    polo: state.filters.polo_list,
  };

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
   Reload + Export (server)
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

function exportCsvServer() {
  // usa /api/export (UTF-8-SIG) -> resolve caracteres especiais
  readFiltersFromUI();

  const url = new URL("/api/export", window.location.origin);

  // scalar
  if (state.filters.status) url.searchParams.set("status", state.filters.status);
  if (state.filters.origem) url.searchParams.set("origem", state.filters.origem);
  if (state.filters.data_ini) url.searchParams.set("data_ini", state.filters.data_ini);
  if (state.filters.data_fim) url.searchParams.set("data_fim", state.filters.data_fim);

  // multi
  (state.filters.curso_list || []).forEach((c) => url.searchParams.append("curso", c));
  (state.filters.polo_list || []).forEach((p) => url.searchParams.append("polo", p));

  // export_limit opcional (se quiser mais que o limite da tela)
  url.searchParams.set("export_limit", "200000");

  // abre download
  window.open(url.toString(), "_blank", "noopener");
}

function bindExport() {
  const btn = $("#btnExportFilters");
  if (!btn) return;

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    showToast("Exportando CSV...", "ok");
    exportCsvServer();
  });
}

/* ============================================================
   Bind filtros (apply/clear + auto refresh)
============================================================ */
function bindFilters() {
  const ids = ["#fStatus", "#fOrigem", "#fIni", "#fFim", "#fLimit"];

  ids.forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("change", loadLeadsAndKpisDebounced);
    el.addEventListener("keyup", loadLeadsAndKpisDebounced);
  });

  // inputs de multi: Enter adiciona chip
  const fCurso = $("#fCurso");
  if (fCurso) {
    fCurso.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addFromInput("curso");
        loadLeadsAndKpisDebounced();
      }
    });
  }

  const fPolo = $("#fPolo");
  if (fPolo) {
    fPolo.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addFromInput("polo");
        loadLeadsAndKpisDebounced();
      }
    });
  }

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

      ["#fStatus", "#fOrigem", "#fIni", "#fFim"].forEach((id) => {
        const el = $(id);
        if (el) el.value = "";
      });

      const lim = $("#fLimit");
      if (lim) lim.value = "500";

      // limpa multis
      state.filters.curso_list = [];
      state.filters.polo_list = [];
      const inCurso = $("#fCurso");
      const inPolo = $("#fPolo");
      if (inCurso) inCurso.value = "";
      if (inPolo) inPolo.value = "";

      renderChips("curso");
      renderChips("polo");

      setUploadStatus("");
      showToast("Filtros limpos.", "ok");
      loadLeadsAndKpis();
    });
  }
}

/* ============================================================
   Bind modais (Cursos/Polos)
============================================================ */
function bindModals() {
  // ---- Cursos
  const modalCursos = $("#modalCursos");
  const btnPickCursos = $("#btnPickCursos");
  const btnCloseCursos = $("#btnCloseCursos");
  const btnCursosApply = $("#btnCursosApply");
  const btnCursosClear = $("#btnCursosClear");
  const searchCursos = $("#modalCursosSearch");

  if (btnPickCursos && modalCursos) {
    btnPickCursos.addEventListener("click", () => {
      buildModalList("curso");
      openModal(modalCursos);
      setTimeout(() => searchCursos?.focus(), 50);
    });
  }

  btnCloseCursos?.addEventListener("click", () => closeModal(modalCursos));
  btnCursosApply?.addEventListener("click", () => {
    applyModalSelection("curso");
    closeModal(modalCursos);
    loadLeadsAndKpisDebounced();
  });
  btnCursosClear?.addEventListener("click", () => clearModalSelection("curso"));
  searchCursos?.addEventListener("input", debounce(() => buildModalList("curso"), 150));

  // fechar clicando fora
  modalCursos?.addEventListener("click", (e) => {
    if (e.target === modalCursos) closeModal(modalCursos);
  });

  // ---- Polos
  const modalPolos = $("#modalPolos");
  const btnPickPolos = $("#btnPickPolos");
  const btnClosePolos = $("#btnClosePolos");
  const btnPolosApply = $("#btnPolosApply");
  const btnPolosClear = $("#btnPolosClear");
  const searchPolos = $("#modalPolosSearch");

  if (btnPickPolos && modalPolos) {
    btnPickPolos.addEventListener("click", () => {
      buildModalList("polo");
      openModal(modalPolos);
      setTimeout(() => searchPolos?.focus(), 50);
    });
  }

  btnClosePolos?.addEventListener("click", () => closeModal(modalPolos));
  btnPolosApply?.addEventListener("click", () => {
    applyModalSelection("polo");
    closeModal(modalPolos);
    loadLeadsAndKpisDebounced();
  });
  btnPolosClear?.addEventListener("click", () => clearModalSelection("polo"));
  searchPolos?.addEventListener("input", debounce(() => buildModalList("polo"), 150));

  modalPolos?.addEventListener("click", (e) => {
    if (e.target === modalPolos) closeModal(modalPolos);
  });

  // ESC fecha qualquer modal aberto
  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    if (modalCursos?.classList.contains("open")) closeModal(modalCursos);
    if (modalPolos?.classList.contains("open")) closeModal(modalPolos);
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
    bindModals();

    // primeira renderização de chips
    renderChips("curso");
    renderChips("polo");

    await loadOptions();
    await loadLeadsAndKpis();

  } catch (e) {
    console.error(e);
    showToast("Erro ao iniciar o painel.", "err");
  }
});
