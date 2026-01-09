async function fetchJSON(url) {
  const res = await fetch(url);
  const data = await res.json();
  if (!res.ok) {
    const msg = data && data.error ? data.error : "Erro na requisição";
    throw new Error(msg);
  }
  return data;
}

function getFilters() {
  return {
    status: document.getElementById("fStatus").value.trim() || "",
    curso: document.getElementById("fCurso").value.trim() || "",
    polo: document.getElementById("fPolo").value.trim() || "",
    origem: document.getElementById("fOrigem").value.trim() || "",
    data_ini: document.getElementById("fIni").value || "",
    data_fim: document.getElementById("fFim").value || "",
    limit: document.getElementById("fLimit").value || "500",
  };
}

function setKpis(kpis) {
  document.getElementById("kpiCount").innerText = String(kpis.total ?? 0);
  document.getElementById("kpiTopStatus").innerText = kpis.top_status?.status || "—";
  document.getElementById("kpiLastDate").innerText = kpis.last_date || "—";
}

function renderTable(rows) {
  const tbody = document.querySelector("#tbl tbody");
  tbody.innerHTML = "";

  const frag = document.createDocumentFragment();
  for (const r of rows) {
    const tr = document.createElement("tr");
    const cols = [
      r.data_inscricao, r.nome, r.cpf, r.celular, r.email,
      r.origem, r.polo, r.curso, r.status, r.consultor
    ];
    for (const c of cols) {
      const td = document.createElement("td");
      td.textContent = (c === null || c === undefined) ? "" : String(c);
      tr.appendChild(td);
    }
    frag.appendChild(tr);
  }
  tbody.appendChild(frag);
}

function downloadCSV(rows) {
  if (!rows || rows.length === 0) return;

  const headers = Object.keys(rows[0]);
  const escape = (v) => {
    const s = (v === null || v === undefined) ? "" : String(v);
    const needs = /[",\n;]/.test(s);
    const out = s.replaceAll('"', '""');
    return needs ? `"${out}"` : out;
  };

  const lines = [];
  lines.push(headers.map(escape).join(";"));
  for (const r of rows) lines.push(headers.map(h => escape(r[h])).join(";"));

  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `leads_${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function fillDatalist(id, values) {
  const dl = document.getElementById(id);
  if (!dl) return;
  dl.innerHTML = "";
  const frag = document.createDocumentFragment();
  for (const v of (values || [])) {
    const opt = document.createElement("option");
    opt.value = v;
    frag.appendChild(opt);
  }
  dl.appendChild(frag);
}

async function loadOptions() {
  try {
    const data = await fetchJSON("/api/options");
    fillDatalist("dlStatus", data.status);
    fillDatalist("dlCurso", data.curso);
    fillDatalist("dlPolo", data.polo);
    fillDatalist("dlOrigem", data.origem);
  } catch (e) {
    console.error("Options error:", e);
  }
}

async function load() {
  const statusLine = document.getElementById("statusLine");
  statusLine.textContent = "Consultando…";

  try {
    const filters = getFilters();
    const qs = new URLSearchParams(filters);

    const [kpis, leads] = await Promise.all([
      fetchJSON(`/api/kpis?${qs.toString()}`),
      fetchJSON(`/api/leads?${qs.toString()}`)
    ]);

    setKpis(kpis);

    const rows = leads.rows || [];
    renderTable(rows);

    statusLine.textContent = `Exibindo ${rows.length} lead(s).`;
    window.__lastRows = rows;

  } catch (e) {
    statusLine.textContent = `Erro: ${e.message}`;
    console.error(e);
  }
}

function clearFilters() {
  document.getElementById("fStatus").value = "";
  document.getElementById("fCurso").value = "";
  document.getElementById("fPolo").value = "";
  document.getElementById("fOrigem").value = "";
  document.getElementById("fIni").value = "";
  document.getElementById("fFim").value = "";
  document.getElementById("fLimit").value = "500";
}

// ======================= UPLOAD =======================
async function uploadFile() {
  const fileInput = document.getElementById("uploadFile");
  const statusEl = document.getElementById("uploadStatus");
  const source = document.getElementById("uploadSource").value.trim() || "UPLOAD_PAINEL";

  if (!fileInput.files || fileInput.files.length === 0) {
    statusEl.textContent = "Selecione um arquivo primeiro.";
    return;
  }

  const file = fileInput.files[0];
  const fd = new FormData();
  fd.append("file", file);
  fd.append("source", source);

  statusEl.textContent = "Enviando e carregando no BigQuery…";

  try {
    const res = await fetch("/api/upload", { method: "POST", body: fd });
    const data = await res.json();

    if (!res.ok) throw new Error(data.error || "Falha no upload");

    statusEl.textContent = `OK! ${data.rows_loaded} linhas em ${data.table}`;
    await load();
  } catch (e) {
    statusEl.textContent = `Erro: ${e.message}`;
  }
}

// ======================= EVENTS =======================
document.getElementById("btnApply").addEventListener("click", load);
document.getElementById("btnClear").addEventListener("click", () => {
  clearFilters();
  load();
});
document.getElementById("btnReload").addEventListener("click", load);
document.getElementById("btnExport").addEventListener("click", () => downloadCSV(window.__lastRows || []));
document.getElementById("btnUpload").addEventListener("click", uploadFile);

// boot
loadOptions();
load();
