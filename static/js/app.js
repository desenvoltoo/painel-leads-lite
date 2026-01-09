async function fetchLeads(params) {
  const qs = new URLSearchParams(params);
  const res = await fetch(`/api/leads?${qs.toString()}`);
  if (!res.ok) throw new Error("Falha ao consultar API");
  return res.json();
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

function setKpis(rows) {
  const count = rows.length;
  document.getElementById("kpiCount").innerText = String(count);

  if (count === 0) {
    document.getElementById("kpiTopStatus").innerText = "—";
    document.getElementById("kpiLastDate").innerText = "—";
    return;
  }

  // Top status
  const freq = {};
  for (const r of rows) {
    const s = (r.status || "").toString();
    freq[s] = (freq[s] || 0) + 1;
  }
  let topS = Object.keys(freq)[0];
  for (const k of Object.keys(freq)) if (freq[k] > freq[topS]) topS = k;
  document.getElementById("kpiTopStatus").innerText = topS || "—";

  // Last date
  const dates = rows.map(r => (r.data_inscricao || "").toString()).filter(Boolean).sort();
  document.getElementById("kpiLastDate").innerText = dates[dates.length - 1] || "—";
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
  for (const r of rows) {
    lines.push(headers.map(h => escape(r[h])).join(";"));
  }
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

async function load() {
  const statusLine = document.getElementById("statusLine");
  statusLine.textContent = "Consultando…";

  try {
    const filters = getFilters();
    const data = await fetchLeads(filters);
    const rows = data.rows || [];
    renderTable(rows);
    setKpis(rows);

    statusLine.textContent = `Exibindo ${rows.length} lead(s).`;
    window.__lastRows = rows;
  } catch (e) {
    statusLine.textContent = "Erro ao consultar. Verifique credenciais/view no BigQuery.";
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

document.getElementById("btnApply").addEventListener("click", load);
document.getElementById("btnClear").addEventListener("click", () => {
  clearFilters();
  load();
});
document.getElementById("btnReload").addEventListener("click", load);
document.getElementById("btnExport").addEventListener("click", () => downloadCSV(window.__lastRows || []));

// carrega ao abrir
load();
