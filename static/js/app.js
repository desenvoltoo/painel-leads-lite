const $ = (sel) => document.querySelector(sel);
let tsCurso, tsPolo;

/* Helpers */
function showToast(msg, type = "ok") {
    const statusLine = $("#statusLine");
    if (statusLine) {
        statusLine.textContent = msg;
        statusLine.className = type === "err" ? "error" : "";
    }
}

function escapeHtml(str) {
    if (!str) return "";
    return String(str).replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[m]));
}

function fmtDate(d) {
    if (!d || d === "None") return "-";
    // Tenta formatar ISO para DD/MM/YYYY ou apenas corta a data
    try {
        const s = String(d);
        return s.slice(0, 10).split('-').reverse().join('/');
    } catch (e) { return String(d); }
}

/* API */
async function apiGet(path, params = {}) {
    const url = new URL(path, window.location.origin);
    Object.entries(params).forEach(([k, v]) => {
        if (!v || (Array.isArray(v) && v.length === 0)) return;
        // Envia listas separadas por || para o backend tratar
        url.searchParams.set(k, Array.isArray(v) ? v.join(" || ") : v);
    });
    const res = await fetch(url.toString(), { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Erro na API");
    return data;
}

/* Inicialização TomSelect */
function initMultiSelects() {
    const config = {
        plugins: ['remove_button'],
        maxItems: null,
        valueField: 'value',
        labelField: 'text',
        searchField: ['text'],
        render: {
            option: function(data, escape) {
                return `<div class="d-flex-option"><span class="checkbox"></span><span>${escape(data.text)}</span></div>`;
            }
        },
        onChange: () => loadLeadsAndKpisDebounced()
    };

    // Inicializa selects inteligentes para Curso e Polo
    if ($("#fCurso")) tsCurso = new TomSelect("#fCurso", config);
    if ($("#fPolo")) tsPolo = new TomSelect("#fPolo", config);
}

async function loadOptions() {
    try {
        const data = await apiGet("/api/options");
        
        if (tsCurso && data.curso) {
            tsCurso.clearOptions();
            data.curso.forEach(c => tsCurso.addOption({ value: c, text: c }));
        }
        if (tsPolo && data.polo) {
            tsPolo.clearOptions();
            data.polo.forEach(p => tsPolo.addOption({ value: p, text: p }));
        }

        // Datalist simples para Status
        const dlStatus = $("#dlStatus");
        if (dlStatus && data.status) {
            dlStatus.innerHTML = data.status.map(v => `<option value="${v}">`).join("");
        }
    } catch (e) { console.error("Erro ao carregar opções:", e); }
}

async function loadLeadsAndKpis() {
    showToast("Consultando BigQuery...", "ok");
    
    const params = {
        status: $("#fStatus")?.value,
        curso: tsCurso?.getValue(),
        polo: tsPolo?.getValue(),
        data_ini: $("#fIni")?.value,
        data_fim: $("#fFim")?.value,
        limit: $("#fLimit")?.value || 500
    };

    try {
        // Busca Leads e KPIs simultaneamente
        const [leads, kpis] = await Promise.all([
            apiGet("/api/leads", params),
            apiGet("/api/kpis", params)
        ]);

        renderTable(leads);
        renderKpis(kpis);
        showToast(`${leads.length} registros encontrados.`, "ok");
    } catch (e) { 
        showToast(e.message, "err"); 
        console.error(e);
    }
}

const loadLeadsAndKpisDebounced = (() => {
    let t; return () => { clearTimeout(t); t = setTimeout(loadLeadsAndKpis, 500); };
})();

function renderTable(rows) {
    const tbody = $("#tbl tbody");
    if (!tbody) return;

    if (rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" style="text-align:center">Nenhum dado encontrado</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map(r => `
        <tr>
            <td>${escapeHtml(fmtDate(r.data))}</td>
            <td>${escapeHtml(r.nome)}</td>
            <td>${escapeHtml(r.cpf)}</td>
            <td>${escapeHtml(r.celular)}</td>
            <td>${escapeHtml(r.canal || '-')}</td>
            <td>${escapeHtml(r.polo || '-')}</td>
            <td>${escapeHtml(r.curso || '-')}</td>
            <td><span class="badge">${escapeHtml(r.status || 'Lead')}</span></td>
            <td>${escapeHtml(r.consultor || '-')}</td>
            <td>${r.matriculado ? '✅' : '❌'}</td>
        </tr>`).join("");
}

function renderKpis(k) {
    if ($("#kpiCount")) $("#kpiCount").textContent = k?.total || 0;
    if ($("#kpiTopStatus")) $("#kpiTopStatus").textContent = k?.top_status ? `${k.top_status.status} (${k.top_status.cnt})` : "-";
}

/* Eventos */
document.addEventListener("DOMContentLoaded", async () => {
    initMultiSelects();
    
    $("#btnApply")?.addEventListener("click", loadLeadsAndKpis);
    $("#btnReload")?.addEventListener("click", loadLeadsAndKpis);
    
    $("#btnClear")?.addEventListener("click", () => {
        if(tsCurso) tsCurso.clear();
        if(tsPolo) tsPolo.clear();
        $("#fStatus").value = "";
        $("#fIni").value = "";
        $("#fFim").value = "";
        loadLeadsAndKpis();
    });

    await loadOptions();
    await loadLeadsAndKpis();
});
