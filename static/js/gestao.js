(function () {
  "use strict";

  const dataNode = document.getElementById("gestao-data");
  if (!dataNode) return;

  const payload = JSON.parse(dataNode.textContent || "{}");
  const operacao = payload.operacao || {};
  const produtividade = payload.produtividade || [];
  const fila = payload.fila_operacional || [];
  const alertas = payload.alertas || {};
  const centroComando = payload.centro_comando || {};
  const qualidadeDados = payload.qualidade_dados || {};
  const topOrigens = payload.top_origens || [];
  const topCursos = payload.top_cursos || [];

  const numberFmt = new Intl.NumberFormat("pt-BR");
  const decimalFmt = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const pctFmt = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

  function num(value) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function fmtNumber(value) { return numberFmt.format(num(value)); }
  function fmtDecimal(value) { return decimalFmt.format(num(value)); }
  function fmtPct(value) { return `${pctFmt.format(num(value))}%`; }
  function fmtHours(value) { return `${fmtDecimal(value)}h`; }

  function fmtDate(value) {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).slice(0, 16);
    return date.toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" });
  }

  function setText(selector, value) {
    document.querySelectorAll(selector).forEach((el) => { el.textContent = value; });
  }

  function formatValue(value, format) {
    if (format === "pct") return fmtPct(value);
    if (format === "hours") return fmtHours(value);
    return typeof value === "string" ? value : fmtNumber(value);
  }

  function hydrateMap(prefix, values) {
    Object.entries(values).forEach(([key, value]) => {
      document.querySelectorAll(`[data-${prefix}="${key}"]`).forEach((el) => {
        el.textContent = formatValue(value, el.dataset.format);
      });
    });
  }

  function hydrateKpis() {
    hydrateMap("kpi", operacao);
    hydrateMap("alert", alertas);
    hydrateMap("command", centroComando);
    hydrateMap("quality", qualidadeDados);
    const generatedAt = document.getElementById("generatedAt");
    if (generatedAt && payload.generated_at) generatedAt.textContent = fmtDate(payload.generated_at);
  }

  function priorityBadge(value) {
    const text = value || "-";
    const slug = String(text).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, "-");
    return `<span class="priority-badge priority-${slug}">${text}</span>`;
  }

  function initProdutividadeTable() {
    $("#produtividadeTable").DataTable({
      data: produtividade,
      responsive: true,
      pageLength: 25,
      order: [[7, "desc"]],
      dom: "Bfrtip",
      buttons: [{ extend: "csvHtml5", text: "Exportar CSV", className: "btn btn-primary btn-sm", bom: true, filename: "produtividade_consultores" }],
      language: { url: "https://cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json" },
      columns: [
        { data: "consultor_comercial", defaultContent: "-" },
        { data: "total_leads", render: fmtNumber },
        { data: "media_horas_ate_matricula", render: fmtHours },
        { data: "media_horas_primeiro_contato", render: fmtHours },
        { data: "leads_sem_status", render: fmtNumber },
        { data: "leads_em_carteira", render: fmtNumber },
        { data: "matriculados", render: fmtNumber },
        { data: "taxa_conversao_pct", render: fmtPct },
        { data: "ultima_atividade", render: fmtDate },
        { data: "score_medio_carteira", render: fmtDecimal },
      ],
    });
  }

  function initFilaTable() {
    $("#filaTable").DataTable({
      data: fila,
      responsive: true,
      pageLength: 25,
      order: [[7, "desc"]],
      dom: "frtip",
      language: { url: "https://cdn.datatables.net/plug-ins/1.13.8/i18n/pt-BR.json" },
      columns: [
        { data: "nome", defaultContent: "-" },
        { data: "celular", defaultContent: "-" },
        { data: "curso", defaultContent: "-" },
        { data: "polo", defaultContent: "-" },
        { data: "origem", defaultContent: "-" },
        { data: "status", defaultContent: "-" },
        { data: "consultor_comercial", defaultContent: "-" },
        { data: "score_prioridade", render: fmtDecimal },
        { data: "nivel_prioridade", render: priorityBadge },
        { data: "dias_sem_acao", render: fmtNumber },
      ],
      initComplete: function () {
        const api = this.api();
        [2, 3, 4, 5, 6].forEach((idx) => {
          const column = api.column(idx);
          const title = $(column.header()).text();
          const select = $(`<select class="form-select form-select-sm table-filter" aria-label="Filtrar ${title}"><option value="">${title}: Todos</option></select>`)
            .appendTo($(column.header()))
            .on("change", function () {
              column.search($.fn.dataTable.util.escapeRegex($(this).val()), true, false).draw();
            });
          column.data().unique().sort().each((value) => {
            if (value) select.append(`<option value="${value}">${value}</option>`);
          });
        });
      },
    });
  }

  function topBy(rows, key, limit) {
    return [...rows]
      .sort((a, b) => num(b[key]) - num(a[key]))
      .slice(0, limit)
      .filter((row) => num(row[key]) > 0);
  }

  function barChart(id, rows, valueKey, color, isPct) {
    const el = document.getElementById(id);
    if (!el) return;
    new Chart(el, {
      type: "bar",
      data: {
        labels: rows.map((row) => row.consultor_comercial || row.dimensao || "Não informado"),
        datasets: [{ data: rows.map((row) => num(row[valueKey])), backgroundColor: color, borderRadius: 10 }],
      },
      options: {
        indexAxis: "y",
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { beginAtZero: true, grid: { color: "rgba(148,163,184,.22)" }, ticks: { callback: (value) => isPct ? `${value}%` : value } },
          y: { grid: { display: false } },
        },
      },
    });
  }

  function initCharts() {
    barChart("chartMatriculas", topBy(produtividade, "matriculados", 10), "matriculados", "rgba(37, 99, 235, .82)", false);
    barChart("chartConversao", topBy(produtividade, "taxa_conversao_pct", 10), "taxa_conversao_pct", "rgba(22, 163, 74, .82)", true);
    barChart("chartOrigensVolume", topBy(topOrigens, "total_leads", 10), "total_leads", "rgba(79, 70, 229, .82)", false);
    barChart("chartOrigensConversao", topBy(topOrigens, "taxa_conversao_pct", 10), "taxa_conversao_pct", "rgba(8, 145, 178, .82)", true);
    barChart("chartCursosVolume", topBy(topCursos, "total_leads", 10), "total_leads", "rgba(217, 119, 6, .82)", false);
    barChart("chartCursosConversao", topBy(topCursos, "taxa_conversao_pct", 10), "taxa_conversao_pct", "rgba(22, 163, 74, .82)", true);
  }

  document.addEventListener("DOMContentLoaded", () => {
    hydrateKpis();
    initCharts();
    initProdutividadeTable();
    initFilaTable();
  });
})();
