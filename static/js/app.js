/**
 * PAINEL LEADS LITE - CORE (VERSÃO INTEGRADA)
 */
const $ = (sel) => document.querySelector(sel);

// Estado dos Filtros
let tsInstances = {};

// Configuração TomSelect para o Estilo Dracula
function initTS(id) {
    const el = $(id);
    if (!el) return;
    tsInstances[id] = new TomSelect(id, {
        plugins: ['checkbox_options', 'remove_button'],
        maxItems: null,
        hideSelected: false,
        render: {
            option: (data, escape) => `
                <div class="ts-opt">
                    <span class="ts-opt-check"></span>
                    <span class="ts-opt-text">${escape(data.text)}</span>
                </div>`,
            item: (data, escape) => `<div class="ts-particle">${escape(data.text)}</div>`
        }
    });
}

// Inicialização Geral
document.addEventListener("DOMContentLoaded", async () => {
    // Inicializa todos os dropdowns multiselleção das imagens
    [
        '#fStatus', '#fCurso', '#fModalidade', '#fTurno', '#fPolo', '#fOrigem',
        '#fConsultorDisparo', '#fConsultorComercial', '#fCanal', '#fCampanha',
        '#fTipoDisparo', '#fTipoNegocio'
    ].forEach(id => initTS(id));

    // Botão de Limpar Tudo
    $("#btnClear")?.addEventListener("click", () => {
        Object.values(tsInstances).forEach(ts => ts.clear());
        $("#fBusca").value = "";
        $("#fIni").value = "";
        $("#fFim").value = "";
        $("#fMatriculado").value = "";
        $("#fLimit").value = "500";
    });

    // Lógica de Busca Rápida (conforme imagem_e6c0bd.png)
    $("#fBusca")?.addEventListener("input", (e) => {
        const val = e.target.value;
        if (val.length > 3) {
            console.log("Detectando tipo de busca para:", val);
            // O backend processa se é CPF, Nome ou Email
        }
    });

    // Carregar opções da API
    try {
        const res = await fetch("/api/options");
        const options = await res.json();
        // Exemplo de preenchimento: fillSelect(tsInstances['#fStatus'], options.status);
    } catch (e) {
        console.error("Erro ao carregar filtros:", e);
    }
});
