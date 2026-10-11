"""Peças visuais do console (CSS e componentes), no estilo do console da AWS.

O Streamlit põe a classe `st-key-<chave>` em todo contêiner com `key`; o CSS abaixo usa isso para
desenhar tabelas, painéis e faixas iguais aos do console, sem depender de componentes externos.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import streamlit as st

# Paleta: barra superior #232f3e, laranja #ff9900, azul de link #006ce0, textos #0f141a/#424650/#656871.
CSS = """
<style>
  html, body, .stApp, .stMarkdown, button, input, textarea, select {
    font-family: "Amazon Ember", "Helvetica Neue", Roboto, Arial, sans-serif; }
  .stApp { background: #ffffff; color: #0f141a; }

  /* cabeçalho do Streamlit: transparente, logo abaixo da barra; ficam só os botões do menu lateral */
  [data-testid="stHeader"] { background: transparent !important; top: 44px !important; height: 2.5rem !important; }
  [data-testid="stToolbarActions"], [data-testid="stMainMenu"], [data-testid="stAppDeployButton"],
  [data-testid="stDecoration"], [data-testid="stHeaderActionElements"], #MainMenu, footer { display: none !important; }
  .block-container { padding-top: 4.2rem !important; padding-bottom: 3rem !important; max-width: 1320px; }

  /* barra superior */
  .barra-console { position: fixed; top: 0; left: 0; right: 0; z-index: 1000002; height: 44px;
    background: #232f3e; color: #fff; display: flex; align-items: center; gap: 18px; padding: 0 20px;
    font-size: 14px; border-bottom: 1px solid #414d5c; }
  .barra-console .marca { color: #ff9900; font-weight: 800; font-size: 16px; letter-spacing: .01em; }
  .barra-console .servicos { color: #fff; font-weight: 700; border-left: 1px solid #414d5c; padding-left: 18px; }
  .barra-console .detalhe { margin-left: auto; color: #d1d5db; font-size: 13px; }
  .barra-console .regiao { color: #fff; font-weight: 700; border-left: 1px solid #414d5c; padding-left: 18px; }
  .barra-console span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  @media (max-width: 760px) { .barra-console span:not(.marca) { display: none; } }

  /* menu lateral (navegação de serviços) */
  [data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #c6c6cd;
    top: 44px !important; height: calc(100vh - 44px) !important; }
  [data-testid="stSidebarHeader"] { height: 2.2rem; padding: 0 8px; }
  .menu-titulo { font-size: 18px; font-weight: 700; color: #0f141a; padding: 0 0 6px 2px; }
  [data-testid="stSidebar"] [role="radiogroup"] { gap: 0; }
  [data-testid="stSidebar"] [role="radiogroup"] > div { display: block; width: 100%; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"] { width: 100%; margin: 0; padding: 7px 12px 7px 14px;
    border-left: 3px solid transparent; cursor: pointer; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"] > div > div:first-child { display: none; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"] p { font-size: 14px; color: #424650; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"]:hover p { color: #006ce0; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"][data-selected="true"] { border-left-color: #006ce0; background: #f0f7ff; }
  [data-testid="stSidebar"] [data-testid="stRadioOption"][data-selected="true"] p { color: #006ce0; font-weight: 700; }
  [data-testid="stSidebar"] [role="radiogroup"] > div::before { display: block; font-size: 12px; font-weight: 700;
    color: #656871; text-transform: uppercase; letter-spacing: .04em; padding: 12px 0 4px 2px; }
  [data-testid="stSidebar"] [role="radiogroup"] > div:nth-child(1)::before { content: "S3"; padding-top: 2px; }
  [data-testid="stSidebar"] [role="radiogroup"] > div:nth-child(2)::before { content: "Glue"; }
  [data-testid="stSidebar"] [role="radiogroup"] > div:nth-child(4)::before { content: "Athena"; }
  [data-testid="stSidebar"] [role="radiogroup"] > div:nth-child(5)::before { content: "Aprender"; }

  /* títulos de página, breadcrumb, textos de apoio */
  .pag-titulo { font-size: 24px; font-weight: 700; color: #0f141a; line-height: 1.3; margin: 2px 0 2px; }
  .pag-titulo .contador { color: #656871; font-weight: 400; }
  .pag-desc { color: #656871; font-size: 14px; margin-bottom: 8px; }
  .trilha-sep { color: #656871; font-size: 14px; }
  .trilha-atual { color: #656871; font-size: 14px; }
  [class*="st-key-trilha"] button p { font-size: 14px !important; }

  /* botões: primário laranja, normal com contorno azul, link azul */
  .stButton button, .stDownloadButton button { border-radius: 20px; font-weight: 700; }
  .stButton button[kind="primary"], .stDownloadButton button[kind="primary"] {
    background: #ff9900; border: 2px solid #ff9900; color: #0f141a; }
  .stButton button[kind="primary"]:hover { background: #fa6f00; border-color: #fa6f00; color: #0f141a; }
  .stButton button[kind="primary"]:disabled { background: #ebebf0; border-color: #ebebf0; color: #9ba7b6; }
  .stButton button[kind="secondary"], .stDownloadButton button[kind="secondary"] {
    background: #fff; border: 2px solid #006ce0; color: #006ce0; }
  .stButton button[kind="secondary"]:hover { background: #f0fbff; border-color: #002b66; color: #002b66; }
  .stButton button[kind="tertiary"] { color: #006ce0; font-weight: 400; padding: 0; min-height: 0;
    justify-content: flex-start; text-align: left; border-radius: 0; }
  .stButton button[kind="tertiary"]:hover { color: #002b66; text-decoration: underline; }
  .stButton button[kind="tertiary"] p { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  /* painéis (contêineres do console) */
  [class*="st-key-painel-"] { border: none !important; border-radius: 16px !important; background: #fff;
    box-shadow: 0 1px 1px 1px #e9ebed, 0 6px 36px rgba(0, 7, 22, .10); padding: 14px 20px 16px !important; }
  .painel-titulo { font-size: 18px; font-weight: 700; color: #0f141a; }
  .painel-titulo .contador { color: #656871; font-weight: 400; }
  .painel-desc { color: #656871; font-size: 14px; margin-top: -4px; }

  /* tabelas do console */
  [class*="st-key-tabcab-"] { border-bottom: 1px solid #c6c6cd; padding: 6px 0 4px; }
  [class*="st-key-tablinha-"] { border-bottom: 1px solid #ebebf0; padding: 3px 0; }
  [class*="st-key-tablinha-"]:hover { background: #f6f6f9; }
  [class*="st-key-tabcab-"] [data-testid="stMarkdownContainer"] p,
  [class*="st-key-tablinha-"] [data-testid="stMarkdownContainer"] p { margin: 0; }
  .tab-titulo { font-weight: 700; color: #424650; font-size: 14px; white-space: nowrap; }
  .tab-texto { font-size: 14px; color: #0f141a; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; display: block; }
  [class*="st-key-tablinha-"] [data-testid="stMarkdown"], [class*="st-key-tabcab-"] [data-testid="stMarkdown"],
  [class*="st-key-tablinha-"] [data-testid="stMarkdownContainer"], [class*="st-key-tabcab-"] [data-testid="stMarkdownContainer"] {
    margin: 0 !important; min-height: 21px; }
  [class*="st-key-tablinha-"] .stButton button { min-height: 0; height: auto; line-height: 21px; }
  [class*="st-key-tablinha-"] .stButton button p { font-size: 14px; line-height: 21px; margin: 0; }
  [class*="st-key-tablinha-"] .tab-texto, [class*="st-key-tablinha-"] .selo { line-height: 21px; }
  .tab-vazio { text-align: center; color: #656871; padding: 18px 0; font-size: 14px; }

  /* selos de status */
  .selo { font-size: 14px; white-space: nowrap; }
  .selo-ok { color: #00802f; } .selo-erro { color: #db0000; } .selo-run { color: #006ce0; } .selo-neutro { color: #656871; }

  /* pares rótulo/valor (visão geral) */
  .kv { display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 14px 24px; margin-top: 6px; }
  .kv-rotulo { color: #656871; font-size: 13px; font-weight: 700; }
  .kv-valor { color: #0f141a; font-size: 14px; word-break: break-all; }

  /* abas */
  [data-testid="stTabs"] [role="tablist"] { border-bottom: 1px solid #c6c6cd; }
  [data-testid="stTab"] p { font-weight: 700; color: #424650; font-size: 15px; }
  [data-testid="stTab"][aria-selected="true"] p { color: #006ce0; }

  /* árvore de dados do Athena */
  [data-testid="stExpander"] details { border-radius: 8px; border-color: #e9ebed; }
  [data-testid="stExpander"] summary p { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 14px; }

  /* editor SQL: fonte de código e calha à esquerda, como o editor do Athena */
  .stTextArea textarea { font-family: Monaco, Menlo, Consolas, "Courier New", monospace; font-size: 14px;
    line-height: 1.55; padding-left: 52px !important; border-radius: 8px;
    background: linear-gradient(to right, #f6f6f9 0, #f6f6f9 40px, #e9ebed 40px, #e9ebed 41px, #fff 41px) local; }

  /* tutorial: o botão que grava "não mostrar mais" fica escondido; o componente do tour não ocupa espaço */
  [class*="st-key-tour-desligar"] { display: none !important; }
  [data-testid="stElementContainer"]:has(> iframe[height="0"]),
  [data-testid="stElementContainer"]:has(iframe[title="st.iframe"][height="0"]) { display: none; }
  [class*="st-key-tour-abrir"] button { width: 100%; }

  /* faixa de aviso (flashbar) */
  [class*="st-key-flash"] { background: #006ce0; border-radius: 12px; padding: 10px 16px !important; margin-bottom: 8px; }
  [class*="st-key-flash"] p, [class*="st-key-flash"] span { color: #fff !important; }
  [class*="st-key-flash"] .stButton button { background: transparent; border: 2px solid #fff; color: #fff; }
  [class*="st-key-flash"] .stButton button:hover { background: rgba(255,255,255,.15); }
  [class*="st-key-flash"] .stButton button[kind="tertiary"] { border: none; text-decoration: underline; }
</style>
"""


@dataclass
class Link:
    """Célula clicável (nome em azul, como os links das tabelas do console)."""
    rotulo: str
    chave: str
    on_click: Callable | None = None
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)


@dataclass
class Html:
    conteudo: str


def _slug(texto: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", texto).strip("-")


def _texto(celula: Any) -> str:
    if isinstance(celula, Link):
        return celula.rotulo
    if isinstance(celula, Html):
        return re.sub(r"<[^>]+>", "", celula.conteudo)
    return str(celula)


def titulo(texto: str, contador: int | None = None, descricao: str | None = None) -> None:
    extra = f' <span class="contador">({contador})</span>' if contador is not None else ""
    st.markdown(f'<div class="pag-titulo">{html.escape(texto)}{extra}</div>', unsafe_allow_html=True)
    if descricao:
        st.markdown(f'<div class="pag-desc">{descricao}</div>', unsafe_allow_html=True)


def trilha(itens: list[tuple[str, str | None, Callable | None, dict]], chave: str) -> None:
    """Breadcrumb: (rótulo, chave do botão, on_click, kwargs). O último item, sem chave, é a página atual."""
    linha = st.container(horizontal=True, vertical_alignment="center", gap="small", key=f"trilha-{chave}")
    for i, (rotulo, chave_botao, ao_clicar, kwargs) in enumerate(itens):
        if chave_botao:
            linha.button(rotulo, key=chave_botao, type="tertiary", on_click=ao_clicar, kwargs=kwargs or {})
        else:
            linha.markdown(f'<span class="trilha-atual">{html.escape(rotulo)}</span>', width="content",
                           unsafe_allow_html=True)
        if i < len(itens) - 1:
            linha.markdown('<span class="trilha-sep">›</span>', width="content", unsafe_allow_html=True)


def painel(chave: str, titulo_painel: str | None = None, contador: int | None = None,
           descricao: str | None = None):
    caixa = st.container(border=True, key=f"painel-{_slug(chave)}")
    if titulo_painel:
        extra = f' <span class="contador">({contador})</span>' if contador is not None else ""
        caixa.markdown(f'<div class="painel-titulo">{html.escape(titulo_painel)}{extra}</div>', unsafe_allow_html=True)
    if descricao:
        caixa.markdown(f'<div class="painel-desc">{descricao}</div>', unsafe_allow_html=True)
    return caixa


def selo(status: str) -> Html:
    classes = {"Sucesso": ("selo-ok", "✔"), "Concluída": ("selo-ok", "✔"), "Falhou": ("selo-erro", "✖"),
               "Em execução": ("selo-run", "⟳")}
    classe, icone = classes.get(status, ("selo-neutro", "–"))
    return Html(f'<span class="selo {classe}">{icone} {html.escape(status)}</span>')


def pares(dados: dict[str, str]) -> None:
    """Visão geral: rótulos cinza com o valor embaixo, em grade (como "Detalhes" no console)."""
    itens = "".join(f'<div><div class="kv-rotulo">{html.escape(k)}</div><div class="kv-valor">{html.escape(str(v))}</div></div>'
                    for k, v in dados.items())
    st.markdown(f'<div class="kv">{itens}</div>', unsafe_allow_html=True)


def tabela(chave: str, colunas: list[tuple[str, float]], linhas: list[list[Any]],
           localizar: bool = False, vazio: str = "Nenhum item.") -> int:
    """Tabela do console: cabeçalho, linhas com divisória, nomes clicáveis e (opcional) caixa "Localizar".

    Devolve quantas linhas foram mostradas (depois do filtro)."""
    if localizar:
        termo = st.text_input("Localizar", key=f"busca:{chave}", placeholder="Localizar…",
                              label_visibility="collapsed")
        if termo:
            linhas = [l for l in linhas if termo.lower() in " ".join(_texto(c) for c in l).lower()]
    pesos = [peso for _, peso in colunas]
    with st.container(key=f"tabcab-{_slug(chave)}"):
        for coluna, (nome, _) in zip(st.columns(pesos, gap="small", vertical_alignment="center"), colunas):
            coluna.markdown(f'<span class="tab-titulo">{html.escape(nome)}</span>', unsafe_allow_html=True)
    if not linhas:
        st.markdown(f'<div class="tab-vazio">{vazio}</div>', unsafe_allow_html=True)
        return 0
    for i, linha in enumerate(linhas):
        with st.container(key=f"tablinha-{_slug(chave)}-{i}"):
            for coluna, celula in zip(st.columns(pesos, gap="small", vertical_alignment="center"), linha):
                if isinstance(celula, Link):
                    coluna.button(celula.rotulo, key=celula.chave, type="tertiary", on_click=celula.on_click,
                                  args=celula.args, kwargs=celula.kwargs)
                elif isinstance(celula, Html):
                    coluna.markdown(celula.conteudo, unsafe_allow_html=True)
                else:
                    coluna.markdown(f'<span class="tab-texto">{html.escape(str(celula))}</span>', unsafe_allow_html=True)
    return len(linhas)


# ------------------------------------------------------------------ tour ------------------------------------------------------------------

_TOUR_JS = r"""
<script>
(function () {
  const P = window.parent, D = P.document;
  const cfg = __CONFIG__;
  const store = (() => { try { return P.sessionStorage; } catch (e) { return null; } })();
  const lido = (k) => { try { return store ? store.getItem(k) : null; } catch (e) { return null; } };
  const grava = (k, v) => { try { store && store.setItem(k, v); } catch (e) {} };

  if (!cfg.passos.length) return;
  const chaveVisto = "tour-visto:" + cfg.contexto, chaveForca = "tour-forca";
  const forcado = cfg.forcar && lido(chaveForca) !== String(cfg.forcar);
  if (!forcado && (!cfg.auto || lido(chaveVisto))) return;
  if (cfg.forcar) grava(chaveForca, String(cfg.forcar));
  if (D.getElementById("tour-raiz")) return;           // já aberto

  const css = D.createElement("style");
  css.id = "tour-css";
  css.textContent = `
    #tour-raiz { position: fixed; inset: 0; z-index: 1000010; pointer-events: none; }
    #tour-raiz .fundo { position: fixed; inset: 0; background: rgba(15,20,26,.55); pointer-events: auto; }
    #tour-raiz .foco { position: fixed; border-radius: 10px; box-shadow: 0 0 0 9999px rgba(15,20,26,.55);
      outline: 3px solid #ff9900; transition: all .2s ease; pointer-events: none; }
    #tour-raiz .balao { position: fixed; width: min(380px, calc(100vw - 32px)); background: #fff; color: #0f141a;
      border-radius: 16px; box-shadow: 0 8px 40px rgba(0,7,22,.35); padding: 18px 20px 14px; pointer-events: auto;
      font: 14px/1.5 "Amazon Ember","Helvetica Neue",Roboto,Arial,sans-serif; }
    #tour-raiz .contagem { color: #656871; font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
    #tour-raiz h3 { margin: 4px 0 6px; font-size: 18px; font-weight: 700; }
    #tour-raiz p { margin: 0 0 12px; color: #424650; }
    #tour-raiz .acoes { display: flex; gap: 8px; align-items: center; }
    #tour-raiz button { border-radius: 20px; font: 700 14px/1 inherit; padding: 8px 16px; cursor: pointer; }
    #tour-raiz .prox { background: #ff9900; border: 2px solid #ff9900; color: #0f141a; margin-left: auto; }
    #tour-raiz .ant { background: #fff; border: 2px solid #006ce0; color: #006ce0; }
    #tour-raiz .fechar { position: absolute; top: 10px; right: 12px; background: none; border: none; font-size: 18px;
      color: #656871; padding: 4px 8px; }
    #tour-raiz .nunca { display: flex; gap: 6px; align-items: center; margin-top: 10px; color: #656871; font-size: 13px; }
  `;
  D.head.appendChild(css);
  const raiz = D.createElement("div");
  raiz.id = "tour-raiz";
  raiz.innerHTML = `<div class="fundo"></div><div class="foco"></div>
    <div class="balao" role="dialog" aria-modal="true">
      <button class="fechar" title="Fechar">✕</button>
      <div class="contagem"></div><h3></h3><p></p>
      <div class="acoes"><button class="ant">Anterior</button><button class="prox">Próximo</button></div>
      <label class="nunca"><input type="checkbox"> Não mostrar mais o tutorial</label>
    </div>`;
  D.body.appendChild(raiz);
  const fundo = raiz.querySelector(".fundo"), foco = raiz.querySelector(".foco"), balao = raiz.querySelector(".balao");
  let i = 0, alvo = null;

  function posicionar() {
    const vw = P.innerWidth, vh = P.innerHeight, bw = balao.offsetWidth, bh = balao.offsetHeight;
    if (!alvo) {
      foco.style.display = "none"; fundo.style.display = "block";
      balao.style.left = ((vw - bw) / 2) + "px"; balao.style.top = ((vh - bh) / 2) + "px";
      return;
    }
    const r = alvo.getBoundingClientRect(), m = 6;
    foco.style.display = "block"; fundo.style.display = "none";
    foco.style.left = (r.left - m) + "px"; foco.style.top = (r.top - m) + "px";
    foco.style.width = (r.width + 2 * m) + "px"; foco.style.height = (r.height + 2 * m) + "px";
    let top = r.bottom + 14;
    if (top + bh > vh - 8) top = r.top - bh - 14;              // sem espaço embaixo: em cima
    if (top < 52) top = Math.min(vh - bh - 8, Math.max(52, r.top + 14));
    let left = r.left;
    if (r.width < bw) left = r.left + r.width / 2 - bw / 2;
    left = Math.max(12, Math.min(left, vw - bw - 12));
    balao.style.left = left + "px"; balao.style.top = top + "px";
  }

  function mostrar(n) {
    i = n;
    const passo = cfg.passos[i];
    alvo = passo.seletor ? D.querySelector(passo.seletor) : null;
    if (alvo && (alvo.getBoundingClientRect().width === 0)) alvo = null;     // escondido: balão no centro
    if (alvo) alvo.scrollIntoView({ block: "center", behavior: "instant" });
    balao.querySelector(".contagem").textContent = "Passo " + (i + 1) + " de " + cfg.passos.length;
    balao.querySelector("h3").textContent = passo.titulo;
    balao.querySelector("p").textContent = passo.texto;
    balao.querySelector(".ant").style.visibility = i === 0 ? "hidden" : "visible";
    balao.querySelector(".prox").textContent = i === cfg.passos.length - 1 ? "Concluir" : "Próximo";
    requestAnimationFrame(posicionar);
    setTimeout(posicionar, 80);                                 // também sem animação (aba em segundo plano)
  }

  function fechar() {
    grava(chaveVisto, "1");
    P.removeEventListener("resize", posicionar);
    P.removeEventListener("scroll", posicionar, true);
    raiz.remove(); css.remove();
    if (balao.querySelector(".nunca input").checked) {
      const botao = D.querySelector('[class*="st-key-tour-desligar"] button');
      if (botao) botao.click();                                 // avisa o Python: grava "tutorial": false
    }
  }

  balao.querySelector(".prox").onclick = () => (i < cfg.passos.length - 1 ? mostrar(i + 1) : fechar());
  balao.querySelector(".ant").onclick = () => i > 0 && mostrar(i - 1);
  balao.querySelector(".fechar").onclick = fechar;
  D.addEventListener("keydown", function esc(e) { if (e.key === "Escape" && D.getElementById("tour-raiz")) { fechar(); D.removeEventListener("keydown", esc); } });
  P.addEventListener("resize", posicionar);
  P.addEventListener("scroll", posicionar, true);
  setTimeout(() => mostrar(0), 350);                           // espera a tela terminar de desenhar
})();
</script>
"""


def tour(contexto: str, passos: list, auto: bool, forcar: int = 0) -> None:
    """Tour em modal: escurece a tela, destaca cada elemento e explica para que ele serve.

    `auto`: abre sozinho na primeira visita a esta tela (nesta aba do navegador).
    `forcar`: um número que muda a cada clique em "Abrir tutorial desta tela" (abre mesmo já visto)."""
    import json
    import streamlit.components.v1 as componentes

    config = {"contexto": contexto, "auto": auto, "forcar": forcar,
              "passos": [{"seletor": p.seletor, "titulo": p.titulo, "texto": p.texto} for p in passos]}
    componentes.html(_TOUR_JS.replace("__CONFIG__", json.dumps(config, ensure_ascii=False)), height=0)
