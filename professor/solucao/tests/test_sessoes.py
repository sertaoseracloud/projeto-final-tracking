from professor.solucao.tests.dados import eventos, itens, jornada


def test_atributos_vem_do_primeiro_evento_e_do_primeiro_plano(spark, job):
    sessoes = job("sessoes")
    ev = eventos(spark,
        {"event_id": "a", "event_name": "page_view", "ts": "2026-09-01 10:00:00", "session_id": "s1"},
        {"event_id": "b", "event_name": "select_item", "ts": "2026-09-01 10:01:00", "session_id": "s1",
         "source": "instagram", "medium": "paid_social"},
        {"event_id": "c", "event_name": "page_view", "ts": "2026-09-01 11:00:00", "session_id": "s2",
         "device_category": "desktop"},
    )
    it = itens(spark,
        {"event_id": "b", "event_name": "select_item", "ts": "2026-09-01 10:01:00", "session_id": "s1",
         "item_id": "PLANO-PRO", "item_category": "plano"},
        {"event_id": "b2", "event_name": "select_item", "ts": "2026-09-01 10:02:00", "session_id": "s1",
         "item_id": "PLANO-BASICO", "item_category": "plano"},
    )
    r = {x.session_id: x for x in sessoes.atributos_sessao(ev, it).collect()}
    assert (r["s1"].canal, r["s1"].plano) == ("google / cpc", "PLANO-PRO")
    assert (r["s2"].plano, r["s2"].dispositivo) == ("(nenhum)", "desktop")


def test_tabela_sessoes_resume_duracao_rolagem_video_e_compra(spark, job):
    sessoes = job("sessoes")
    linhas = jornada("s1", ["page_view", "scroll", "video_start", "video_progress", "purchase"])
    linhas[1]["percent_scrolled"] = 75
    linhas[2]["video_percent"] = 0
    linhas[3]["video_percent"] = 25
    linhas += jornada("s2", ["page_view"], inicio="2026-09-01 12:00:00", user_pseudo_id="anon-2")
    ev = eventos(spark, *linhas)
    tabela = sessoes.tabela_sessoes(ev, sessoes.atributos_sessao(ev, itens(spark)))
    r = {x.session_id: x for x in tabela.collect()}
    assert (r["s1"].duracao_s, r["s1"].eventos, r["s1"].scroll_max, r["s1"].video_max) == (240, 5, 75, 25)
    assert r["s1"].viu_video is True and r["s1"].comprou is True and r["s1"].fez_lead is False
    assert (r["s2"].scroll_max, r["s2"].video_max, r["s2"].comprou) == (0, None, False)
