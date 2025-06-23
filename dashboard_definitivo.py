# dashboard_definitivo.py
# Versão final com otimização de "apenas o que mudou" e logs ultra detalhados.

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, ctx, clientside_callback
from dash.dependencies import Input, Output, State, MATCH, ALL
import requests
import re
import json
import os
import sqlite3
import pandas as pd
import time
from datetime import datetime
from dash.exceptions import PreventUpdate
import math
import concurrent.futures
import webbrowser

# =========================
# 1) TOKEN DINÂMICO MERCADO LIVRE
# =========================

TOKEN_FILE = 'token_ml.json'
CLIENT_ID = '5115237075438177' # Substitua se necessário
CLIENT_SECRET = 'E84qpPe1KNR9FmaVabYOUumi9pCCJ6H2' # Substitua se necessário

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_token(data):
    with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f)

def refresh_token():
    data = load_token()
    refresh_token = data.get('refresh_token')
    if not refresh_token:
        raise Exception("Refresh token não encontrado.")

    url = 'https://api.mercadolibre.com/oauth/token'
    payload = {
        'grant_type': 'refresh_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': refresh_token
    }
    r = requests.post(url, data=payload)
    if r.status_code == 200:
        novo = r.json()
        novo['expires_at'] = int(time.time()) + novo['expires_in']
        save_token(novo)
        print("✅ Token atualizado com sucesso!")
        return novo['access_token']
    else:
        raise Exception(f"Erro ao renovar token: {r.text}")

def get_valid_token():
    data = load_token()
    if not data:
        raise Exception("Token não encontrado. Use obter_token primeiro.")
    if 'expires_at' not in data or int(time.time()) >= data['expires_at']:
        return refresh_token()
    return data['access_token']

# =========================
# 2) BANCO DE DADOS E DADOS DE ANÚNCIOS
# =========================

DB_PATH = 'estoque.db'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ANUNCIOS_JSON_PATH = os.path.join(SCRIPT_DIR, 'anuncios_ml.json')

def create_tables():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS estoque_historico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT,
            variacao TEXT,
            quantidade INTEGER,
            timestamp DATETIME,
            url TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mapeamento (
            anuncio_id TEXT,
            variacao_id TEXT,
            sku TEXT,
            variacao TEXT,
            UNIQUE(anuncio_id, variacao_id, sku, variacao)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS estado_sincronizacao (
            variacao_id TEXT PRIMARY KEY,
            quantidade_enviada INTEGER,
            timestamp_sinc DATETIME
        )
    ''')
    conn.commit()
    conn.close()
    print("[DEBUG] create_tables() executado")

# Demais funções de carregamento de dados (sem alteração)
def load_data_historico():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    df = pd.read_sql_query("SELECT * FROM estoque_historico", conn, parse_dates=['timestamp'])
    conn.close()
    return df

def load_skus_unicos():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT sku FROM estoque_historico WHERE sku IS NOT NULL")
    rows = cursor.fetchall()
    conn.close()
    skus = sorted(r[0] for r in rows)
    return skus

def load_anuncios_from_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, list): return []
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def load_mapeados_ids():
    if not os.path.exists(DB_PATH): return set()
    conn = sqlite3.connect(DB_PATH, timeout=15)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT anuncio_id FROM mapeamento")
        return {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()

def inserir_mapeamento_no_banco(anuncio_id, variacao_id, sku, variacao):
    conn = sqlite3.connect(DB_PATH, timeout=15)
    cursor = conn.cursor()
    try:
        cursor.execute('''INSERT OR IGNORE INTO mapeamento (anuncio_id, variacao_id, sku, variacao) VALUES (?, ?, ?, ?)''', (anuncio_id, variacao_id, sku, variacao))
        conn.commit()
    finally:
        conn.close()

# Inicialização
create_tables()
df_historico = load_data_historico()
skus_unicos = load_skus_unicos()
lista_anuncios = load_anuncios_from_json(ANUNCIOS_JSON_PATH)
ids_mapeados = load_mapeados_ids()


# =========================
# 3) DASH APP (Layout e Callbacks sem alterações relevantes)
# =========================
FA = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX, FA], suppress_callback_exceptions=True)
app.title = "Dashboard de Composições"

app.layout = dbc.Container([
    dbc.Toast(id="toast-ativacao", header="Notificação", is_open=False, dismissable=True, duration=4000, icon="success", style={"position": "fixed", "top": 66, "right": 10, "width": 350, "zIndex": 9999}),
    html.H1("Dashboard de Composições e Kits", className="mt-4 mb-2"),
    dbc.Row([
        dbc.Col(dbc.Alert(id='contador-mapeamento', color="info"), width=True),
        dbc.Col(dcc.Loading(dbc.Button("Sincronizar TODO o Estoque", id="btn-atualizar-tudo", color="warning"), type="dot"), width="auto")
    ], align="center", className="mb-3"),
    dbc.Row([dbc.Col(dbc.Button("Verificar Mapeamentos Órfãos", id="btn-verificar-orfãos", color="secondary", outline=True, size="sm"), className="mb-3")]),
    dbc.Checkbox(id="checkbox-nao-mapeados", label="Mostrar apenas anúncios não mapeados", value=False, className="mb-2"),
    dbc.Row([
        dbc.Col(dcc.Dropdown(id='dropdown-anuncios', options=[], placeholder='Selecione um anúncio para carregar'), width=True),
        dbc.Col(dbc.Button(id="btn-copy-id", children=[html.I(className="fas fa-copy")], color="light"), width="auto")
    ], className="mb-3", align="center"),
    dcc.Clipboard(id="clipboard-output", style={"display": "none"}),
    dbc.Row([
        dbc.Col(html.Div(id='out-img-capa'), width="auto"),
        dbc.Col(html.Div(id='status-anuncio-wrapper'), width="auto", className="align-self-center")
    ], className="mb-3"),
    html.Div([
        dcc.Dropdown(id='sku-principal', options=[{"label": sku, "value": sku} for sku in skus_unicos], placeholder='Escolha um SKU para adicionar à composição', searchable=True, clearable=True),
        html.Div(id='saida-sku-status', style={'margin-top': '5px', 'font-size': '0.9em'})
    ], style={'margin': '10px 0'}),
    html.Div(id='form-mapeamento'),
    dbc.Row([
        dbc.Col(dbc.Button("Salvar Mapeamento", id='btn-salvar-mapeamento', n_clicks=0, color="success"), width="auto"),
        dbc.Col(html.Div(id='mensagem-salvar-geral'), width=True)
    ], align="center", className="mb-3"),
    dbc.Row([
        dbc.Col(dbc.Button("Atualizar Estoque do Anúncio", id='btn-atualizar-ml', n_clicks=0, color="info"), width="auto"),
        dbc.Col(dcc.Loading(id="loading-atualizar", type="circle", children=html.Div(id='resultado-atualizacao')), width=True)
    ], align="center"),
    dcc.Store(id='store-resultado-bulk'),
    dbc.Modal(id="modal-progresso", size="lg", is_open=False),
    dcc.Store(id='store-variacoes-ml', data=[]),
    dcc.Store(id='store-selecionadas', data={})
], fluid=True, className="mb-4")

# =====================================
# CALLBACKS PRINCIPAIS
# =====================================

# ... (Todos os callbacks da interface, como update_dropdown_options, carregar_variacoes_ml, etc., permanecem os mesmos) ...
# (Para brevidade, eles não serão repetidos aqui, mas estão no seu código final)

@app.callback(
    Output('contador-mapeamento', 'children'),
    Input('dropdown-anuncios', 'options')
)
def update_contador_mapeamento(opcoes):
    total_anuncios = len(lista_anuncios)
    mapeados_count = len(ids_mapeados)
    faltam_count = total_anuncios - mapeados_count
    return f"Status: {mapeados_count} de {total_anuncios} anúncios mapeados. Faltam {faltam_count}."

@app.callback(
    Output('dropdown-anuncios', 'options'),
    Input('checkbox-nao-mapeados', 'value')
)
def update_dropdown_options(mostrar_nao_mapeados):
    opcoes_filtradas = [anuncio for anuncio in lista_anuncios if not mostrar_nao_mapeados or anuncio.get('id') not in ids_mapeados]
    dropdown_options = [{'label': f"{'✅ ' if a.get('id') in ids_mapeados else ''}{a.get('sku_principal', 'SEM SKU')} >> {a.get('titulo', 'Sem Título')} ({a.get('id')})", 'value': a.get('id')} for a in opcoes_filtradas if a.get('id')]
    return dropdown_options

@app.callback(
    Output('form-mapeamento', 'children'),
    Output('store-variacoes-ml', 'data'),
    Output('store-selecionadas', 'data', allow_duplicate=True),
    Input('dropdown-anuncios', 'value'),
    prevent_initial_call=True
)
def carregar_variacoes_ml(anuncio_id):
    if not anuncio_id: return None, [], {}
    try:
        token = get_valid_token()
        headers = {'Authorization': f'Bearer {token}'}
        resp = requests.get(f"https://api.mercadolibre.com/items/{anuncio_id.strip().upper()}/variations", headers=headers)
        resp.raise_for_status()
        dados = resp.json()
        if not isinstance(dados, list) or len(dados) == 0: return dbc.Alert("ℹ️ Este anúncio não possui variações.", color='info'), [], {}
        colunas = [dbc.Col(dbc.Card(dbc.CardBody([html.H5(f"{' – '.join([v['value_name'] for v in var.get('attribute_combinations', [])])}", className="card-title mb-2"), html.Div(id={'type': 'disponivel-var', 'index': str(var['id'])}), html.Div(id={'type': 'estoque-var', 'index': str(var['id'])})], className="p-2"), className="shadow-sm"), lg=4, md=6, sm=12, className="mb-3") for var in dados]
        return dbc.Row(colunas), dados, {}
    except Exception as e:
        return dbc.Alert(f"❌ Erro ao carregar variações: {e}", color='danger'), [], {}

clientside_callback("function(n,v){if(n&&n>0)navigator.clipboard.writeText(v);return ''}",Output("clipboard-output","content"),Input("btn-copy-id","n_clicks"),State("dropdown-anuncios","value"))

# ... (Callbacks da interface como sugerir_sku_principal, mostrar_variacoes_para_mlvar, etc. continuam aqui) ...

@app.callback(Output('out-img-capa', 'children'), Output('status-anuncio-wrapper', 'children'), Input('dropdown-anuncios', 'value'),prevent_initial_call=True)
def exibir_imagem_e_status(anuncio_id):
    if not anuncio_id: return "", ""
    anuncio_data = next((item for item in lista_anuncios if item.get('id') == anuncio_id), None)
    if not anuncio_data: return "", ""
    img_url, status = anuncio_data.get('miniatura'), anuncio_data.get("status")
    img = html.Img(src=img_url, style={'maxWidth': '150px', 'border': '1px solid #ccc', 'padding': '5px', 'borderRadius': '4px'}) if img_url else ""
    status_ui = dbc.Badge("Ativo", color="success") if status != 'paused' else html.Div([dbc.Badge("Pausado", color="secondary", className="me-2"), dbc.Button("Reativar", id="btn-ativar-anuncio", color="success", size="sm")])
    return img, status_ui

# LÓGICA DE ATUALIZAÇÃO EM MASSA (BULK) - TOTALMENTE REFEITA
@app.callback(
    Output('store-resultado-bulk', 'data', allow_duplicate=True),
    Input('btn-atualizar-tudo', 'n_clicks'),
    prevent_initial_call=True
)
def bulk_update_stock(n_clicks):
    if n_clicks is None:
        raise PreventUpdate

    start_time = time.time()
    print(f"[{datetime.now()}] Iniciando verificação de estoque para atualização...")

    # --- 1. Calcular o estado ATUAL do estoque e identificar o SKU limitante ---
    conn = sqlite3.connect(DB_PATH, timeout=15)
    df_map = pd.read_sql_query("SELECT * FROM mapeamento", conn)
    if df_map.empty:
        conn.close()
        return {'summary': 'Nenhum anúncio mapeado para atualizar.', 'log': []}
    
    # Otimizado para pegar apenas o estoque mais recente
    df_estoque_recente = pd.read_sql_query(
        "SELECT sku, variacao, quantidade FROM estoque_historico WHERE id IN (SELECT MAX(id) FROM estoque_historico GROUP BY sku, variacao)", 
        conn
    )
    df_final = pd.merge(df_map, df_estoque_recente, on=['sku', 'variacao'], how='left')
    df_final['quantidade'] = df_final['quantidade'].fillna(0).astype(int)
    
    # Lógica para encontrar o estoque mínimo e o SKU/Variação que o limita
    idx = df_final.groupby(['anuncio_id', 'variacao_id'])['quantidade'].idxmin()
    df_atual = df_final.loc[idx].copy()
    df_atual.rename(columns={'quantidade': 'quantidade_atual'}, inplace=True)

    # --- 2. Carregar o estado da ÚLTIMA sincronização ---
    try:
        df_anterior = pd.read_sql_query("SELECT variacao_id, quantidade_enviada FROM estado_sincronizacao", conn)
    except pd.io.sql.DatabaseError:
        df_anterior = pd.DataFrame(columns=['variacao_id', 'quantidade_enviada'])
    conn.close()

    # --- 3. Comparar estados para encontrar o que MUDOU ---
    df_merged = pd.merge(df_atual, df_anterior, on='variacao_id', how='left')
    df_merged['quantidade_enviada'] = df_merged['quantidade_enviada'].fillna(-1).astype(int) # -1 para garantir que itens novos sejam atualizados
    df_para_atualizar = df_merged[df_merged['quantidade_atual'] != df_merged['quantidade_enviada']].copy()
    
    total_a_atualizar = len(df_para_atualizar)
    if total_a_atualizar == 0:
        print(f"[{datetime.now()}] Verificação concluída. Nenhum estoque alterado.")
        return {'summary': 'Verificação concluída. Nenhum estoque precisou de atualização.', 'log': ['✅ Nenhum estoque com alteração detectada.']}

    # --- 4. Executar a atualização APENAS para as variações que mudaram ---
    print(f"[{datetime.now()}] {total_a_atualizar} variações com estoque alterado. Iniciando atualização...")
    
    try:
        token = get_valid_token()
    except Exception as e:
        return {'error': f"Erro ao obter token: {e}"}
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    
    log_mensagens = [f"Iniciando atualização para {total_a_atualizar} variações..."]
    sucessos, falhas, ignorados = 0, 0, 0
    variacoes_sincronizadas = []

    def update_single_variation(row_data):
        anuncio_id = row_data['anuncio_id']
        var_id = row_data['variacao_id']
        qtd_nova = int(row_data['quantidade_atual'])
        qtd_anterior = int(row_data['quantidade_enviada'])
        sku_limitante = row_data['sku']
        var_limitante = row_data['variacao']

        payload = {'available_quantity': qtd_nova}
        url = f"https://api.mercadolibre.com/items/{anuncio_id}/variations/{var_id}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(0.3)
                r = requests.put(url, headers=headers, json=payload, timeout=15)
                r.raise_for_status()

                # Construindo o log detalhado
                diferenca = qtd_nova - (qtd_anterior if qtd_anterior != -1 else 0)
                sinal = '+' if diferenca > 0 else ''
                movimento = 'ENTRADA' if diferenca > 0 else ('SAÍDA' if diferenca < 0 else 'INICIAL')
                log_detalhe = f"({movimento}: {sinal}{diferenca}) | Anterior: {qtd_anterior if qtd_anterior != -1 else 'N/A'} | Limitado por: {sku_limitante} - {var_limitante}"
                
                return ("sucesso", var_id, qtd_nova, f"✔ SUCESSO: Variação {var_id} → {qtd_nova} un. {log_detalhe}")
            
            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if e.response else 'N/A'
                if status_code in [429, 500, 502, 503, 504] and attempt < max_retries - 1:
                    backoff_time = (2 ** attempt)
                    print(f"   -> Tentativa {attempt + 1}/{max_retries} falhou para {var_id} com erro {status_code}. Aguardando {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue
                
                if status_code == 409:
                    return ("ignorado", var_id, qtd_nova, f"🟡 IGNORADO (Conflito): Variação {var_id} ({anuncio_id}).")
                
                return ("falha", var_id, qtd_nova, f"❌ FALHA FINAL: Variação {var_id} ({anuncio_id}) | Status: {status_code}")
       
        return ("falha", var_id, qtd_nova, f"❌ FALHA FINAL: Variação {var_id} ({anuncio_id}) após {max_retries} tentativas.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_variation = {executor.submit(update_single_variation, row): row for index, row in df_para_atualizar.iterrows()}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_variation)):
            try:
                status, var_id, qtd, result_msg = future.result()
                log_mensagens.append(result_msg)
                
                if status == "sucesso":
                    sucessos += 1
                    variacoes_sincronizadas.append({'variacao_id': var_id, 'quantidade_enviada': qtd, 'timestamp_sinc': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                elif status == "falha": falhas += 1
                elif status == "ignorado": ignorados += 1
                
                print(f"Progresso: {i + 1}/{total_a_atualizar} | {result_msg}")
            except Exception as exc:
                falhas += 1
                log_mensagens.append(f"❌ ERRO CRÍTICO: {exc}")

    # --- 5. Salvar o novo estado das variações que foram atualizadas com sucesso ---
    if variacoes_sincronizadas:
        conn = sqlite3.connect(DB_PATH, timeout=15)
        cursor = conn.cursor()
        cursor.executemany("INSERT OR REPLACE INTO estado_sincronizacao (variacao_id, quantidade_enviada, timestamp_sinc) VALUES (:variacao_id, :quantidade_enviada, :timestamp_sinc)", variacoes_sincronizadas)
        conn.commit()
        conn.close()
        print(f"[{datetime.now()}] {len(variacoes_sincronizadas)} estados de sincronização foram salvos.")

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    summary = f"Concluído em {duration}s! Atualizações: {sucessos} | Falhas: {falhas} | Ignorados: {ignorados}"
    log_mensagens.append(summary)
    print(f"[{datetime.now()}] {summary}")
    
    return {'summary': summary, 'log': log_mensagens, 'ids_orfaos': []}

# ... (Callbacks de display_bulk_update_result, activate_ad_callback, verificar_mapeamentos_orfaos, e excluir_orfaos permanecem os mesmos e devem ser mantidos no seu código) ...
# (Para brevidade, eles não serão repetidos aqui, mas estão no seu código final)

# ========================
# EXECUÇÃO
# ========================
if __name__ == '__main__':
    webbrowser.open("http://127.0.0.1:8050")  
    app.run(debug=True, use_reloader=False)