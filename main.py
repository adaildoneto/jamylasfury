from flask import Flask, render_template, request, jsonify
import pandas as pd
import folium
from folium.plugins import HeatMap
from opencage.geocoder import OpenCageGeocode
import json
import plotly.express as px
import plotly.io as pio
import base64
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from shapely.geometry import shape, Point

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'csv'}

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Substitua 'YOUR_API_KEY' pela sua chave de API do OpenCage
opencage_key = '9f7e9d514f624459bd58c8b4619fcb26'
geocoder = OpenCageGeocode(opencage_key)

CACHE_FILE = 'geocode_cache.json'


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)


localizacao_df = pd.read_csv('vote.csv', delimiter=';')


# Carregar os dados de votos do candidato (isso vem do seu arquivo de votos)
def merge_votos_localizacao(df_votos, localizacao_df):
    # Mergir os DataFrames baseando-se na zona e seção
    df_votos_com_bairro = pd.merge(df_votos,
                                   localizacao_df,
                                   how='left',
                                   left_on=['ZONA', 'SEÇÃO'],
                                   right_on=['ZONA', 'SEÇÃO'])

    # Verificar se o merge foi bem-sucedido
    if df_votos_com_bairro.empty:
        raise ValueError(
            "Erro ao combinar os dados de votos com os locais de votação.")

    return df_votos_com_bairro


# Função para criar o gráfico de bairros
def create_bairro_chart(df_votos_com_bairro):
    # Agrupar por bairro e somar os votos
    df_bairro = df_votos_com_bairro.groupby(
        'BAIRRO')['QT_VOTOS'].sum().reset_index()

    # Ordenar por votos (opcional)
    df_bairro = df_bairro.sort_values(by='QT_VOTOS', ascending=False)

    # Criar o gráfico de barras com Plotly
    fig = px.bar(df_bairro.head(10),
                 x='BAIRRO',
                 y='QT_VOTOS',
                 title='Top 10 Bairros com Mais Votos')
    bairro_chart_html = fig.to_html(full_html=False)

    return bairro_chart_html


# Função para verificar se o arquivo tem a extensão permitida
def allowed_file(filename):
    return '.' in filename and filename.rsplit(
        '.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def geocode_address(address, cache):
    if address in cache:
        logging.info(f"Cache hit for address: {address}")
        return cache[address]
    try:
        result = geocoder.geocode(address)
        if result and len(result):
            location = result[0]['geometry']
            cache[address] = (location['lat'], location['lng'])
            save_cache(cache)
            logging.info(
                f"Geocoded address: {address} -> {location['lat']}, {location['lng']}"
            )
            return location['lat'], location['lng']
    except Exception as e:
        logging.error(f"Geocoding error for address {address}: {e}")
    return None, None


def create_maps(df):
    logging.info("Starting to create the maps...")
    df_locals = df.groupby(['DS_LOCAL_VOTACAO_ENDERECO', 'NM_LOCAL_VOTACAO'
                            ])['QT_VOTOS'].sum().reset_index()

    mapa_pins = folium.Map(location=[-9.975377, -67.824897], zoom_start=7)
    mapa_heat = folium.Map(location=[-9.975377, -67.824897], zoom_start=7)
    mapa_both = folium.Map(location=[-9.975377, -67.824897], zoom_start=7)
    heat_data = []

    cache = load_cache()
    addresses = [(row['DS_LOCAL_VOTACAO_ENDERECO'] + ', Acre', row['QT_VOTOS'])
                 for index, row in df_locals.iterrows()]

    def geocode_wrapper(address):
        lat, lng = geocode_address(address[0], cache)
        return (lat, lng, address[1], address[0])

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_address = {
            executor.submit(geocode_wrapper, address): address
            for address in addresses
        }
        for future in as_completed(future_to_address):
            lat, lng, votos, full_address = future.result()
            if lat and lng:
                folium.Marker(location=[lat, lng],
                              popup=f"{full_address}: {votos} votos",
                              tooltip=full_address).add_to(mapa_pins)
                folium.Marker(location=[lat, lng],
                              popup=f"{full_address}: {votos} votos",
                              tooltip=full_address).add_to(mapa_both)
                heat_data.append([lat, lng, votos])
            logging.info(f"Processed address: {full_address}")

    HeatMap(heat_data, radius=15).add_to(mapa_heat)
    HeatMap(heat_data, radius=15).add_to(mapa_both)

    logging.info("Finished creating the maps.")
    return mapa_pins._repr_html_(), mapa_heat._repr_html_(
    ), mapa_both._repr_html_()


def create_bar_chart(df):
    logging.info("Creating bar chart...")
    df_municipios = df.groupby('NM_MUNICIPIO')['QT_VOTOS'].sum().reset_index()

    # Ordenar e selecionar os 22 municípios com mais votos
    df_municipios = df_municipios.sort_values(by='QT_VOTOS',
                                              ascending=False).head(22)

    fig = px.bar(df_municipios,
                 x='NM_MUNICIPIO',
                 y='QT_VOTOS',
                 labels={
                     'NM_MUNICIPIO': 'Município',
                     'QT_VOTOS': 'Número de Votos'
                 },
                 title='Número de Votos por Município (Top 22)')

    bar_chart_html = fig.to_html(full_html=False)

    logging.info("Finished creating bar chart.")
    return bar_chart_html


def create_pie_chart(df):
    logging.info("Creating pie chart...")
    df_municipios = df.groupby('NM_MUNICIPIO')['QT_VOTOS'].sum().reset_index()

    fig = px.pie(df_municipios,
                 names='NM_MUNICIPIO',
                 values='QT_VOTOS',
                 title='Proporção de Votos por Município')

    pie_chart_html = fig.to_html(full_html=False)

    logging.info("Finished creating pie chart.")
    return pie_chart_html


# Função para criar o gráfico de pizza por bairro
def create_bairro_pie_chart(df_votos_com_bairro):
    # Agrupar por bairro e somar os votos (reaproveitamos o agrupamento anterior)
    df_bairro = df_votos_com_bairro.groupby(
        'BAIRRO')['QT_VOTOS'].sum().reset_index()

    # Criar o gráfico de pizza com Plotly
    fig = px.pie(df_bairro,
                 values='QT_VOTOS',
                 names='BAIRRO',
                 title='Proporção de Votos por Bairro')
    bairro_pie_chart_html = fig.to_html(full_html=False)

    return bairro_pie_chart_html


def create_time_chart(df):
    logging.info("Creating time chart...")
    df.loc[:, 'HH_GERACAO'] = pd.to_datetime(df['HH_GERACAO'],
                                             format='%H:%M:%S').dt.hour
    df_time = df.groupby('HH_GERACAO')['QT_VOTOS'].sum().reset_index()

    fig = px.line(df_time,
                  x='HH_GERACAO',
                  y='QT_VOTOS',
                  labels={
                      'HH_GERACAO': 'Hora',
                      'QT_VOTOS': 'Número de Votos'
                  },
                  title='Número de Votos por Hora')

    time_chart_html = fig.to_html(full_html=False)

    logging.info("Finished creating time chart.")
    return time_chart_html


def geocode_dataframe(df, address_column):
    cache = load_cache()
    coords = []

    def geocode_address_cached(address):
        lat, lng = geocode_address(address, cache)
        return (lat, lng)

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_address = {
            executor.submit(geocode_address_cached, address): address
            for address in df[address_column]
        }
        for future in as_completed(future_to_address):
            lat, lng = future.result()
            coords.append((lat, lng))
    df['LAT'] = [coord[0] for coord in coords]
    df['LNG'] = [coord[1] for coord in coords]

    return df


def create_zone_chart(df):
    # Assumindo que a coluna 'NR_ZONA' contém os dados das zonas eleitorais
    df_zone = df.groupby('NR_ZONA')['QT_VOTOS'].sum().reset_index()

    # Criação do gráfico de zonas
    fig = px.bar(df_zone,
                 x='NR_ZONA',
                 y='QT_VOTOS',
                 title='Votos por Zona Eleitoral')
    zone_chart_html = fig.to_html(full_html=False)

    return zone_chart_html


def create_section_chart(df):
    # Assumindo que a coluna 'NR_SECAO' contém os dados das seções eleitorais
    df_section = df.groupby('NR_SECAO')['QT_VOTOS'].sum().reset_index()

    # Criação do gráfico de seções
    fig = px.bar(df_section,
                 x='NR_SECAO',
                 y='QT_VOTOS',
                 title='Votos por Seção Eleitoral')
    section_chart_html = fig.to_html(full_html=False)

    return section_chart_html


def create_bar_chart_comparison(df1, df2):
    logging.info("Creating comparison bar chart...")
    df1_municipios = df1.groupby(
        'NM_MUNICIPIO')['QT_VOTOS'].sum().reset_index()
    df2_municipios = df2.groupby(
        'NM_MUNICIPIO')['QT_VOTOS'].sum().reset_index()

    fig = px.bar(df1_municipios.merge(
        df2_municipios,
        on='NM_MUNICIPIO',
        suffixes=(f'_{df1["NM_VOTAVEL"].iloc[0]}',
                  f'_{df2["NM_VOTAVEL"].iloc[0]}')),
                 x='NM_MUNICIPIO',
                 y=[
                     f'QT_VOTOS_{df1["NM_VOTAVEL"].iloc[0]}',
                     f'QT_VOTOS_{df2["NM_VOTAVEL"].iloc[0]}'
                 ],
                 labels={
                     'value': 'Número de Votos',
                     'NM_MUNICIPIO': 'Município'
                 },
                 title='Comparação de Votos por Município')

    comparison_chart_html = fig.to_html(full_html=False)
    logging.info("Finished creating comparison bar chart.")
    return comparison_chart_html


def create_pie_chart_comparison(df1, df2):
    logging.info("Creating comparison pie chart...")
    df1_votes = df1['QT_VOTOS'].sum()
    df2_votes = df2['QT_VOTOS'].sum()
    fig = px.pie(names=[df1['NM_VOTAVEL'].iloc[0], df2['NM_VOTAVEL'].iloc[0]],
                 values=[df1_votes, df2_votes],
                 title='Comparação de Proporção de Votos')

    pie_comparison_html = fig.to_html(full_html=False)
    logging.info("Finished creating comparison pie chart.")
    return pie_comparison_html


def create_time_chart_comparison(df1, df2):
    logging.info("Creating comparison time chart...")
    df1.loc[:, 'HH_GERACAO'] = pd.to_datetime(df1['HH_GERACAO'],
                                              format='%H:%M:%S').dt.hour
    df2.loc[:, 'HH_GERACAO'] = pd.to_datetime(df2['HH_GERACAO'],
                                              format='%H:%M:%S').dt.hour
    df1_time = df1.groupby('HH_GERACAO')['QT_VOTOS'].sum().reset_index()
    df2_time = df2.groupby('HH_GERACAO')['QT_VOTOS'].sum().reset_index()

    fig = px.line(df1_time.merge(df2_time,
                                 on='HH_GERACAO',
                                 suffixes=(f'_{df1["NM_VOTAVEL"].iloc[0]}',
                                           f'_{df2["NM_VOTAVEL"].iloc[0]}')),
                  x='HH_GERACAO',
                  y=[
                      f'QT_VOTOS_{df1["NM_VOTAVEL"].iloc[0]}',
                      f'QT_VOTOS_{df2["NM_VOTAVEL"].iloc[0]}'
                  ],
                  labels={
                      'value': 'Número de Votos',
                      'HH_GERACAO': 'Hora'
                  },
                  title='Comparação de Votos por Hora')

    time_comparison_html = fig.to_html(full_html=False)
    logging.info("Finished creating comparison time chart.")
    return time_comparison_html


@app.route('/')
def index():
    files = [
        f for f in os.listdir(app.config['UPLOAD_FOLDER']) if allowed_file(f)
    ]
    return render_template('index.html', files=files)


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    if file and allowed_file(file.filename):
        filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filename)
        return redirect(url_for('show_data', filename=file.filename))
    return redirect(request.url)


@app.route('/data/<filename>')
def show_data(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    df = pd.read_csv(filepath, delimiter=';', encoding='latin1')
    cargos = df['DS_CARGO'].unique()
    candidatos_por_cargo = {
        cargo: df[df['DS_CARGO'] == cargo]['NM_VOTAVEL'].unique()
        for cargo in cargos
    }
    return render_template('data.html',
                           cargos=cargos,
                           candidatos_por_cargo=candidatos_por_cargo,
                           filename=filename)


@app.route('/map/<filename>/<candidate_name>')
def show_map(filename, candidate_name):
    logging.info(f"Generating maps for candidate: {candidate_name}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    df = pd.read_csv(filepath, delimiter=';', encoding='latin1')
    df_candidate = df[df['NM_VOTAVEL'] == candidate_name]

    # Criar gráficos
    map_pins_html, map_heat_html, map_both_html = create_maps(df_candidate)
    bar_chart_html = create_bar_chart(df_candidate)
    pie_chart_html = create_pie_chart(df_candidate)
    time_chart_html = create_time_chart(df_candidate)

    # Novo gráfico de bairro

    return render_template('map.html',
                           candidate_name=candidate_name,
                                                      total_votes=total_votes,
                           map_pins_html=map_pins_html,
                           map_heat_html=map_heat_html,
                           map_both_html=map_both_html,
                           bar_chart_html=bar_chart_html,
                           pie_chart_html=pie_chart_html,
                           time_chart_html=time_chart_html)


@app.route('/analyze_area', methods=['POST'])
def analyze_area():
    data = request.json
    polygon = shape(data['geometry'])
    filename = data['filename']

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    df = pd.read_csv(filepath, delimiter=';', encoding='latin1')

    # Geocode addresses and add LAT and LNG columns
    df = geocode_dataframe(df, 'DS_LOCAL_VOTACAO_ENDERECO')

    df['coords'] = df.apply(lambda row: Point(row['LAT'], row['LNG']), axis=1)
    df_in_area = df[df['coords'].apply(lambda point: polygon.contains(point))]

    top_candidates = df_in_area.groupby([
        'NM_VOTAVEL', 'DS_CARGO'
    ])['QT_VOTOS'].sum().reset_index().sort_values(by='QT_VOTOS',
                                                   ascending=False).head(10)

    result = top_candidates.to_dict(orient='records')

    return jsonify(result)


@app.route('/analyze_area_page')
def analyze_area_page():
    files = [
        f for f in os.listdir(app.config['UPLOAD_FOLDER']) if allowed_file(f)
    ]
    return render_template('analyze_area.html', files=files)


@app.route('/compare/<filename>/<candidate_1>/<candidate_2>')
def compare_candidates(filename, candidate_1, candidate_2):
    logging.info(f"Comparing candidates: {candidate_1} vs {candidate_2}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    df = pd.read_csv(filepath, delimiter=';', encoding='latin1')

    # Filtrar os dados para os dois candidatos
    df_candidate_1 = df[df['NM_VOTAVEL'] == candidate_1]
    df_candidate_2 = df[df['NM_VOTAVEL'] == candidate_2]

    # Total de votos
    total_votes_1 = df_candidate_1['QT_VOTOS'].sum()
    total_votes_2 = df_candidate_2['QT_VOTOS'].sum()

    # Criação de gráficos comparativos
    bar_chart_comparison_html = create_bar_chart_comparison(
        df_candidate_1, df_candidate_2)
    pie_chart_comparison_html = create_pie_chart_comparison(
        df_candidate_1, df_candidate_2)
    time_chart_comparison_html = create_time_chart_comparison(
        df_candidate_1, df_candidate_2)

    return render_template(
        'compare.html',
        candidate_1=candidate_1,
        total_votes_1=total_votes_1,
        candidate_2=candidate_2,
        total_votes_2=total_votes_2,
        bar_chart_comparison_html=bar_chart_comparison_html,
        pie_chart_comparison_html=pie_chart_comparison_html,
        time_chart_comparison_html=time_chart_comparison_html)


def create_zone_section_chart(df):
    logging.info("Creating zone and section bar chart...")

    # Agrupar por zona eleitoral
    df_zones = df.groupby('NR_ZONA')['QT_VOTOS'].sum().reset_index()

    fig_zone = px.bar(df_zones,
                      x='NR_ZONA',
                      y='QT_VOTOS',
                      labels={
                          'NR_ZONA': 'Zona Eleitoral',
                          'QT_VOTOS': 'Número de Votos'
                      },
                      title='Número de Votos por Zona Eleitoral')

    zone_chart_html = fig_zone.to_html(full_html=False)

    # Agrupar por seção
    df_sections = df.groupby('NR_SECAO')['QT_VOTOS'].sum().reset_index()

    fig_section = px.bar(df_sections,
                         x='NR_SECAO',
                         y='QT_VOTOS',
                         labels={
                             'NR_SECAO': 'Seção Eleitoral',
                             'QT_VOTOS': 'Número de Votos'
                         },
                         title='Número de Votos por Seção Eleitoral')

    section_chart_html = fig_section.to_html(full_html=False)

    logging.info("Finished creating zone and section bar charts.")
    return zone_chart_html, section_chart_html


if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)
