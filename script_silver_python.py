import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, monotonically_increasing_id

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Tratamento de Tabelas Eleitorais") \
    .getOrCreate()

# Diretórios de entrada e saída
diretorio_tabelas = "C:/Users/asantalm/Documents/PROJETO/dataset"
diretorio_saida = "C:/Users/asantalm/Documents/PROJETO/bases_csv"

# Criar diretório de saída, se necessário
os.makedirs(diretorio_saida, exist_ok=True)

# Obter a lista de arquivos no diretório
arquivos = [os.path.join(diretorio_tabelas, arquivo) 
            for arquivo in os.listdir(diretorio_tabelas) 
            if arquivo.endswith('.csv')]

# Função para carregar os arquivos como DataFrames
def carregar_tabela(arquivo):
    try:
        df = spark.read.format("csv") \
            .option("header", True) \
            .option("delimiter", ";") \
            .option("inferSchema", True) \
            .option("encoding", "ISO-8859-1") \
            .load(arquivo)
        return df
    except Exception as e:
        print(f"Erro ao carregar o arquivo {arquivo}: {e}")
        return None

# Função para aplicar os tratamentos no DataFrame
def tratar_tabela(df, colunas_excluir, adicionar_id=False):
    # Ajustar os nomes das colunas
    colunas_ajustadas = [
        c.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("ã", "a")
        .replace("â", "a")
        .replace("á", "a")
        .replace("ç", "c")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ô", "o")
        for c in df.columns
    ]
    df = df.toDF(*colunas_ajustadas)

    # Normalizar também as colunas a serem excluídas
    colunas_excluir_ajustadas = [
        c.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("ã", "a")
        .replace("â", "a")
        .replace("á", "a")
        .replace("ç", "c")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ô", "o")
        for c in colunas_excluir
    ]

    # Verificar as colunas ajustadas
    print(f"Colunas ajustadas no DataFrame: {df.columns}")
    print(f"Colunas ajustadas para exclusão: {colunas_excluir_ajustadas}")

    # Excluir colunas especificadas
    colunas_existentes = df.columns
    colunas_para_excluir = [col for col in colunas_excluir_ajustadas if col in colunas_existentes]
    
    if colunas_para_excluir:
        df = df.drop(*colunas_para_excluir)
    else:
        print(f"Nenhuma coluna a ser excluída encontrada para: {colunas_excluir_ajustadas}")
    
    # Adicionar ID incremental apenas para a tabela 'candidatos'
    if adicionar_id:
        df = df.withColumn("id", monotonically_increasing_id() + 1)
        colunas = ["id"] + [col for col in df.columns if col != "id"]
        df = df.select(*colunas)

    # Tratar o conteúdo das colunas
    for c in df.columns:
        df = df.withColumn(c, lower(col(c)))
        df = df.withColumn(c, regexp_replace(col(c), "ã", "a"))
        df = df.withColumn(c, regexp_replace(col(c), "â", "a"))
        df = df.withColumn(c, regexp_replace(col(c), "á", "a"))
        df = df.withColumn(c, regexp_replace(col(c), "ç", "c"))
        df = df.withColumn(c, regexp_replace(col(c), "é", "e"))
        df = df.withColumn(c, regexp_replace(col(c), "ê", "e"))
        df = df.withColumn(c, regexp_replace(col(c), "í", "i"))
        df = df.withColumn(c, regexp_replace(col(c), "ô", "o"))
        df = df.withColumn(c, regexp_replace(col(c), r"(?i)#ne|#ne#|#nulo|nulo#|não divulgável", "nao informado"))

    return df

# Dicionário de colunas a excluir por tabela
colunas_excluir_por_tabela = {
    "candidatos": [
        "etnia_indigena", "identidade_genero", "orientacao_sexual", "quilombola",
        "situacao_de_cassacao", "situacao_desconssituacao_de_desconstituicaotituicao", "situacao_de_julgamento", "data_de_carga",
        "quantidade_de_candidatos_nao_informados"
    ],
    "comparecimento_abstencao": [
        "cor_raca", "identidade_genero", "interprete_libras", "quilombola", "data_de_carga"
    ],
    "deficientes": [
        "detalhe_tipo_deficiencia", "situacao_totalizacao", "data_de_carga"
    ],
    "detalhe_votacao": [
        "quantidade_votos_anulados_separado", "quantidade_votos_nominais_anulados",
        "quantidade_votos_legenda_anulados", "quantidade_votos_nominais_anulados_subjudice",
        "quantidade_votos_legenda_anulados_subjudice", "data_de_carga"
    ],
    "eleitorado_eleicao_capilaridade": [
        "data_de_carga"
    ],
    "eleitores_faltosos": [
        "estado_civil", "data_de_carga"
    ],
    "filiado_eleicao": [
        "raca_cor", "interprete_libras", "quilombola", "data_de_carga"
    ],
    "pesquisas_eleitorais": [
        "codigo_do_registro_no_conre", "dados_municipios", "estatistico_responsavel",
        "metodologia_da_pesquisa", "nome_fantasia_da_empresa", "plano_amostral",
        "sistema_de_controle", "quantidade_de_pesquisa", "valor_da_pesquisa", "data_de_carga"
    ],
    "quocientes_eleitoral": [
        "quantidade_vagas_preenchidas_qp", "quantidade_vagas_obtidas_qp",
        "quantidade_votos_coligacao_qp", "data_de_carga"
    ],
    "vagas": [
        "municipio", "uf", "data_de_eleicao", "data_de_posse", "eleicao", "data_de_carga"
    ],
    "votacao_candidatos": [
        "data_de_carga"
    ],
    "votacao_partida": [
        "data_de_carga"
    ]
}

# Criar dicionário de tabelas
tabelas = {}
for arquivo in arquivos:
    nome_tabela = os.path.splitext(os.path.basename(arquivo))[0]
    if nome_tabela not in colunas_excluir_por_tabela.keys():
        print(f"Tabela '{nome_tabela}' ignorada.")
        continue
    
    tabela = carregar_tabela(arquivo)
    if tabela is not None:
        tabelas[nome_tabela] = tabela

# Processar cada tabela no dicionário
for nome, tabela in tabelas.items():
    caminho_temp = None
    try:
        colunas_excluir = colunas_excluir_por_tabela.get(nome, [])
        adicionar_id = (nome == "candidatos")
        tabela_tratada = tratar_tabela(tabela, colunas_excluir, adicionar_id)

        tabela_unica = tabela_tratada.coalesce(1)
        caminho_temp = os.path.join(diretorio_saida, f"{nome}_temp")
        tabela_unica.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ";") \
            .mode("overwrite") \
            .save(caminho_temp)

        arquivos_csv = [arq for arq in os.listdir(caminho_temp) if arq.startswith("part-") and arq.endswith(".csv")]
        if arquivos_csv:
            caminho_origem = os.path.join(caminho_temp, arquivos_csv[0])
            caminho_destino = os.path.join(diretorio_saida, f"{nome}.csv")
            shutil.move(caminho_origem, caminho_destino)
            print(f"Tabela '{nome}' salva como '{caminho_destino}'")
        else:
            print(f"Nenhum arquivo part-* encontrado em '{caminho_temp}'")
        shutil.rmtree(caminho_temp, ignore_errors=True)

    except Exception as e:
        print(f"Erro ao salvar a tabela '{nome}': {e}")
        if caminho_temp and os.path.exists(caminho_temp):
            shutil.rmtree(caminho_temp, ignore_errors=True)
