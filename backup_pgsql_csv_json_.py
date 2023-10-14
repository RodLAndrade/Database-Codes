from sqlalchemy import create_engine
import pandas as pd
import sys
import json

def show_tables():
    return """SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'public';
"""

def select_data(tabela):
    return f"""SELECT * FROM {tabela}"""

# VALIDANDO OS PARAMETROS INICIAIS

if len(sys.argv) < 2:
    print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
    sys.exit(1)
tipo_arquivo = sys.argv[1].upper().strip()
if tipo_arquivo not in ["C", "J"]:
    print("Formato inválido. Use 'C' para CSV ou 'J' para JSON.")
    sys.exit(1)

#   EXECUTAR O CÓDIGO

try:
    engine = create_engine("postgresql://postgres:1234@localhost:5432/db_teste")
    if engine:
        print("Connected!")
        print()
except:
    print("Cannot connect!")

    # PUXANDO O NOME DAS TABELAS NO BD

nomes_tabelas = pd.read_sql_query(show_tables(), engine) 
convertendo_tabelas =  nomes_tabelas.to_dict()

for k, v in convertendo_tabelas.items():
    dict_tabelas = v

tabelas_a_salvar = []

for k, v in dict_tabelas.items():
    tabelas_a_salvar.append(v)

    # SALVANDO OS DADOS EM ARQUIVOS

if tipo_arquivo == "C": #CSV
    print("Tipo de arquivo selecionado: CSV")
    for tabela in tabelas_a_salvar:
        df = pd.read_sql_query(select_data(tabela), engine)
        df.to_csv(f'dados_tabela_{tabela}.csv', index=False)

elif tipo_arquivo == "J": #JSON

    print("Tipo de arquivo selecionado: JSON")

    if len(sys.argv) < 3:
        print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
        sys.exit(1)
    opcao_json = sys.argv[2].upper().strip()

    if opcao_json not in ["S", "J"]:
        print("Opção inválida para opção quantos arquivos JSON. Use 'S' para arquivos separados ou 'J' para um único arquivo.")
        sys.exit(1)

    if opcao_json == "S": #JSON EM ARQUIVOS SEPARADOS

        print("Criando um arquivo por tabela...")

        for tabela in tabelas_a_salvar:
            df = pd.read_sql_query(select_data(tabela), engine)
            df.to_json(f'dados_tabela_{tabela}.json', orient="records", force_ascii=False)

    elif opcao_json == "J": #JSON EM ARQUIVOS JUNTOS

        print("Criando arquivo único...") 

        dicionarios = {}
        for tabela in tabelas_a_salvar:
            dicionarios[tabela] = json.loads(pd.read_sql_query(select_data(tabela), engine).to_json(orient="records", force_ascii=False))

        with open("dados.json", 'w', encoding='utf8') as json_file:
            json_file.write(json.dumps(dicionarios, ensure_ascii=False))


   





